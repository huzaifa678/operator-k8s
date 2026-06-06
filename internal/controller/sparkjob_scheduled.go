/*
Copyright 2026 huzaifa678.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	computev1alpha1 "github.com/huzaifa678/compute-operator/api/v1alpha1"
)

// reconcileScheduled handles SparkJobs that act as cron templates: it never
// creates a driver pod for the template itself, only spawns child SparkJob
// runs at each fire time.
func (r *SparkJobReconciler) reconcileScheduled(ctx context.Context, tmpl *computev1alpha1.SparkJob) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("schedule", *tmpl.Spec.Schedule)

	sched, loc, err := parseSchedule(tmpl)
	if err != nil {
		// Validation should normally reject this, but we may still see it
		// before the webhook fires (e.g. CR created via raw client).
		log.Error(err, "invalid schedule; will not requeue until updated")
		return ctrl.Result{}, nil
	}

	// Suspend short-circuits everything except status refresh.
	if tmpl.Spec.Suspend != nil && *tmpl.Spec.Suspend {
		log.V(1).Info("template suspended; not scheduling")
		if err := r.refreshScheduledStatus(ctx, tmpl, nil, time.Time{}, safeNext(sched, time.Now().In(loc))); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeueNormal}, nil
	}

	now := time.Now().In(loc)

	children, err := r.listChildren(ctx, tmpl)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("list children: %w", err)
	}

	// Decide the fire time we should be acting on, accounting for misses.
	last := lastScheduleTime(tmpl, sched)
	missed := nextRunToActOn(last, now, sched, tmpl.Spec.StartingDeadlineSeconds)

	if !missed.IsZero() {
		active := activeChildren(children)
		if shouldRun, replaced, err := r.applyConcurrencyPolicy(ctx, tmpl, active); err != nil {
			return ctrl.Result{}, err
		} else if shouldRun {
			child, err := r.spawnChild(ctx, tmpl, missed)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("spawn child: %w", err)
			}
			children = append(children, *child)
			tmpl.Status.LastScheduleTime = &metav1.Time{Time: missed}
			log.Info("scheduled child run", "name", child.Name, "fireTime", missed, "replaced", replaced)
		} else {
			log.V(1).Info("skipping fire — concurrency policy forbids", "policy", tmpl.Spec.ConcurrencyPolicy)
		}
	}

	if err := r.pruneHistory(ctx, tmpl, children); err != nil {
		return ctrl.Result{}, fmt.Errorf("prune: %w", err)
	}

	// Reload children after potential prune to keep status accurate.
	children, err = r.listChildren(ctx, tmpl)
	if err != nil {
		return ctrl.Result{}, err
	}

	next := safeNext(sched, time.Now().In(loc))
	if err := r.refreshScheduledStatus(ctx, tmpl, children, derefTime(tmpl.Status.LastScheduleTime), next); err != nil {
		return ctrl.Result{}, err
	}

	// Wake up at the next fire time (clamped so we still tick periodically
	// for child-status changes that may not flow through Owns). Impossible
	// schedules (next == zero) get the steady-tick fallback only.
	requeue := requeueNormal
	if !next.IsZero() {
		requeue = time.Until(next)
		if requeue < time.Second {
			requeue = time.Second
		}
		if requeue > 5*time.Minute {
			requeue = 5 * time.Minute
		}
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

func parseSchedule(tmpl *computev1alpha1.SparkJob) (cron.Schedule, *time.Location, error) {
	loc := time.UTC
	if tz := tmpl.Spec.TimeZone; tz != nil && *tz != "" {
		l, err := time.LoadLocation(*tz)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid timeZone %q: %w", *tz, err)
		}
		loc = l
	}
	sched, err := cronParser.Parse(*tmpl.Spec.Schedule)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid schedule %q: %w", *tmpl.Spec.Schedule, err)
	}
	return sched, loc, nil
}

// lastScheduleTime picks where the catch-up walk starts: either the recorded
// last run, or one cron tick before now if this template has never fired.
func lastScheduleTime(tmpl *computev1alpha1.SparkJob, sched cron.Schedule) time.Time {
	if tmpl.Status.LastScheduleTime != nil {
		return tmpl.Status.LastScheduleTime.Time
	}
	// First reconcile: pretend the previous tick was tmpl.CreationTimestamp so
	// we don't back-fill history we never had.
	return tmpl.CreationTimestamp.Time
}

// nextRunToActOn returns the most recent fire time strictly between `last`
// (exclusive) and `now` (inclusive) that hasn't been skipped by the deadline.
// Mirrors k8s/k8s pkg/controller/cronjob/cronjob_controllerv2.go logic.
//
// Defensive against impossible schedules (e.g. "0 0 31 2 *"): robfig/cron
// returns time.Time{} after a 5-year scan, then Next(zero) keeps spinning
// from year 0001. We treat zero or non-advancing returns as "no next fire"
// and bail.
func nextRunToActOn(last, now time.Time, sched cron.Schedule, deadline *int64) time.Time {
	var latest time.Time
	t := sched.Next(last)
	for !t.IsZero() && !t.After(now) {
		latest = t
		if deadline != nil && now.Sub(t) > time.Duration(*deadline)*time.Second {
			latest = time.Time{} // missed past the deadline; skip
		}
		nextT := sched.Next(t)
		if nextT.IsZero() || !nextT.After(t) {
			break // impossible schedule or cron implementation regressed
		}
		t = nextT
	}
	return latest
}

// safeNext wraps sched.Next so callers don't have to worry about zero-time
// returns sending us into the 5-year-from-year-0001 path on the next call.
// Returns the zero time for impossible schedules — the caller decides what
// to do (typically: don't requeue on a schedule basis).
func safeNext(sched cron.Schedule, from time.Time) time.Time {
	t := sched.Next(from)
	if t.IsZero() || !t.After(from) {
		return time.Time{}
	}
	return t
}

func activeChildren(children []computev1alpha1.SparkJob) []computev1alpha1.SparkJob {
	out := children[:0:0]
	for _, c := range children {
		switch c.Status.Phase {
		case computev1alpha1.PhaseSucceeded, computev1alpha1.PhaseFailed:
			continue
		default:
			out = append(out, c)
		}
	}
	return out
}

// applyConcurrencyPolicy returns (shouldRun, replacedCount, error).
func (r *SparkJobReconciler) applyConcurrencyPolicy(
	ctx context.Context, tmpl *computev1alpha1.SparkJob, active []computev1alpha1.SparkJob,
) (bool, int, error) {
	policy := tmpl.Spec.ConcurrencyPolicy
	if policy == "" {
		policy = concurrencyForbid
	}
	if len(active) == 0 || policy == concurrencyAllow {
		return true, 0, nil
	}
	if policy == concurrencyForbid {
		return false, 0, nil
	}
	// Replace: delete all active children, then run.
	for i := range active {
		if err := r.Delete(ctx, &active[i], client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil &&
			!apierrors.IsNotFound(err) {
			return false, 0, fmt.Errorf("replace: delete %s: %w", active[i].Name, err)
		}
	}
	return true, len(active), nil
}

func (r *SparkJobReconciler) spawnChild(ctx context.Context, tmpl *computev1alpha1.SparkJob, fireTime time.Time) (*computev1alpha1.SparkJob, error) {
	childName := fmt.Sprintf("%s-%d", tmpl.Name, fireTime.Unix())
	spec := tmpl.Spec.DeepCopy()
	// Children are concrete runs; strip the scheduling fields so they execute.
	spec.Schedule = nil
	spec.Suspend = nil
	spec.TimeZone = nil
	spec.ConcurrencyPolicy = ""
	spec.StartingDeadlineSeconds = nil
	spec.SuccessfulJobsHistoryLimit = nil
	spec.FailedJobsHistoryLimit = nil

	labels := map[string]string{}
	for k, v := range tmpl.Labels {
		labels[k] = v
	}
	labels[parentLabel] = tmpl.Name
	labels[scheduledLabel] = "true"
	labels[runAtLabel] = fmt.Sprintf("%d", fireTime.Unix())

	child := &computev1alpha1.SparkJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:        childName,
			Namespace:   tmpl.Namespace,
			Labels:      labels,
			Annotations: map[string]string{"compute.compute.example.com/scheduled-for": fireTime.UTC().Format(time.RFC3339)},
		},
		Spec: *spec,
	}
	if err := controllerutil.SetControllerReference(tmpl, child, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, child); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Same fire time already created — fine, treat as success.
			return child, nil
		}
		return nil, err
	}
	return child, nil
}

func (r *SparkJobReconciler) listChildren(ctx context.Context, tmpl *computev1alpha1.SparkJob) ([]computev1alpha1.SparkJob, error) {
	var list computev1alpha1.SparkJobList
	if err := r.List(ctx, &list,
		client.InNamespace(tmpl.Namespace),
		client.MatchingLabels{parentLabel: tmpl.Name},
	); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// pruneHistory keeps the newest N Succeeded / Failed children, deleting older.
func (r *SparkJobReconciler) pruneHistory(ctx context.Context, tmpl *computev1alpha1.SparkJob, children []computev1alpha1.SparkJob) error {
	keepSucceeded := int32(3)
	if tmpl.Spec.SuccessfulJobsHistoryLimit != nil {
		keepSucceeded = *tmpl.Spec.SuccessfulJobsHistoryLimit
	}
	keepFailed := int32(3)
	if tmpl.Spec.FailedJobsHistoryLimit != nil {
		keepFailed = *tmpl.Spec.FailedJobsHistoryLimit
	}

	var succ, fail []computev1alpha1.SparkJob
	for _, c := range children {
		switch c.Status.Phase {
		case computev1alpha1.PhaseSucceeded:
			succ = append(succ, c)
		case computev1alpha1.PhaseFailed:
			fail = append(fail, c)
		}
	}
	sortNewestFirst := func(s []computev1alpha1.SparkJob) {
		sort.Slice(s, func(i, j int) bool {
			return s[i].CreationTimestamp.After(s[j].CreationTimestamp.Time)
		})
	}
	sortNewestFirst(succ)
	sortNewestFirst(fail)

	prune := func(s []computev1alpha1.SparkJob, keep int32) error {
		if int32(len(s)) <= keep {
			return nil
		}
		for _, victim := range s[keep:] {
			if err := r.Delete(ctx, &victim, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil &&
				!apierrors.IsNotFound(err) {
				return err
			}
		}
		return nil
	}
	if err := prune(succ, keepSucceeded); err != nil {
		return err
	}
	return prune(fail, keepFailed)
}

func (r *SparkJobReconciler) refreshScheduledStatus(
	ctx context.Context,
	tmpl *computev1alpha1.SparkJob,
	children []computev1alpha1.SparkJob,
	lastFire, nextFire time.Time,
) error {
	prev := tmpl.Status.DeepCopy()

	active := []corev1.ObjectReference{}
	for _, c := range activeChildren(children) {
		active = append(active, corev1.ObjectReference{
			APIVersion: computev1alpha1.GroupVersion.String(),
			Kind:       "SparkJob",
			Namespace:  c.Namespace,
			Name:       c.Name,
			UID:        c.UID,
		})
	}
	sort.Slice(active, func(i, j int) bool { return active[i].Name < active[j].Name })
	tmpl.Status.Active = active

	if !lastFire.IsZero() {
		tmpl.Status.LastScheduleTime = &metav1.Time{Time: lastFire}
	}
	tmpl.Status.NextScheduleTime = &metav1.Time{Time: nextFire}

	// Scheduled templates never reach a terminal Phase; they keep ticking.
	// Surface "Running" if any child is active, else "Pending".
	if len(active) > 0 {
		tmpl.Status.Phase = computev1alpha1.PhaseRunning
	} else {
		tmpl.Status.Phase = computev1alpha1.PhasePending
	}

	if scheduledStatusEqual(prev, &tmpl.Status) {
		return nil
	}
	if err := r.Status().Update(ctx, tmpl); err != nil && !apierrors.IsConflict(err) {
		return err
	}
	return nil
}

func scheduledStatusEqual(a, b *computev1alpha1.SparkJobStatus) bool {
	if a.Phase != b.Phase || len(a.Active) != len(b.Active) {
		return false
	}
	if !timesEqual(a.LastScheduleTime, b.LastScheduleTime) ||
		!timesEqual(a.NextScheduleTime, b.NextScheduleTime) {
		return false
	}
	for i := range a.Active {
		if a.Active[i].UID != b.Active[i].UID {
			return false
		}
	}
	return true
}

func timesEqual(a, b *metav1.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Time.Equal(b.Time)
}

func derefTime(t *metav1.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.Time
}
