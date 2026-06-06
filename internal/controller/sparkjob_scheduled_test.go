/*
Copyright 2026 huzaifa678.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package controller

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	computev1alpha1 "github.com/huzaifa678/compute-operator/api/v1alpha1"
)

func ptr[T any](v T) *T { return &v }

var _ = Describe("Scheduled SparkJob template", func() {
	const tmplName = "scheduled-tmpl"
	ctx := context.Background()
	nsn := types.NamespacedName{Name: tmplName, Namespace: "default"}

	AfterEach(func() {
		tmpl := &computev1alpha1.SparkJob{}
		if err := k8sClient.Get(ctx, nsn, tmpl); err == nil {
			Expect(k8sClient.Delete(ctx, tmpl)).To(Succeed())
		}
		var kids computev1alpha1.SparkJobList
		_ = k8sClient.List(ctx, &kids)
		for i := range kids.Items {
			if kids.Items[i].Name != tmplName {
				_ = k8sClient.Delete(ctx, &kids.Items[i])
			}
		}
	})

	createTemplate := func(schedule string) {
		Expect(k8sClient.Create(ctx, &computev1alpha1.SparkJob{
			ObjectMeta: metav1.ObjectMeta{Name: tmplName, Namespace: "default"},
			Spec: computev1alpha1.SparkJobSpec{
				Image:               "apache/spark:3.5.3",
				Type:                "Python",
				MainApplicationFile: "s3a://bucket/jobs/scrape.py",
				Schedule:            ptr(schedule),
				ConcurrencyPolicy:   "Forbid",
				MinExecutors:        1,
				MaxExecutors:        2,
			},
		})).To(Succeed())
	}

	It("handles impossible cron expressions without hanging", func() {
		createTemplate("0 0 31 2 *") // Feb 31 — never fires
		r := &SparkJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

		done := make(chan error, 1)
		go func() {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nsn})
			done <- err
		}()
		Eventually(done, "10s").Should(Receive(BeNil()),
			"reconcile must return quickly even for impossible schedules")
	})

	It("spawns a child SparkJob after a fire time has passed", func() {
		// "* * * * *" fires every minute — guaranteed in the past after creation.
		createTemplate("* * * * *")

		// Back-date CreationTimestamp so nextRunToActOn sees an elapsed tick.
		tmpl := &computev1alpha1.SparkJob{}
		Expect(k8sClient.Get(ctx, nsn, tmpl)).To(Succeed())
		tmpl.CreationTimestamp = metav1.NewTime(time.Now().Add(-5 * time.Minute))
		// CreationTimestamp is immutable via API, but the in-memory object passed
		// to the reconciler isn't — emulate by stuffing LastScheduleTime instead.
		tmpl.Status.LastScheduleTime = &metav1.Time{Time: time.Now().Add(-5 * time.Minute)}
		Expect(k8sClient.Status().Update(ctx, tmpl)).To(Succeed())

		r := &SparkJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nsn})
		Expect(err).NotTo(HaveOccurred())

		var kids computev1alpha1.SparkJobList
		Expect(k8sClient.List(ctx, &kids)).To(Succeed())
		var childCount int
		for _, c := range kids.Items {
			if c.Labels[parentLabel] == tmplName {
				childCount++
				Expect(c.Spec.Schedule).To(BeNil(), "child must not inherit Schedule")
				Expect(c.Spec.MainApplicationFile).To(Equal("s3a://bucket/jobs/scrape.py"))
			}
		}
		Expect(childCount).To(BeNumerically(">=", 1))
	})

	It("Suspend=true prevents new children", func() {
		createTemplate("* * * * *")
		tmpl := &computev1alpha1.SparkJob{}
		Expect(k8sClient.Get(ctx, nsn, tmpl)).To(Succeed())
		tmpl.Spec.Suspend = ptr(true)
		Expect(k8sClient.Update(ctx, tmpl)).To(Succeed())

		r := &SparkJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nsn})
		Expect(err).NotTo(HaveOccurred())

		var kids computev1alpha1.SparkJobList
		Expect(k8sClient.List(ctx, &kids)).To(Succeed())
		for _, c := range kids.Items {
			Expect(c.Labels[parentLabel]).NotTo(Equal(tmplName))
		}
	})
})

var _ = Describe("scheduled-mode unit logic", func() {
	It("parseSchedule rejects garbage cron", func() {
		_, _, err := parseSchedule(&computev1alpha1.SparkJob{
			Spec: computev1alpha1.SparkJobSpec{Schedule: ptr("not a cron")},
		})
		Expect(err).To(HaveOccurred())
	})
	It("parseSchedule defaults to UTC when no TZ", func() {
		_, loc, err := parseSchedule(&computev1alpha1.SparkJob{
			Spec: computev1alpha1.SparkJobSpec{Schedule: ptr("0 0 * * *")},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(loc.String()).To(Equal("UTC"))
	})
	It("nextRunToActOn returns zero when no fire elapsed", func() {
		sched, _, _ := parseSchedule(&computev1alpha1.SparkJob{
			Spec: computev1alpha1.SparkJobSpec{Schedule: ptr("0 0 1 1 *")}, // Jan 1
		})
		// last == now → no tick between them
		got := nextRunToActOn(time.Now(), time.Now(), sched, nil)
		Expect(got.IsZero()).To(BeTrue())
	})
	It("nextRunToActOn returns the most recent elapsed fire", func() {
		sched, _, _ := parseSchedule(&computev1alpha1.SparkJob{
			Spec: computev1alpha1.SparkJobSpec{Schedule: ptr("* * * * *")},
		})
		last := time.Now().Add(-10 * time.Minute).Truncate(time.Minute)
		now := time.Now()
		got := nextRunToActOn(last, now, sched, nil)
		Expect(got.IsZero()).To(BeFalse())
		Expect(got.After(last)).To(BeTrue())
		Expect(got.After(now)).To(BeFalse())
	})

	// Silence the unused-import vet check when only some tests reference errors.
	_ = errors.IsNotFound
})
