/*
Copyright 2026 huzaifa678.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	computev1alpha1 "github.com/huzaifa678/compute-operator/api/v1alpha1"
)

const (
	ownerLabel    = "compute.compute.example.com/sparkjob"
	roleLabel     = "compute.compute.example.com/role"
	roleDriver    = "driver"
	roleExecutor  = "executor"
	requeueNormal = 15 * time.Second
)

type SparkJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=compute.compute.example.com,resources=sparkjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=compute.compute.example.com,resources=sparkjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=compute.compute.example.com,resources=sparkjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *SparkJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var job computev1alpha1.SparkJob
	if err := r.Get(ctx, req.NamespacedName, &job); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if job.Status.Phase == computev1alpha1.PhaseSucceeded ||
		job.Status.Phase == computev1alpha1.PhaseFailed {
		return ctrl.Result{}, nil
	}

	desired := desiredExecutorCount(&job)

	driverName := job.Name + "-driver"
	driver := &corev1.Pod{}
	err := r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: driverName}, driver)
	switch {
	case apierrors.IsNotFound(err):
		driver = buildDriverPod(&job, driverName)
		if err := controllerutil.SetControllerReference(&job, driver, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, driver); err != nil {
			return ctrl.Result{}, fmt.Errorf("create driver: %w", err)
		}
		log.Info("created driver pod", "pod", driverName)
		return ctrl.Result{RequeueAfter: requeueNormal}, nil
	case err != nil:
		return ctrl.Result{}, err
	}

	executors, err := r.listExecutors(ctx, &job)
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.scaleExecutors(ctx, &job, executors, desired); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.updateStatus(ctx, &job, driver, desired); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueNormal}, nil
}

// desiredExecutorCount picks an executor count from either the explicit field
// or the auto-tuner. Heuristic: ~1 executor per 256MB of input, clamped to
// [MinExecutors, MaxExecutors].
func desiredExecutorCount(job *computev1alpha1.SparkJob) int32 {
	if job.Spec.Executors > 0 {
		return job.Spec.Executors
	}
	if job.Spec.ResourceHint.InputSizeMB > 0 {
		n := int32(job.Spec.ResourceHint.InputSizeMB / 256)
		if n < job.Spec.MinExecutors {
			n = job.Spec.MinExecutors
		}
		if n > job.Spec.MaxExecutors {
			n = job.Spec.MaxExecutors
		}
		return n
	}
	if job.Spec.MinExecutors > 0 {
		return job.Spec.MinExecutors
	}
	return 1
}

func (r *SparkJobReconciler) listExecutors(ctx context.Context, job *computev1alpha1.SparkJob) ([]corev1.Pod, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{ownerLabel: job.Name, roleLabel: roleExecutor},
	); err != nil {
		return nil, err
	}
	return pods.Items, nil
}

func (r *SparkJobReconciler) scaleExecutors(ctx context.Context, job *computev1alpha1.SparkJob, existing []corev1.Pod, desired int32) error {
	have := int32(len(existing))
	if have < desired {
		for i := have; i < desired; i++ {
			pod := buildExecutorPod(job, i)
			if err := controllerutil.SetControllerReference(job, pod, r.Scheme); err != nil {
				return err
			}
			if err := r.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("create executor %d: %w", i, err)
			}
		}
	} else if have > desired {
		for i := desired; i < have; i++ {
			if err := r.Delete(ctx, &existing[i]); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}

func (r *SparkJobReconciler) updateStatus(ctx context.Context, job *computev1alpha1.SparkJob, driver *corev1.Pod, desired int32) error {
	executors, err := r.listExecutors(ctx, job)
	if err != nil {
		return err
	}

	running := int32(0)
	for _, p := range executors {
		if p.Status.Phase == corev1.PodRunning {
			running++
		}
	}

	prev := job.Status.DeepCopy()
	job.Status.DesiredExecutors = desired
	job.Status.RunningExecutors = running
	job.Status.DriverPod = driver.Name

	switch driver.Status.Phase {
	case corev1.PodPending:
		job.Status.Phase = computev1alpha1.PhasePending
	case corev1.PodRunning:
		job.Status.Phase = computev1alpha1.PhaseRunning
		if job.Status.StartTime == nil {
			now := metav1.Now()
			job.Status.StartTime = &now
		}
	case corev1.PodSucceeded:
		job.Status.Phase = computev1alpha1.PhaseSucceeded
		if job.Status.CompletionTime == nil {
			now := metav1.Now()
			job.Status.CompletionTime = &now
		}
	case corev1.PodFailed:
		if job.Status.Retries < job.Spec.Spot.MaxRetries {
			job.Status.Retries++
			_ = r.Delete(ctx, driver)
			job.Status.Phase = computev1alpha1.PhaseResuming
		} else {
			job.Status.Phase = computev1alpha1.PhaseFailed
			if job.Status.CompletionTime == nil {
				now := metav1.Now()
				job.Status.CompletionTime = &now
			}
		}
	}

	job.Status.EstimatedCostUSD = estimateCost(job.Status.StartTime, job.Status.CompletionTime,
		1+running, job.Spec.ResourceHint.CostPerHourUSD)

	if equalStatus(prev, &job.Status) {
		return nil
	}
	return r.Status().Update(ctx, job)
}

func equalStatus(a, b *computev1alpha1.SparkJobStatus) bool {
	return a.Phase == b.Phase &&
		a.RunningExecutors == b.RunningExecutors &&
		a.DesiredExecutors == b.DesiredExecutors &&
		a.DriverPod == b.DriverPod &&
		a.Retries == b.Retries &&
		a.EstimatedCostUSD == b.EstimatedCostUSD
}

func estimateCost(start, end *metav1.Time, podCount int32, costPerHour string) string {
	if start == nil {
		return "0.00"
	}
	rate, err := strconv.ParseFloat(costPerHour, 64)
	if err != nil || rate <= 0 {
		rate = 0.10
	}
	finish := time.Now()
	if end != nil {
		finish = end.Time
	}
	hours := finish.Sub(start.Time).Hours()
	if hours < 0 {
		hours = 0
	}
	return fmt.Sprintf("%.4f", hours*rate*float64(podCount))
}

func buildDriverPod(job *computev1alpha1.SparkJob, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: job.Namespace,
			Labels: map[string]string{
				ownerLabel: job.Name,
				roleLabel:  roleDriver,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Affinity:      spotAffinity(job.Spec.Spot),
			Containers: []corev1.Container{{
				Name:      "driver",
				Image:     job.Spec.Image,
				Command:   driverCommand(job),
				Resources: job.Spec.DriverResources,
				Env: []corev1.EnvVar{
					{Name: "SPARK_ROLE", Value: "driver"},
					{Name: "SPARK_APPLICATION_FILE", Value: job.Spec.MainApplicationFile},
				},
			}},
		},
	}
}

func buildExecutorPod(job *computev1alpha1.SparkJob, idx int32) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-exec-%d", job.Name, idx),
			Namespace: job.Namespace,
			Labels: map[string]string{
				ownerLabel: job.Name,
				roleLabel:  roleExecutor,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Affinity:      spotAffinity(job.Spec.Spot),
			Containers: []corev1.Container{{
				Name:      "executor",
				Image:     job.Spec.Image,
				Command:   []string{"/bin/sh", "-c", "echo executor " + strconv.Itoa(int(idx)) + " ready && sleep 3600"},
				Resources: executorResources(job),
				Env: []corev1.EnvVar{
					{Name: "SPARK_ROLE", Value: "executor"},
					{Name: "SPARK_EXECUTOR_ID", Value: strconv.Itoa(int(idx))},
					{Name: "SPARK_DRIVER_HOST", Value: job.Name + "-driver"},
				},
			}},
		},
	}
}

func executorResources(job *computev1alpha1.SparkJob) corev1.ResourceRequirements {
	if len(job.Spec.ExecutorResources.Requests) > 0 || len(job.Spec.ExecutorResources.Limits) > 0 {
		return job.Spec.ExecutorResources
	}
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
	}
}

func driverCommand(job *computev1alpha1.SparkJob) []string {
	// Placeholder: real impl would build spark-submit invocation.
	// Sleep keeps the driver alive long enough for executors to register.
	args := "echo driver starting " + job.Spec.MainApplicationFile + " && sleep 3600"
	return []string{"/bin/sh", "-c", args}
}

func spotAffinity(p computev1alpha1.SpotPolicy) *corev1.Affinity {
	if !p.Enabled {
		return nil
	}
	term := corev1.NodeSelectorTerm{
		MatchExpressions: []corev1.NodeSelectorRequirement{{
			Key:      "node.kubernetes.io/lifecycle",
			Operator: corev1.NodeSelectorOpIn,
			Values:   []string{"spot"},
		}},
	}
	if p.Mode == "Required" {
		return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{term},
			},
		}}
	}
	return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{
			Weight: 100, Preference: term,
		}},
	}}
}

func (r *SparkJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&computev1alpha1.SparkJob{}).
		Owns(&corev1.Pod{}, builder.MatchEveryOwner).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(mapPodToOwner)).
		Named("sparkjob").
		Complete(r)
}

func mapPodToOwner(_ context.Context, obj client.Object) []ctrl.Request {
	labels := obj.GetLabels()
	if name, ok := labels[ownerLabel]; ok {
		return []ctrl.Request{{NamespacedName: types.NamespacedName{
			Namespace: obj.GetNamespace(), Name: name,
		}}}
	}
	return nil
}
