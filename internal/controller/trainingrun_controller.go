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
	trainingOwnerLabel = "compute.compute.example.com/trainingrun"
	rankLabel          = "compute.compute.example.com/rank"
)

type TrainingRunReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=compute.compute.example.com,resources=trainingruns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=compute.compute.example.com,resources=trainingruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=compute.compute.example.com,resources=trainingruns/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete

func (r *TrainingRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var run computev1alpha1.TrainingRun
	if err := r.Get(ctx, req.NamespacedName, &run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if run.Status.Phase == computev1alpha1.PhaseSucceeded ||
		run.Status.Phase == computev1alpha1.PhaseFailed {
		return ctrl.Result{}, nil
	}

	if err := r.ensureHeadlessService(ctx, &run); err != nil {
		return ctrl.Result{}, fmt.Errorf("svc: %w", err)
	}

	workers, err := r.listWorkers(ctx, &run)
	if err != nil {
		return ctrl.Result{}, err
	}

	existing := map[int32]*corev1.Pod{}
	for i := range workers {
		if rs, ok := workers[i].Labels[rankLabel]; ok {
			if rank, err := strconv.Atoi(rs); err == nil {
				existing[int32(rank)] = &workers[i]
			}
		}
	}

	for rank := int32(0); rank < run.Spec.WorldSize; rank++ {
		if _, ok := existing[rank]; ok {
			continue
		}
		pod := buildWorkerPod(&run, rank)
		if err := controllerutil.SetControllerReference(&run, pod, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, fmt.Errorf("create worker rank %d: %w", rank, err)
		}
		log.Info("created worker", "rank", rank)
	}

	for rank, pod := range existing {
		if rank >= run.Spec.WorldSize {
			_ = r.Delete(ctx, pod)
		}
	}

	if err := r.updateTrainingStatus(ctx, &run, existing); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueNormal}, nil
}

func (r *TrainingRunReconciler) listWorkers(ctx context.Context, run *computev1alpha1.TrainingRun) ([]corev1.Pod, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(run.Namespace),
		client.MatchingLabels{trainingOwnerLabel: run.Name},
	); err != nil {
		return nil, err
	}
	return pods.Items, nil
}

func (r *TrainingRunReconciler) ensureHeadlessService(ctx context.Context, run *computev1alpha1.TrainingRun) error {
	svc := &corev1.Service{}
	name := run.Name + "-workers"
	err := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: name}, svc)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	svc = &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: run.Namespace},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  map[string]string{trainingOwnerLabel: run.Name},
			Ports:     []corev1.ServicePort{{Name: "ddp", Port: 29500}},
		},
	}
	if err := controllerutil.SetControllerReference(run, svc, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, svc)
}

func (r *TrainingRunReconciler) updateTrainingStatus(ctx context.Context, run *computev1alpha1.TrainingRun, workers map[int32]*corev1.Pod) error {
	ready, failed, succeeded := int32(0), int32(0), int32(0)
	for _, p := range workers {
		switch p.Status.Phase {
		case corev1.PodRunning:
			if isPodReady(p) {
				ready++
			}
		case corev1.PodSucceeded:
			succeeded++
		case corev1.PodFailed:
			failed++
		}
	}

	// deep copy to avoid mutating the original status in case of update conflict
	prev := run.Status.DeepCopy()
	run.Status.ReadyWorkers = ready

	switch {
	case succeeded == run.Spec.WorldSize && run.Spec.WorldSize > 0:
		run.Status.Phase = computev1alpha1.PhaseSucceeded
		if run.Status.CompletionTime == nil {
			now := metav1.Now()
			run.Status.CompletionTime = &now
		}
	case failed > 0:
		if run.Status.Resumes < run.Spec.Spot.MaxRetries {
			run.Status.Resumes++
			for _, p := range workers {
				if p.Status.Phase == corev1.PodFailed {
					_ = r.Delete(ctx, p)
				}
			}
			run.Status.Phase = computev1alpha1.PhaseResuming
		} else {
			run.Status.Phase = computev1alpha1.PhaseFailed
			if run.Status.CompletionTime == nil {
				now := metav1.Now()
				run.Status.CompletionTime = &now
			}
		}
	case ready == run.Spec.WorldSize && run.Spec.WorldSize > 0:
		run.Status.Phase = computev1alpha1.PhaseRunning
		if run.Status.StartTime == nil {
			now := metav1.Now()
			run.Status.StartTime = &now
		}
	default:
		run.Status.Phase = computev1alpha1.PhasePending
	}

	run.Status.EstimatedCostUSD = estimateCost(run.Status.StartTime, run.Status.CompletionTime,
		run.Spec.WorldSize, run.Spec.ResourceHint.CostPerHourUSD)

	if equalTrainingStatus(prev, &run.Status) {
		return nil
	}
	return r.Status().Update(ctx, run)
}

func equalTrainingStatus(a, b *computev1alpha1.TrainingRunStatus) bool {
	return a.Phase == b.Phase &&
		a.ReadyWorkers == b.ReadyWorkers &&
		a.Resumes == b.Resumes &&
		a.EstimatedCostUSD == b.EstimatedCostUSD &&
		a.LastCheckpoint == b.LastCheckpoint
}

func isPodReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func buildWorkerPod(run *computev1alpha1.TrainingRun, rank int32) *corev1.Pod {
	masterAddr := fmt.Sprintf("%s-rank-0.%s-workers.%s.svc.cluster.local",
		run.Name, run.Name, run.Namespace)

	cmd := run.Spec.Command
	args := run.Spec.Args
	if len(cmd) == 0 {
		cmd = []string{"/bin/sh", "-c",
			fmt.Sprintf("echo worker rank=%d world=%d master=%s && sleep 3600",
				rank, run.Spec.WorldSize, masterAddr)}
	}

	env := append([]corev1.EnvVar{
		{Name: "RANK", Value: strconv.Itoa(int(rank))},
		{Name: "WORLD_SIZE", Value: strconv.Itoa(int(run.Spec.WorldSize))},
		{Name: "MASTER_ADDR", Value: masterAddr},
		{Name: "MASTER_PORT", Value: "29500"},
	}, run.Spec.Env...)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-rank-%d", run.Name, rank),
			Namespace: run.Namespace,
			Labels: map[string]string{
				trainingOwnerLabel: run.Name,
				rankLabel:          strconv.Itoa(int(rank)),
			},
			Annotations: map[string]string{
				"compute.compute.example.com/resumes": strconv.Itoa(int(run.Status.Resumes)),
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Subdomain:     run.Name + "-workers",
			Hostname:      fmt.Sprintf("%s-rank-%d", run.Name, rank),
			Affinity:      spotAffinity(run.Spec.Spot),
			Containers: []corev1.Container{{
				Name:      "worker",
				Image:     run.Spec.Image,
				Command:   cmd,
				Args:      args,
				Env:       env,
				Resources: workerResources(run),
			}},
		},
	}

	if run.Spec.DatasetPVC != "" {
		pod.Spec.Volumes = []corev1.Volume{{
			Name: "dataset",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: run.Spec.DatasetPVC,
				},
			},
		}}
		pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{
			{Name: "dataset", MountPath: "/data", ReadOnly: true},
		}
	}

	return pod
}

func workerResources(run *computev1alpha1.TrainingRun) corev1.ResourceRequirements {
	if len(run.Spec.WorkerResources.Requests) > 0 || len(run.Spec.WorkerResources.Limits) > 0 {
		return run.Spec.WorkerResources
	}
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
	}
}

func (r *TrainingRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&computev1alpha1.TrainingRun{}).
		Owns(&corev1.Pod{}, builder.MatchEveryOwner).
		Owns(&corev1.Service{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(mapPodToTrainingOwner)).
		Named("trainingrun").
		Complete(r)
}

func mapPodToTrainingOwner(_ context.Context, obj client.Object) []ctrl.Request {
	labels := obj.GetLabels()
	if name, ok := labels[trainingOwnerLabel]; ok {
		return []ctrl.Request{{NamespacedName: types.NamespacedName{
			Namespace: obj.GetNamespace(), Name: name,
		}}}
	}
	return nil
}
