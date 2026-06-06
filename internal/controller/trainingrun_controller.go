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
	"maps"
	"strconv"
	"strings"

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
	"github.com/huzaifa678/compute-operator/internal/metrics"
)

const trainingScriptKey = "train.py"

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
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete

func (r *TrainingRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, retErr error) {
	log := logf.FromContext(ctx)

	defer func() {
		outcome := "success"
		if retErr != nil {
			outcome = "error"
		}
		metrics.TrainingRunReconciles.WithLabelValues(outcome).Inc()
	}()

	var run computev1alpha1.TrainingRun
	if err := r.Get(ctx, req.NamespacedName, &run); err != nil {
		if apierrors.IsNotFound(err) {
			metrics.DeleteTraining(req.Namespace, req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if run.Status.Phase == computev1alpha1.PhaseSucceeded ||
		run.Status.Phase == computev1alpha1.PhaseFailed {
		return ctrl.Result{}, nil
	}

	if err := r.ensureHeadlessService(ctx, &run); err != nil {
		return ctrl.Result{}, fmt.Errorf("svc: %w", err)
	}

	if err := r.ensureScriptConfigMap(ctx, &run); err != nil {
		return ctrl.Result{}, fmt.Errorf("script cm: %w", err)
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

// effectiveScript returns the Python the controller should mount at
// /scripts/train.py. Precedence: Spec.Script (explicit user code) wins over
// Spec.BuiltinTrainer (controller-supplied). Returns "" when neither is set.
func effectiveScript(run *computev1alpha1.TrainingRun) string {
	if run.Spec.Script != "" {
		return run.Spec.Script
	}
	switch run.Spec.BuiltinTrainer {
	case "BERTClassifier":
		return bertClassifierScript
	}
	return ""
}

// ensureScriptConfigMap creates (or updates) a ConfigMap holding the
// effective training script (user-supplied OR controller built-in). No-op
// when neither is set.
func (r *TrainingRunReconciler) ensureScriptConfigMap(ctx context.Context, run *computev1alpha1.TrainingRun) error {
	script := effectiveScript(run)
	if script == "" {
		return nil
	}
	name := run.Name + "-script"
	desired := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: run.Namespace},
		Data:       map[string]string{trainingScriptKey: script},
	}
	if err := controllerutil.SetControllerReference(run, desired, r.Scheme); err != nil {
		return err
	}

	existing := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: name}, existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	if existing.Data[trainingScriptKey] == script {
		return nil
	}
	existing.Data = desired.Data
	return r.Update(ctx, existing)
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

	metrics.SetTrainingPhase(run.Namespace, run.Name, string(run.Status.Phase))
	metrics.TrainingRunWorkersReady.WithLabelValues(run.Namespace, run.Name).Set(float64(ready))
	metrics.TrainingRunResumes.WithLabelValues(run.Namespace, run.Name).Set(float64(run.Status.Resumes))
	if cost, err := strconv.ParseFloat(run.Status.EstimatedCostUSD, 64); err == nil {
		metrics.TrainingRunCostUSD.WithLabelValues(run.Namespace, run.Name).Set(cost)
	}

	if equalTrainingStatus(prev, &run.Status) {
		return nil
	}
	// Conflict is benign — pod watch events fire faster than status writes,
	// and the next reconcile will pick up the latest resourceVersion.
	if err := r.Status().Update(ctx, run); err != nil && !apierrors.IsConflict(err) {
		return err
	}
	return nil
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

	procPerNode := int32(1)
	if run.Spec.GPU.Enabled && run.Spec.GPU.PerWorker > 0 {
		procPerNode = run.Spec.GPU.PerWorker
	}

	script := effectiveScript(run)
	cmd := run.Spec.Command
	args := run.Spec.Args
	if len(cmd) == 0 {
		switch {
		case script != "":
			// Self-contained mode: pip-install Packages (if any) then torchrun
			// the mounted /scripts/train.py. Env vars are interpolated by sh.
			pipInstall := ""
			if len(run.Spec.Packages) > 0 {
				pipInstall = "pip install --quiet --no-cache-dir " +
					strings.Join(run.Spec.Packages, " ") + " && "
			}
			torchrun := fmt.Sprintf("exec torchrun"+
				" --nnodes=$WORLD_SIZE"+
				" --nproc_per_node=%d"+
				" --node_rank=$RANK"+
				" --master_addr=$MASTER_ADDR"+
				" --master_port=$MASTER_PORT"+
				" /scripts/%s", procPerNode, trainingScriptKey)
			cmd = []string{"/bin/sh", "-c", pipInstall + torchrun}
		default:
			cmd = []string{"/bin/sh", "-c",
				fmt.Sprintf("echo worker rank=%d world=%d master=%s && sleep 3600",
					rank, run.Spec.WorldSize, masterAddr)}
		}
	}

	backend := run.Spec.GPU.Backend
	if backend == "" {
		if run.Spec.GPU.Enabled {
			backend = "nccl"
		} else {
			backend = "gloo"
		}
	}

	env := append([]corev1.EnvVar{
		{Name: "RANK", Value: strconv.Itoa(int(rank))},
		{Name: "WORLD_SIZE", Value: strconv.Itoa(int(run.Spec.WorldSize))},
		{Name: "MASTER_ADDR", Value: masterAddr},
		{Name: "MASTER_PORT", Value: "29500"},
		{Name: "TORCH_DISTRIBUTED_DEFAULT_BACKEND", Value: backend},
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

	applyGPU(pod, run.Spec.GPU)

	if run.Spec.DatasetPVC != "" {
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: "dataset",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: run.Spec.DatasetPVC,
				},
			},
		})
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{Name: "dataset", MountPath: "/data", ReadOnly: true})
	}

	if script != "" {
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: "script",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: run.Name + "-script",
					},
				},
			},
		})
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{Name: "script", MountPath: "/scripts", ReadOnly: true})
	}

	return pod
}

// applyGPU mutates the pod in place to add: nvidia.com/gpu resource request +
// limit (k8s requires both for extended resources), the node selector that
// targets GPU nodes, the nvidia runtime class if configured, and the standard
// toleration for the taint the GPU Operator places on GPU nodes.
func applyGPU(pod *corev1.Pod, g computev1alpha1.GPUSpec) {
	if !g.Enabled {
		return
	}
	count := max(g.PerWorker, 1)
	gpuQty := resource.MustParse(strconv.Itoa(int(count)))

	c := &pod.Spec.Containers[0]
	if c.Resources.Requests == nil {
		c.Resources.Requests = corev1.ResourceList{}
	}
	if c.Resources.Limits == nil {
		c.Resources.Limits = corev1.ResourceList{}
	}
	c.Resources.Requests[corev1.ResourceName("nvidia.com/gpu")] = gpuQty
	c.Resources.Limits[corev1.ResourceName("nvidia.com/gpu")] = gpuQty

	sel := g.NodeSelector
	if len(sel) == 0 {
		sel = map[string]string{"nvidia.com/gpu.present": "true"}
	}
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = map[string]string{}
	}
	maps.Copy(pod.Spec.NodeSelector, sel)

	if g.RuntimeClass != "" {
		rc := g.RuntimeClass
		pod.Spec.RuntimeClassName = &rc
	}

	pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{
		Key:      "nvidia.com/gpu",
		Operator: corev1.TolerationOpExists,
		Effect:   corev1.TaintEffectNoSchedule,
	})
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
		Owns(&corev1.ConfigMap{}).
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
