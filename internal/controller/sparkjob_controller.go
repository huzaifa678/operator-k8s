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
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
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
	sparkRoleKey  = "spark-role"
	sparkRoleExec = "executor"
	requeueNormal = 15 * time.Second

	driverPort       = 7078
	blockManagerPort = 7079
	defaultSparkImg  = "apache/spark:3.5.3"
)

type SparkJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=compute.compute.example.com,resources=sparkjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=compute.compute.example.com,resources=sparkjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=compute.compute.example.com,resources=sparkjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete

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

	saName, err := r.ensureDriverRBAC(ctx, &job)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("rbac: %w", err)
	}

	if err := r.ensureDriverService(ctx, &job); err != nil {
		return ctrl.Result{}, fmt.Errorf("svc: %w", err)
	}

	driverName := job.Name + "-driver"
	driver := &corev1.Pod{}
	err = r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: driverName}, driver)
	switch {
		case apierrors.IsNotFound(err):
			driver = buildDriverPod(&job, driverName, saName, desired)
			if err := controllerutil.SetControllerReference(&job, driver, r.Scheme); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.Create(ctx, driver); err != nil {
				return ctrl.Result{}, fmt.Errorf("create driver: %w", err)
			}
			log.Info("created spark-submit driver pod", "pod", driverName,
				"executors", desired, "main", job.Spec.MainApplicationFile)
			return ctrl.Result{RequeueAfter: requeueNormal}, nil
		case err != nil:
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

// ensureDriverRBAC creates a ServiceAccount + Role + RoleBinding scoped to the
// job's namespace, giving the driver permission to manage executor pods. If
// the user provided spec.serviceAccount we trust it and skip creation.
func (r *SparkJobReconciler) ensureDriverRBAC(ctx context.Context, job *computev1alpha1.SparkJob) (string, error) {
	if job.Spec.ServiceAccount != "" {
		return job.Spec.ServiceAccount, nil
	}
	saName := job.Name + "-driver-sa"
	roleName := job.Name + "-driver-role"

	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: saName, Namespace: job.Namespace}}
	if err := controllerutil.SetControllerReference(job, sa, r.Scheme); err != nil {
		return "", err
	}
	if err := r.Create(ctx, sa); err != nil && !apierrors.IsAlreadyExists(err) {
		return "", err
	}

	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: roleName, Namespace: job.Namespace},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"pods"},
				Verbs: []string{"get", "list", "watch", "create", "delete", "deletecollection", "patch"}},
			{APIGroups: []string{""}, Resources: []string{"pods/log", "pods/exec"},
				Verbs: []string{"get", "create"}},
			{APIGroups: []string{""}, Resources: []string{"configmaps", "services"},
				Verbs: []string{"get", "list", "watch", "create", "delete", "patch"}},
			{APIGroups: []string{""}, Resources: []string{"persistentvolumeclaims"},
				Verbs: []string{"get", "list", "create", "delete"}},
		},
	}
	if err := controllerutil.SetControllerReference(job, role, r.Scheme); err != nil {
		return "", err
	}
	if err := r.Create(ctx, role); err != nil && !apierrors.IsAlreadyExists(err) {
		return "", err
	}

	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: roleName + "-binding", Namespace: job.Namespace},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: saName, Namespace: job.Namespace}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: roleName},
	}
	if err := controllerutil.SetControllerReference(job, rb, r.Scheme); err != nil {
		return "", err
	}
	if err := r.Create(ctx, rb); err != nil && !apierrors.IsAlreadyExists(err) {
		return "", err
	}
	return saName, nil
}

// ensureDriverService creates the headless service Spark executors use to
// reach the driver in client mode. We point spark.driver.host at this DNS.
func (r *SparkJobReconciler) ensureDriverService(ctx context.Context, job *computev1alpha1.SparkJob) error {
	name := job.Name + "-driver-svc"
	svc := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: name}, svc)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	svc = &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: job.Namespace},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  map[string]string{ownerLabel: job.Name, roleLabel: roleDriver},
			Ports: []corev1.ServicePort{
				{Name: "driver-rpc", Port: driverPort, TargetPort: intOrString(driverPort)},
				{Name: "blockmgr", Port: blockManagerPort, TargetPort: intOrString(blockManagerPort)},
			},
		},
	}
	if err := controllerutil.SetControllerReference(job, svc, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, svc)
}

// listExecutorPods finds the executor pods Spark created via the K8s scheduler
// backend. Spark labels them spark-role=executor and sets the driver pod as
// ownerRef, so we filter on both.
func (r *SparkJobReconciler) listExecutorPods(ctx context.Context, job *computev1alpha1.SparkJob, driverUID string) ([]corev1.Pod, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{sparkRoleKey: sparkRoleExec},
	); err != nil {
		return nil, err
	}
	out := pods.Items[:0]
	for _, p := range pods.Items {
		for _, or := range p.OwnerReferences {
			if string(or.UID) == driverUID {
				out = append(out, p)
				break
			}
		}
	}
	return out, nil
}

func (r *SparkJobReconciler) updateStatus(ctx context.Context, job *computev1alpha1.SparkJob, driver *corev1.Pod, desired int32) error {
	execs, err := r.listExecutorPods(ctx, job, string(driver.UID))
	if err != nil {
		return err
	}
	running := int32(0)
	for _, p := range execs {
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

func buildDriverPod(job *computev1alpha1.SparkJob, name, sa string, executors int32) *corev1.Pod {
	image := job.Spec.Image
	if image == "" {
		image = defaultSparkImg
	}
	driverHost := fmt.Sprintf("%s-driver-svc.%s.svc.cluster.local", job.Name, job.Namespace)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: job.Namespace,
			Labels: map[string]string{
				ownerLabel:   job.Name,
				roleLabel:    roleDriver,
				sparkRoleKey: "driver",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:      corev1.RestartPolicyNever,
			ServiceAccountName: sa,
			Hostname:           name,
			Subdomain:          job.Name + "-driver-svc",
			Affinity:           spotAffinity(job.Spec.Spot),
			Containers: []corev1.Container{{
				Name:      "spark-submit",
				Image:     image,
				Command:   []string{"/bin/sh", "-c"},
				Args:      []string{sparkSubmitScript(job, name, driverHost, executors, image)},
				Resources: driverResources(job),
				Ports: []corev1.ContainerPort{
					{Name: "driver-rpc", ContainerPort: driverPort},
					{Name: "blockmgr", ContainerPort: blockManagerPort},
				},
				Env: []corev1.EnvVar{
					{Name: "SPARK_DRIVER_BIND_ADDRESS", ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}},
					{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
				},
			}},
		},
	}
	return pod
}

// sparkSubmitScript assembles a real spark-submit invocation using Spark's
// native Kubernetes scheduler backend in client mode. The driver pod is the
// spark-submit process; Spark spawns executors itself via the k8s API.
func sparkSubmitScript(job *computev1alpha1.SparkJob, podName, driverHost string, executors int32, image string) string {
	confs := map[string]string{
		"spark.kubernetes.namespace":                              job.Namespace,
		"spark.kubernetes.driver.pod.name":                        podName,
		"spark.kubernetes.container.image":                        image,
		"spark.kubernetes.authenticate.driver.serviceAccountName": job.Spec.ServiceAccount,
		"spark.executor.instances":                                strconv.Itoa(int(executors)),
		"spark.driver.host":                                       driverHost,
		"spark.driver.port":                                       strconv.Itoa(driverPort),
		"spark.driver.blockManager.port":                          strconv.Itoa(blockManagerPort),
		"spark.kubernetes.executor.label." + ownerLabel:           job.Name,
		"spark.kubernetes.executor.podTemplateContainerName":      "executor",
		"spark.kubernetes.driver.ownerReference.controller":       "true",
	}
	if confs["spark.kubernetes.authenticate.driver.serviceAccountName"] == "" {
		confs["spark.kubernetes.authenticate.driver.serviceAccountName"] = job.Name + "-driver-sa"
	}
	if req, ok := job.Spec.ExecutorResources.Requests[corev1.ResourceCPU]; ok {
		confs["spark.executor.cores"] = req.String()
	}
	if req, ok := job.Spec.ExecutorResources.Requests[corev1.ResourceMemory]; ok {
		confs["spark.executor.memory"] = humanMem(req)
	}
	for k, v := range job.Spec.SparkConf {
		confs[k] = v
	}

	// Deterministic --conf ordering for cache-friendly pod specs.
	keys := make([]string, 0, len(confs))
	for k := range confs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString("/opt/spark/bin/spark-submit \\\n")
	b.WriteString("  --master k8s://https://kubernetes.default.svc:443 \\\n")
	b.WriteString("  --deploy-mode client \\\n")
	b.WriteString("  --name " + shellQuote(job.Name) + " \\\n")
	if job.Spec.MainClass != "" {
		b.WriteString("  --class " + shellQuote(job.Spec.MainClass) + " \\\n")
	}
	for _, k := range keys {
		b.WriteString("  --conf " + shellQuote(k+"="+confs[k]) + " \\\n")
	}
	b.WriteString("  " + shellQuote(job.Spec.MainApplicationFile))
	for _, a := range job.Spec.Arguments {
		b.WriteString(" " + shellQuote(a))
	}
	return b.String()
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

// humanMem converts a k8s resource.Quantity (bytes) to Spark's "512m" / "2g" form.
func humanMem(q resource.Quantity) string {
	bytes := q.Value()
	const mi = 1024 * 1024
	const gi = 1024 * mi
	if bytes%gi == 0 {
		return fmt.Sprintf("%dg", bytes/gi)
	}
	return fmt.Sprintf("%dm", bytes/mi)
}

func driverResources(job *computev1alpha1.SparkJob) corev1.ResourceRequirements {
	if len(job.Spec.DriverResources.Requests) > 0 || len(job.Spec.DriverResources.Limits) > 0 {
		return job.Spec.DriverResources
	}
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("250m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
	}
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

func intOrString(p int32) intstr.IntOrString {
	return intstr.IntOrString{Type: intstr.Int, IntVal: p}
}

func (r *SparkJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&computev1alpha1.SparkJob{}).
		Owns(&corev1.Pod{}, builder.MatchEveryOwner).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
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
