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
	"errors"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
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
	"github.com/huzaifa678/compute-operator/internal/metrics"
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

	// Scheduled-mode labels: set on every child run so the template can find
	// its descendants without a controller-ref List walk.
	parentLabel    = "compute.compute.example.com/sparkjob-parent"
	runAtLabel     = "compute.compute.example.com/run-at" // unix seconds
	scheduledLabel = "compute.compute.example.com/scheduled"

	concurrencyAllow   = "Allow"
	concurrencyForbid  = "Forbid"
	concurrencyReplace = "Replace"
)

// cronParser accepts the standard 5-field form ("min hour dom mon dow"), the
// same dialect Kubernetes' batch/v1 CronJob uses.
var cronParser = cron.NewParser(
	cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow,
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
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *SparkJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, retErr error) {
	log := logf.FromContext(ctx)

	defer func() {
		outcome := "success"
		if retErr != nil {
			outcome = "error"
		}
		metrics.SparkJobReconciles.WithLabelValues(outcome).Inc()
	}()

	var job computev1alpha1.SparkJob
	if err := r.Get(ctx, req.NamespacedName, &job); err != nil {
		if apierrors.IsNotFound(err) {
			// CR was deleted — drop its gauges so we don't keep scraping stale series.
			metrics.DeleteSpark(req.Namespace, req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Scheduled template? Branch into the cron-driven path; it never spawns a
	// driver pod, only child SparkJob runs.
	if job.Spec.Schedule != nil && *job.Spec.Schedule != "" {
		return r.reconcileScheduled(ctx, &job)
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
		execEnvConf, err := r.derivedExecutorEnvConf(ctx, &job)
		if err != nil {
			if dep, ok := asMissingDep(err); ok {
				// Wait politely: don't spawn the driver, mark the job Pending with
				// a clear condition, requeue at a slow cadence. No stack trace.
				// Log at V(1) — the status condition tells the user what they need
				// to know via `kubectl describe`; spamming INFO every reconcile is
				// just noise (controller-runtime can re-queue many times per sec).
				log.V(1).Info("waiting for dependency", "kind", dep.kind, "name", dep.name)
				if err := r.markAwaiting(ctx, &job, dep); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
			}
			return ctrl.Result{}, fmt.Errorf("derive executor env: %w", err)
		}
		driver = buildDriverPod(&job, driverName, saName, desired, execEnvConf)
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
		n = max(n, job.Spec.MinExecutors)
		n = min(n, job.Spec.MaxExecutors)
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
				Verbs: []string{"get", "list", "watch", "create", "delete", "deletecollection", "patch"}},
			{APIGroups: []string{""}, Resources: []string{"persistentvolumeclaims"},
				Verbs: []string{"get", "list", "watch", "create", "delete", "deletecollection", "patch"}},
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

	// Emit metrics whether or not status changed — Prometheus needs the
	// latest sample for "now" even when no field shifted.
	metrics.SetSparkPhase(job.Namespace, job.Name, string(job.Status.Phase))
	metrics.SparkJobExecutorsRunning.WithLabelValues(job.Namespace, job.Name).Set(float64(running))
	metrics.SparkJobExecutorsDesired.WithLabelValues(job.Namespace, job.Name).Set(float64(desired))
	metrics.SparkJobRetries.WithLabelValues(job.Namespace, job.Name).Set(float64(job.Status.Retries))
	if cost, err := strconv.ParseFloat(job.Status.EstimatedCostUSD, 64); err == nil {
		metrics.SparkJobCostUSD.WithLabelValues(job.Namespace, job.Name).Set(cost)
	}

	if equalStatus(prev, &job.Status) {
		return nil
	}
	// Optimistic concurrency: another reconcile may already be updating the
	// SparkJob. Ignore conflicts — the next reconcile (which we'll be
	// re-queued for) will pick up the latest version.
	if err := r.Status().Update(ctx, job); err != nil && !apierrors.IsConflict(err) {
		return err
	}
	return nil
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

func buildDriverPod(job *computev1alpha1.SparkJob, name, sa string, executors int32, execEnvConf map[string]string) *corev1.Pod {
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
				Args:      []string{sparkSubmitScript(job, name, driverHost, executors, image, execEnvConf)},
				Resources: driverResources(job),
				Ports: []corev1.ContainerPort{
					{Name: "driver-rpc", ContainerPort: driverPort},
					{Name: "blockmgr", ContainerPort: blockManagerPort},
				},
				// Built-ins first so user-supplied Env can override if they really want to
				// (e.g. force a specific POD_NAME for debugging).
				Env: append([]corev1.EnvVar{
					{Name: "SPARK_DRIVER_BIND_ADDRESS", ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}},
					{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
				}, job.Spec.Env...),
				EnvFrom: job.Spec.EnvFrom,
			}},
		},
	}
	return pod
}

// sparkSubmitScript assembles a real spark-submit invocation using Spark's
// native Kubernetes scheduler backend in client mode. The driver pod is the
// spark-submit process; Spark spawns executors itself via the k8s API.
//
// execEnvConf carries spark.kubernetes.executor.{secret,configMap}KeyRef.*
// entries derived from spec.envFrom / spec.env so executors get the same
// env as the driver without the user repeating themselves in sparkConf.
func sparkSubmitScript(job *computev1alpha1.SparkJob, podName, driverHost string, executors int32, image string, execEnvConf map[string]string) string {
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
		// Spark expects a whole-integer core count (>=1). K8s CPU may be
		// fractional ("250m"); round millicores up to the nearest whole core.
		cores := max((req.MilliValue()+999)/1000, 1)
		confs["spark.executor.cores"] = strconv.FormatInt(cores, 10)
	}
	if req, ok := job.Spec.ExecutorResources.Requests[corev1.ResourceMemory]; ok {
		confs["spark.executor.memory"] = humanMem(req)
	}
	if req, ok := job.Spec.DriverResources.Requests[corev1.ResourceCPU]; ok {
		cores := max((req.MilliValue()+999)/1000, 1)
		confs["spark.driver.cores"] = strconv.FormatInt(cores, 10)
	}
	if req, ok := job.Spec.DriverResources.Requests[corev1.ResourceMemory]; ok {
		confs["spark.driver.memory"] = humanMem(req)
	}
	// Derived first, then user SparkConf — so explicit user values win.
	maps.Copy(confs, execEnvConf)
	maps.Copy(confs, job.Spec.SparkConf)

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
		// Owning child SparkJobs lets a scheduled template wake up when one of
		// its children finishes (for prune / Active list refresh).
		Owns(&computev1alpha1.SparkJob{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(mapPodToOwner)).
		Named("sparkjob").
		Complete(r)
}

// missingDepError signals that a referenced Secret / ConfigMap doesn't exist yet.
// Reconcile treats this as "wait politely" rather than an error: no stack trace,
// no retry storm — just a clear status condition + slow requeue.
type missingDepError struct{ kind, name string }

func (e *missingDepError) Error() string {
	return fmt.Sprintf("%s %q not found", e.kind, e.name)
}

func asMissingDep(err error) (*missingDepError, bool) {
	var m *missingDepError
	if err != nil && errors.As(err, &m) {
		return m, true
	}
	return nil, false
}

// markAwaiting sets Phase=Pending and a DependenciesReady=False condition with
// a human-readable message naming the missing object. Idempotent: status is
// only written when something changed.
func (r *SparkJobReconciler) markAwaiting(ctx context.Context, job *computev1alpha1.SparkJob, dep *missingDepError) error {
	wantMsg := fmt.Sprintf("waiting for %s %q", dep.kind, dep.name)
	cond := metav1.Condition{
		Type:               "DependenciesReady",
		Status:             metav1.ConditionFalse,
		Reason:             "AwaitingDependency",
		Message:            wantMsg,
		LastTransitionTime: metav1.Now(),
		ObservedGeneration: job.Generation,
	}

	// Find an existing matching condition; bail if nothing changed so we don't
	// thrash status updates every 30s.
	for i := range job.Status.Conditions {
		c := &job.Status.Conditions[i]
		if c.Type == cond.Type {
			if c.Status == cond.Status && c.Reason == cond.Reason && c.Message == cond.Message &&
				job.Status.Phase == computev1alpha1.PhasePending {
				return nil
			}
			job.Status.Conditions[i] = cond
			job.Status.Phase = computev1alpha1.PhasePending
			return r.Status().Update(ctx, job)
		}
	}
	job.Status.Conditions = append(job.Status.Conditions, cond)
	job.Status.Phase = computev1alpha1.PhasePending
	return r.Status().Update(ctx, job)
}

// derivedExecutorEnvConf turns the driver's Env / EnvFrom into Spark confs
// so executor pods (which Spark creates, not us) inherit the same env.
//
// For Env entries with valueFrom.{Secret,ConfigMap}KeyRef we already have all
// the info. For EnvFrom (bulk Secret/ConfigMap), we fetch the referenced
// object to enumerate its keys — otherwise Spark wouldn't know what env vars
// to project.
//
// Returns a *missingDepError when a non-optional ref doesn't exist; the caller
// is expected to surface that as a status condition and requeue, not crash.
func (r *SparkJobReconciler) derivedExecutorEnvConf(ctx context.Context, job *computev1alpha1.SparkJob) (map[string]string, error) {
	out := map[string]string{}

	for _, e := range job.Spec.Env {
		if e.ValueFrom == nil {
			continue
		}
		switch {
		case e.ValueFrom.SecretKeyRef != nil:
			ref := e.ValueFrom.SecretKeyRef
			out["spark.kubernetes.executor.secretKeyRef."+e.Name] = ref.Name + ":" + ref.Key
		case e.ValueFrom.ConfigMapKeyRef != nil:
			ref := e.ValueFrom.ConfigMapKeyRef
			out["spark.kubernetes.executor.configMapKeyRef."+e.Name] = ref.Name + ":" + ref.Key
		}
	}

	for _, ef := range job.Spec.EnvFrom {
		switch {
		case ef.SecretRef != nil:
			var sec corev1.Secret
			err := r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: ef.SecretRef.Name}, &sec)
			if err != nil {
				if apierrors.IsNotFound(err) {
					if ef.SecretRef.Optional != nil && *ef.SecretRef.Optional {
						continue
					}
					return nil, &missingDepError{kind: "Secret", name: ef.SecretRef.Name}
				}
				return nil, fmt.Errorf("envFrom secret %q: %w", ef.SecretRef.Name, err)
			}
			for k := range sec.Data {
				out["spark.kubernetes.executor.secretKeyRef."+ef.Prefix+k] = ef.SecretRef.Name + ":" + k
			}
		case ef.ConfigMapRef != nil:
			var cm corev1.ConfigMap
			err := r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: ef.ConfigMapRef.Name}, &cm)
			if err != nil {
				if apierrors.IsNotFound(err) {
					if ef.ConfigMapRef.Optional != nil && *ef.ConfigMapRef.Optional {
						continue
					}
					return nil, &missingDepError{kind: "ConfigMap", name: ef.ConfigMapRef.Name}
				}
				return nil, fmt.Errorf("envFrom configMap %q: %w", ef.ConfigMapRef.Name, err)
			}
			for k := range cm.Data {
				out["spark.kubernetes.executor.configMapKeyRef."+ef.Prefix+k] = ef.ConfigMapRef.Name + ":" + k
			}
		}
	}
	return out, nil
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
