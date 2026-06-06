/*
Copyright 2026 huzaifa678.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SparkJobSpec defines a Spark application to submit and supervise.
type SparkJobSpec struct {
	// Container image with a Spark distribution. Must contain /opt/spark/bin/spark-submit.
	// Defaults to apache/spark:3.5.3.
	// +optional
	// +kubebuilder:default="apache/spark:3.5.3"
	Image string `json:"image,omitempty"`

	// Application type. Affects how the driver command is constructed.
	// +kubebuilder:validation:Enum=Scala;Python;R
	// +kubebuilder:default=Scala
	// +optional
	Type string `json:"type,omitempty"`

	// JAR or .py file the driver runs. May be local:///path baked into the image,
	// or a fetchable http(s):// / s3a:// URI.
	// +kubebuilder:validation:Required
	MainApplicationFile string `json:"mainApplicationFile"`

	// Fully-qualified main class (Scala/Java only).
	// +optional
	MainClass string `json:"mainClass,omitempty"`

	// Extra --conf key=value pairs passed to spark-submit.
	// +optional
	SparkConf map[string]string `json:"sparkConf,omitempty"`

	// ServiceAccount the driver runs as. If empty, the controller creates
	// `<job>-driver-sa` with the minimum RBAC Spark needs to create executor pods.
	// +optional
	ServiceAccount string `json:"serviceAccount,omitempty"`

	// Arguments passed to the application.
	// +optional
	Arguments []string `json:"arguments,omitempty"`

	// Env adds environment variables to the spark-submit driver container.
	// In --deploy-mode client (what this operator runs), the driver container
	// IS spark-submit, so these are visible to any Hadoop / AWS SDK credential
	// chain evaluated during prepareSubmitEnvironment.
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`

	// EnvFrom adds bulk environment sources (Secret / ConfigMap) to the driver
	// container. Use this to project AWS credentials without baking them into
	// sparkConf: see config/samples for the aws-creds Secret pattern.
	// +optional
	EnvFrom []corev1.EnvFromSource `json:"envFrom,omitempty"`

	// Driver pod resource requests.
	// +optional
	DriverResources corev1.ResourceRequirements `json:"driverResources,omitempty"`

	// Executor pod resource requests.
	// +optional
	ExecutorResources corev1.ResourceRequirements `json:"executorResources,omitempty"`

	// Static executor count. If 0 and ResourceHint.InputSizeMB > 0, the
	// controller auto-tunes based on input size.
	// +optional
	// +kubebuilder:default=0
	Executors int32 `json:"executors,omitempty"`

	// Min/max bounds for the auto-tuner.
	// +optional
	// +kubebuilder:default=1
	MinExecutors int32 `json:"minExecutors,omitempty"`
	// +optional
	// +kubebuilder:default=20
	MaxExecutors int32 `json:"maxExecutors,omitempty"`

	// +optional
	ResourceHint ResourceHint `json:"resourceHint,omitempty"`

	// +optional
	Spot SpotPolicy `json:"spot,omitempty"`

	// +optional
	Checkpoint CheckpointSpec `json:"checkpoint,omitempty"`

	// Schedule is a cron expression (5-field, "min hour dom mon dow"). When set,
	// the SparkJob behaves as a *template*: the controller never spawns a driver
	// pod for it directly, instead creating a child SparkJob (with Schedule=nil)
	// on every fire time. Leave unset for one-shot jobs.
	// +optional
	Schedule *string `json:"schedule,omitempty"`

	// Suspend pauses scheduling without deleting the template. Existing children
	// continue to run; no new children are created while true.
	// +optional
	Suspend *bool `json:"suspend,omitempty"`

	// IANA time zone (e.g. "Etc/UTC", "America/New_York") used to evaluate the
	// cron expression. Defaults to UTC.
	// +optional
	TimeZone *string `json:"timeZone,omitempty"`

	// ConcurrencyPolicy controls how to treat in-flight children when a new
	// fire time arrives. Defaults to Forbid.
	//   Allow   – run a new child even if previous is still running
	//   Forbid  – skip this tick if any active child exists
	//   Replace – delete active children, then run a new one
	// +optional
	// +kubebuilder:validation:Enum=Allow;Forbid;Replace
	// +kubebuilder:default=Forbid
	ConcurrencyPolicy string `json:"concurrencyPolicy,omitempty"`

	// StartingDeadlineSeconds is the deadline (in seconds since a missed fire
	// time) past which a missed run will be skipped rather than back-filled.
	// Mirrors batch/v1 CronJob.spec.startingDeadlineSeconds.
	// +optional
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty"`

	// SuccessfulJobsHistoryLimit keeps this many Succeeded children for audit;
	// older Succeeded children are garbage-collected. Defaults to 3.
	// +optional
	// +kubebuilder:default=3
	SuccessfulJobsHistoryLimit *int32 `json:"successfulJobsHistoryLimit,omitempty"`

	// FailedJobsHistoryLimit keeps this many Failed children for audit. Defaults to 3.
	// +optional
	// +kubebuilder:default=3
	FailedJobsHistoryLimit *int32 `json:"failedJobsHistoryLimit,omitempty"`
}

// SparkJobStatus reflects observed state.
type SparkJobStatus struct {
	// +optional
	Phase Phase `json:"phase,omitempty"`

	// Number of executor pods currently running.
	// +optional
	RunningExecutors int32 `json:"runningExecutors,omitempty"`

	// Number of executors the auto-tuner decided on.
	// +optional
	DesiredExecutors int32 `json:"desiredExecutors,omitempty"`

	// Driver pod name (once created).
	// +optional
	DriverPod string `json:"driverPod,omitempty"`

	// Retry attempts consumed.
	// +optional
	Retries int32 `json:"retries,omitempty"`

	// Estimated cost in USD accrued so far (driver + executors * uptime).
	// +optional
	EstimatedCostUSD string `json:"estimatedCostUSD,omitempty"`

	// When the job was started (driver first scheduled).
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// When the job completed (Succeeded or Failed).
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// LastScheduleTime is when the most recent child run was started
	// (scheduled-mode only).
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`

	// NextScheduleTime is the next planned fire time (scheduled-mode only).
	// +optional
	NextScheduleTime *metav1.Time `json:"nextScheduleTime,omitempty"`

	// Active is the list of currently running child SparkJob runs
	// (scheduled-mode only).
	// +optional
	Active []corev1.ObjectReference `json:"active,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Executors",type=integer,JSONPath=`.status.runningExecutors`
// +kubebuilder:printcolumn:name="Cost",type=string,JSONPath=`.status.estimatedCostUSD`
// +kubebuilder:printcolumn:name="NextRun",type=date,JSONPath=`.status.nextScheduleTime`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// SparkJob is the Schema for the sparkjobs API.
type SparkJob struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec SparkJobSpec `json:"spec"`
	// +optional
	Status SparkJobStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

type SparkJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []SparkJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SparkJob{}, &SparkJobList{})
}
