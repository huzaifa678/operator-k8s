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
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Executors",type=integer,JSONPath=`.status.runningExecutors`
// +kubebuilder:printcolumn:name="Cost",type=string,JSONPath=`.status.estimatedCostUSD`
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
