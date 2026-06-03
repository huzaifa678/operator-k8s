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

// TrainingRunSpec describes a distributed training job.
type TrainingRunSpec struct {
	// Framework used to launch workers.
	// +kubebuilder:validation:Enum=PyTorch;Ray
	// +kubebuilder:default=PyTorch
	// +optional
	Framework string `json:"framework,omitempty"`

	// Container image with the training code + framework.
	// +kubebuilder:validation:Required
	Image string `json:"image"`

	// Command to run inside each worker. Defaults to torchrun for PyTorch.
	// +optional
	Command []string `json:"command,omitempty"`

	// +optional
	Args []string `json:"args,omitempty"`

	// Number of worker pods (one is also the master / rank 0).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	WorldSize int32 `json:"worldSize,omitempty"`

	// Per-worker resource requests; set nvidia.com/gpu here for GPU jobs.
	// +optional
	WorkerResources corev1.ResourceRequirements `json:"workerResources,omitempty"`

	// Dataset mount. Either a PVC name or an S3 URI (handled by an init container).
	// +optional
	DatasetPVC string `json:"datasetPVC,omitempty"`
	// +optional
	DatasetS3URI string `json:"datasetS3URI,omitempty"`

	// +optional
	Checkpoint CheckpointSpec `json:"checkpoint,omitempty"`

	// +optional
	Spot SpotPolicy `json:"spot,omitempty"`

	// +optional
	ResourceHint ResourceHint `json:"resourceHint,omitempty"`

	// Environment variables passed to every worker.
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`
}

// TrainingRunStatus reflects observed state.
type TrainingRunStatus struct {
	// +optional
	Phase Phase `json:"phase,omitempty"`

	// Number of workers currently running and Ready.
	// +optional
	ReadyWorkers int32 `json:"readyWorkers,omitempty"`

	// Latest checkpoint URI written by the workload.
	// +optional
	LastCheckpoint string `json:"lastCheckpoint,omitempty"`

	// Times the run has been resumed from a checkpoint (e.g. after spot eviction).
	// +optional
	Resumes int32 `json:"resumes,omitempty"`

	// +optional
	EstimatedCostUSD string `json:"estimatedCostUSD,omitempty"`

	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`
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
// +kubebuilder:printcolumn:name="Workers",type=integer,JSONPath=`.status.readyWorkers`
// +kubebuilder:printcolumn:name="Resumes",type=integer,JSONPath=`.status.resumes`
// +kubebuilder:printcolumn:name="Cost",type=string,JSONPath=`.status.estimatedCostUSD`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// TrainingRun is the Schema for the trainingruns API.
type TrainingRun struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec TrainingRunSpec `json:"spec"`
	// +optional
	Status TrainingRunStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

type TrainingRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []TrainingRun `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TrainingRun{}, &TrainingRunList{})
}
