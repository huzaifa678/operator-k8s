/*
Copyright 2026 huzaifa678.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1alpha1

// CheckpointSpec configures where state is persisted so a job can resume after
// pod eviction (e.g. spot interruption).
type CheckpointSpec struct {
	// +optional
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// S3-compatible URI (e.g. s3://bucket/prefix). MinIO works.
	// +optional
	S3URI string `json:"s3URI,omitempty"`

	// Name of a Secret with keys: accessKey, secretKey, endpoint.
	// +optional
	CredentialsSecret string `json:"credentialsSecret,omitempty"`

	// How often the workload should checkpoint.
	// +optional
	// +kubebuilder:default="5m"
	Interval string `json:"interval,omitempty"`
}

// SpotPolicy controls how the controller schedules onto preemptible nodes.
type SpotPolicy struct {
	// If true, the controller will prefer (or require) spot nodes.
	// +optional
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// Required: must land on a spot node. Preferred: best-effort.
	// +kubebuilder:validation:Enum=Required;Preferred
	// +kubebuilder:default=Preferred
	// +optional
	Mode string `json:"mode,omitempty"`

	// Max retries after eviction before the job is marked Failed.
	// +kubebuilder:default=3
	// +optional
	MaxRetries int32 `json:"maxRetries,omitempty"`
}

// ResourceHint lets the auto-tuner make decisions without the user
// over-specifying executor/worker counts up front.
type ResourceHint struct {
	// Estimated input size in MB. Used to compute executor count.
	// +optional
	InputSizeMB int64 `json:"inputSizeMB,omitempty"`

	// Estimated cost per hour per pod, in USD. Used for status.estimatedCost.
	// +optional
	// +kubebuilder:default="0.10"
	CostPerHourUSD string `json:"costPerHourUSD,omitempty"`
}

// GPUSpec is the high-level GPU request. The controller translates it into
// pod resource limits, node selector, runtime class, tolerations, and the
// right torchrun --nproc_per_node value. Assumes the NVIDIA GPU Operator
// (or equivalent device-plugin + driver stack) is installed on the cluster;
// see `make install-gpu-operator`.
type GPUSpec struct {
	// If false, no GPU plumbing is added (CPU-only run). Defaults to false.
	// +optional
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// GPUs requested per worker pod. Becomes nvidia.com/gpu in resources and
	// torchrun --nproc_per_node. Defaults to 1.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	PerWorker int32 `json:"perWorker,omitempty"`

	// Node selector applied so the pod only schedules on GPU nodes.
	// Defaults to {"nvidia.com/gpu.present": "true"} which the NVIDIA GPU
	// Operator labels GPU nodes with.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// RuntimeClass for the pod. Set this to "nvidia" on clusters where the
	// NVIDIA container runtime is registered as a RuntimeClass. Empty leaves
	// runtimeClassName unset (works on clusters where nvidia is the default
	// runtime, e.g. nodes configured with nvidia-container-toolkit's
	// default-runtime).
	// +optional
	RuntimeClass string `json:"runtimeClass,omitempty"`

	// Collective backend for torch.distributed. Defaults to nccl when GPUs
	// are present (required for inter-GPU all-reduce), gloo otherwise.
	// +optional
	// +kubebuilder:validation:Enum=nccl;gloo;mpi
	Backend string `json:"backend,omitempty"`
}

// Phase is a coarse high-level status shared by both CRDs.
// +kubebuilder:validation:Enum=Pending;Running;Checkpointing;Resuming;Succeeded;Failed
type Phase string

const (
	PhasePending       Phase = "Pending"
	PhaseRunning       Phase = "Running"
	PhaseCheckpointing Phase = "Checkpointing"
	PhaseResuming      Phase = "Resuming"
	PhaseSucceeded     Phase = "Succeeded"
	PhaseFailed        Phase = "Failed"
)
