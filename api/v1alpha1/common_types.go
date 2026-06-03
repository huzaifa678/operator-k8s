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
