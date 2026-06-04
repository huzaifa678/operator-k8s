/*
Copyright 2026 huzaifa678.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package metrics registers the compute-operator's domain-specific Prometheus
// metrics with controller-runtime's metrics registry. controller-runtime
// auto-exposes Go runtime + workqueue metrics on the manager's --metrics-bind-address;
// these metrics live alongside them at /metrics.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	namespace      = "compute_operator"
	sparkSubsystem = "sparkjob"
	trainSubsystem = "trainingrun"
)

var (
	// SparkJobReconciles counts every reconcile pass. `result` is "success"
	// or "error". Diff against rate() to see baseline traffic vs problems.
	SparkJobReconciles = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: sparkSubsystem,
		Name: "reconciles_total",
		Help: "Total SparkJob reconcile passes by outcome.",
	}, []string{"result"})

	// SparkJobPhase is 1 for the currently-active phase of each SparkJob, 0
	// for all other phases. A timeseries graph by phase gives a clean visual
	// of every job's lifecycle.
	SparkJobPhase = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace, Subsystem: sparkSubsystem,
		Name: "phase",
		Help: "Current phase of each SparkJob (1 = active, 0 = inactive).",
	}, []string{"namespace", "name", "phase"})

	// SparkJobExecutorsRunning is the live count of executor pods per job.
	SparkJobExecutorsRunning = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace, Subsystem: sparkSubsystem,
		Name: "executors_running",
		Help: "Number of executor pods currently Running per SparkJob.",
	}, []string{"namespace", "name"})

	// SparkJobExecutorsDesired is what the controller wants based on auto-tune
	// or explicit spec.executors. Compare to executors_running to detect
	// scheduling drag.
	SparkJobExecutorsDesired = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace, Subsystem: sparkSubsystem,
		Name: "executors_desired",
		Help: "Number of executor pods the controller wants per SparkJob.",
	}, []string{"namespace", "name"})

	// SparkJobRetries tracks how many spot-retry attempts each job has used.
	SparkJobRetries = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace, Subsystem: sparkSubsystem,
		Name: "retries_total",
		Help: "Spot-retry attempts consumed per SparkJob.",
	}, []string{"namespace", "name"})

	// SparkJobCostUSD is the controller's running cost estimate.
	SparkJobCostUSD = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace, Subsystem: sparkSubsystem,
		Name: "cost_usd",
		Help: "Estimated cost in USD for each SparkJob (driver + executors * uptime * rate).",
	}, []string{"namespace", "name"})

	// --- TrainingRun mirrors ---

	TrainingRunReconciles = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: trainSubsystem,
		Name: "reconciles_total",
		Help: "Total TrainingRun reconcile passes by outcome.",
	}, []string{"result"})

	TrainingRunPhase = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace, Subsystem: trainSubsystem,
		Name: "phase",
		Help: "Current phase of each TrainingRun.",
	}, []string{"namespace", "name", "phase"})

	TrainingRunWorkersReady = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace, Subsystem: trainSubsystem,
		Name: "workers_ready",
		Help: "Number of worker pods currently Ready per TrainingRun.",
	}, []string{"namespace", "name"})

	TrainingRunResumes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace, Subsystem: trainSubsystem,
		Name: "resumes_total",
		Help: "Spot-resume attempts consumed per TrainingRun.",
	}, []string{"namespace", "name"})

	TrainingRunCostUSD = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace, Subsystem: trainSubsystem,
		Name: "cost_usd",
		Help: "Estimated cost in USD for each TrainingRun.",
	}, []string{"namespace", "name"})
)

func init() {
	metrics.Registry.MustRegister(
		SparkJobReconciles, SparkJobPhase,
		SparkJobExecutorsRunning, SparkJobExecutorsDesired,
		SparkJobRetries, SparkJobCostUSD,
		TrainingRunReconciles, TrainingRunPhase,
		TrainingRunWorkersReady, TrainingRunResumes, TrainingRunCostUSD,
	)
}

// SetSparkPhase sets the gauge to 1 for the active phase and clears every
// other phase label, so a graph shows a clean step function instead of
// stacked lines.
func SetSparkPhase(namespace, name, active string) {
	for _, p := range []string{"Pending", "Running", "Checkpointing", "Resuming", "Succeeded", "Failed"} {
		val := 0.0
		if p == active {
			val = 1
		}
		SparkJobPhase.WithLabelValues(namespace, name, p).Set(val)
	}
}

// SetTrainingPhase is the TrainingRun counterpart.
func SetTrainingPhase(namespace, name, active string) {
	for _, p := range []string{"Pending", "Running", "Checkpointing", "Resuming", "Succeeded", "Failed"} {
		val := 0.0
		if p == active {
			val = 1
		}
		TrainingRunPhase.WithLabelValues(namespace, name, p).Set(val)
	}
}

// DeleteSpark clears every gauge for a deleted SparkJob so Prometheus stops
// scraping stale series. Call from the reconcile path after observing a
// NotFound on the CR.
func DeleteSpark(namespace, name string) {
	for _, p := range []string{"Pending", "Running", "Checkpointing", "Resuming", "Succeeded", "Failed"} {
		SparkJobPhase.DeleteLabelValues(namespace, name, p)
	}
	SparkJobExecutorsRunning.DeleteLabelValues(namespace, name)
	SparkJobExecutorsDesired.DeleteLabelValues(namespace, name)
	SparkJobRetries.DeleteLabelValues(namespace, name)
	SparkJobCostUSD.DeleteLabelValues(namespace, name)
}

// DeleteTraining is the TrainingRun counterpart.
func DeleteTraining(namespace, name string) {
	for _, p := range []string{"Pending", "Running", "Checkpointing", "Resuming", "Succeeded", "Failed"} {
		TrainingRunPhase.DeleteLabelValues(namespace, name, p)
	}
	TrainingRunWorkersReady.DeleteLabelValues(namespace, name)
	TrainingRunResumes.DeleteLabelValues(namespace, name)
	TrainingRunCostUSD.DeleteLabelValues(namespace, name)
}
