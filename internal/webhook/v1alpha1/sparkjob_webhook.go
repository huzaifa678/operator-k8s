/*
Copyright 2026 huzaifa678.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1alpha1

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	computev1alpha1 "github.com/huzaifa678/compute-operator/api/v1alpha1"
)

var sparkjoblog = logf.Log.WithName("sparkjob-webhook")

// SetupSparkJobWebhookWithManager registers the validating webhook.
func SetupSparkJobWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &computev1alpha1.SparkJob{}).
		WithValidator(&SparkJobCustomValidator{}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-compute-compute-example-com-v1alpha1-sparkjob,mutating=false,failurePolicy=fail,sideEffects=None,groups=compute.compute.example.com,resources=sparkjobs,verbs=create;update,versions=v1alpha1,name=vsparkjob-v1alpha1.kb.io,admissionReviewVersions=v1

// SparkJobCustomValidator enforces semantic rules CRD OpenAPI validation can't.
type SparkJobCustomValidator struct{}

func (v *SparkJobCustomValidator) ValidateCreate(_ context.Context, obj *computev1alpha1.SparkJob) (admission.Warnings, error) {
	sparkjoblog.V(1).Info("validate create", "name", obj.GetName())
	return validateSparkJob(obj)
}

func (v *SparkJobCustomValidator) ValidateUpdate(_ context.Context, _, newObj *computev1alpha1.SparkJob) (admission.Warnings, error) {
	sparkjoblog.V(1).Info("validate update", "name", newObj.GetName())
	return validateSparkJob(newObj)
}

func (v *SparkJobCustomValidator) ValidateDelete(_ context.Context, _ *computev1alpha1.SparkJob) (admission.Warnings, error) {
	return nil, nil
}

// validateSparkJob runs all semantic checks. Returns admission warnings
// (non-blocking) and an aggregated invalid-resource error (blocking).
func validateSparkJob(job *computev1alpha1.SparkJob) (admission.Warnings, error) {
	var (
		errs     field.ErrorList
		warnings admission.Warnings
		specPath = field.NewPath("spec")
	)

	if strings.TrimSpace(job.Spec.MainApplicationFile) == "" {
		errs = append(errs, field.Required(specPath.Child("mainApplicationFile"),
			"must be set (e.g. local:///opt/app/job.jar or http://…/main.py)"))
	}

	// --- type <-> mainClass coherence ---
	if job.Spec.Type == "Scala" && strings.TrimSpace(job.Spec.MainClass) == "" {
		warnings = append(warnings,
			"spec.mainClass is empty; spark-submit will look up Main-Class from the JAR manifest. "+
				"This works for fat-JARs but typically fails for examples — set spec.mainClass explicitly.")
	}
	if job.Spec.Type == "Python" && strings.TrimSpace(job.Spec.MainClass) != "" {
		warnings = append(warnings,
			"spec.mainClass is ignored when spec.type=Python.")
	}

	if job.Spec.Executors < 0 {
		errs = append(errs, field.Invalid(specPath.Child("executors"), job.Spec.Executors,
			"must be >= 0 (use 0 to enable the auto-tuner)"))
	}
	if job.Spec.MinExecutors < 0 {
		errs = append(errs, field.Invalid(specPath.Child("minExecutors"), job.Spec.MinExecutors, "must be >= 0"))
	}
	if job.Spec.MaxExecutors < 0 {
		errs = append(errs, field.Invalid(specPath.Child("maxExecutors"), job.Spec.MaxExecutors, "must be >= 0"))
	}
	if job.Spec.MinExecutors > 0 && job.Spec.MaxExecutors > 0 &&
		job.Spec.MinExecutors > job.Spec.MaxExecutors {
		errs = append(errs, field.Invalid(specPath.Child("minExecutors"), job.Spec.MinExecutors,
			fmt.Sprintf("must be <= spec.maxExecutors (%d)", job.Spec.MaxExecutors)))
	}
	if job.Spec.Executors > 0 && (job.Spec.MinExecutors > 0 || job.Spec.MaxExecutors > 0) {
		warnings = append(warnings,
			"spec.executors is set; spec.minExecutors/maxExecutors are ignored (auto-tuner disabled).")
	}

	if job.Spec.Spot.MaxRetries < 0 {
		errs = append(errs, field.Invalid(specPath.Child("spot", "maxRetries"), job.Spec.Spot.MaxRetries,
			"must be >= 0"))
	}

	if job.Spec.Checkpoint.Enabled && strings.TrimSpace(job.Spec.Checkpoint.S3URI) == "" {
		errs = append(errs, field.Required(specPath.Child("checkpoint", "s3URI"),
			"required when spec.checkpoint.enabled=true"))
	}
	if job.Spec.Checkpoint.S3URI != "" &&
		!strings.HasPrefix(job.Spec.Checkpoint.S3URI, "s3://") &&
		!strings.HasPrefix(job.Spec.Checkpoint.S3URI, "s3a://") {
		errs = append(errs, field.Invalid(specPath.Child("checkpoint", "s3URI"), job.Spec.Checkpoint.S3URI,
			"must start with s3:// or s3a://"))
	}

	if c := job.Spec.ResourceHint.CostPerHourUSD; c != "" {
		if v, err := strconv.ParseFloat(c, 64); err != nil || v < 0 {
			errs = append(errs, field.Invalid(specPath.Child("resourceHint", "costPerHourUSD"), c,
				"must be a non-negative number (e.g. \"0.08\")"))
		}
	}

	if req, ok := job.Spec.ExecutorResources.Requests[corev1.ResourceCPU]; ok {
		if req.MilliValue() <= 0 {
			errs = append(errs, field.Invalid(
				specPath.Child("executorResources", "requests", "cpu"), req.String(),
				"must be > 0 (will be rounded up to whole spark.executor.cores)"))
		}
	}

	for k := range job.Spec.SparkConf {
		if !strings.HasPrefix(k, "spark.") {
			warnings = append(warnings, fmt.Sprintf(
				"spec.sparkConf[%q] does not start with \"spark.\" — Spark will likely ignore it", k))
		}
	}

	if len(errs) > 0 {
		gk := schema.GroupKind{Group: computev1alpha1.GroupVersion.Group, Kind: "SparkJob"}
		return warnings, apierrors.NewInvalid(gk, job.Name, errs)
	}
	return warnings, nil
}
