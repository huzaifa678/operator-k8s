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
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	computev1alpha1 "github.com/huzaifa678/compute-operator/api/v1alpha1"
)

var trainingrunlog = logf.Log.WithName("trainingrun-webhook")

func SetupTrainingRunWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &computev1alpha1.TrainingRun{}).
		WithValidator(&TrainingRunCustomValidator{}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-compute-compute-example-com-v1alpha1-trainingrun,mutating=false,failurePolicy=fail,sideEffects=None,groups=compute.compute.example.com,resources=trainingruns,verbs=create;update,versions=v1alpha1,name=vtrainingrun-v1alpha1.kb.io,admissionReviewVersions=v1

type TrainingRunCustomValidator struct{}

func (v *TrainingRunCustomValidator) ValidateCreate(_ context.Context, obj *computev1alpha1.TrainingRun) (admission.Warnings, error) {
	trainingrunlog.V(1).Info("validate create", "name", obj.GetName())
	return validateTrainingRun(obj)
}

func (v *TrainingRunCustomValidator) ValidateUpdate(_ context.Context, _, newObj *computev1alpha1.TrainingRun) (admission.Warnings, error) {
	trainingrunlog.V(1).Info("validate update", "name", newObj.GetName())
	return validateTrainingRun(newObj)
}

func (v *TrainingRunCustomValidator) ValidateDelete(_ context.Context, _ *computev1alpha1.TrainingRun) (admission.Warnings, error) {
	return nil, nil
}

func validateTrainingRun(run *computev1alpha1.TrainingRun) (admission.Warnings, error) {
	var (
		errs     field.ErrorList
		warnings admission.Warnings
		specPath = field.NewPath("spec")
	)

	if strings.TrimSpace(run.Spec.Image) == "" {
		errs = append(errs, field.Required(specPath.Child("image"), "must be set"))
	}

	if run.Spec.WorldSize < 1 {
		errs = append(errs, field.Invalid(specPath.Child("worldSize"), run.Spec.WorldSize,
			"must be >= 1"))
	}

	if run.Spec.DatasetPVC != "" && run.Spec.DatasetS3URI != "" {
		errs = append(errs, field.Forbidden(specPath.Child("datasetS3URI"),
			"set either spec.datasetPVC or spec.datasetS3URI, not both"))
	}
	if run.Spec.DatasetS3URI != "" &&
		!strings.HasPrefix(run.Spec.DatasetS3URI, "s3://") &&
		!strings.HasPrefix(run.Spec.DatasetS3URI, "s3a://") {
		errs = append(errs, field.Invalid(specPath.Child("datasetS3URI"), run.Spec.DatasetS3URI,
			"must start with s3:// or s3a://"))
	}

	if run.Spec.Script != "" && len(run.Spec.Command) > 0 {
		warnings = append(warnings,
			"spec.script is set AND spec.command is overridden. The default torchrun "+
				"command (which runs /scripts/train.py) is bypassed — make sure your custom "+
				"command invokes the mounted script if you still want it to run.")
	}
	if run.Spec.Script != "" && run.Spec.BuiltinTrainer != "" {
		warnings = append(warnings,
			"spec.script and spec.builtinTrainer are both set — spec.script wins; "+
				"the builtin is ignored. Drop one of them.")
	}
	if run.Spec.BuiltinTrainer != "" {
		switch run.Spec.BuiltinTrainer {
		case "BERTClassifier":
		default:
			errs = append(errs, field.NotSupported(specPath.Child("builtinTrainer"),
				run.Spec.BuiltinTrainer, []string{"BERTClassifier"}))
		}
	}
	if len(run.Spec.Packages) > 0 && len(run.Spec.Command) > 0 {
		warnings = append(warnings,
			"spec.packages relies on the default sh -c command to run `pip install` before "+
				"torchrun. With spec.command overridden, pip install does NOT happen — bake "+
				"packages into your image instead, or invoke pip yourself.")
	}

	if run.Spec.GPU.Enabled {
		if run.Spec.GPU.PerWorker < 1 {
			errs = append(errs, field.Invalid(specPath.Child("gpu", "perWorker"), run.Spec.GPU.PerWorker,
				"must be >= 1 when spec.gpu.enabled=true"))
		}
		if run.Spec.GPU.Backend == "gloo" {
			warnings = append(warnings,
				"spec.gpu.backend=gloo with GPUs enabled — gloo is the CPU backend; "+
					"all-reduce will run over TCP and waste your GPU interconnect. "+
					"Set backend to \"nccl\" for GPU clusters.")
		}
	} else {
		if run.Spec.GPU.Backend == "nccl" {
			warnings = append(warnings,
				"spec.gpu.backend=nccl but spec.gpu.enabled=false — NCCL requires CUDA. "+
					"PyTorch will fail at dist.init_process_group(). Set backend to \"gloo\" "+
					"or enable GPUs.")
		}
		if run.Spec.GPU.PerWorker > 1 {
			warnings = append(warnings,
				"spec.gpu.perWorker is set but spec.gpu.enabled=false — value is ignored.")
		}
	}

	if run.Spec.Spot.MaxRetries < 0 {
		errs = append(errs, field.Invalid(specPath.Child("spot", "maxRetries"), run.Spec.Spot.MaxRetries,
			"must be >= 0"))
	}

	if run.Spec.Checkpoint.Enabled && strings.TrimSpace(run.Spec.Checkpoint.S3URI) == "" {
		errs = append(errs, field.Required(specPath.Child("checkpoint", "s3URI"),
			"required when spec.checkpoint.enabled=true"))
	}
	if run.Spec.Checkpoint.S3URI != "" &&
		!strings.HasPrefix(run.Spec.Checkpoint.S3URI, "s3://") &&
		!strings.HasPrefix(run.Spec.Checkpoint.S3URI, "s3a://") {
		errs = append(errs, field.Invalid(specPath.Child("checkpoint", "s3URI"), run.Spec.Checkpoint.S3URI,
			"must start with s3:// or s3a://"))
	}

	if c := run.Spec.ResourceHint.CostPerHourUSD; c != "" {
		if v, err := strconv.ParseFloat(c, 64); err != nil || v < 0 {
			errs = append(errs, field.Invalid(specPath.Child("resourceHint", "costPerHourUSD"), c,
				"must be a non-negative number"))
		}
	}

	if len(errs) > 0 {
		gk := schema.GroupKind{Group: computev1alpha1.GroupVersion.Group, Kind: "TrainingRun"}
		return warnings, apierrors.NewInvalid(gk, run.Name, errs)
	}
	return warnings, nil
}
