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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	computev1alpha1 "github.com/huzaifa678/compute-operator/api/v1alpha1"
)

var _ = Describe("TrainingRun validating webhook", func() {
	var (
		validator TrainingRunCustomValidator
		ctx       = context.Background()
		base      *computev1alpha1.TrainingRun
	)

	BeforeEach(func() {
		base = &computev1alpha1.TrainingRun{
			Spec: computev1alpha1.TrainingRunSpec{
				Framework: "PyTorch",
				Image:     "pytorch/pytorch:2.5.1-cuda12.4-cudnn9-runtime",
				WorldSize: 2,
				Script:    "print('hi')",
			},
		}
	})

	It("admits a valid TrainingRun", func() {
		warnings, err := validator.ValidateCreate(ctx, base)
		Expect(err).NotTo(HaveOccurred())
		Expect(warnings).To(BeEmpty())
	})

	It("rejects empty image", func() {
		base.Spec.Image = ""
		_, err := validator.ValidateCreate(ctx, base)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("image"))
	})

	It("rejects worldSize < 1", func() {
		base.Spec.WorldSize = 0
		_, err := validator.ValidateCreate(ctx, base)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("worldSize"))
	})

	It("rejects both datasetPVC and datasetS3URI", func() {
		base.Spec.DatasetPVC = "my-pvc"
		base.Spec.DatasetS3URI = "s3://bucket/data"
		_, err := validator.ValidateCreate(ctx, base)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("datasetS3URI"))
	})

	It("rejects non-s3 datasetS3URI", func() {
		base.Spec.DatasetS3URI = "http://example.com/data"
		_, err := validator.ValidateCreate(ctx, base)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("s3://"))
	})

	It("warns when script + custom command coexist", func() {
		base.Spec.Command = []string{"python", "/somewhere/else.py"}
		warnings, err := validator.ValidateCreate(ctx, base)
		Expect(err).NotTo(HaveOccurred())
		Expect(warnings).To(ContainElement(ContainSubstring("script is set AND spec.command is overridden")))
	})

	It("warns when packages + custom command coexist", func() {
		base.Spec.Script = ""
		base.Spec.Command = []string{"python", "/app/train.py"}
		base.Spec.Packages = []string{"transformers"}
		warnings, err := validator.ValidateCreate(ctx, base)
		Expect(err).NotTo(HaveOccurred())
		Expect(warnings).To(ContainElement(ContainSubstring("pip install does NOT happen")))
	})

	It("rejects gpu.enabled=true with perWorker=0", func() {
		base.Spec.GPU = computev1alpha1.GPUSpec{Enabled: true, PerWorker: 0}
		_, err := validator.ValidateCreate(ctx, base)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("perWorker"))
	})

	It("warns when gpu.enabled=false but backend=nccl", func() {
		base.Spec.GPU = computev1alpha1.GPUSpec{Enabled: false, Backend: "nccl"}
		warnings, err := validator.ValidateCreate(ctx, base)
		Expect(err).NotTo(HaveOccurred())
		Expect(warnings).To(ContainElement(ContainSubstring("NCCL requires CUDA")))
	})

	It("warns when gpu.enabled=true but backend=gloo", func() {
		base.Spec.GPU = computev1alpha1.GPUSpec{Enabled: true, PerWorker: 1, Backend: "gloo"}
		warnings, err := validator.ValidateCreate(ctx, base)
		Expect(err).NotTo(HaveOccurred())
		Expect(warnings).To(ContainElement(ContainSubstring("gloo is the CPU backend")))
	})

	It("rejects checkpoint enabled without s3URI", func() {
		base.Spec.Checkpoint.Enabled = true
		_, err := validator.ValidateCreate(ctx, base)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("checkpoint.s3URI"))
	})

	It("rejects non-numeric costPerHourUSD", func() {
		base.Spec.ResourceHint.CostPerHourUSD = "free"
		_, err := validator.ValidateCreate(ctx, base)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("costPerHourUSD"))
	})

	It("ValidateDelete is a no-op", func() {
		warnings, err := validator.ValidateDelete(ctx, base)
		Expect(err).NotTo(HaveOccurred())
		Expect(warnings).To(BeEmpty())
	})
})
