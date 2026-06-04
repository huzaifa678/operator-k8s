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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	computev1alpha1 "github.com/huzaifa678/compute-operator/api/v1alpha1"
)

var _ = Describe("SparkJob validating webhook", func() {
	var (
		validator SparkJobCustomValidator
		ctx       = context.Background()
		base      *computev1alpha1.SparkJob
	)

	BeforeEach(func() {
		base = &computev1alpha1.SparkJob{
			Spec: computev1alpha1.SparkJobSpec{
				Image:               "apache/spark:3.5.3",
				Type:                "Scala",
				MainClass:           "org.apache.spark.examples.SparkPi",
				MainApplicationFile: "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar",
				MinExecutors:        1,
				MaxExecutors:        4,
			},
		}
	})

	It("admits a valid SparkJob with no warnings", func() {
		warnings, err := validator.ValidateCreate(ctx, base)
		Expect(err).NotTo(HaveOccurred())
		Expect(warnings).To(BeEmpty())
	})

	It("rejects an empty mainApplicationFile", func() {
		base.Spec.MainApplicationFile = ""
		_, err := validator.ValidateCreate(ctx, base)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("mainApplicationFile"))
	})

	It("rejects negative executor counts", func() {
		base.Spec.Executors = -1
		_, err := validator.ValidateCreate(ctx, base)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("executors"))
	})

	It("rejects min > max executors", func() {
		base.Spec.MinExecutors = 10
		base.Spec.MaxExecutors = 4
		_, err := validator.ValidateCreate(ctx, base)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("minExecutors"))
	})

	It("warns when explicit executors + auto-tune bounds both set", func() {
		base.Spec.Executors = 3
		warnings, err := validator.ValidateCreate(ctx, base)
		Expect(err).NotTo(HaveOccurred())
		Expect(warnings).To(ContainElement(ContainSubstring("auto-tuner disabled")))
	})

	It("warns on Scala type with empty mainClass", func() {
		base.Spec.MainClass = ""
		warnings, err := validator.ValidateCreate(ctx, base)
		Expect(err).NotTo(HaveOccurred())
		Expect(warnings).To(ContainElement(ContainSubstring("mainClass is empty")))
	})

	It("warns on Python type with mainClass set", func() {
		base.Spec.Type = "Python"
		base.Spec.MainApplicationFile = "local:///app/main.py"
		warnings, err := validator.ValidateCreate(ctx, base)
		Expect(err).NotTo(HaveOccurred())
		Expect(warnings).To(ContainElement(ContainSubstring("ignored when spec.type=Python")))
	})

	It("rejects checkpoint enabled without s3URI", func() {
		base.Spec.Checkpoint.Enabled = true
		_, err := validator.ValidateCreate(ctx, base)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("checkpoint.s3URI"))
	})

	It("rejects non-s3 checkpoint URI", func() {
		base.Spec.Checkpoint.S3URI = "http://nope.example.com/ckpt"
		_, err := validator.ValidateCreate(ctx, base)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("s3://"))
	})

	It("rejects non-numeric costPerHourUSD", func() {
		base.Spec.ResourceHint.CostPerHourUSD = "lots"
		_, err := validator.ValidateCreate(ctx, base)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("costPerHourUSD"))
	})

	It("rejects zero CPU request on executor", func() {
		zero := resource.MustParse("0")
		base.Spec.ExecutorResources.Requests = corev1.ResourceList{corev1.ResourceCPU: zero}
		_, err := validator.ValidateCreate(ctx, base)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("cpu"))
	})

	It("warns on sparkConf key without spark. prefix", func() {
		base.Spec.SparkConf = map[string]string{"random.key": "value"}
		warnings, err := validator.ValidateCreate(ctx, base)
		Expect(err).NotTo(HaveOccurred())
		Expect(warnings).To(ContainElement(ContainSubstring("random.key")))
	})

	It("rejects negative spot.maxRetries", func() {
		base.Spec.Spot.MaxRetries = -2
		_, err := validator.ValidateCreate(ctx, base)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("maxRetries"))
	})

	It("ValidateDelete is a no-op", func() {
		warnings, err := validator.ValidateDelete(ctx, base)
		Expect(err).NotTo(HaveOccurred())
		Expect(warnings).To(BeEmpty())
	})

	It("ValidateUpdate applies the same rules", func() {
		base.Spec.MainApplicationFile = ""
		_, err := validator.ValidateUpdate(ctx, base, base)
		Expect(err).To(HaveOccurred())
	})
})
