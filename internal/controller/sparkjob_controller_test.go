/*
Copyright 2026 huzaifa678.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	computev1alpha1 "github.com/huzaifa678/compute-operator/api/v1alpha1"
)

var _ = Describe("SparkJob Controller", func() {
	Context("When reconciling a SparkJob", func() {
		const resourceName = "test-sparkjob"
		ctx := context.Background()
		nsn := types.NamespacedName{Name: resourceName, Namespace: "default"}

		BeforeEach(func() {
			By("creating the SparkJob CR")
			existing := &computev1alpha1.SparkJob{}
			if err := k8sClient.Get(ctx, nsn, existing); err != nil && errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, &computev1alpha1.SparkJob{
					ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: "default"},
					Spec: computev1alpha1.SparkJobSpec{
						Image:               "apache/spark:3.5.3",
						Type:                "Scala",
						MainClass:           "org.apache.spark.examples.SparkPi",
						MainApplicationFile: "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar",
						MinExecutors:        1,
						MaxExecutors:        4,
						ResourceHint:        computev1alpha1.ResourceHint{InputSizeMB: 512, CostPerHourUSD: "0.08"},
					},
				})).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &computev1alpha1.SparkJob{}
			Expect(k8sClient.Get(ctx, nsn, resource)).To(Succeed())
			By("cleaning up the SparkJob")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("ensures driver RBAC, headless Service, and a spark-submit driver pod", func() {
			r := &SparkJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nsn})
			Expect(err).NotTo(HaveOccurred())

			By("creating the driver ServiceAccount")
			sa := &corev1.ServiceAccount{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-driver-sa", Namespace: "default",
			}, sa)).To(Succeed())

			By("creating the driver Role + RoleBinding")
			role := &rbacv1.Role{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-driver-role", Namespace: "default",
			}, role)).To(Succeed())
			Expect(role.Rules).NotTo(BeEmpty())

			rb := &rbacv1.RoleBinding{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-driver-role-binding", Namespace: "default",
			}, rb)).To(Succeed())
			Expect(rb.RoleRef.Name).To(Equal(resourceName + "-driver-role"))

			By("creating the headless driver Service")
			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-driver-svc", Namespace: "default",
			}, svc)).To(Succeed())
			Expect(svc.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))

			By("creating the driver pod running spark-submit")
			pod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-driver", Namespace: "default",
			}, pod)).To(Succeed())
			Expect(pod.Spec.ServiceAccountName).To(Equal(resourceName + "-driver-sa"))
			Expect(pod.Spec.Containers).To(HaveLen(1))
			cmd := pod.Spec.Containers[0].Args[0]
			Expect(cmd).To(ContainSubstring("/opt/spark/bin/spark-submit"))
			Expect(cmd).To(ContainSubstring("--deploy-mode client"))
			Expect(cmd).To(ContainSubstring("spark.executor.instances=2")) // 512MB/256 = 2 executors auto-tuned
			Expect(cmd).To(ContainSubstring("org.apache.spark.examples.SparkPi"))
		})
	})

	Context("desiredExecutorCount unit", func() {
		It("returns explicit Executors when set", func() {
			job := &computev1alpha1.SparkJob{Spec: computev1alpha1.SparkJobSpec{Executors: 5}}
			Expect(desiredExecutorCount(job)).To(Equal(int32(5)))
		})
		It("auto-tunes from InputSizeMB clamped to [min,max]", func() {
			job := &computev1alpha1.SparkJob{Spec: computev1alpha1.SparkJobSpec{
				MinExecutors: 1, MaxExecutors: 4,
				ResourceHint: computev1alpha1.ResourceHint{InputSizeMB: 2048}, // -> 8, clamped to 4
			}}
			Expect(desiredExecutorCount(job)).To(Equal(int32(4)))
		})
		It("falls back to MinExecutors when no hint", func() {
			job := &computev1alpha1.SparkJob{Spec: computev1alpha1.SparkJobSpec{MinExecutors: 3}}
			Expect(desiredExecutorCount(job)).To(Equal(int32(3)))
		})
		It("defaults to 1 when nothing set", func() {
			job := &computev1alpha1.SparkJob{}
			Expect(desiredExecutorCount(job)).To(Equal(int32(1)))
		})
	})
})
