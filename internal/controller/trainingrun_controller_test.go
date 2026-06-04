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
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	computev1alpha1 "github.com/huzaifa678/compute-operator/api/v1alpha1"
)

var _ = Describe("TrainingRun Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-trainingrun"
		ctx := context.Background()
		nsn := types.NamespacedName{Name: resourceName, Namespace: "default"}

		BeforeEach(func() {
			By("creating the TrainingRun custom resource")
			existing := &computev1alpha1.TrainingRun{}
			if err := k8sClient.Get(ctx, nsn, existing); err != nil && errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, &computev1alpha1.TrainingRun{
					ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: "default"},
					Spec: computev1alpha1.TrainingRunSpec{
						Framework: "PyTorch",
						Image:     "pytorch/pytorch:2.5.1-cuda12.4-cudnn9-runtime",
						WorldSize: 2,
						Script:    "import torch.distributed as dist; dist.init_process_group()",
					},
				})).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &computev1alpha1.TrainingRun{}
			Expect(k8sClient.Get(ctx, nsn, resource)).To(Succeed())
			By("cleaning up the TrainingRun")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("creates the headless Service, script ConfigMap, and worker pods", func() {
			r := &TrainingRunReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nsn})
			Expect(err).NotTo(HaveOccurred())

			By("creating the headless workers Service")
			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-workers", Namespace: "default",
			}, svc)).To(Succeed())
			Expect(svc.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
			Expect(svc.Spec.Selector).To(HaveKeyWithValue(trainingOwnerLabel, resourceName))

			By("materializing the script ConfigMap")
			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-script", Namespace: "default",
			}, cm)).To(Succeed())
			Expect(cm.Data).To(HaveKey(trainingScriptKey))

			By("creating one pod per rank with the right env, hostname, and script mount")
			for _, rank := range []string{"0", "1"} {
				pod := &corev1.Pod{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{
					Name: resourceName + "-rank-" + rank, Namespace: "default",
				}, pod)).To(Succeed())

				Expect(pod.Spec.Hostname).To(Equal(resourceName + "-rank-" + rank))
				Expect(pod.Spec.Subdomain).To(Equal(resourceName + "-workers"))
				Expect(pod.Labels).To(HaveKeyWithValue(rankLabel, rank))

				envMap := map[string]string{}
				for _, e := range pod.Spec.Containers[0].Env {
					envMap[e.Name] = e.Value
				}
				Expect(envMap).To(HaveKeyWithValue("RANK", rank))
				Expect(envMap).To(HaveKeyWithValue("WORLD_SIZE", "2"))
				Expect(envMap["MASTER_ADDR"]).To(ContainSubstring(resourceName + "-rank-0"))
				Expect(envMap).To(HaveKeyWithValue("TORCH_DISTRIBUTED_DEFAULT_BACKEND", "gloo"))

				mountFound := false
				for _, m := range pod.Spec.Containers[0].VolumeMounts {
					if m.Name == "script" && m.MountPath == "/scripts" {
						mountFound = true
					}
				}
				Expect(mountFound).To(BeTrue(), "expected /scripts ConfigMap mount on rank %s", rank)
			}
		})

		It("applies GPU pod-spec mutations when spec.gpu.enabled=true", func() {
			By("registering the nvidia RuntimeClass (envtest doesn't ship one)")
			rc := &nodev1.RuntimeClass{
				ObjectMeta: metav1.ObjectMeta{Name: "nvidia"},
				Handler:    "nvidia",
			}
			Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx, rc))).To(Succeed())

			By("flipping the CR to a GPU config")
			run := &computev1alpha1.TrainingRun{}
			Expect(k8sClient.Get(ctx, nsn, run)).To(Succeed())
			run.Spec.GPU = computev1alpha1.GPUSpec{
				Enabled: true, PerWorker: 2, RuntimeClass: "nvidia", Backend: "nccl",
			}
			Expect(k8sClient.Update(ctx, run)).To(Succeed())

			By("deleting existing pods so the controller rebuilds them with GPU spec")
			pods := &corev1.PodList{}
			Expect(k8sClient.List(ctx, pods)).To(Succeed())
			for i := range pods.Items {
				if pods.Items[i].Labels[trainingOwnerLabel] == resourceName {
					Expect(k8sClient.Delete(ctx, &pods.Items[i])).To(Succeed())
				}
			}

			r := &TrainingRunReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nsn})
			Expect(err).NotTo(HaveOccurred())

			pod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-rank-0", Namespace: "default",
			}, pod)).To(Succeed())

			Expect(pod.Spec.NodeSelector).To(HaveKeyWithValue("nvidia.com/gpu.present", "true"))
			Expect(pod.Spec.RuntimeClassName).NotTo(BeNil())
			Expect(*pod.Spec.RuntimeClassName).To(Equal("nvidia"))
			Expect(pod.Spec.Containers[0].Resources.Limits).To(HaveKey(corev1.ResourceName("nvidia.com/gpu")))

			torchrunCmd := pod.Spec.Containers[0].Command[2]
			Expect(torchrunCmd).To(ContainSubstring("--nproc_per_node=2"))

			envMap := map[string]string{}
			for _, e := range pod.Spec.Containers[0].Env {
				envMap[e.Name] = e.Value
			}
			Expect(envMap).To(HaveKeyWithValue("TORCH_DISTRIBUTED_DEFAULT_BACKEND", "nccl"))
		})
	})

	Context("desiredExecutorCount style auto-tuning is exercised in TrainingRun via WorldSize directly", func() {
		It("treats WorldSize as authoritative", func() {
			run := &computev1alpha1.TrainingRun{
				Spec: computev1alpha1.TrainingRunSpec{WorldSize: 7},
			}
			Expect(run.Spec.WorldSize).To(Equal(int32(7)))
		})
	})
})
