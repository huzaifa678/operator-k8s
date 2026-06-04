# Image URL to use all building/pushing image targets
IMG ?= controller:latest
# YEAR defines the year value used for substituting the YEAR placeholder in the boilerplate header.
YEAR ?= $(shell date +%Y)

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)ff
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	"$(CONTROLLER_GEN)" rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	"$(CONTROLLER_GEN)" object:headerFile="hack/boilerplate.go.txt",year=$(YEAR) paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet setup-envtest ## Run tests.
	KUBEBUILDER_ASSETS="$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

# TODO(user): To use a different vendor for e2e tests, modify the setup under 'tests/e2e'.
# The default setup assumes Kind is pre-installed and builds/loads the Manager Docker image locally.
# kubectl kuberc is disabled by default for test isolation; enable with:
# - KUBECTL_KUBERC=true
# CertManager is installed by default; skip with:
# - CERT_MANAGER_INSTALL_SKIP=true
KIND_CLUSTER ?= k8s-operator-test-e2e

.PHONY: setup-test-e2e
setup-test-e2e: ## Set up a Kind cluster for e2e tests if it does not exist
	@command -v $(KIND) >/dev/null 2>&1 || { \
		echo "Kind is not installed. Please install Kind manually."; \
		exit 1; \
	}
	@case "$$($(KIND) get clusters)" in \
		*"$(KIND_CLUSTER)"*) \
			echo "Kind cluster '$(KIND_CLUSTER)' already exists. Skipping creation." ;; \
		*) \
			echo "Creating Kind cluster '$(KIND_CLUSTER)'..."; \
			$(KIND) create cluster --name $(KIND_CLUSTER) ;; \
	esac

.PHONY: test-e2e
test-e2e: setup-test-e2e manifests generate fmt vet ## Run the e2e tests. Expected an isolated environment using Kind.
	KIND=$(KIND) KIND_CLUSTER=$(KIND_CLUSTER) go test -tags=e2e ./test/e2e/ -v -ginkgo.v
	$(MAKE) cleanup-test-e2e

.PHONY: cleanup-test-e2e
cleanup-test-e2e: ## Tear down the Kind cluster used for e2e tests
	@$(KIND) delete cluster --name $(KIND_CLUSTER)

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	"$(GOLANGCI_LINT)" run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	"$(GOLANGCI_LINT)" run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	"$(GOLANGCI_LINT)" config verify

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -o bin/manager cmd/main.go

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host (webhooks OFF — no TLS certs locally).
	ENABLE_WEBHOOKS=false go run ./cmd/main.go

.PHONY: gen-webhook-certs
gen-webhook-certs: ## Generate a self-signed cert at $TMPDIR/k8s-webhook-server/serving-certs/ for local webhook dev.
	./scripts/gen-webhook-certs.sh

.PHONY: run-with-webhooks
run-with-webhooks: manifests generate fmt vet gen-webhook-certs ## Run with webhooks ON. Auto-generates self-signed certs first.
	go run ./cmd/main.go

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name k8s-operator-builder
	$(CONTAINER_TOOL) buildx use k8s-operator-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm k8s-operator-builder
	rm Dockerfile.cross

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default > dist/install.yaml

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" apply -f -; else echo "No CRDs to install; skipping."; fi

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -; else echo "No CRDs to delete; skipping."; fi

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -

##@ Observability (Prometheus + Grafana)

PROM_NAMESPACE        ?= monitoring
PROM_RELEASE          ?= kube-prom
KUBE_PROM_VERSION     ?= 65.0.0   # kube-prometheus-stack chart version
CERT_MANAGER_VERSION  ?= v1.16.1
# Whether to install the Prometheus Operator's own admission webhooks
# (validates ServiceMonitor/Prometheus/Alertmanager CRs). ON by default for
# real clusters; the `install-prometheus-local` target flips it OFF for
# k3d/k3s where the bundled kube-webhook-certgen Job can't authenticate.
PROM_ADMISSION_WEBHOOKS ?= true
# When true, the Prometheus Operator's admission webhook gets its TLS cert
# from cert-manager instead of the kube-webhook-certgen Job — the only way
# to keep webhooks ON on k3d. Requires `make install-cert-manager` first.
PROM_USE_CERTMANAGER ?= false

.PHONY: _helm-repo-prometheus
_helm-repo-prometheus:
	@# repo add + update are flaky on slow connections — retry up to 3 times
	@for i in 1 2 3; do \
	  helm repo add prometheus-community https://prometheus-community.github.io/helm-charts && break || \
	    { echo "[helm-repo] repo add failed (attempt $$i/3), retrying in 5s..."; sleep 5; }; \
	done
	@for i in 1 2 3; do \
	  helm repo update prometheus-community && break || \
	    { echo "[helm-repo] repo update failed (attempt $$i/3), retrying in 5s..."; sleep 5; }; \
	done

.PHONY: _helm-repo-jetstack
_helm-repo-jetstack:
	@for i in 1 2 3; do \
	  helm repo add jetstack https://charts.jetstack.io && break || \
	    { echo "[helm-repo] jetstack repo add failed (attempt $$i/3), retrying in 5s..."; sleep 5; }; \
	done
	@for i in 1 2 3; do \
	  helm repo update jetstack && break || \
	    { echo "[helm-repo] jetstack repo update failed (attempt $$i/3), retrying in 5s..."; sleep 5; }; \
	done

.PHONY: install-cert-manager
install-cert-manager: _helm-repo-jetstack ## Install cert-manager via Helm. Required when PROM_USE_CERTMANAGER=true.
	helm upgrade --install --wait --timeout 5m \
		--namespace cert-manager --create-namespace \
		--version $(CERT_MANAGER_VERSION) \
		cert-manager jetstack/cert-manager \
		--set crds.enabled=true
	@echo "[install-cert-manager] cert-manager $(CERT_MANAGER_VERSION) installed:"
	@$(KUBECTL) -n cert-manager get pods

.PHONY: uninstall-cert-manager
uninstall-cert-manager: ## Remove cert-manager.
	helm uninstall -n cert-manager cert-manager || true
	$(KUBECTL) delete ns cert-manager --ignore-not-found

.PHONY: install-prometheus
install-prometheus: _helm-repo-prometheus ## Install kube-prometheus-stack. PROM_ADMISSION_WEBHOOKS=true|false, PROM_USE_CERTMANAGER=true|false.
	@if [ "$(PROM_USE_CERTMANAGER)" = "true" ] && ! $(KUBECTL) get deployment -n cert-manager cert-manager-webhook >/dev/null 2>&1; then \
	  echo "[install-prometheus] PROM_USE_CERTMANAGER=true but cert-manager is not installed."; \
	  echo "                     Run \`make install-cert-manager\` first."; \
	  exit 1; \
	fi
	helm upgrade --install --wait --timeout 10m \
		--namespace $(PROM_NAMESPACE) --create-namespace \
		--version $(KUBE_PROM_VERSION) \
		$(PROM_RELEASE) prometheus-community/kube-prometheus-stack \
		--set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
		--set prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues=false \
		--set prometheusOperator.admissionWebhooks.enabled=$(PROM_ADMISSION_WEBHOOKS) \
		--set prometheusOperator.admissionWebhooks.patch.enabled=$(if $(filter true,$(PROM_USE_CERTMANAGER)),false,$(PROM_ADMISSION_WEBHOOKS)) \
		--set prometheusOperator.admissionWebhooks.certManager.enabled=$(PROM_USE_CERTMANAGER) \
		--set prometheusOperator.tls.enabled=$(PROM_ADMISSION_WEBHOOKS)
	@echo
	@echo "Prometheus + Grafana up."
	@echo "  admission webhooks: $(PROM_ADMISSION_WEBHOOKS)   cert source: $(if $(filter true,$(PROM_USE_CERTMANAGER)),cert-manager,kube-webhook-certgen Job)"
	@echo "  kubectl -n $(PROM_NAMESPACE) get pods"
	@echo "  make prometheus-ui   # http://localhost:9090"
	@echo "  make grafana-ui      # http://localhost:3000  admin / prom-operator"

.PHONY: install-prometheus-local
install-prometheus-local: ## Install kube-prometheus-stack for k3d/k3s/kind — admission webhooks OFF (no cert-manager dependency).
	$(MAKE) install-prometheus PROM_ADMISSION_WEBHOOKS=false

.PHONY: install-prometheus-certmanager
install-prometheus-certmanager: install-cert-manager ## Install kube-prometheus-stack with admission webhooks ON, certs from cert-manager (works on k3d).
	$(MAKE) install-prometheus PROM_ADMISSION_WEBHOOKS=true PROM_USE_CERTMANAGER=true

.PHONY: enable-metrics-monitoring
enable-metrics-monitoring: ## Apply the ServiceMonitor so Prometheus scrapes the controller (requires kube-prometheus-stack).
	$(KUBECTL) apply -k config/prometheus
	@echo
	@echo "ServiceMonitor applied. Verify scrape target:"
	@echo "  kubectl -n $(PROM_NAMESPACE) port-forward svc/$(PROM_RELEASE)-kube-promet-prometheus 9090:9090"
	@echo "  open http://localhost:9090/targets  and look for controller-manager-metrics-monitor"

.PHONY: prometheus-ui
prometheus-ui: ## Port-forward Prometheus to http://localhost:9090.
	@echo "Prometheus at http://localhost:9090 (Ctrl-C to stop)"
	$(KUBECTL) -n $(PROM_NAMESPACE) port-forward svc/$(PROM_RELEASE)-kube-promet-prometheus 9090:9090

.PHONY: grafana-ui
grafana-ui: ## Port-forward Grafana to http://localhost:3000 (admin / prom-operator).
	@echo "Grafana at http://localhost:3000  admin / prom-operator  (Ctrl-C to stop)"
	$(KUBECTL) -n $(PROM_NAMESPACE) port-forward svc/$(PROM_RELEASE)-grafana 3000:80

.PHONY: uninstall-prometheus
uninstall-prometheus: ## Remove the kube-prometheus-stack release.
	helm uninstall -n $(PROM_NAMESPACE) $(PROM_RELEASE) || true
	$(KUBECTL) delete ns $(PROM_NAMESPACE) --ignore-not-found

##@ GitOps (ArgoCD)

ARGOCD_NAMESPACE ?= argocd

.PHONY: install-argocd
install-argocd: ## Install ArgoCD into the cluster and print the admin password + port-forward command.
	./gitops/install-argocd.sh

.PHONY: gitops-bootstrap
gitops-bootstrap: ## Apply the root Application manifests (controller auto-syncs, samples are manual).
	kubectl apply -n $(ARGOCD_NAMESPACE) -f gitops/applications/compute-operator.yaml
	kubectl apply -n $(ARGOCD_NAMESPACE) -f gitops/applications/samples.yaml
	@echo
	@echo "Applications registered. Sync with:"
	@echo "  argocd app sync compute-operator    # the controller"
	@echo "  argocd app sync samples             # the example CRs (manual on purpose)"

.PHONY: gitops-bootstrap-appset
gitops-bootstrap-appset: ## Also install the per-sample ApplicationSet (one App per sample file).
	kubectl apply -n $(ARGOCD_NAMESPACE) -f gitops/applicationset-per-sample.yaml

.PHONY: gitops-sync
gitops-sync: ## Trigger a sync of every compute-operator-owned Application.
	argocd app sync compute-operator
	argocd app sync samples
	@for app in $$(argocd app list -o name | grep '^sample-' || true); do \
	  argocd app sync "$$app"; \
	done

.PHONY: gitops-sync-local
gitops-sync-local: ## Sync the controller from the local working tree (no git push required).
	@# argocd resolves --local from / not CWD; absolute path avoids "no such directory".
	argocd app sync compute-operator --local "$(CURDIR)/config/default"

.PHONY: gitops-sync-samples-local
gitops-sync-samples-local: ## Sync sample CRs from the local working tree (no git push required). Requires argocd login.
	argocd app sync samples --local "$(CURDIR)/config/samples" --force --replace --prune

.PHONY: gitops-sync-samples
gitops-sync-samples: ## Sync only the sample CRs (manual trigger; matches the no-auto-sync policy).
	argocd app sync samples

.PHONY: gitops-status
gitops-status: ## Print sync + health status for every compute-operator Application.
	argocd app list | head -1
	argocd app list | grep -E 'compute-operator|samples|^sample-' || true

##@ GPU stack

GPU_OPERATOR_VERSION   ?= v24.9.1
GPU_OPERATOR_NAMESPACE ?= gpu-operator
# Flip these to false if you ran scripts/install-nvidia-drivers.sh on the host first.
GPU_DRIVER_ENABLED     ?= true
GPU_TOOLKIT_ENABLED    ?= true

.PHONY: install-nvidia-drivers
install-nvidia-drivers: ## (Run on each GPU node) Install host NVIDIA driver + container toolkit.
	@echo "This must run on the GPU node itself, as root."
	@echo "Copy scripts/install-nvidia-drivers.sh to the node and run:"
	@echo "  sudo ./install-nvidia-drivers.sh --reboot"

.PHONY: install-gpu-operator
install-gpu-operator: ## Install the NVIDIA GPU Operator via Helm (requires GPU nodes in the cluster).
	helm repo add nvidia https://helm.ngc.nvidia.com/nvidia || true
	helm repo update nvidia
	helm upgrade --install --wait \
		--namespace $(GPU_OPERATOR_NAMESPACE) --create-namespace \
		--version $(GPU_OPERATOR_VERSION) \
		gpu-operator nvidia/gpu-operator \
		--set toolkit.enabled=$(GPU_TOOLKIT_ENABLED) \
		--set driver.enabled=$(GPU_DRIVER_ENABLED) \
		--set devicePlugin.enabled=true \
		--set nodeStatusExporter.enabled=true
	@echo
	@echo "GPU Operator installed. Verify with:"
	@echo "  kubectl -n $(GPU_OPERATOR_NAMESPACE) get pods"
	@echo "  kubectl get nodes -L nvidia.com/gpu.present -L nvidia.com/gpu.count"
	@echo
	@echo "If you pre-installed drivers with scripts/install-nvidia-drivers.sh, re-run with:"
	@echo "  make install-gpu-operator GPU_DRIVER_ENABLED=false GPU_TOOLKIT_ENABLED=false"

.PHONY: uninstall-gpu-operator
uninstall-gpu-operator: ## Remove the NVIDIA GPU Operator.
	helm uninstall -n $(GPU_OPERATOR_NAMESPACE) gpu-operator || true
	"$(KUBECTL)" delete ns $(GPU_OPERATOR_NAMESPACE) --ignore-not-found

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p "$(LOCALBIN)"

## Tool Binaries
KUBECTL ?= kubectl
KIND ?= kind
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
KUSTOMIZE_VERSION ?= v5.8.1
CONTROLLER_TOOLS_VERSION ?= v0.20.1

#ENVTEST_VERSION is the version of controller-runtime release branch to fetch the envtest setup script (i.e. release-0.20)
ENVTEST_VERSION ?= $(shell v='$(call gomodver,sigs.k8s.io/controller-runtime)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_VERSION manually (controller-runtime replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?([0-9]+)\.([0-9]+).*/release-\1.\2/')

#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell v='$(call gomodver,k8s.io/api)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_K8S_VERSION manually (k8s.io/api replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?[0-9]+\.([0-9]+).*/1.\1/')

GOLANGCI_LINT_VERSION ?= v2.11.4
.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))
	@test -f .custom-gcl.yml && { \
		echo "Building custom golangci-lint with plugins..." && \
		$(GOLANGCI_LINT) custom --destination $(LOCALBIN) --name golangci-lint-custom && \
		mv -f $(LOCALBIN)/golangci-lint-custom $(GOLANGCI_LINT); \
	} || true

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f "$(1)" ;\
GOBIN="$(LOCALBIN)" go install $${package} ;\
mv "$(LOCALBIN)/$$(basename "$(1)")" "$(1)-$(3)" ;\
} ;\
ln -sf "$$(realpath "$(1)-$(3)")" "$(1)"
endef

define gomodver
$(shell go list -m -f '{{if .Replace}}{{.Replace.Version}}{{else}}{{.Version}}{{end}}' $(1) 2>/dev/null)
endef
