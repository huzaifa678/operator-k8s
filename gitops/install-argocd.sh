#!/usr/bin/env bash
# install-argocd.sh — one-shot ArgoCD install for the compute-operator project.
#
# Installs into namespace `argocd`, waits for readiness, prints the initial
# admin password and a port-forward command.

set -euo pipefail

ARGO_VERSION="${ARGO_VERSION:-v3.0.6}"
NAMESPACE="${NAMESPACE:-argocd}"

log() { printf '\033[1;36m[argocd-install]\033[0m %s\n' "$*"; }

log "creating namespace $NAMESPACE"
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

log "installing ArgoCD $ARGO_VERSION"
kubectl apply -n "$NAMESPACE" \
  -f "https://raw.githubusercontent.com/argoproj/argo-cd/$ARGO_VERSION/manifests/install.yaml"

log "waiting for argocd-server to be Ready (up to 5 min)..."
kubectl -n "$NAMESPACE" wait \
  --for=condition=available deployment/argocd-server \
  --timeout=300s

log "fetching initial admin password"
PASSWORD=$(kubectl -n "$NAMESPACE" get secret argocd-initial-admin-secret \
  -o jsonpath='{.data.password}' | base64 -d)

cat <<EOF

==============================================================================
 ArgoCD is up. Next steps:

   1. Port-forward the UI (leave running in a separate terminal):
        kubectl -n $NAMESPACE port-forward svc/argocd-server 8080:443

   2. Open https://localhost:8080  (accept the self-signed cert)

      Username: admin
      Password: $PASSWORD

   3. Or use the CLI (brew install argocd):
        argocd login localhost:8080 --username admin --password '$PASSWORD' --insecure

   4. Bootstrap the compute-operator apps:
        make gitops-bootstrap

==============================================================================
EOF
