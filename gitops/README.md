# GitOps with ArgoCD

This directory wires the operator into ArgoCD without forcing every change through git push.

## Files

| File | What it is |
|------|-----------|
| `install-argocd.sh` | One-shot ArgoCD install + admin password printer |
| `applications/compute-operator.yaml` | `Application` that deploys CRDs + controller from `config/default`. **Auto-sync ON.** |
| `applications/samples.yaml` | `Application` that deploys all sample CRs from `config/samples`. **Manual sync only.** |
| `applicationset-per-sample.yaml` | `ApplicationSet` that creates one `Application` per file under `config/samples/` — drop a new file, get a new App for free |

## Bootstrap

```bash
# 1. Install ArgoCD itself
./gitops/install-argocd.sh

# 2. (Optional but recommended) install the argocd CLI
brew install argocd

# 3. Apply the root Applications
make gitops-bootstrap

# 4. (Optional) install the ApplicationSet so each sample gets its own App
kubectl apply -f gitops/applicationset-per-sample.yaml
```

`compute-operator` will auto-sync on every commit. Samples wait for you to push the button.

## Editing `Application` files before bootstrap

Open `applications/compute-operator.yaml` and `applications/samples.yaml` and change:

```yaml
repoURL: https://github.com/huzaifa678/compute-operator.git    # <- your fork
```

If you forked under a different name, also update `applicationset-per-sample.yaml`.

## The three operating modes (no git push needed for modes 2 & 3)

### Mode 1 — Git auto-sync (default for `compute-operator`)

You merge a PR that bumps the controller image to `v0.2.0`. Within ~60 s ArgoCD detects the change and rolls out the Deployment. Nothing else to do.

### Mode 2 — Local sync (`--local`)

Pushed nothing yet, want to test? Sync from your working tree:

```bash
# Push the local Kustomize output directly to ArgoCD without committing
argocd app sync compute-operator --local ./config/default

# Same for samples
argocd app sync samples --local ./config/samples
```

ArgoCD diffs your local files against the live cluster state and applies the delta. Next git-driven sync will overwrite anything you pushed this way — useful for "does my change work?" without polluting history.

### Mode 3 — Parameter overrides (`argocd app set`)

Tweak one thing without editing YAML at all:

```bash
# Bump the controller image
argocd app set compute-operator \
  --kustomize-image controller=ghcr.io/me/compute-operator:dev-$(date +%s)
argocd app sync compute-operator

# Disable auto-sync temporarily
argocd app set compute-operator --sync-policy none

# Re-enable
argocd app set compute-operator --sync-policy automated --auto-prune --self-heal
```

These changes persist in ArgoCD's internal store (the `Application` object in the `argocd` namespace), not in git. To make them permanent, mirror them into the YAML in this directory.

## Common workflows

### "I just changed `*_types.go`"

```bash
make manifests generate                 # regenerate CRD YAML + deepcopy
git add config/crd && git commit && git push   # if you want auto-sync to ship it
# — OR —
argocd app sync compute-operator --local ./config/default   # ship now, commit later
```

### "I just changed controller code and built a new image"

```bash
make docker-build docker-push IMG=ghcr.io/me/compute-operator:v0.2.0
argocd app set compute-operator --kustomize-image controller=ghcr.io/me/compute-operator:v0.2.0
argocd app sync compute-operator
```

### "I want to (re)run the SparkPi sample"

```bash
# Either via Makefile:
make gitops-sync-samples

# Or targeting a single resource:
argocd app sync samples --resource '*:SparkJob:sparkpi-sample'
```

### "ArgoCD is showing OutOfSync and I just want to nuke + redeploy"

```bash
argocd app sync compute-operator --force --replace
```

## Differences vs `kubectl apply`

| | `kubectl apply` | ArgoCD `app sync` |
|---|---|---|
| Source of truth | Whatever you `apply` last | Git repo (mode 1), local tree (mode 2), or `Application` params (mode 3) |
| Drift detection | None | Continuous; reverts manual edits if `selfHeal: true` |
| Resource cleanup | Manual | `prune: true` deletes resources removed from the source |
| Multi-cluster | Per-context | One Application targets one `destination.server` |
| History / rollback | None | `argocd app history`, `argocd app rollback <rev>` |

## Tearing it all down

```bash
argocd app delete compute-operator samples
kubectl delete -f gitops/applicationset-per-sample.yaml
helm uninstall argocd -n argocd 2>/dev/null || \
  kubectl delete -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/v3.0.6/manifests/install.yaml
kubectl delete ns argocd
```
