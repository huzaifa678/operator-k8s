#!/usr/bin/env bash
# install-nvidia-drivers.sh
#
# Host-level prerequisite for the NVIDIA GPU Operator. Run this ONCE on every
# GPU node *before* `make install-gpu-operator`.
#
# Installs:
#   1. NVIDIA proprietary driver           (kernel module, libcuda, nvidia-smi)
#   2. NVIDIA Container Toolkit             (lets containerd/docker expose GPUs)
#   3. Configures the container runtime    (adds the `nvidia` runtime)
#
# Why not let the GPU Operator install drivers?
#   The Operator's driver DaemonSet only works on a narrow set of base OSes
#   (Ubuntu 20.04/22.04/24.04, RHEL 8/9). On everything else — and on cloud
#   VMs that come with pre-built kernels — host installation is more reliable.
#   When you pre-install, run:
#       helm install ... --set driver.enabled=false
#
# Tested on: Ubuntu 22.04 / 24.04, Debian 12, RHEL 9, Rocky 9, Amazon Linux 2023.
#
# Usage:
#   sudo ./scripts/install-nvidia-drivers.sh [--driver-branch 550] [--reboot]
#
# Flags:
#   --driver-branch N   Pin to a specific driver branch (default: latest stable)
#   --reboot            Reboot at the end (driver kmod needs a fresh boot)
#   --skip-toolkit      Install driver only, skip container toolkit
#   --skip-driver       Install container toolkit only (driver already present)

set -euo pipefail

DRIVER_BRANCH=""
DO_REBOOT=0
SKIP_TOOLKIT=0
SKIP_DRIVER=0

while [[ $# -gt 0 ]]; do
  case $1 in
    --driver-branch) DRIVER_BRANCH=$2; shift 2 ;;
    --reboot)        DO_REBOOT=1; shift ;;
    --skip-toolkit)  SKIP_TOOLKIT=1; shift ;;
    --skip-driver)   SKIP_DRIVER=1; shift ;;
    -h|--help)       sed -n '2,30p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 1 ;;
  esac
done

log() { printf '\033[1;32m[nvidia-setup]\033[0m %s\n' "$*"; }
err() { printf '\033[1;31m[nvidia-setup]\033[0m %s\n' "$*" >&2; }

if [[ $EUID -ne 0 ]]; then
  err "must run as root (try: sudo $0 $*)"
  exit 1
fi


if [[ ! -f /etc/os-release ]]; then
  err "/etc/os-release missing; cannot detect distro"
  exit 1
fi
# shellcheck disable=SC1091
. /etc/os-release
DISTRO_ID=${ID,,}
DISTRO_LIKE=${ID_LIKE:-}
VERSION_ID_MAJOR=${VERSION_ID%%.*}

case "$DISTRO_ID:$DISTRO_LIKE" in
  ubuntu:*|debian:*|*:debian|*:ubuntu)  PKG=apt ;;
  rhel:*|rocky:*|almalinux:*|centos:*|*:rhel|*:fedora|amzn:*) PKG=dnf ;;
  *)
    err "unsupported distro: $DISTRO_ID (ID_LIKE=$DISTRO_LIKE)"
    exit 1
    ;;
esac
log "detected distro=$DISTRO_ID version=$VERSION_ID pkg=$PKG"


if ! lspci 2>/dev/null | grep -qiE 'nvidia|3d controller'; then
  err "no NVIDIA GPU detected via lspci. Aborting."
  err "Re-run with --skip-driver if you only want the container toolkit."
  if [[ $SKIP_DRIVER -ne 1 ]]; then exit 1; fi
fi


if [[ $SKIP_DRIVER -eq 0 ]] && lsmod | grep -q '^nouveau'; then
  log "blacklisting nouveau (will take effect after reboot)"
  cat >/etc/modprobe.d/blacklist-nouveau.conf <<EOF
blacklist nouveau
options nouveau modeset=0
EOF
  if command -v update-initramfs >/dev/null; then
    update-initramfs -u
  elif command -v dracut >/dev/null; then
    dracut --force
  fi
  DO_REBOOT=1
fi


install_driver_apt() {
  log "installing NVIDIA driver via apt"
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y -qq software-properties-common ca-certificates curl gnupg
  # Ubuntu's ubuntu-drivers ships a server-class meta-package per branch.
  apt-get install -y -qq ubuntu-drivers-common || true
  if [[ -n "$DRIVER_BRANCH" ]]; then
    apt-get install -y -qq "nvidia-driver-${DRIVER_BRANCH}-server" || \
      apt-get install -y -qq "nvidia-driver-${DRIVER_BRANCH}"
  else
    if command -v ubuntu-drivers >/dev/null; then
      ubuntu-drivers install --gpgpu
    else
      # Debian fallback: pull from contrib/non-free
      add-apt-repository -y contrib non-free non-free-firmware 2>/dev/null || true
      apt-get update -qq
      apt-get install -y -qq nvidia-driver firmware-misc-nonfree
    fi
  fi
}

install_driver_dnf() {
  log "installing NVIDIA driver via dnf"
  dnf install -y -q dnf-plugins-core kernel-devel-"$(uname -r)" kernel-headers-"$(uname -r)" || true
  # CUDA repo carries the matching driver RPMs and is the supported path.
  local repo_distro
  case "$DISTRO_ID" in
    rhel|rocky|almalinux|centos) repo_distro="rhel${VERSION_ID_MAJOR}" ;;
    amzn)                        repo_distro="amzn2023" ;;
    *)                           repo_distro="rhel${VERSION_ID_MAJOR}" ;;
  esac
  dnf config-manager --add-repo \
    "https://developer.download.nvidia.com/compute/cuda/repos/${repo_distro}/$(uname -m)/cuda-${repo_distro}.repo"
  dnf clean expire-cache -q
  if [[ -n "$DRIVER_BRANCH" ]]; then
    dnf module install -y -q "nvidia-driver:${DRIVER_BRANCH}-dkms"
  else
    dnf module install -y -q nvidia-driver:latest-dkms
  fi
  DO_REBOOT=1
}

if [[ $SKIP_DRIVER -eq 0 ]]; then
  if command -v nvidia-smi >/dev/null && nvidia-smi >/dev/null 2>&1; then
    log "driver already installed ($(nvidia-smi --query-gpu=driver_version --format=csv,noheader | head -1)); skipping"
  else
    case $PKG in
      apt) install_driver_apt ;;
      dnf) install_driver_dnf ;;
    esac
  fi
fi

install_toolkit_apt() {
  log "installing NVIDIA Container Toolkit via apt"
  curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey \
    | gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
  curl -fsSL https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list \
    | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' \
    > /etc/apt/sources.list.d/nvidia-container-toolkit.list
  apt-get update -qq
  apt-get install -y -qq nvidia-container-toolkit
}

install_toolkit_dnf() {
  log "installing NVIDIA Container Toolkit via dnf"
  curl -fsSL https://nvidia.github.io/libnvidia-container/stable/rpm/nvidia-container-toolkit.repo \
    -o /etc/yum.repos.d/nvidia-container-toolkit.repo
  dnf install -y -q nvidia-container-toolkit
}

configure_runtime() {
  log "configuring container runtime"
  if systemctl list-unit-files | grep -q '^containerd\.service'; then
    nvidia-ctk runtime configure --runtime=containerd --set-as-default
    systemctl restart containerd
    log "containerd configured with nvidia as default runtime"
  elif systemctl list-unit-files | grep -q '^docker\.service'; then
    nvidia-ctk runtime configure --runtime=docker --set-as-default
    systemctl restart docker
    log "docker configured with nvidia as default runtime"
  else
    err "no containerd or docker service found — configure your runtime manually:"
    err "  nvidia-ctk runtime configure --runtime=<your-runtime>"
  fi
}

if [[ $SKIP_TOOLKIT -eq 0 ]]; then
  case $PKG in
    apt) install_toolkit_apt ;;
    dnf) install_toolkit_dnf ;;
  esac
  configure_runtime
fi

log "verification"
if command -v nvidia-smi >/dev/null && nvidia-smi >/dev/null 2>&1; then
  nvidia-smi --query-gpu=index,name,driver_version,memory.total --format=csv
else
  err "nvidia-smi not responsive yet — likely needs a reboot to load the kmod."
  DO_REBOOT=1
fi
if command -v nvidia-ctk >/dev/null; then
  nvidia-ctk --version
fi


cat <<EOF

==============================================================================
 Host setup complete. Next steps:

   1. Reboot this node if you haven't already (driver kmod loads on boot):
        sudo reboot

   2. After reboot, verify the runtime can see GPUs:
        sudo nvidia-ctk cdi list      # or
        sudo ctr run --rm --gpus 0 -t docker.io/nvidia/cuda:12.4.1-base-ubuntu22.04 \\
             smoke nvidia-smi

   3. On a control-plane node, install the GPU Operator with the *driver*
      component disabled (since we just installed it on the host):
        helm upgrade --install gpu-operator nvidia/gpu-operator \\
          --namespace gpu-operator --create-namespace \\
          --set driver.enabled=false \\
          --set toolkit.enabled=false

      Or use the project's Makefile target (uncomment the flags first if needed):
        make install-gpu-operator

   4. Confirm K8s sees the GPUs:
        kubectl get nodes -L nvidia.com/gpu.present -L nvidia.com/gpu.count

   5. Apply the GPU sample:
        kubectl apply -f config/samples/compute_v1alpha1_trainingrun_gpu.yaml
==============================================================================
EOF

if [[ $DO_REBOOT -eq 1 ]]; then
  log "rebooting in 5s (Ctrl-C to cancel)..."
  sleep 5
  systemctl reboot
fi
