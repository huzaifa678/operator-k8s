#!/usr/bin/env bash
# gen-webhook-certs.sh — generate a self-signed cert for local webhook dev.
#
# controller-runtime expects webhook serving certs at:
#   ${TMPDIR}/k8s-webhook-server/serving-certs/{tls.crt,tls.key}
# (default --webhook-cert-path). This script creates that directory and a
# self-signed cert valid for localhost / 127.0.0.1, so `make run-with-webhooks`
# can boot the server without cert-manager.
#
# IMPORTANT: the cluster's ValidatingWebhookConfiguration still points at the
# in-cluster Service — it cannot reach a webhook running on your laptop. This
# script is for *server boot* testing only. For end-to-end webhook enforcement
# install cert-manager + `make deploy IMG=...` instead.

set -euo pipefail

CERT_DIR="${TMPDIR:-/tmp}/k8s-webhook-server/serving-certs"
DAYS="${DAYS:-365}"

mkdir -p "$CERT_DIR"

if [[ -f "$CERT_DIR/tls.crt" && -f "$CERT_DIR/tls.key" ]]; then
  if openssl x509 -in "$CERT_DIR/tls.crt" -checkend 86400 -noout >/dev/null 2>&1; then
    echo "[gen-webhook-certs] reusing existing cert at $CERT_DIR (still valid >24h)"
    exit 0
  fi
  echo "[gen-webhook-certs] existing cert expires soon; regenerating"
fi

openssl req \
  -x509 -newkey rsa:2048 -nodes -days "$DAYS" \
  -keyout "$CERT_DIR/tls.key" \
  -out    "$CERT_DIR/tls.crt" \
  -subj "/CN=localhost/O=compute-operator-dev" \
  -addext "subjectAltName = DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1" \
  >/dev/null 2>&1

chmod 600 "$CERT_DIR/tls.key"

echo "[gen-webhook-certs] wrote self-signed cert to $CERT_DIR"
openssl x509 -in "$CERT_DIR/tls.crt" -noout -subject -dates
