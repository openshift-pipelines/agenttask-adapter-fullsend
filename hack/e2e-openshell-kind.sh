#!/usr/bin/env bash
# Copyright 2026 The AgentTask Authors
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=openshell-versions.env
source "$ROOT/hack/openshell-versions.env"

CLUSTER_NAME="${CLUSTER_NAME:-agenttask-openshell-package}"
KUBECTL_CONTEXT="kind-$CLUSTER_NAME"
KEEP_CLUSTER="${KEEP_CLUSTER:-false}"
OPENSHELL_NAMESPACE="${OPENSHELL_NAMESPACE:-openshell-system}"
tmp="$(mktemp -d)"
created=false
forward_pid=""
exec_pid=""
sandbox_created=false

cleanup() {
  status=$?
  if [[ -n "$exec_pid" ]]; then
    kill "$exec_pid" >/dev/null 2>&1 || true
    wait "$exec_pid" >/dev/null 2>&1 || true
  fi
  if [[ "$sandbox_created" == true ]]; then
    HOME="$tmp/home" "$tmp/openshell" sandbox delete package-proof >/dev/null 2>&1 || true
  fi
  if [[ -n "$forward_pid" ]]; then
    kill "$forward_pid" >/dev/null 2>&1 || true
    wait "$forward_pid" >/dev/null 2>&1 || true
  fi
  rm -rf "$tmp"
  if [[ "$created" == true && "$KEEP_CLUSTER" != true ]]; then
    kind delete cluster --name "$CLUSTER_NAME" >/dev/null
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

for command in curl docker helm kind kubectl openssl; do
  command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 1; }
done
command -v sha256sum >/dev/null || command -v shasum >/dev/null || {
  echo "sha256sum or shasum is required" >&2
  exit 1
}

sha256() {
  if command -v sha256sum >/dev/null; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

curl_args=(--fail --silent --show-error --location --retry 4 --retry-delay 2 --retry-all-errors --connect-timeout 10 --max-time 120)

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) platform=linux/arm64; cli_target=aarch64-apple-darwin ;;
  Linux-aarch64|Linux-arm64) platform=linux/arm64; cli_target=aarch64-unknown-linux-musl ;;
  Linux-x86_64|Linux-amd64) platform=linux/amd64; cli_target=x86_64-unknown-linux-musl ;;
  *) echo "unsupported host for OpenShell E2E: $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

if kind get clusters | grep -qx "$CLUSTER_NAME"; then
  echo "cluster $CLUSTER_NAME already exists; the E2E only operates on a cluster it creates" >&2
  exit 1
fi
created=true
kind create cluster --name "$CLUSTER_NAME" --wait 120s

images=(
  "registry.k8s.io/agent-sandbox/agent-sandbox-controller:$AGENT_SANDBOX_VERSION@$AGENT_SANDBOX_CONTROLLER_DIGEST"
  "ghcr.io/nvidia/openshell/gateway:$OPENSHELL_VERSION@$OPENSHELL_GATEWAY_DIGEST"
  "ghcr.io/nvidia/openshell/supervisor:$OPENSHELL_VERSION@$OPENSHELL_SUPERVISOR_DIGEST"
  "$OPENSHELL_E2E_SANDBOX_IMAGE"
)
for image in "${images[@]}"; do
  DOCKER_CLI_HINTS=false docker pull --platform "$platform" "$image" >/dev/null
  local_tag="${image%@*}"
  docker tag "$image" "$local_tag"
  kind load docker-image --name "$CLUSTER_NAME" "$local_tag" >/dev/null 2>&1
done

KUBECTL_CONTEXT="$KUBECTL_CONTEXT" \
  OPENSHELL_NAMESPACE="$OPENSHELL_NAMESPACE" \
  OPENSHELL_AUTH_MODE=kind-mtls \
  OPENSHELL_PLATFORM_VALUES_FILE="$ROOT/config/openshell/values-kind.yaml" \
  "$ROOT/hack/install-openshell.sh" >/dev/null

test "$(kubectl --context "$KUBECTL_CONTEXT" -n agent-sandbox-system get deployment agent-sandbox-controller \
  -o jsonpath='{.spec.template.spec.containers[0].image}')" = \
  "registry.k8s.io/agent-sandbox/agent-sandbox-controller:$AGENT_SANDBOX_VERSION@$AGENT_SANDBOX_CONTROLLER_DIGEST"
test "$(kubectl --context "$KUBECTL_CONTEXT" -n "$OPENSHELL_NAMESPACE" get statefulset openshell \
  -o jsonpath='{.spec.template.spec.containers[0].image}')" = \
  "ghcr.io/nvidia/openshell/gateway:$OPENSHELL_VERSION@$OPENSHELL_GATEWAY_DIGEST"
gateway_config="$(kubectl --context "$KUBECTL_CONTEXT" -n "$OPENSHELL_NAMESPACE" \
  get configmap openshell-config -o jsonpath='{.data.gateway\.toml}')"
[[ "$gateway_config" == *'client_ca_path'* ]]
[[ "$gateway_config" != *'disable_tls'* ]]
[[ "$gateway_config" == *'allow_unauthenticated_users = true'* ]]
[[ "$gateway_config" == *"default_image         = \"ghcr.io/nvidia/openshell-community/sandboxes/base@$OPENSHELL_BASE_SANDBOX_DIGEST\""* ]]
[[ "$gateway_config" == *"supervisor_image      = \"ghcr.io/nvidia/openshell/supervisor:$OPENSHELL_VERSION@$OPENSHELL_SUPERVISOR_DIGEST\""* ]]

release="https://github.com/NVIDIA/OpenShell/releases/download/v$OPENSHELL_VERSION"
curl "${curl_args[@]}" "$release/openshell-checksums-sha256.txt" -o "$tmp/checksums"
test "$(sha256 "$tmp/checksums")" = "$OPENSHELL_CLI_CHECKSUMS_SHA256"
archive="openshell-$cli_target.tar.gz"
curl "${curl_args[@]}" "$release/$archive" -o "$tmp/openshell.tgz"
expected_cli_sha256="$(awk -v file="$archive" '$2 == file {print $1}' "$tmp/checksums")"
test -n "$expected_cli_sha256"
test "$(sha256 "$tmp/openshell.tgz")" = "$expected_cli_sha256"
tar -xzf "$tmp/openshell.tgz" -C "$tmp"

client_dir="$tmp/home/.config/openshell/gateways/k8s/mtls"
mkdir -p "$client_dir"
for key in ca.crt tls.crt tls.key; do
  kubectl --context "$KUBECTL_CONTEXT" -n "$OPENSHELL_NAMESPACE" get secret openshell-client-tls \
    -o "jsonpath={.data.${key//./\\.}}" | base64 -d >"$client_dir/$key"
done
chmod 600 "$client_dir/tls.key"

kubectl --context "$KUBECTL_CONTEXT" -n "$OPENSHELL_NAMESPACE" port-forward service/openshell 18443:8080 \
  >"$tmp/port-forward.log" 2>&1 &
forward_pid=$!
for _ in $(seq 1 30); do
  grep -q 'Forwarding from' "$tmp/port-forward.log" && break
  sleep 1
done
grep -q 'Forwarding from' "$tmp/port-forward.log"

HOME="$tmp/home" "$tmp/openshell" gateway add https://127.0.0.1:18443 --local --name k8s >/dev/null
if ! HOME="$tmp/home" "$tmp/openshell" status >/dev/null; then
  echo "OpenShell client could not authenticate to the packaged gateway" >&2
  exit 1
fi
if printf 'GET / HTTP/1.1\r\nHost: localhost\r\n\r\n' | \
  openssl s_client -quiet -connect 127.0.0.1:18443 -servername localhost \
    -CAfile "$client_dir/ca.crt" -verify_return_error >"$tmp/no-client-tls.log" 2>&1; then
  echo "OpenShell gateway accepted a TLS client without a certificate" >&2
  exit 1
fi
openssl s_client -connect 127.0.0.1:18443 -servername localhost \
  -CAfile "$client_dir/ca.crt" -cert "$client_dir/tls.crt" -key "$client_dir/tls.key" \
  -verify_return_error </dev/null >"$tmp/tls-handshake.log" 2>&1 || true
if ! grep -q 'Verify return code: 0 (ok)' "$tmp/tls-handshake.log"; then
  echo "OpenShell server certificate verification did not succeed" >&2
  tail -5 "$tmp/tls-handshake.log" >&2
  exit 1
fi
echo "openshell_mtls_gate=passed"

sandbox_created=true
HOME="$tmp/home" "$tmp/openshell" sandbox create --name package-proof \
  --from "$OPENSHELL_E2E_SANDBOX_IMAGE" \
  --no-tty --no-auto-providers --detach -- sh -c 'while :; do sleep 3600; done' >/dev/null
test "$(kubectl --context "$KUBECTL_CONTEXT" -n "$OPENSHELL_NAMESPACE" get pod/default--package-proof \
  -o jsonpath='{.spec.initContainers[?(@.name=="openshell-supervisor-install")].image}')" = \
  "ghcr.io/nvidia/openshell/supervisor:$OPENSHELL_VERSION@$OPENSHELL_SUPERVISOR_DIGEST"
test "$(kubectl --context "$KUBECTL_CONTEXT" -n "$OPENSHELL_NAMESPACE" get pod/default--package-proof \
  -o jsonpath='{.spec.containers[?(@.name=="agent")].image}')" = "$OPENSHELL_E2E_SANDBOX_IMAGE"
if KUBECTL_CONTEXT="$KUBECTL_CONTEXT" OPENSHELL_NAMESPACE="$OPENSHELL_NAMESPACE" \
  "$ROOT/hack/uninstall-openshell.sh" >"$tmp/active-uninstall.log" 2>&1; then
  echo "OpenShell uninstall succeeded while a sandbox was active" >&2
  exit 1
fi
grep -q 'refusing to remove a gateway' "$tmp/active-uninstall.log"
kubectl --context "$KUBECTL_CONTEXT" -n "$OPENSHELL_NAMESPACE" get statefulset/openshell >/dev/null

for _ in 1 2 3; do
  HOME="$tmp/home" "$tmp/openshell" sandbox exec --name package-proof --no-tty --timeout 10 -- \
    sh -c 'printf openshell-sandbox-ok' >"$tmp/exec.out" 2>"$tmp/exec.err" &
  exec_pid=$!
  for _ in $(seq 1 30); do
    kill -0 "$exec_pid" >/dev/null 2>&1 || break
    sleep 1
  done
  if ! kill -0 "$exec_pid" >/dev/null 2>&1 && wait "$exec_pid" && grep -q openshell-sandbox-ok "$tmp/exec.out"; then
    exec_pid=""
    break
  fi
  kill "$exec_pid" >/dev/null 2>&1 || true
  wait "$exec_pid" >/dev/null 2>&1 || true
  exec_pid=""
done
grep -q openshell-sandbox-ok "$tmp/exec.out"
HOME="$tmp/home" "$tmp/openshell" sandbox delete package-proof >/dev/null

for _ in $(seq 1 60); do
  remaining=false
  for resource in sandbox/default--package-proof pod/default--package-proof pvc/workspace-default--package-proof; do
    kubectl --context "$KUBECTL_CONTEXT" -n "$OPENSHELL_NAMESPACE" get "$resource" >/dev/null 2>&1 && remaining=true
  done
  [[ "$remaining" == true ]] || break
  sleep 1
done
for resource in sandbox/default--package-proof pod/default--package-proof pvc/workspace-default--package-proof; do
  if kubectl --context "$KUBECTL_CONTEXT" -n "$OPENSHELL_NAMESPACE" get "$resource" >/dev/null 2>&1; then
    echo "OpenShell cleanup left $resource behind" >&2
    exit 1
  fi
done
sandbox_created=false

KUBECTL_CONTEXT="$KUBECTL_CONTEXT" OPENSHELL_NAMESPACE="$OPENSHELL_NAMESPACE" \
  "$ROOT/hack/uninstall-openshell.sh" >/dev/null
! helm --kube-context "$KUBECTL_CONTEXT" status openshell -n "$OPENSHELL_NAMESPACE" >/dev/null 2>&1
kubectl --context "$KUBECTL_CONTEXT" -n agent-sandbox-system get deployment/agent-sandbox-controller >/dev/null
for resource in \
  pvc/openshell-data-openshell-0 \
  secret/openshell-client-tls \
  secret/openshell-server-tls \
  secret/openshell-jwt-keys \
  secret/openshell-credential-storage-key-encryption-key; do
  kubectl --context "$KUBECTL_CONTEXT" -n "$OPENSHELL_NAMESPACE" get "$resource" >/dev/null
done
for resource in \
  serviceaccount/openshell-certgen \
  role.rbac.authorization.k8s.io/openshell-certgen \
  rolebinding.rbac.authorization.k8s.io/openshell-certgen; do
  ! kubectl --context "$KUBECTL_CONTEXT" -n "$OPENSHELL_NAMESPACE" get "$resource" >/dev/null 2>&1
done
echo "openshell_package_e2e=passed version=$OPENSHELL_VERSION"
