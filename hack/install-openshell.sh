#!/usr/bin/env bash
# Copyright 2026 The AgentTask Authors
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=openshell-versions.env
source "$ROOT/hack/openshell-versions.env"

: "${KUBECTL_CONTEXT:?set KUBECTL_CONTEXT to the cluster that should receive OpenShell}"
OPENSHELL_NAMESPACE="${OPENSHELL_NAMESPACE:-openshell-system}"
OPENSHELL_RELEASE="${OPENSHELL_RELEASE:-openshell}"
OPENSHELL_VALUES_FILE="${OPENSHELL_VALUES_FILE:-$ROOT/config/openshell/values.yaml}"
OPENSHELL_PLATFORM_VALUES_FILE="${OPENSHELL_PLATFORM_VALUES_FILE:-}"
OPENSHELL_EXTRA_VALUES_FILE="${OPENSHELL_EXTRA_VALUES_FILE:-}"
OPENSHELL_AUTH_MODE="${OPENSHELL_AUTH_MODE:-oidc}"
OPENSHELL_TIMEOUT="${OPENSHELL_TIMEOUT:-5m}"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

for command in curl helm kubectl; do
  command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 1; }
done
command -v sha256sum >/dev/null || command -v shasum >/dev/null || {
  echo "sha256sum or shasum is required" >&2
  exit 1
}
[[ -f "$OPENSHELL_VALUES_FILE" ]] || { echo "values file not found: $OPENSHELL_VALUES_FILE" >&2; exit 1; }
for optional_values in "$OPENSHELL_PLATFORM_VALUES_FILE" "$OPENSHELL_EXTRA_VALUES_FILE"; do
  [[ -z "$optional_values" || -f "$optional_values" ]] || {
    echo "values file not found: $optional_values" >&2
    exit 1
  }
done
kubectl config get-contexts "$KUBECTL_CONTEXT" >/dev/null

sha256() {
  if command -v sha256sum >/dev/null; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

verify_sha256() {
  local file="$1" expected="$2" actual
  actual="$(sha256 "$file")"
  [[ "$actual" == "$expected" ]] || {
    echo "checksum mismatch for $(basename "$file"): got $actual, want $expected" >&2
    exit 1
  }
}

curl_args=(--fail --silent --show-error --location --retry 4 --retry-delay 2 --retry-all-errors --connect-timeout 10 --max-time 120)
agent_sandbox_manifest="$tmp/agent-sandbox.yaml"
agent_sandbox_pinned="$tmp/agent-sandbox-pinned.yaml"
curl "${curl_args[@]}" "https://github.com/kubernetes-sigs/agent-sandbox/releases/download/$AGENT_SANDBOX_VERSION/manifest.yaml" \
  -o "$agent_sandbox_manifest"
verify_sha256 "$agent_sandbox_manifest" "$AGENT_SANDBOX_MANIFEST_SHA256"

controller_tag="registry.k8s.io/agent-sandbox/agent-sandbox-controller:$AGENT_SANDBOX_VERSION"
controller_image="registry.k8s.io/agent-sandbox/agent-sandbox-controller:$AGENT_SANDBOX_VERSION@$AGENT_SANDBOX_CONTROLLER_DIGEST"
[[ "$(grep -Fc "$controller_tag" "$agent_sandbox_manifest")" == 1 ]] || {
  echo "Agent Sandbox manifest does not contain exactly one expected controller image" >&2
  exit 1
}
sed "s|$controller_tag|$controller_image|" "$agent_sandbox_manifest" >"$agent_sandbox_pinned"
(
  cd "$tmp"
  helm pull oci://ghcr.io/nvidia/openshell/helm-chart --version "$OPENSHELL_VERSION" >/dev/null
)
chart="$tmp/helm-chart-$OPENSHELL_VERSION.tgz"
verify_sha256 "$chart" "$OPENSHELL_CHART_SHA256"

values=(--values "$OPENSHELL_VALUES_FILE")
for optional_values in "$OPENSHELL_PLATFORM_VALUES_FILE" "$OPENSHELL_EXTRA_VALUES_FILE"; do
  [[ -z "$optional_values" ]] || values+=(--values "$optional_values")
done
pins=(
  --set-string "image.repository=ghcr.io/nvidia/openshell/gateway"
  --set-string "image.tag=$OPENSHELL_VERSION@$OPENSHELL_GATEWAY_DIGEST"
  --set-string "supervisor.image.repository=ghcr.io/nvidia/openshell/supervisor"
  --set-string "supervisor.image.tag=$OPENSHELL_VERSION@$OPENSHELL_SUPERVISOR_DIGEST"
  --set-string "server.sandboxImage=ghcr.io/nvidia/openshell-community/sandboxes/base@$OPENSHELL_BASE_SANDBOX_DIGEST"
  --set server.disableTls=false
  --set server.telemetryEnabled=false
)
case "$OPENSHELL_AUTH_MODE" in
  oidc)
    auth=(--set server.auth.allowUnauthenticatedUsers=false)
    ;;
  kind-mtls)
    [[ "$KUBECTL_CONTEXT" == kind-* ]] || {
      echo "kind-mtls authentication is allowed only for a Kind context" >&2
      exit 1
    }
    auth=(
      --set server.auth.allowUnauthenticatedUsers=true
      --set-string server.oidc.issuer=
      --set pkiInitJob.enabled=true
      --set certManager.enabled=false
      --set-string server.tls.certSecretName=openshell-server-tls
      --set-string server.tls.clientCaSecretName=openshell-server-client-ca
      --set-string server.tls.clientTlsSecretName=openshell-client-tls
    )
    ;;
  *)
    echo "unsupported OPENSHELL_AUTH_MODE: $OPENSHELL_AUTH_MODE" >&2
    exit 1
    ;;
esac

validate_gateway_config() {
  local config="$1"
  if [[ "$config" != *"cert_path"* || "$config" == *"disable_tls"* ]]; then
    echo "effective OpenShell configuration must keep TLS enabled" >&2
    return 1
  fi
  if [[ "$config" != *"default_image         = \"ghcr.io/nvidia/openshell-community/sandboxes/base@$OPENSHELL_BASE_SANDBOX_DIGEST\""* ||
        "$config" != *"supervisor_image      = \"ghcr.io/nvidia/openshell/supervisor:$OPENSHELL_VERSION@$OPENSHELL_SUPERVISOR_DIGEST\""* ]]; then
    echo "effective OpenShell configuration does not use the packaged sandbox images" >&2
    return 1
  fi
  case "$OPENSHELL_AUTH_MODE" in
    oidc)
      if [[ "$config" == *"allow_unauthenticated_users"* ]] ||
         ! grep -q '^\[openshell.gateway.oidc\]$' <<<"$config" ||
         ! grep -Eq '^issuer[[:space:]]*=[[:space:]]*"https://[^"]+"$' <<<"$config" ||
         ! grep -Eq '^audience[[:space:]]*=[[:space:]]*"[^"]+"$' <<<"$config"; then
        echo "OIDC mode requires an HTTPS issuer, a non-empty audience, and unauthenticated access disabled" >&2
        return 1
      fi
      ;;
    kind-mtls)
      if [[ "$config" != *"client_ca_path"* || "$config" != *"allow_unauthenticated_users = true"* ||
            "$config" == *"[openshell.gateway.oidc]"* ]]; then
        echo "Kind mode requires client-certificate TLS, no OIDC block, and the local developer principal" >&2
        return 1
      fi
      ;;
  esac
}

rendered="$tmp/openshell-rendered.yaml"
helm template "$OPENSHELL_RELEASE" "$chart" --namespace "$OPENSHELL_NAMESPACE" \
  "${values[@]}" "${pins[@]}" "${auth[@]}" \
  --set agentSandbox.preflight.enabled=false >"$rendered"
rendered_config="$(awk '/^  gateway.toml: \|$/ { found=1; next } found && /^---$/ { exit } found { sub(/^    /, ""); print }' "$rendered")"
validate_gateway_config "$rendered_config"

controller_present=false
if ! controller_result="$(kubectl --context "$KUBECTL_CONTEXT" -n agent-sandbox-system get \
  deployment/agent-sandbox-controller -o jsonpath='{.spec.template.spec.containers[0].image}' 2>&1)"; then
  if ! grep -Eiq '\(NotFound\)|not found' <<<"$controller_result"; then
    printf '%s\n' "$controller_result" >&2
    exit 1
  fi
else
  controller_present=true
fi
if [[ "$controller_present" == true && "$controller_result" != "$controller_image" ]]; then
  echo "existing Agent Sandbox controller does not match packaged image: $controller_result" >&2
  exit 1
fi
if [[ "$controller_present" == false ]]; then
  kubectl --context "$KUBECTL_CONTEXT" apply --server-side \
    --field-manager=agenttask-fullsend-packager -f "$agent_sandbox_pinned" >/dev/null
fi
kubectl --context "$KUBECTL_CONTEXT" -n agent-sandbox-system rollout status \
  deployment/agent-sandbox-controller --timeout="$OPENSHELL_TIMEOUT" >/dev/null

helm --kube-context "$KUBECTL_CONTEXT" upgrade --install "$OPENSHELL_RELEASE" "$chart" \
  --namespace "$OPENSHELL_NAMESPACE" --create-namespace --atomic --wait \
  --timeout "$OPENSHELL_TIMEOUT" "${values[@]}" "${pins[@]}" "${auth[@]}" \
  --set agentSandbox.preflight.enabled=true >/dev/null

gateway_workload="$(kubectl --context "$KUBECTL_CONTEXT" -n "$OPENSHELL_NAMESPACE" get statefulset,deployment \
  -l "app.kubernetes.io/instance=$OPENSHELL_RELEASE" -o name)"
[[ "$(wc -w <<<"$gateway_workload" | tr -d ' ')" == 1 ]] || {
  echo "expected exactly one OpenShell gateway workload, got: $gateway_workload" >&2
  exit 1
}
kubectl --context "$KUBECTL_CONTEXT" -n "$OPENSHELL_NAMESPACE" rollout status \
  "$gateway_workload" --timeout="$OPENSHELL_TIMEOUT" >/dev/null
installed_image="$(kubectl --context "$KUBECTL_CONTEXT" -n "$OPENSHELL_NAMESPACE" get "$gateway_workload" \
  -o jsonpath='{.spec.template.spec.containers[0].image}')"
[[ "$installed_image" == "ghcr.io/nvidia/openshell/gateway:$OPENSHELL_VERSION@$OPENSHELL_GATEWAY_DIGEST" ]]
gateway_configmap="$(kubectl --context "$KUBECTL_CONTEXT" -n "$OPENSHELL_NAMESPACE" get configmap \
  -l "app.kubernetes.io/instance=$OPENSHELL_RELEASE" -o name | grep -- '-config$')"
[[ "$(wc -w <<<"$gateway_configmap" | tr -d ' ')" == 1 ]]
installed_config="$(kubectl --context "$KUBECTL_CONTEXT" -n "$OPENSHELL_NAMESPACE" get "$gateway_configmap" \
  -o jsonpath='{.data.gateway\.toml}')"
validate_gateway_config "$installed_config"

echo "openshell_package=ready version=$OPENSHELL_VERSION auth=$OPENSHELL_AUTH_MODE namespace=$OPENSHELL_NAMESPACE release=$OPENSHELL_RELEASE"
