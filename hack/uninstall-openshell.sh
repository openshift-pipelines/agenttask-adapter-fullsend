#!/usr/bin/env bash
# Copyright 2026 The AgentTask Authors
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

: "${KUBECTL_CONTEXT:?set KUBECTL_CONTEXT to the cluster containing OpenShell}"
OPENSHELL_NAMESPACE="${OPENSHELL_NAMESPACE:-openshell-system}"
OPENSHELL_RELEASE="${OPENSHELL_RELEASE:-openshell}"
OPENSHELL_TIMEOUT="${OPENSHELL_TIMEOUT:-5m}"

[[ "$OPENSHELL_RELEASE" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || {
  echo "invalid OpenShell Helm release name" >&2
  exit 1
}

cleanup_hook_rbac() {
  local namespace_result hook_output resource
  local -a hook_resources=()

  if ! namespace_result="$(kubectl --context "$KUBECTL_CONTEXT" get namespace "$OPENSHELL_NAMESPACE" -o name 2>&1)"; then
    if grep -Eiq '\(NotFound\)|not found' <<<"$namespace_result"; then
      return
    fi
    printf '%s\n' "$namespace_result" >&2
    return 1
  fi
  if ! hook_output="$(kubectl --context "$KUBECTL_CONTEXT" -n "$OPENSHELL_NAMESPACE" get \
    serviceaccount,role.rbac.authorization.k8s.io,rolebinding.rbac.authorization.k8s.io \
    -l "app.kubernetes.io/instance=$OPENSHELL_RELEASE" -o name 2>&1)"; then
    printf '%s\n' "$hook_output" >&2
    return 1
  fi
  while IFS= read -r resource; do
    [[ "$resource" == *-certgen ]] && hook_resources+=("$resource")
  done <<<"$hook_output"
  if ((${#hook_resources[@]})); then
    kubectl --context "$KUBECTL_CONTEXT" -n "$OPENSHELL_NAMESPACE" delete \
      "${hook_resources[@]}" --ignore-not-found >/dev/null
  fi
}

if ! releases="$(helm --kube-context "$KUBECTL_CONTEXT" list --namespace "$OPENSHELL_NAMESPACE" \
  --all --short --filter "^$OPENSHELL_RELEASE$" 2>&1)"; then
  printf '%s\n' "$releases" >&2
  exit 1
fi
if ! grep -qx "$OPENSHELL_RELEASE" <<<"$releases"; then
  cleanup_hook_rbac
  echo "OpenShell release is already absent; certificate-hook RBAC cleanup is complete"
  exit 0
fi

configmap="$(kubectl --context "$KUBECTL_CONTEXT" -n "$OPENSHELL_NAMESPACE" get configmap \
  -l "app.kubernetes.io/instance=$OPENSHELL_RELEASE" -o name | grep -- '-config$')"
[[ "$(wc -w <<<"$configmap" | tr -d ' ')" == 1 ]] || {
  echo "cannot identify exactly one OpenShell gateway configuration" >&2
  exit 1
}
gateway_config="$(kubectl --context "$KUBECTL_CONTEXT" -n "$OPENSHELL_NAMESPACE" get "$configmap" \
  -o jsonpath='{.data.gateway\.toml}')"
gateway_id="$(sed -n 's/^[[:space:]]*gateway_id[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' <<<"$gateway_config" | sort -u)"
[[ "$(wc -w <<<"$gateway_id" | tr -d ' ')" == 1 && "$gateway_id" =~ ^[A-Za-z0-9]([-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?$ ]] || {
  echo "cannot determine a label-safe OpenShell gateway identity" >&2
  exit 1
}

crd_present=false
if ! crd_result="$(kubectl --context "$KUBECTL_CONTEXT" get crd sandboxes.agents.x-k8s.io -o name 2>&1)"; then
  if ! grep -Eiq '\(NotFound\)|not found' <<<"$crd_result"; then
    printf '%s\n' "$crd_result" >&2
    exit 1
  fi
else
  crd_present=true
fi
if [[ "$crd_present" == true ]]; then
  active="$(kubectl --context "$KUBECTL_CONTEXT" get sandboxes.agents.x-k8s.io --all-namespaces \
    -l "openshell.ai/gateway-id=$gateway_id" -o custom-columns='NAMESPACE:.metadata.namespace,NAME:.metadata.name' \
    --no-headers)"
  unknown="$(kubectl --context "$KUBECTL_CONTEXT" get sandboxes.agents.x-k8s.io --all-namespaces \
    -l 'openshell.ai/managed-by=openshell,!openshell.ai/gateway-id' \
    -o custom-columns='NAMESPACE:.metadata.namespace,NAME:.metadata.name' --no-headers)"
  if [[ -n "$active" || -n "$unknown" ]]; then
    echo "refusing to remove a gateway while OpenShell Sandbox resources may depend on it" >&2
    [[ -z "$active" ]] || printf '%s\n' "$active" >&2
    [[ -z "$unknown" ]] || printf '%s\n' "$unknown" >&2
    exit 1
  fi
fi

helm --kube-context "$KUBECTL_CONTEXT" uninstall "$OPENSHELL_RELEASE" \
  --namespace "$OPENSHELL_NAMESPACE" --wait --timeout "$OPENSHELL_TIMEOUT"
cleanup_hook_rbac

echo "OpenShell gateway removed"
echo "Retained shared Agent Sandbox resources and namespaced data, PKI, JWT, and KEK material"
