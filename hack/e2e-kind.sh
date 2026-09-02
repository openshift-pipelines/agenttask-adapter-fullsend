#!/usr/bin/env bash
# Copyright 2026 The AgentTask Authors
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CORE_DIR="${CORE_DIR:-$(cd "$ROOT/../agenttask" && pwd)}"
CLUSTER_NAME="${CLUSTER_NAME:-agenttask-fullsend-poc}"
CONTEXT="kind-$CLUSTER_NAME"
KEEP_CLUSTER="${KEEP_CLUSTER:-false}"
created=false
overlay="$(mktemp -d)"

cleanup() {
  status=$?
  rm -rf "$overlay"
  if [[ "$created" == true && "$KEEP_CLUSTER" != true ]]; then
    kind delete cluster --name "$CLUSTER_NAME" >/dev/null
  fi
  exit "$status"
}
trap cleanup EXIT

if ! kind get clusters | grep -qx "$CLUSTER_NAME"; then
  kind create cluster --name "$CLUSTER_NAME" --wait 120s
  created=true
fi

kubectl --context "$CONTEXT" apply -f \
  https://github.com/tektoncd/pipeline/releases/download/v1.0.2/release.yaml >/dev/null
kubectl --context "$CONTEXT" -n tekton-pipelines wait --for=condition=Available \
  deployment/tekton-pipelines-controller deployment/tekton-pipelines-webhook \
  --timeout=180s >/dev/null
kubectl --context "$CONTEXT" delete namespace agenttask-system --ignore-not-found --wait=true >/dev/null
kubectl --context "$CONTEXT" apply -f "$CORE_DIR/config/crd/bases/agent.tekton.dev_agenttasks.yaml" >/dev/null

docker_host="${DOCKER_HOST:-$(docker context inspect --format '{{.Endpoints.docker.Host}}')}"
fixture_image="$(cd "$ROOT" && DOCKER_HOST="$docker_host" KO_DOCKER_REPO=ko.local \
  ko build --local --platform="linux/$(go env GOARCH)" ./cmd/fixture)"
controller_image="$(cd "$ROOT" && DOCKER_HOST="$docker_host" KO_DOCKER_REPO=ko.local \
  ko build --local --platform="linux/$(go env GOARCH)" ./cmd/controller)"
kind load docker-image --name "$CLUSTER_NAME" "$fixture_image" "$controller_image" >/dev/null

cp -R "$ROOT/config/." "$overlay/"
CONTROLLER_IMAGE="$controller_image" FIXTURE_IMAGE="$fixture_image" yq -i '
  .spec.template.spec.containers[0].image = strenv(CONTROLLER_IMAGE) |
  (.spec.template.spec.containers[0].env[] | select(.name == "FULLSEND_IMAGE").value) = strenv(FIXTURE_IMAGE)
' "$overlay/deployment.yaml"
kubectl --context "$CONTEXT" apply -k "$overlay" >/dev/null
kubectl --context "$CONTEXT" -n agenttask-system wait --for=condition=Available \
  deployment/fullsend-agenttask-adapter --timeout=120s >/dev/null

service_account="system:serviceaccount:agenttask-system:fullsend-agenttask-adapter"
test "$(kubectl --context "$CONTEXT" auth can-i --as="$service_account" get secrets -n agenttask-system)" = no
test "$(kubectl --context "$CONTEXT" auth can-i --as="$service_account" delete pods -n agenttask-system)" = no
test "$(kubectl --context "$CONTEXT" auth can-i --as="$service_account" create pods/exec -n agenttask-system)" = no
test "$(kubectl --context "$CONTEXT" auth can-i --as="$service_account" impersonate users)" = no
test "$(kubectl --context "$CONTEXT" auth can-i --as="$service_account" patch customruns/finalizers.tekton.dev -n agenttask-system)" = yes
test "$(kubectl --context "$CONTEXT" auth can-i --as="$service_account" update customruns.tekton.dev -n agenttask-system)" = no
test "$(kubectl --context "$CONTEXT" auth can-i --as="$service_account" create jobs.batch -n agenttask-system)" = yes

kubectl --context "$CONTEXT" create -f "$ROOT/examples/fullsend-fixture.yaml" >/dev/null
for _ in $(seq 1 120); do
  customrun="$(kubectl --context "$CONTEXT" -n agenttask-system get customruns -o json 2>/dev/null | \
    jq -r '.items[] | select(any(.metadata.ownerReferences[]?; .name == "fullsend-agenttask-poc")) | .metadata.name' | head -1)"
  [[ -n "$customrun" ]] && break
  sleep 1
done
test -n "${customrun:-}"
customrun_uid="$(kubectl --context "$CONTEXT" -n agenttask-system get customrun "$customrun" -o jsonpath='{.metadata.uid}')"
for _ in $(seq 1 120); do
  job="$(kubectl --context "$CONTEXT" -n agenttask-system get jobs -o json 2>/dev/null | \
    jq -r --arg uid "$customrun_uid" '.items[] | select(any(.metadata.ownerReferences[]?; .uid == $uid)) | .metadata.name' | head -1)"
  [[ -n "$job" ]] && break
  sleep 1
done
test -n "${job:-}"
job_uid="$(kubectl --context "$CONTEXT" -n agenttask-system get job "$job" -o jsonpath='{.metadata.uid}')"
kubectl --context "$CONTEXT" -n agenttask-system wait --for=condition=Succeeded \
  pipelinerun/fullsend-agenttask-poc --timeout=240s >/dev/null
outcome="$(kubectl --context "$CONTEXT" -n agenttask-system get customrun "$customrun" -o jsonpath='{.status.results[?(@.name=="outcome")].value}')"
output_pvc="$(kubectl --context "$CONTEXT" -n agenttask-system get customrun "$customrun" -o jsonpath='{.status.results[?(@.name=="output-pvc")].value}')"
output_path="$(kubectl --context "$CONTEXT" -n agenttask-system get customrun "$customrun" -o jsonpath='{.status.results[?(@.name=="output-path")].value}')"
output_digest="$(kubectl --context "$CONTEXT" -n agenttask-system get customrun "$customrun" -o jsonpath='{.status.results[?(@.name=="output-digest")].value}')"
execution_uid="$(kubectl --context "$CONTEXT" -n agenttask-system get customrun "$customrun" -o jsonpath='{.status.extraFields.executionRef.uid}')"
test "$outcome" = completed
test "$output_pvc" = agenttask-output
test "$output_path" = "runs/$job/output.txt"
[[ "$output_digest" =~ ^sha256:[a-f0-9]{64}$ ]]
test "$execution_uid" = "$job_uid"
report_taskrun="$(kubectl --context "$CONTEXT" -n agenttask-system get taskruns -l tekton.dev/pipelineRun=fullsend-agenttask-poc -o json | \
  jq -r '.items[] | select(.metadata.labels["tekton.dev/pipelineTask"] == "report") | .metadata.name')"
test -n "$report_taskrun"
test "$(kubectl --context "$CONTEXT" -n agenttask-system get taskrun "$report_taskrun" -o json | jq -r '.spec.params[] | select(.name == "outcome") | .value')" = completed
test "$(kubectl --context "$CONTEXT" -n agenttask-system get taskrun "$report_taskrun" -o json | jq -r '.spec.params[] | select(.name == "output-path") | .value')" = "$output_path"
test "$(kubectl --context "$CONTEXT" -n agenttask-system get job "$job" -o jsonpath='{.spec.template.spec.serviceAccountName}')" = fullsend-job
test "$(kubectl --context "$CONTEXT" -n agenttask-system get job "$job" -o jsonpath='{.spec.template.spec.automountServiceAccountToken}')" = false

sed -e 's/name: fullsend-agenttask-poc/name: fullsend-agenttask-cancel/' \
  -e 's/value: success/value: sleep/' "$ROOT/examples/fullsend-fixture.yaml" >"$overlay/cancel.yaml"
kubectl --context "$CONTEXT" apply -f "$overlay/cancel.yaml" >/dev/null
for _ in $(seq 1 120); do
  cancel_run="$(kubectl --context "$CONTEXT" -n agenttask-system get customruns -o json 2>/dev/null | \
    jq -r '.items[] | select(any(.metadata.ownerReferences[]?; .name == "fullsend-agenttask-cancel")) | .metadata.name' | head -1)"
  [[ -n "$cancel_run" ]] && break
  sleep 1
done
test -n "${cancel_run:-}"
cancel_uid="$(kubectl --context "$CONTEXT" -n agenttask-system get customrun "$cancel_run" -o jsonpath='{.metadata.uid}')"
for _ in $(seq 1 120); do
  cancel_job="$(kubectl --context "$CONTEXT" -n agenttask-system get jobs -o json 2>/dev/null | \
    jq -r --arg uid "$cancel_uid" '.items[] | select(any(.metadata.ownerReferences[]?; .uid == $uid)) | .metadata.name' | head -1)"
  [[ -n "$cancel_job" ]] && break
  sleep 1
done
test -n "${cancel_job:-}"
for _ in $(seq 1 120); do
  [[ "$(kubectl --context "$CONTEXT" -n agenttask-system get job "$cancel_job" -o jsonpath='{.status.active}' 2>/dev/null || true)" == 1 ]] && break
  sleep 1
done
test "$(kubectl --context "$CONTEXT" -n agenttask-system get job "$cancel_job" -o jsonpath='{.status.active}')" = 1
cancel_job_uid="$(kubectl --context "$CONTEXT" -n agenttask-system get job "$cancel_job" -o jsonpath='{.metadata.uid}')"
kubectl --context "$CONTEXT" -n agenttask-system delete pod -l app=fullsend-agenttask-adapter --wait=true >/dev/null
kubectl --context "$CONTEXT" -n agenttask-system rollout status deployment/fullsend-agenttask-adapter --timeout=120s >/dev/null
test "$(kubectl --context "$CONTEXT" -n agenttask-system get job "$cancel_job" -o jsonpath='{.metadata.uid}')" = "$cancel_job_uid"
test "$(kubectl --context "$CONTEXT" -n agenttask-system get jobs -o json | jq -r --arg uid "$cancel_uid" '[.items[] | select(any(.metadata.ownerReferences[]?; .uid == $uid))] | length')" = 1

kubectl --context "$CONTEXT" -n agenttask-system patch pipelinerun/fullsend-agenttask-cancel \
  --type=merge -p '{"spec":{"status":"Cancelled"}}' >/dev/null
kubectl --context "$CONTEXT" -n agenttask-system wait --for=condition=Succeeded=False \
  customrun/"$cancel_run" --timeout=180s >/dev/null
cancel_reason="$(kubectl --context "$CONTEXT" -n agenttask-system get customrun "$cancel_run" -o jsonpath='{.status.conditions[?(@.type=="Succeeded")].reason}')"
test "$cancel_reason" = CustomRunCancelled
for _ in $(seq 1 120); do
  if ! kubectl --context "$CONTEXT" -n agenttask-system get job "$cancel_job" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if kubectl --context "$CONTEXT" -n agenttask-system get job "$cancel_job" >/dev/null 2>&1; then
  echo "cancelled Fullsend Job still exists" >&2
  exit 1
fi
remaining_pods="$(kubectl --context "$CONTEXT" -n agenttask-system get pods -o json | \
  jq -r --arg uid "$cancel_job_uid" '[.items[] | select(any(.metadata.ownerReferences[]?; .uid == $uid))] | length')"
test "$remaining_pods" = 0

echo "e2e_kind=passed customrun=$customrun job=$job outcome=$outcome restarted=$cancel_job cancelled=$cancel_run"
