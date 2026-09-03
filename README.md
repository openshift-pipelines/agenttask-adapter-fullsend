# Fullsend AgentTask Adapter

Experimental AgentTask Adapter that maps a Tekton `CustomRun` to a deterministic
Kubernetes Job using the lifecycle proposed by
[TEP-0170](https://github.com/tektoncd/community/pull/1263).

The current `fixture-v1` profile proves the Job boundary before integrating the
real Fullsend/OpenShell runtime. It creates or adopts one Job per CustomRun
attempt, persists the server-assigned Job UID, consumes one bounded termination
record, exposes declared results, and foreground-deletes the Job during
cancellation.

## Contract

Selector:

```text
fullsend.ai/agenttask-adapter
```

The fixed `fixture-v1` profile accepts one string param, `request`, and declares
exactly these results:

- `outcome`: `completed` or `skipped`;
- `output-pvc`;
- `output-path`; and
- `output-digest`.

The controller receives the Job image through `FULLSEND_IMAGE`; Pipeline input
cannot select an image, ServiceAccount, or namespace. Deployments must pin that
image by digest; the local Kind test is the only tag-based exception. The image must write one
`fullsend.ai/agenttask-result/v1alpha1` JSON object to its termination message.
Detailed output stays on the fixed `agenttask-output` PVC and is represented by
a credential-free Kubernetes reference.

The repository includes `cmd/fixture`, a deterministic contract fixture used by
unit and Kind end-to-end tests. It is not the Fullsend harness.

## Security boundary

- the controller is namespace-scoped;
- Jobs run as non-root with a read-only root filesystem, dropped capabilities,
  runtime-default seccomp, and no mounted ServiceAccount token;
- the Job uses the fixed, unprivileged `fullsend-job` ServiceAccount;
- the controller cannot read Secrets, exec into Pods, impersonate identities,
  or delete Pods directly;
- adoption requires the CustomRun owner UID, attempt identity, trusted-profile
  digest, immutable Job shape, and persisted Job UID;
- an uncertain create must remain absent for a bounded settle interval before
  pre-creation cancellation completes;
- cancellation uses a UID precondition and foreground Job deletion, waits until
  no owned Pod remains, and reports `CleanupFailed` after the PoC deadline; and
- transcripts and raw Fullsend output are never copied into CustomRun status.

The output PVC is intentionally retained as the profile's artifact-retention
policy. `CleanupFailed` retains the native reference and cleanup finalizer; a
framework-level finalizer-release deadline remains follow-up work.

## Development

```sh
make verify
make test
make e2e-kind
make e2e-openshell-kind
```

The fixture E2E proves result substitution, restart-safe Job adoption,
correlated cancellation, Pod cleanup, and denied Secret, Pod-delete, exec, and
impersonation permissions. The OpenShell E2E creates its own disposable cluster
and proves package installation, client-certificate enforcement, sandbox
execution and cleanup, safe uninstall refusal, and retained-state behavior.

## Packaged OpenShell

The repository packages, but does not embed or reimplement, the OpenShell
runtime required by Fullsend. The installer deploys the official OpenShell Helm
chart at `0.0.116`, the version pinned by Fullsend v0.39.0, plus Agent Sandbox
v0.5.0. Downloaded artifacts and all packaged runtime images are pinned and
verified by digest or SHA-256.

An authenticated shared-cluster install keeps TLS enabled and requires an
operator-supplied OIDC configuration. The installer rejects an absent or
non-HTTPS issuer:

```sh
KUBECTL_CONTEXT=my-context \
OPENSHELL_EXTRA_VALUES_FILE=/path/to/oidc-values.yaml \
make install-openshell
```

For a disposable Kind cluster only, the test overlay keeps mutual TLS enabled
but maps holders of the generated client certificate to one local developer
principal:

```sh
KUBECTL_CONTEXT=kind-agenttask-fullsend-poc make install-openshell-kind
```

Quiesce gateway clients before uninstalling. `make uninstall-openshell` refuses
to remove a gateway with active or unattributed Sandbox resources. After
removal it retains the shared Agent
Sandbox controller and the namespaced data PVC plus generated PKI/JWT/KEK
Secrets so an operator can reinstall without losing identity or encrypted
state; unused certificate-hook RBAC is removed. Delete that namespace
separately only after confirming the retained state is no longer needed. Installation fails rather than replacing an
existing Agent Sandbox controller at a different version.

The adapter, gateway, and sandbox supervisor remain separate workloads and
ServiceAccounts. Treat one gateway installation as one trust domain; do not
share its provider and sandbox records across mutually untrusted tenants.

OpenShell's current OpenShift installation is experimental and requires the
sandbox ServiceAccount to use the `privileged` SCC. For a disposable evaluation
cluster, review and grant that SCC explicitly, then use the packaged overlay so
OpenShift assigns the gateway UID and FS group:

```sh
oc --context=my-openshift-context create namespace openshell-system
oc --context=my-openshift-context adm policy add-scc-to-user privileged \
  -z openshell-sandbox -n openshell-system
KUBECTL_CONTEXT=my-openshift-context \
OPENSHELL_EXTRA_VALUES_FILE=/path/to/oidc-values.yaml \
make install-openshell-openshift
```

The SCC grant outlives Helm resources. Revoke it when removing this evaluation:

```sh
KUBECTL_CONTEXT=my-openshift-context make uninstall-openshell
oc --context=my-openshift-context adm policy remove-scc-from-user privileged \
  -z openshell-sandbox -n openshell-system
```

The installer still forces TLS on. Do not use the privileged sandbox topology
or OpenShell's TLS-disabled evaluation configuration on a shared cluster.

## Real Fullsend harness follow-up

The packaged gateway supplies the sandbox control plane, but `fullsend run`
still does not expose the bounded termination-record and deterministic cleanup
seams used by this adapter. A real-harness profile must add those seams and a
reviewed Kubernetes OpenShell topology; it must not disable sandboxing or mount
a host container socket to make the test pass.

This repository remains an experimental PoC. It does not imply TEP acceptance,
API compatibility, or product support.

## License

Apache License 2.0.
