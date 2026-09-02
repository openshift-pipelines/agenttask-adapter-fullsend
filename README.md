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
```

The E2E proves result substitution, restart-safe Job adoption, correlated
cancellation, Pod cleanup, and denied Secret, Pod-delete, exec, and impersonation
permissions.

## Real Fullsend harness follow-up

Fullsend v0.39.0's runner image contains the host-side CLI and OpenShell client,
but requires a separately deployed OpenShell gateway, sandbox supervisor, and
client mTLS configuration. `fullsend run` also does not yet expose the bounded
termination-record seam used here. A real-harness profile must add that stable
result seam and a reviewed Kubernetes OpenShell topology; it must not disable
sandboxing or mount a host container socket to make the test pass.

This repository remains an experimental PoC. It does not imply TEP acceptance,
API compatibility, or product support.

## License

Apache License 2.0.
