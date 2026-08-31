# Fullsend AgentTask Adapter

Experimental AgentTask Adapter for mapping a Tekton `CustomRun` to a Kubernetes
Job running the existing Fullsend harness. It is being prototyped alongside
[TEP-0170: AgentTask and Pluggable Agent Execution](https://github.com/tektoncd/community/pull/1263).

This repository is an experimental PoC scaffold. TEP-0170 is proposed, the
shared API is unstable, and the adapter is not usable yet. Publication does not
imply TEP acceptance, API compatibility, or product support.

## Current slice

Implemented:

- a fail-closed shell that compiles against the shared `AgentTaskAdapter`
  interface and selects `fullsend.ai/agenttask-adapter`;
- strict parsing and validation for the versioned, bounded Pod termination
  record that a future Job wrapper will produce.

Not implemented:

- a controller or Kubernetes deployment;
- Job creation, adoption, observation, or foreground deletion;
- source-workspace and revision verification;
- the wrapper that invokes `fullsend run` and produces the termination record;
- OpenShell gateway or sandbox integration;
- cancellation, cleanup, status writing, restart recovery, RBAC, or conformance
  tests.

Every lifecycle method returns `ErrNotImplemented` without creating or mutating
anything. A future PoC deployment will use one active controller leader and
will not implement distributed adapter claiming. This scaffold is therefore
**not TEP-0170 conformant**.

The parser accepts one JSON object no larger than 4096 bytes. It validates the
schema version, final process status, pre-script skip decision, and a
credential-free reference to output retained on a namespaced PVC. It never
parses or copies Fullsend transcripts, `output.jsonl`, `metrics.json`, findings,
or archives into Tekton status.

## Development

```sh
make verify
make test
```

`go.mod` pins an experimental `github.com/openshift-pipelines/agenttask`
version. Update that pin deliberately with any matching adapter contract
change.

## License

Apache License 2.0.
