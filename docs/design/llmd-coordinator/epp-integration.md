# Direct EPP integration

## Contract

Praxis invokes llm-d inference-scheduler as an Envoy-compatible `ext_proc`
service for every stage selection. It sends one of:

```text
EPP-Profile: encode
EPP-Profile: prefill
EPP-Profile: decode
```

The EPP `header-profile-handler` chooses the matching role-filtered scheduling
profile and returns `x-gateway-destination-endpoint`. Encode fan-out opens an
independent exchange per child. Praxis does not choose, cache, or reuse the
replica decision across stages.

The configured EPP identity is trusted; client routing headers are stripped.
The destination mutation is accepted only at the expected ext_proc phase and
is validated as an allowed HTTP(S) worker authority/address. Other attempts to
mutate protected stage or transfer headers fail closed.

## Conditional decode

Praxis sets `Prefer: if-available` with the decode profile. The selected decode
worker, or the EPP path supporting this policy, returns `412` when remote
prefill is needed. Praxis consumes that response and continues orchestration.
Selection success is therefore separate from cache availability.

## Local scheduler/deployment changes

Add local examples to llm-d-inference-scheduler for one EPP with three
role-filtered profiles (or three role-scoped EPPs where the existing harness
requires it), direct Praxis ext_proc connectivity, encode/prefill/decode worker
pools, and conditional decode. Preserve existing coordinator deployments and
Go code. Praxis replaces only the coordinator Service/Deployment in PoC overlays.

Plaintext is acceptable for isolated local testing. Optional EPP and worker TLS
configure CA trust, client identity, and server name independently. Health and
readiness distinguish listener readiness from EPP dependency diagnostics;
transient EPP failure must not silently bypass scheduling.

## Evidence and tests

Mock tests assert profile headers, protected-header handling, independently
selected replicas, destination validation, EPP timeout/unavailable/malformed
messages, worker connection failures, and `412` behavior. Real-stack evidence
records EPP logs/profile decisions, selected pod names, worker logs, and final
responses for all topologies without credentials or prompt bodies.
