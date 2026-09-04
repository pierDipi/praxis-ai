# Praxis-native llm-d coordinator architecture

## Status and intent

This document specifies a local proof of concept (PoC) that replaces the
llm-d Go coordinator's orchestration role with a Praxis native pipeline. The
Go coordinator remains available and unchanged. llm-d inference-scheduler
(EPP) remains the scheduling authority: Praxis decides which stage runs next,
while EPP independently selects the replica for every stage request.

No GitHub issues, pull requests, pushes, or production rollout are part of the
PoC. SGLang connectors and compatibility with the coordinator's Prometheus
metric names are explicitly out of scope.

## Component boundaries

```text
client
  -> Praxis listener
     -> request parsing / media replacement / rendering
     -> iterative request router (IRR)
        -> conditional decode: ext_proc(EPP) -> selected decode worker
        -> encode fan-out: each item -> ext_proc(EPP) -> selected encoder
        -> prefill: ext_proc(EPP) -> selected prefill worker
        -> decode: ext_proc(EPP) -> selected decode worker -> streamed client response
```

Praxis owns request admission, preprocessing, stage sequencing, wire
transformations, bounded fan-out, transfer metadata, error normalization, and
the terminal stream. EPP owns role-filtered replica selection. Workers own
model validation and inference. Praxis validates only fields required to proxy
or transform the request.

The endpoint selected by EPP is returned through
`x-gateway-destination-endpoint`. Praxis accepts that header only from the
configured `ext_proc` exchange, removes any client-provided copy, and passes
the trusted value to `endpoint_selector`.

## Request state and ownership

A request-scoped coordinator state contains the original method, path, safe
headers, OpenAI body or tokens-in body, token IDs, multimodal entries,
placeholder ranges, per-item hashes and rendering data, EC descriptors, KV
handoff data, and final response state. Bodies and completed stage results are
moved between filters. Shared immutable data may use reference counting.
Streaming chunks are never cloned or accumulated to satisfy local control
flow. Full buffering is limited to admitted client input and non-streaming
render, encode, and prefill responses.

On fan-out, each child receives a correlation key and an isolated child
context. Results are merged in input order. A child cannot mutate the parent
or another child. Partial EC/KV state is discarded when the stage fails.

## Topologies

The ordered configured steps define the topology:

| Topology | Behavior |
|---|---|
| EPD | Decode replica handles the whole request; no remote encode/prefill handoff. |
| P/D | Optional conditional decode, then prefill and decode. |
| E/PD | Encode each multimodal item, then a combined prefill/decode worker. |
| E/P/D | Encode fan-out, prefill, then independent streamed decode. |

Text-only requests skip encode even when configured. Multimodal requests fail
configuration validation when required render/encode or connector stages are
missing. `/v1/chat/completions`, `/v1/completions`, and
`/inference/v1/generate` are orchestrated. Other paths are passed through to a
scheduler-selected decode replica, subject to the same body and header rules.

## Stage state machine

Conditional decode is attempted only where configured. It sends
`Prefer: if-available` and `EPP-Profile: decode`. A successful response is the
terminal response. `412 Precondition Failed` is consumed and advances the
pipeline; other errors are terminal. Encode produces ordered EC descriptors.
Prefill consumes EC state and produces KV state. Decode consumes available
handoff state and is always terminal.

## Failure and security model

- Invalid input, blocked media, admission-limit failures, and applicable
  worker `4xx` responses remain client errors.
- EPP transport/protocol failures, invalid destinations, worker transport
  failures, and worker `5xx` responses become gateway errors.
- Prompt-bearing worker error bodies are not returned or logged.
- Media resolution validates every redirect and resolved address, blocks
  loopback, link-local, metadata, CGNAT, and unique-local networks, and permits
  RFC1918 only when explicitly configured.
- Request count, body bytes, media bytes, placeholder/token counts, fan-out
  concurrency, retained response bytes, IRR depth, and deadlines are bounded.
- Client disconnect or deadline expiry cancels render and outstanding stage
  requests, including fan-out children.

## Observability and migration

Use Praxis request, IRR, subrequest, latency, error, cancellation, and tracing
facilities. Add stage/profile and outcome attributes with bounded cardinality;
never use model input, destination addresses, request IDs, or media URLs as
metric labels. Do not reproduce legacy coordinator series.

The PoC runs alongside the Go coordinator with separate listener/service
names. Comparison covers status, payload, selected roles, handoff metadata,
stream completion, latency, and resource use. Production replacement,
deprecation, and schema removal require a later decision.
