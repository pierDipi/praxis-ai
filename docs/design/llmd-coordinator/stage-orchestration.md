# Stage orchestration

## Pipeline contract

IRR carries a typed coordinator state through filters for conditional decode,
encode preparation/merge, prefill preparation/capture, and terminal decode.
Before every worker call the stage sets `EPP-Profile`; `ext_proc` obtains a
trusted destination from EPP; `endpoint_selector` routes only to that value.
Stage calls preserve the inbound API path in OpenAI mode and use
`/inference/v1/generate` in tokens-in mode.

## Conditional decode

For an eligible text request, send a decode request with
`Prefer: if-available`. `2xx` becomes the terminal buffered or streaming
client response. `412` has no client-visible body and advances to remote
prefill. Any other `4xx`, `5xx`, invalid response, or transport failure follows
normal error mapping. Multimodal requests bypass this optimization.

## Encode

Create one `FanOutRequest` per multimodal entry, keyed by stable input index/hash.
Each child independently executes EPP selection and calls an encoder, so replicas
may differ within one client request. OpenAI children contain the appropriate
original request/media context; tokens-in children contain the item feature and
rendered tokens needed by the worker. Responses are buffered, bounded, checked
for matching hashes/descriptors, and merged in input order into EC state.

## Prefill and decode

Prefill is a single independently scheduled call containing rendered token
state, EC handoff metadata when present, and the selected KV connector request
fields. Its bounded response is consumed internally and validated to obtain KV
handoff metadata. Decode is independently scheduled and incorporates that KV
state. A non-streaming decode response is forwarded as JSON. An SSE response is
pulled and forwarded with backpressure; Praxis does not buffer the full stream.
Headers/status are committed only after the terminal response is valid.

## Topology resolution

- EPD: route directly to decode without generated remote transfer state.
- P/D: prefill then decode, optionally preceded by conditional decode.
- E/PD: encode fan-out then route the combined prefill/decode call as terminal.
- E/P/D: encode fan-out, prefill, then decode.

Text skips encode without changing subsequent stage order. `/v1/completions`
cannot invent multimodal entries. Passthrough paths receive only decode profile
routing and no body transformation.

## Error and cancellation semantics

Applicable worker `4xx` status is preserved with a sanitized local error;
worker/EPP `5xx`, malformed coordination data, and transports become gateway
errors. Prompt-bearing upstream bodies are drained only as required for reuse
and never exposed. Failure prevents later stages. Client disconnect, deadline,
or stage error cancels all in-flight calls and releases state.

Tests cover four topologies times two wire modes, text/single/two-image inputs,
distinct encode destinations, conditional hit/fallback, every failure boundary,
JSON/SSE, slow consumers, disconnect, deadline, passthrough, and spoofed headers.
