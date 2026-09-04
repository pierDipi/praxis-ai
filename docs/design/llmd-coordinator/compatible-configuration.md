# Coordinator-compatible configuration

## Selection and translation

The praxis-ai binary recognizes the coordinator top-level schema (`server`,
`gateway`, and `pipeline`) and translates it into a normal Praxis listener and
filter pipeline before validation/build. Native Praxis YAML continues to work.
A document containing both native and coordinator roots is rejected rather
than applying ambiguous precedence. Unknown coordinator keys and unsupported
connector names are errors with their YAML path.

The same parse, translate, validate, and atomic pipeline-swap path is used at
startup and reload. A failed reload leaves the last valid pipeline active.

## Field mapping

`server.listen_addr`, read/write/shutdown timeouts, maximum request body size,
metrics port, and log level map to the corresponding Praxis facilities. The
metrics endpoint exposes native Praxis metrics only.

`gateway.address` is interpreted by this PoC as the EPP `ext_proc` endpoint,
not an Inference Gateway HTTP entry point. Pool and timeout settings map to EPP
and worker client facilities where semantically equivalent. New optional
`gateway.epp_tls` and `gateway.worker_tls` are recognized so configurations
fail clearly, but non-empty blocks are rejected by this local PoC because the
current direct EPP and dynamically selected worker transports cannot apply
custom CA or client identity settings. Plaintext is the supported local mode;
TLS enablement requires explicit transport work rather than silently ignoring
security settings.

`pipeline.steps` retains order and step parameters. Global
`kv_connector`, `ec_connector`, and `use_openai_format` are defaults overridden
by an explicit step value. Supported connectors are exactly `kv-nixl`,
`kv-shared-storage`, `ec-nixl`, and `ec-shared-storage`. Any `kv-sglang` value
fails validation as unsupported in this PoC.

## Cross-field validation

- Tokens-in (`use_openai_format: false`) requires render output unless the
  inbound request is already `/inference/v1/generate`.
- Encode requires a supported EC connector and fan-out bounds; decode/prefill
  pairings require a supported KV connector.
- Decode is terminal. Conditional decode cannot follow a terminal step.
- Required addresses, positive timeouts, body limits, and TLS file references
  are validated before listener replacement.
- Every ordered step sequence must map to EPD, P/D, E/PD, or E/P/D; unsupported
  or ambiguous sequences fail with the normalized sequence in the message.

## Compatibility limits

The objective is input-schema convenience for the PoC, not byte-for-byte
runtime or metric compatibility. Environment variable overrides already used
by coordinator configs are applied before translation where implemented and
documented; CLI precedence is CLI, environment, YAML, then default. The
translated native config can be logged only with secrets and sensitive paths
redacted.

Tests use golden translations for each topology/wire mode, overrides,
unknown/mixed schema, unsupported SGLang, invalid step order, TLS, and atomic
reload failure/success.
