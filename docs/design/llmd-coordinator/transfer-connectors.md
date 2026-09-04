# NIXL and shared-storage transfer connectors

## Scope and invariants

EC (encode to prefill) and KV (prefill to decode) are independent. The PoC
supports `ec-nixl`, `ec-shared-storage`, `kv-nixl`, and
`kv-shared-storage`; mixed EC/KV pairs are valid. SGLang is not implemented,
configured, documented as available, or tested.

Connector code is a typed adapter around the exact worker JSON contract. It
must preserve unknown worker-owned values inside the connector payload when
forwarding is required, while validating proxy-needed type, size, cardinality,
and item identity. Connector metadata is sensitive and is redacted in logs.

## EC connectors

`ec-nixl` encoder responses associate every `mm_hash` with a descriptor
containing `peer_host`, `peer_port`, `size_bytes`, and
`nixl_agent_metadata_b64`. Praxis rejects missing/duplicate/unrequested hashes,
invalid hosts/ports, invalid base64, or excessive descriptor sizes. Ordered
descriptors are merged into `ec_transfer_params` on the prefill request.

`ec-shared-storage` carries the worker-produced storage location/identifier and
item hash from each encoder response into the prefill request. Praxis treats
locations as opaque bounded identifiers and does not read or delete artifacts.
Cleanup ownership stays with the worker/storage deployment contract.

## KV connectors

`kv-nixl` prefill output provides the values required by decode, including
`remote_engine_id`, `remote_block_ids`, `remote_host`, and `remote_port` in
`kv_transfer_params`. Praxis checks required types and bounds, retains the
complete controlled object, and forwards it to decode without rewriting worker
identities.

`kv-shared-storage` forwards the prefill-produced opaque shared-storage KV
location/identifier in `kv_transfer_params`. Praxis validates presence and
bounded shape only; workers define storage semantics and lifecycle.

## Wire-mode placement and failures

For OpenAI mode, connector fields are added to the original API body at the
worker-agreed extension location. For tokens-in mode they are placed in the
`/inference/v1/generate` request's coordination fields/features. Inbound
client connector fields are discarded; only current-stage worker results can
populate subsequent state.

Malformed, conflicting, oversized, or mismatched descriptors fail before the
next worker call. A failed fan-out never yields partial EC metadata. Tests use
contract fixtures captured from truthful supported workers or controlled
synthetic evidence and cover each connector independently, mixed pairs, both
wire modes, multiple images, malformed fields, spoofing, and redaction.
