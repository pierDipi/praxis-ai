# Request preprocessing

## Accepted inputs

The parser accepts OpenAI `/v1/chat/completions`, `/v1/completions`, and the
tokens-in `/inference/v1/generate` contract. It extracts only coordination data
while retaining the original request for the selected wire mode. Model/backend
parameter validation remains with workers. Unsupported paths use decode
passthrough without render or transfer mutation.

## Media normalization

For chat image parts, inline `data:` values are decoded under request/media
limits. HTTP(S) URLs are fetched concurrently up to
`max_concurrent_downloads`, with per-download timeout and byte limit. Content
length is an early check, never the only check. DNS results and every redirect
target are validated against scheme, optional domain allowlist, and prohibited
address ranges. DNS rebinding is prevented by connecting only to the validated
resolution. MIME type and encoding errors are client failures.

Normalized data replaces remote URLs only in the owned stage request that
requires it. The implementation avoids retaining both large raw and encoded
copies beyond the necessary transformation boundary. Logs contain counts,
sizes, and redacted origins, never data URIs or prompt/media bytes.

## Rendering and multimodal state

The render service receives the normalized request and returns token IDs plus,
for each multimodal item, its stable hash, preprocessing `kwargs_data`, and
placeholder offset/length. The response is checked for item cardinality,
unique/corresponding hashes, non-overlapping in-range placeholders, token and
placeholder totals, and bounded payload size. In tokens-in mode these results
form the generate request. In OpenAI mode the original request shape is kept
and the minimal coordination `tokens`/feature information is added.

An inbound generate request already supplies token IDs/features and always
uses generate format regardless of the global flag. It is structurally
validated for proxy-required fields but is not rendered again.

## Headers and ownership

Forward an explicit safe set of content negotiation, tracing, authorization,
and application headers according to existing Praxis policy. Remove hop-by-hop
headers and all client-supplied internal routing/transfer headers, including
`EPP-Profile` and `x-gateway-destination-endpoint`. Generate/propagate one safe
request correlation value across stages.

Parsed slices borrow the admitted body where lifetimes allow. The state owns
only values crossing asynchronous boundaries. Media buffers are moved into
render/encode children and released after merge. No streaming response chunk
or provider payload is cloned merely for filter control flow.

## Failures and tests

Malformed JSON, invalid data URLs, prohibited origins, excessive entries,
downloads, tokens, placeholders, or request bytes yield bounded `4xx` errors.
Render timeout/protocol/`5xx` failures yield gateway errors with no upstream
body leakage. Tests include completions/chat/generate, text and multiple media,
redirect and DNS security cases, concurrency, limits, cancellation, malformed
render mappings, header spoofing, and log redaction.
