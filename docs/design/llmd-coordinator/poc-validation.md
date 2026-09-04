# Local PoC validation

## Completion gates

The PoC is complete only when deterministic mock validation and the real llm-d
Kubernetes development stack both pass. Work remains local: do not create
issues, PRs, pushes, releases, or credentialed recordings without separate
authorization. Keep the Go coordinator runnable for side-by-side comparison.

## Deterministic environment

Provide mock ext_proc EPP, rendering service, encoder replicas, prefill
replicas, combined PD replica, and buffered/streaming decode replicas. Selection
is scripted by profile and request/item key, with a request log that excludes
prompt/media data. Mocks can emit valid connector data, `412`, delayed chunks,
malformed metadata, bounded `4xx`/`5xx`, and transport failure.

The matrix covers EPD, P/D, E/PD, E/P/D; OpenAI and tokens-in; text, one image,
and two images; NIXL and shared storage wherever a handoff exists. Required
scenarios include two encode children selecting different replicas,
conditional-decode hit and `412` fallback, remote/inline media and SSRF rules,
render limits/failure, connector corruption/spoofing, JSON/SSE, slow consumers,
disconnect, deadline, EPP/worker failure, reload, and header trust.

Assertions cover call order, profile, path, selected endpoint, sanitized
headers, exact connector placement, bounded concurrency, stable merge order,
status/body, cancellation, and absence of prompt-bearing logs/errors.

## Real-stack environment

Build a local praxis-ai image with the llm-d ext_proc integration and deploy it
into the existing coordinator development/e2e environment under a unique tag.
Apply local overlays that point Praxis directly at EPP and retain original
coordinator manifests for rollback/comparison. Use existing real stage worker
images and model/accelerator prerequisites; do not embed credentials in files.

Run and record successful requests for:

1. EPD: decode handles the complete request.
2. P/D: independently selected prefill then decode.
3. E/PD: parallel encode then selected combined PD.
4. E/P/D: parallel encode, selected prefill, streamed decode.

For each applicable topology run text, single-image, and two-image requests in
both wire modes. Exercise shared-storage and NIXL when supported by that stack;
record an environment limitation explicitly rather than substituting synthetic
success. Verify conditional decode hit/fallback separately.

## Evidence record

Append dated runs to this file (or a local ignored evidence file linked here)
with git revisions/worktree state, build command and image digest, Kubernetes
context/namespace, redacted config/topology, request class, selected role/pods,
connector, status, stream completion, test command/result, and relevant log
locations. Never record tokens, credentials, full prompts, media, or transfer
secrets.

## Repository checks

- Praxis core: focused IRR/schema/integration tests, `make test`, `make lint`,
  and `make doc`.
- praxis-ai: unit/integration/environment tests, example functional tests,
  inference fixture manifest and generated README synchronization,
  `make test`, `make lint`, and `make doc`.
- llm-d-inference-scheduler: focused EPP/deployment tests and `make presubmit`.
- Search configs/code/tests to confirm the PoC introduces no SGLang path and
  no legacy coordinator metric compatibility.

If required Kubernetes, accelerator, model, container-runtime, or credential
access is unavailable, mock completion may be reported but the PoC remains
incomplete until the real-stack gate is run.

## Evidence: 2026-09-03 local simulator stack

- Cluster: `kind-llm-d-router-dev`, namespace `default`; llm-d inference
  simulator `v0.10.2`, EPP `v0.4.0-rc.1` development image.
- Local Praxis image digest: `sha256:a2877af82182f1690b713f34a0e75782a71dab0d107470dcc7bc13ad51ba83cd`.
- EPD OpenAI completions: HTTP 200. EPP selected
  `vllm-d-5687d844b-rgr9l:8000`; streamed/chunked response completed.
- P/D OpenAI completions with NIXL-shaped handoff: HTTP 200 after the Praxis
  prefill and decode loop; decode response identified the selected pod.
- E/PD tokens-in reached encoder orchestration, but this EPP build's parser
  rejected `/inference/v1/generate` as an OpenAI completions request. The
  repository's proposed header-profile config also used schema fields absent
  from this EPP version and was replaced with the supported disaggregation
  handler configuration.
- E/P/D and multimodal OpenAI were not claimed as live successes: they require
  either EPP tokens-in parser support or a compatible multimodal render/model
  deployment. Unit/IRR fan-out coverage remains the deterministic evidence for
  those paths.
- Local EPP TLS was disabled because the current Praxis ext_proc client cannot
  configure trust for the development EPP's self-signed certificate. Production
  TLS remains an explicit follow-up and non-empty TLS config is rejected.

## Evidence: 2026-09-03 current-branch EPP

- Built `/home/pdipilat/redhat/rhoai/llm-d-inference-scheduler` `main` locally
  as `llm-d-router-endpoint-picker:praxis-branch`, image digest
  `sha256:9b0584d35d026eeebc7f489bbcb8d8e2a77d4b0f61bfce5171c3277a60dab3f0`.
- The current EPP supplies `vllmhttp-parser` in its default parser chain and
  successfully parsed `/inference/v1/generate`; this removes the v0.4 parser
  blocker recorded above.
- A native generate request in the E/PD-configured Praxis pipeline correctly
  skipped encode as an empty fan-out batch, was classified into the EPP decode
  profile, selected `vllm-d-5454487ccb-pjfhh:8000`, and completed with HTTP 200.
- The tested Praxis image digest was
  `sha256:c4db5444a6cfc989b76aed774934e97379b6d83bd7c29e0ec8ed04908f7f9c26`.
- The older ODH routing-sidecar image in the fixture had to be pointed at the
  simulator's actual model-server port, 8200. This was deployment drift, not
  an EPP or Praxis scheduling failure.
- This validates current-EPP tokens-in parsing and the no-media E/PD path.

## Evidence: 2026-09-03 controlled multimodal E/PD

- Added and deployed the deterministic `praxis-render-fixture`, which rendered
  two controlled OpenAI `image_url` items into token IDs, hashes, placeholder
  ranges, and synthetic encoder kwargs. This proves orchestration and transfer
  shapes; it does not claim real multimodal preprocessing or model fidelity.
- Built current scheduler EPP from source and enabled its Alpha
  `header-profile-handler` with `--allow-experimental-plugins`. The tested
  Praxis image was `localhost/praxis-ai:llmd-poc-multimodal-v5`, digest
  `sha256:9e3e684233d9496519afd6be14718fc806fa3f4ce0e90a547fd0c44d46917e66`.
- A two-image OpenAI chat request completed with HTTP 200. EPP independently
  ran the `encode` profile twice and selected `vllm-e-58c8f6c689-n57kc:8000`
  for both items (the fixture had one ready encoder), then ran `decode` and
  selected `vllm-d-dbdfbb66b-7qlfm:8000`.
- The run exposed and fixed two mixed-wire/pre-read integration gaps: native
  decode bodies now contain token IDs/features rather than the original
  OpenAI messages, and ext_proc observes the effective rewritten path and
  stage header mutations before it asks EPP to parse and schedule the request.
- This closes the controlled live encode-fan-out gate for E/PD. E/P/D remains
  to be run with the same fixture, and a compatible real multimodal model is
  still required before claiming model-semantic validation.
