# IRR bounded parallel fan-out

## Configuration and API

An IRR step may declare:

```yaml
fan_out:
  max_concurrency: 8
  max_requests: 128
```

Both values are positive and bounded by implementation-wide safety maxima.
Fan-out is disabled when the block is absent. The request extension API is:

```text
FanOutRequest  { key, request }
FanOutRequests [FanOutRequest]
FanOutResponse { key, response }
FanOutResponses [FanOutResponse]
```

`key` is opaque, unique within a batch, and used to correlate merge results.
The producer installs `FanOutRequests`; the runner replaces it with ordered
`FanOutResponses`. Duplicate keys, an oversized batch, streaming children, or
a missing batch are configuration/runtime errors before outbound work starts.

## Execution semantics

Each child runs the step's complete filter chain in a fresh request context,
including `ext_proc` and `endpoint_selector`. Pipeline resources are shared
through their normal immutable/owned handles, but mutable request extensions,
headers, bodies, and response state are isolated. This guarantees a separate
EPP decision and potentially different encoder replica for every item.

At most `max_concurrency` children are in flight. Input order, not completion
order, determines output order. An empty valid batch succeeds immediately.
After all children succeed, IRR receives a synthetic successful stage response
and advances once; the batch counts as one IRR iteration, not N iterations.

## Bounds, cancellation, and errors

Fan-out shares the parent step and overall IRR deadline. Existing per-body
limits apply to children, and the runner additionally limits total retained
response bytes and total request state. Responses remain buffered because
encode responses are small coordination objects; streaming fan-out is rejected.

The first observed child failure is the stage failure. The runner cancels all
other children, awaits their cleanup, discards partial results, and preserves
the failing status through normal IRR error mapping. Parent cancellation and
client disconnect propagate to every child. Panics/aborted tasks are converted
to internal gateway failure, never a partial successful batch.

## Compatibility and diagnostics

Ordinary IRR steps are unchanged. Depth headers, circuit breakers, request
sanitization, deadlines, and response limits are applied exactly as for
single-call IRR execution. Traces link each child span to the parent and record
batch index/key safely. Metrics record batch size, active children, duration,
cancellation, and bounded failure category without endpoint labels.

## Acceptance tests

Tests cover schema defaults and invalid bounds; no/empty/single/many batches;
stable ordering under reversed completion; concurrency caps; independent EPP
destinations; duplicate keys; child `4xx`/`5xx`; transport failure; cancellation;
deadline; circuit breaking; depth, per-response, and aggregate-state limits;
streaming rejection; and absence of partial state after failure.
