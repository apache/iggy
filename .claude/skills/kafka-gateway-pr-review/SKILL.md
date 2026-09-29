---
name: kafka-gateway-pr-review
description: |
 Review a Kafka gateway (gateways/kafka/) PR, branch, or diff against patterns
 real human reviewers have actually blocked on in past Kafka-gateway PRs:
 Java-client/broker parity, SDK usage and bridge performance, wire-decode
 memory safety, concurrency/auth-path bugs, docs-vs-code drift, test quality.
argument-hint: "[PR number | branch | ref range]"
disable-model-invocation: true
---

# Kafka Gateway PR Review

`<TARGET>` = `$ARGUMENTS`: a PR number, a branch, or a ref range. Empty means `origin/master..HEAD`.

Get the diff first (`gh pr diff <PR>` for a PR number, otherwise
`git diff $(git merge-base origin/master HEAD)..HEAD` for a branch/range), then review it against
every section below. Cite `file:line` for every finding. Go handler-by-handler through
`gateways/kafka/src/`.

## Java-client / real-broker parity

- Trace every non-zero Kafka error code this PR returns back to actual client behavior, not
  just the spec text. Check against real client source: does this code make librdkafka/the
  Java client retry forever (livelock), treat it as fatal when a real broker wouldn't, or
  silently stop making progress? Known bad codes: 35/42 misused for cases that should be
  53/31/7; TOPIC_AUTHORIZATION_FAILED(29) used for the gateway's OWN Iggy-login failure
  (blames the client's ACLs instead of surfacing a gateway problem).
- Any per-request cap (topic count, partition count, name count) must not permanently break
  a client whose *cumulative* state crosses it - Java's ProducerMetadata/consumer resend
  their full accumulated set every retry, so a hard per-call cap with no recovery path
  bricks the client forever, not just that one call.
- `CreatableTopicResult` / error responses: `error_message` must be `None`, never `Some("")`
  - empty-string error text propagates into Java exception messages.
- KIP semantics you must get right, not guess: -1 sentinels (`num_partitions=-1` = broker
  default in CreateTopics v4+, not INVALID_PARTITIONS; -1 timestamp in ListOffsets).

## Bridge/SDK usage and performance

- Every bridge call that reaches the SDK needs a deadline, not just `connect()`. Grep for
  any `bridge.*(` call not wrapped in `with_request_timeout`/`timeout_at` - bare bridge
  calls under a stalled Iggy address park indefinitely.
- Any timeout wrapping a *batch* of independent SDK calls (multiple topics/partitions in one
  loop) must apply the deadline per-item against a shared deadline (`timeout_at`), and keep
  already-resolved results. A single `timeout` around the whole loop throws away finished
  work and fails identically on every retry.
- Remember the SDK's single `IggyClient` is lockstep (one request in flight, stream mutex
  held across write+flush+read) - flag any change that adds more serialized round-trips per
  Kafka request, or that could get stuck behind a stalled call with no way out.
- Linear scans/re-sorts of partition or topic vectors on a per-call basis: if the data is
  already sorted/deduped, this should be `binary_search_by_key`, not `.find()` or a fresh
  sort every call.
- Response/record encoding: flag `BytesMut::new()`/unbounded push on a hot path where the
  final size is already computable from known fields - should be `with_capacity`.
- Anything doing real CPU work (partition-count-proportional encode/build) with no yield
  point on the async runtime - flag for `spawn_blocking` or chunking above a measured
  threshold.

## Memory safety on the wire-decode path

- Any `with_capacity`/`reserve`/`zeroed` sized from an untrusted wire-supplied count (record
  count, header count, declared decompressed length) must be checked against actual
  remaining bytes before allocating - not just checked for sign/non-negativity.
- Decoded request structs that outlive an `.await` (group join/sync/heartbeat, anything
  holding a handshake open): confirm they hold an OWNED copy, not a slice/clone that keeps
  the original inbound frame `Bytes` buffer pinned. A cloned `StrBytes`/`Bytes` field kept
  alive across a multi-minute await is a real unauth-OOM vector at scale (N waiters x
  max_frame_size).
- Any refused/partial charge against a budget (records, bytes) must actually refund on the
  rejected path - check for a debit that never gets credited back.

## Concurrency / auth path

- Any lock or per-connection state that's touched from a periodic tick/heartbeat: does the
  tick scale with total connections/groups, or just the one it's named for? Flag global
  locks not held across await but still walked O(n) per tick at scale.
- Auth/login: confirm a timed-out permit actually cancels the in-flight work (not just frees
  the semaphore slot) - a detached spawn or lockstep SDK call can keep running after its
  owning connection gives up, exhausting a bounded pool from the "logged in already" side.
  Check RST-vs-FIN: dropping a stream with unread bytes queued sends RST and discards any
  error body you meant to deliver - `shutdown()` + drain first.

## Docs vs code

- Spot-check every behavioral claim in README.md/SCOPE.md/AUTHENTICATION.md/any docs this
  PR touches against the actual code path it describes - this repo has a repeated pattern of
  docs asserting a close/error-code/timeout behavior the code doesn't actually do (or no
  longer does after a later fix).

## Test quality (don't just count coverage)

- Reject tests that assert `X == X` / duplicate a source table verbatim and claim it's a
  cross-check against something external - it's a change detector, not an oracle.
- Flag near-duplicate test suites/scenarios (same behavior asserted 3-4 times across
  differently-named files) over adding a 5th copy.
- Any test touching a socket/docker/server-spawn must have an explicit timeout - no bare
  `.read().await` or `docker run` without one; these turn a regression into a CI hang
  instead of a failure.
- For anything decode/wire-related: is there at least one malformed/attacker-controlled
  input case, or does every test only round-trip bytes this same code encoded?

## Scope check

- If this PR adds a new reverse-dependency on `core/sdk` or another crate, note the blast
  radius (e.g. it now reruns on every core/sdk PR) even if not fixable here - call it out as
  a known cost rather than silently accepting it.

## Output

One finding per line: `[sev] file:line - problem. Fix: action.` `sev` is `critical` (blocks
merge: correctness, data loss, security, client livelock), `warning` (real defect or perf
hit), or `nit` (style). End with `Verdict: APPROVE | REQUEST CHANGES - reason`.

## Provenance

Every bullet above traces to an actual human reviewer comment on a merged/reviewed Kafka
gateway PR in apache/iggy (not generic advice): PR #4263, #4270, #4043 (error-code
livelock), #4258, #4259 (cumulative caps, batch-deadline-drops-results), #3519 (-1
sentinels, docs contradiction), #4229 (alloc capacity, unbounded-alloc-from-wire-count),
#4282 (budget refund), #4222 (auth path), #4245 (test quality). Re-derive this list with
`gh pr list --repo apache/iggy --state merged --search "kafka gateway"` plus
`gh api repos/apache/iggy/pulls/<N>/comments --paginate` if this skill needs updating after
new PRs land.
