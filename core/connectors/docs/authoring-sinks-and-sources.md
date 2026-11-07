# Authoring Apache Iggy sink and source connectors

> Reference guide for `core/connectors/` docs. For the narrative version
> suitable for the Apache Iggy blog, see [`BLOG_POST.md`](../BLOG_POST.md).
> Companion materials:
>
> - Review checklist skill: `.claude/skills/connector-pr-review/SKILL.md`
> - Sink template: `core/connectors/sinks/sink_template` (README: HTTP and non-HTTP paths)
> - Source template: `core/connectors/sources/source_template` (same)
> - Testing skill: `.claude/skills/connector-testing/SKILL.md`

Connector PRs in Apache Iggy often spend multiple review rounds on the
**same classes of issues**: secrets handling, retry/error mapping,
idempotency claims that the backend does not enforce, README defaults
that disagree with code, and tests that look green while missing the
mandatory source-state suite.

This guide turns those review patterns into a strict authoring path.
Follow it and a first review can focus on the backend-specific parts
(connection + fetch/push) instead of re-teaching the framework.

## The only code you should invent

Copy the closest existing plugin by shape:

| Shape | Start from |
| ----- | ---------- |
| DB write sink | `core/connectors/sinks/postgres_sink` |
| HTTP sink | `core/connectors/sinks/http_sink` |
| Polling DB source | `core/connectors/sources/postgres_source` |
| Minimal source + state tests | `core/connectors/sources/random_source` |
| Minimal sink | `core/connectors/sinks/stdout_sink` |
| Fill-in-the-blank sink | `core/connectors/sinks/sink_template` |
| Fill-in-the-blank source | `core/connectors/sources/source_template` |

`sink_template` / `source_template` are compiling, tested starting
points: config parsing, `SecretString`, retry loops, circuit breaker,
the time-budgeted delivery loop (sink), ACK/NACK cursor staging (source),
sleep-first poll, lock discipline, and unit-test stubs are already
wired up. Copy the crate, rename the package, and fill in the
`TODO(ConnectorDeveloper)` spots in `src/lib.rs` - see each crate's `README.md`,
which has one section for an HTTP backend and one for a non-HTTP backend.

**Your implementation surface is intentionally small:**

1. Build a client from `connection_string` (secret).
2. Probe connectivity in `open()`.
3. **Sink:** push one batch.
4. **Source:** fetch the next batch and compute the next cursor.

Everything else should match siblings.

## Hard rules (review blockers)

### Secrets

- Credentials are `secrecy::SecretString`, which hides the value in `Debug` output. It has no `Display` impl, so `{}` does not compile.
  The `iggy_common::serde_secret::serialize_secret` attribute the plugins add only
  lets the config derive `Serialize`: it writes the plaintext, so never serialize
  a config into logs, errors or state. Use `serialize_redacted` where a redacted
  copy is needed.
- Never log connection strings, API keys, or tokens.
- Never persist URL userinfo / bearers into source state files.

### Errors and retries

- `max_retries` means **total attempts** (default **3**). README must match.
- Map auth/schema/bad-data failures to permanent variants
  (`PermanentHttpError`, `SchemaMismatch`, `InvalidRecordValue`).
  `Error::is_permanent()` is the list the runtime and the templates act on.
  `InitError` is not on it.
- Map network/5xx/retryable SQLSTATE to transient variants.
- Do **not** decide retryability by substring-matching `err.to_string()`.
- Cap backoff so a dead backend cannot stall shutdown. A sink's whole
  `consume()` call needs one time budget, because it blocks a runtime
  worker thread and a stop waits for it (see below).

### Delivery semantics (document in README)

Every plugin README needs a short paragraph that answers:

1. What happens on transient failure?
2. What happens on permanent failure?
3. What is the duplication window?
4. What is the dedup key (or “none”)?

Do not claim “no data loss” or “exactly once” unless the backend unique
key **and** the runtime path actually enforce it.

The two sides of a connector differ:

- **Sources** are at-least-once unless `exactly_once = true` is set. State is
  saved after the Iggy send, and a failed batch is re-polled.
- **Sinks** are at-most-once from Iggy by default. The server stores the
  consumer offset when it answers the poll, before the plugin sees the
  messages. The runtime logs and counts an `Err` from `consume()` and
  fetches the next batch; it never redelivers the failed one. Return `Err`
  for a failed batch so it is logged and counted, but do not expect it to
  be retried. The only second chance a batch gets is the retrying the
  plugin does itself.
- A sink whose connector config has `[delivery] guarantee =
  "at_least_once"` is at-least-once instead. The runtime stores the offset
  of a partition only after the plugin proved the messages delivered, hands
  a failed call over again, and stops delivery of that topic at the offset of a permanent
  failure. See [Sink delivery guarantees](../runtime/README.md#sink-delivery-guarantees).
  A sink configured for it with a plugin that does not support it refuses
  to start.

### Source-specific

Since #3855, sources use a formal one-in-flight-batch ACK/NACK
handshake, not just a convention: `poll()` returns *candidate* state;
the runtime sends the batch, saves that state only on success, then
calls `on_batch_result(SourceBatchResult::Ack | Nack)`. Only
`on_batch_result` may commit staged work, and the SDK will not call
`poll()` again until it returns.

- `async fn poll(&self)` with state behind `tokio::sync::Mutex`.
- **Never move the cursor past a record you could not convert.** Send the
  records before it, stop, and fail the poll when it is the first record.
  Skipping is a policy to choose on purpose, with a log line and a counter.
  A `continue` that drops the record while the cursor takes the next record's
  value loses it silently, and one that leaves the cursor behind a bad last
  record re-fetches it forever.
- **Sleep first**, then fetch.
- Brief lock → drop → I/O → brief lock write.
- **Stage, don't commit, in `poll()`.** Cursor advances and destructive
  work (delete-after-read, mark-processed) go into a `pending_state` /
  staged field, applied only in `on_batch_result()` on `Ack`; a `Nack`
  discards the staged work so the same range is redelivered.
- Always return state for a poll that made progress; return `state:
  None` for an empty poll that made none.
- The default `on_batch_result` is a no-op - override it (and stage
  accordingly) whenever `poll()` advances a cursor or does destructive
  work. Returning `Err` from it stops the SDK from polling further.
- Ship the **six canonical tests**: four state tests (restore /
  no-state / invalid-state / round-trip) plus two ACK/NACK tests
  (commit-on-Ack, keep-committed-on-Nack) if `on_batch_result` is
  overridden. Copy `random_source`.

### Sink-specific

- `async fn consume(&self, …)`.
- Never convert a failed batch into bare `Ok(())`.
- **Keep trying a batch within one time budget.** Retry transient errors
  with jittered backoff until a per-call budget (`max_delivery_time`,
  template default `20s`) runs out. Keep it well under a minute: the call
  blocks a runtime worker thread (one per CPU core by default), and a stop
  waits for it. Share the budget across the chunks of one call, and cut off an
  attempt still running when it runs out (`tokio::time::timeout` on the remaining
  budget), because the retry middleware's own timeouts and `Retry-After` sleeps
  can otherwise run past it.
- **Use the circuit breaker to pace attempts, not to discard batches.**
  While it is open, wait for the cool-down instead of returning an error,
  because a refused batch is gone for good. Record one failure per failed
  round, and keep the cool-down shorter than the budget.
- **When you give up, say what was lost.** Log the stream, topic,
  partition and offset range, count the messages as failed, and stop
  instead of attempting the rest of the call. At most once, an operator
  can replay that range from Iggy. At least once, the runtime hands the
  failed call over again.
- Never open the breaker for a permanent error. A data problem says
  nothing about whether the destination is up. At most once, skip that
  error's chunk. At least once, fail the call instead, because the runtime
  stores the offset past every message of a successful call and a skipped
  chunk would be lost for good. An unclassified error also stops the call at
  its chunk, so no later chunk is written before the runtime redelivers the batch.
- **Decide on at-least-once.** `enable_at_least_once()` runs when the sink
  is configured for it, and the default returns `Error::Unsupported`, so a
  sink refuses the mode until it overrides the method. Accept only if a
  successful `consume()` proves the destination has every message, as it
  does for a sink that writes inside `consume()`, and fail the call for a
  permanent rejection instead of skipping it. A sink that buffers across
  calls also implements `flush()` and keeps buffers per topic and partition.
- **Make the destination idempotent.** Retries, replays and a server
  failover all repeat messages. Key writes on `stream:topic:partition:offset`
  (or a natural backend key), which names a log position for good and never
  collides across different messages. Use `message.id` only if your
  producers guarantee unique ids, because producers can reuse one. No random
  UUIDs per emit.
- Batching comes from the sink's `[[streams]]` entry: `batch_length`,
  `max_wait` (default `1s`) and `min_count` (default 1, at most
  `batch_length`). The template waits for full batches (`min_count =
  batch_length`, `max_wait = "2s"`) so the destination gets fewer, larger
  writes. They replace `poll_interval`, and a sink that sets it fails to
  start.
- Use `payload.try_to_bytes()` - do not clone `Payload::Json`.

### Config canon

| Concept | Field name |
| ------- | ---------- |
| Timeout | `timeout` |
| Retry attempts | `max_retries` |
| Base backoff | `retry_delay` |
| Backoff ceiling | `retry_max_delay` |
| Breaker | `circuit_breaker_threshold`, `circuit_breaker_cool_down` |
| Sink delivery budget | `max_delivery_time` |
| Source cadence | `poll_interval` |
| Sink batching (stream entry) | `max_wait`, `min_count` |
| Verbosity | `verbose_logging` |

Use `Option<T>` + humantime strings for durations, apply defaults in
`new()`; a bad duration falls back to 1 s with a `warn!`, and prefer
`#[serde(deny_unknown_fields)]` so typo’d knobs fail loud.

Validate structure and connectivity in `open()`, never on the
first hot-path call after sleep.

## Required artifacts in the PR

1. Plugin crate with `[lib] crate-type = ["cdylib", "lib"]`.
2. Unit tests (BDD `given_X_when_Y_should_Z` / `given_X_should_Y`).
3. Sources: four canonical state tests, plus two ACK/NACK tests if `on_batch_result` is overridden.
4. External backend: real-infra integration test under
   `core/integration/tests/connectors/<backend>/` (`#[iggy_harness]` +
   testcontainers or wiremock). No false-green mocks.
5. Row in `sinks/README.md` or `sources/README.md`.
6. Sample TOML under `runtime/example_config/connectors/` with plugin
   path `target/release/lib…`.
7. README defaults **byte-equal** to code consts.
8. Honest delivery-semantics paragraph.

## Verification before `/ready`

```bash
cargo fmt --all
cargo sort --no-format --workspace
cargo clippy -p iggy_connector_<name>_{sink|source} --all-targets -- -D warnings
cargo test -p iggy_connector_<name>_{sink|source}
# if applicable:
cargo test -p integration -- connectors::<backend>::
```

Keep `Cargo.lock` churn limited to this plugin’s dependencies. Do not
touch unrelated foreign SDKs in the same PR.

## Pre-flight checklist (paste into the PR)

```text
[ ] SecretString on all credentials; no secret logs/state
[ ] consume/poll never returns Ok(()) after a failed batch
[ ] Transient vs permanent errors mapped (no Display substring matching)
[ ] Sources: a record that cannot be converted never lets the cursor pass it (stop before it, or skip it on purpose with a log and a counter)
[ ] Sinks: one delivery time budget per consume(); breaker paces attempts and never discards a batch; cool-down shorter than the budget; offset range logged when giving up
[ ] Sinks: writes idempotent on stream:topic:partition:offset (or a natural key); no random UUID per emit
[ ] Sinks: at-least-once either left refused (the default) or supported (a permanent rejection fails the call and is never skipped, buffered writes proved by flush)
[ ] README delivery semantics paragraph present and honest
[ ] README defaults match code consts
[ ] deny_unknown_fields on plugin config
[ ] Canonical knob names (timeout, max_retries, retry_delay, max_delivery_time for sinks, poll_interval for sources)
[ ] Sources: 4 state tests (+2 ACK/NACK if on_batch_result overridden); sleep-first poll; state staged in poll, committed only in on_batch_result
[ ] External backend: real-infra integration test (not a lying mock)
[ ] example_config + sinks/sources README row
[ ] fmt / sort --no-format / clippy -D warnings / unit tests green
[ ] Cargo.lock churn limited to this crate's deps
```

Agents and reviewers: load `.claude/skills/connector-pr-review/SKILL.md`
and treat unchecked blockers as CHANGES_REQUESTED.

## Why this exists

Across recent connector PRs (SurrealDB, Meilisearch, OpenSearch, Quickwit
convention bring-up, MySQL/JDBC, Doris retry, Redshift, Airflow, Fluss,
and others), the expensive review threads were rarely about the novel
backend call. They were about framework contracts: secrets, retries,
idempotency honesty, config names, README drift, and missing state
tests. Codifying those contracts as a skill + templates is how we make
connector contributions predictable without lowering the bar.
