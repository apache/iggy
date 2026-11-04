# Template sink connector

Starting point for a new Apache Iggy **sink** connector. Everything except
pushing data to your actual destination is already implemented and follows
the project's required resilience and security patterns. See the module-level
doc comment at the top of `src/lib.rs` for the rationale, and the
`connector-pr-review` skill and the "Building Connectors That Pass Review"
blog post for the checklist this template is built against.

## How a batch is delivered

Read this before you write `push_batch()`. It is the contract the template is
built on, and it is not the one most message consumers expect.

The runtime polls Iggy for the sink using deferred (long) polling. **The server
stores the consumer offset when it answers a poll, before your plugin sees the
messages.** The runtime then calls `consume()`, and if that returns an error it
logs and counts it and moves on to the next batch. It never fetches the failed
batch again. So:

- From Iggy to your plugin, delivery is **at-most-once**. The only second
  chance a batch gets is whatever your plugin does inside `consume()`.
- The template therefore keeps trying one batch for up to `max_delivery_time`
  (default `20s`) before it gives up.
- Giving up loses that batch from the destination's point of view. The messages
  are still in the Iggy topic, and the log line says which offsets to replay.

What `consume()` does with each chunk of up to `batch_size` messages, in order:

1. If the circuit breaker is open, it waits for the cool-down instead of
   refusing the batch. Nothing reaches the destination while it waits, and the
   runtime does not poll Iggy again until `consume()` returns.
2. It calls `push_batch()`.
3. A transient error (`HttpRequestFailed`, `Connection`, `CannotStoreData`)
   counts as one failed round toward the breaker. It waits with jittered
   exponential backoff (`retry_delay` up to `retry_max_delay`) and tries again.
4. A permanent error (`PermanentHttpError`) skips that chunk, counts it and goes
   on to the next one. It never touches the breaker, because bad data says
   nothing about whether the destination is up.
5. Any other error is not retried. The chunk is counted as failed, the error is
   returned at the end, and the remaining chunks are still tried.
6. When `max_delivery_time` runs out on a transient error, `consume()` stops,
   does not attempt the remaining chunks, counts every undelivered message as
   failed and logs, at `error` level, the stream, topic, partition and offset
   range it gave up on.

The budget covers the whole `consume()` call, not each chunk.

## Delivery semantics

Every connector README has to answer these four questions. For this template:

1. **Transient failure**: the chunk is retried with backoff for up to
   `max_delivery_time`. After that `consume()` returns the error and the
   messages from that chunk onward are lost to the destination. The runtime logs
   the error and keeps polling.
2. **Permanent failure** (bad schema, the destination rejects the payload
   shape): that chunk is skipped and counted. The breaker is not affected.
3. **Duplication window**: at-most-once from Iggy to the plugin, for the reason
   above. Duplicates come from your own retries (a request that failed part way
   through a write), from replaying an offset range after a logged loss, and
   from a server failover before the stored offset replicated.
4. **Dedup key**: none by default, because `push_batch()` is a stub. The
   `dedup_key()` helper builds `stream:topic:partition:offset`. Make your
   destination's write idempotent on it (upsert, `ON CONFLICT`, unique index),
   and the first three points stop mattering for correctness. See
   `connector-pr-review` B5.

### Why the key uses the offset

Partition plus offset names one position in one partition's log for good. A
redelivered or replayed message always gets the same key, and two different
messages never share one. `message.id` is a worse default because producers can
choose it, and two different messages with the same id would overwrite each
other in the destination, which is silent data loss. Use `message.id` instead
only when your producers guarantee unique ids and you also want duplicates
produced at different offsets collapsed.

### Recovering after a logged loss

The error log names the partition and an offset range such as `1200..=1299`.
Move the consumer group's offset on that partition back to the first offset and
let the connector run. Because the writes are idempotent, anything that did reach
the destination is overwritten with identical data.

## Configuration

Plugin settings go under `[plugin_config]` in `config.toml`. Unknown keys are
rejected when the config is loaded.

| Setting | Default | Meaning |
| --- | --- | --- |
| `connection_string` | required | Destination URL or DSN. A `SecretString`. |
| `target` | required | Table, index or collection name, validated in `open()`. |
| `auth_token` | none | Optional secret, e.g. a bearer token. |
| `health_check_path` | none | Path probed in `open()`. The probe is skipped when unset. |
| `batch_size` | `100` | Messages per `push_batch()` call. |
| `timeout` | `30s` | Per-request timeout of the HTTP client. |
| `max_retries` | `3` | Total attempts per request in the HTTP retry middleware. |
| `retry_delay` | `500ms` | Base backoff, for both the middleware and the delivery rounds. |
| `retry_max_delay` | `5s` | Backoff cap. |
| `max_open_retries` | `10` | Total attempts of the startup probe. |
| `open_retry_max_delay` | `60s` | Backoff cap of the startup probe. |
| `circuit_breaker_threshold` | `3` | Failed rounds in a row before the breaker opens. |
| `circuit_breaker_cool_down` | `10s` | How long the breaker stays open. |
| `max_delivery_time` | `20s` | Budget for one `consume()` call. |
| `verbose_logging` | `false` | Per-batch log line at `info` instead of `debug`. |

Two rules `open()` enforces beyond parsing:

- `circuit_breaker_cool_down` must be shorter than `max_delivery_time`. A call
  that meets an open breaker could otherwise never reach a retry.
- `connection_string` must not be empty, and `target` must pass the identifier
  check.

### How many requests the destination sees

While the destination is down, the SDK breaker closes completely after each
cool-down and needs `circuit_breaker_threshold` new failures to open again. With
the defaults that is at most 3 failed rounds per cool-down cycle, each round
being up to `max_retries` (3) HTTP attempts: at most 9 requests per cycle, about
one every two seconds on average, in short bursts. Nothing is sent while the
breaker is open.

### Stream settings

Batching from Iggy is set in the `[[streams]]` entry, not in the plugin:

| Setting | Template value | Meaning |
| --- | --- | --- |
| `batch_length` | `100` | Most messages in one poll. |
| `min_count` | `100` | A poll returns once this many messages are ready. |
| `max_wait` | `2s` | A poll returns after this long even if fewer are ready. |

Waiting for a full batch means fewer, larger writes, and an idle topic costs one
request to the server per `max_wait`. The cost is up to `max_wait` of latency on
a quiet topic, and a bigger batch is a bigger loss if the destination is down for
longer than `max_delivery_time`. `min_count` must not exceed `batch_length`,
`max_wait` must be above zero, and the server caps it at 30 s. `poll_interval`
no longer exists for sinks; the runtime refuses to start a sink that sets it.

## Known limits

- **A crash between the poll and the write loses that batch.** The offset is
  already stored. Only an idempotent replay recovers it.
- **A long outage loses one batch per `max_delivery_time`.** Each give-up is
  logged with its offsets. True zero loss needs a runtime change, such as storing
  the offset only after `consume()` succeeds. That is not part of this template.
- **`consume()` blocks a runtime worker thread.** The call is synchronous inside
  the runtime, and the runtime has one worker per CPU core by default. If as many
  sink topics as there are cores wait on a dead destination at once, the HTTP
  API, sources and other sinks stall until the budgets run out. Keep
  `max_delivery_time` short, and do not point more topics at one failing
  destination than the machine has cores.
- **A stop waits for the call in progress.** Closing the plugin waits for an
  in-flight `consume()`, up to the remaining `max_delivery_time`. The runtime's
  own close budget is the poll timeout plus 60 s, so 20 s fits inside it.
- **The SDK breaker is not a single-probe breaker.** It resets fully after the
  cool-down, which is why `circuit_breaker_threshold` defaults to 3.

## What's already done for you

- Config parsing with `#[serde(deny_unknown_fields)]`, so a typo in a TOML file
  fails loudly instead of doing nothing.
- Config validation in `open()` (`new()` has no way to return an error),
  including `target` against an allowlist pattern before it can reach a query,
  path or URL, and the cool-down versus budget rule above.
- `connection_string` and `auth_token` typed as `SecretString`.
- A retry-wrapped HTTP client (`iggy_connector_sdk::retry::build_retry_client`)
  and a startup connectivity probe with its own backoff.
- The delivery loop described above, with the breaker used as a pacing device and
  failures recorded once per round.
- Batching: `consume()` chunks the incoming messages by `batch_size`.
- Failure accounting: written and failed counters, and an `error` log with the
  offset range of anything that was given up on or skipped.
- `dedup_key()`, a stable per-message key.
- The `sink_connector!` FFI macro and a `Cargo.toml` with the right
  `crate-type`, workspace-pinned dependencies and license header.
- `verbose_logging` upgrading the per-batch log line from `debug!` to `info!`.
- Unit tests for config validation, the delivery loop (recovery, giving up,
  attempt bounds, no calls while the breaker is open, permanent and unclassified
  errors), the cool-down rule and the dedup key.

## What you need to fill in

Search for `TODO(ConnectorDeveloper)` in `src/lib.rs`. One spot requires code,
and two more are conditional or documentation:

**`push_batch()`** (required): send one chunk to your destination using
`self.config.connection_string` and `self.config.target` (already validated), via
`self.client` (already retry-wrapped). The error you return steers the loop:
transient errors are retried, `PermanentHttpError` skips the chunk, anything else
is not retried. Use `message.payload.try_to_bytes()` and do not clone
`Payload::Json`. If the destination's call can succeed at the transport level
while rejecting rows (a bulk API answering 200 with per-item errors), count the
accepted rows yourself and return an error when none were accepted. Do not return
`Ok(())` for a batch that was not written.

**`connection_string`'s doc comment** (documentation only): describe the exact
shape your connector expects.

**`build_raw_client()`** (only if the destination is not HTTP): swap
`reqwest::Client` for your driver's connection or pool, store it on `TemplateSink`
in place of the HTTP-specific `client` field, and adjust the
`check_connectivity_with_retry` call in `open()` to whatever probe your driver
offers. See `core/connectors/sinks/postgres_sink` or
`core/connectors/sinks/s3_sink` for non-HTTP examples.

## Using it

1. Copy this directory, rename it and the package in `Cargo.toml`
   (`iggy_connector_<yourname>_sink`), and add it to the `members` list in the
   workspace root `Cargo.toml`.
2. Fill in the `TODO(ConnectorDeveloper)` section(s).
3. Update `config.toml` with your real `connection_string` and `target` and any
   settings specific to your system. Delete `auth_token` if you don't need it, or
   add fields of your own the same way (see `TemplateSinkConfig`).
4. `cargo build --release -p iggy_connector_<yourname>_sink`, point a runtime
   connector config file's `path` at the built `.so`, `.dylib` or `.dll`, and run
   the connector runtime. See `core/connectors/README.md` for the runtime
   quick-start.
5. Before opening a PR: `cargo test`, `cargo clippy --all-targets`,
   `cargo fmt --check`, and re-read the `connector-pr-review` checklist. Most
   review round-trips come from one of its items, not from the connector-specific
   logic in `push_batch()`.
