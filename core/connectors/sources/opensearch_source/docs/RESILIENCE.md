# OpenSearch Source — Resilience & Delivery Semantics

Design reference for HTTP retry, circuit breaker, at-least-once delivery, and
backfill behavior. Phase 1 (retry + circuit breaker) is implemented in the
connector; later phases are documented for future work.

## Contents

- [HTTP retry and circuit breaker](#http-retry-and-circuit-breaker)
- [At-least-once delivery](#at-least-once-delivery)
- [Backfill gap](#backfill-gap)
- [Implementation phases](#implementation-phases)
- [Non-goals](#non-goals)

## HTTP retry and circuit breaker

### Problem

Each poll performs HTTP calls to OpenSearch (`HEAD` index exists at `open()`,
`POST /{index}/_search` at `poll()`). Without retry, transient `503`, `429`, or
network blips fail the poll immediately. The connector only retries on the next
`polling_interval` cycle.

### Constraint

InfluxDB uses `reqwest` + `build_retry_client` middleware. This connector uses
`opensearch-rs` (`TransportBuilder`). Retry is implemented at the **plugin level**
using `iggy_connector_sdk::retry` helpers (`exponential_backoff`, `jitter`,
`parse_retry_after`).

### Configuration (`plugin_config`)

| Field | Default | Purpose |
| ----- | ------- | ------- |
| `max_retries` | `3` | Total attempts per HTTP operation during `poll()` |
| `retry_delay` | `1s` | Base backoff between attempts |
| `retry_max_delay` | `30s` | Cap per retry wait |
| `max_open_retries` | `5` | Startup attempts for index-exists check in `open()` |
| `open_retry_max_delay` | `30s` | Cap for open probe backoff |
| `circuit_breaker_threshold` | `5` | Consecutive failures before circuit opens |
| `circuit_breaker_cool_down` | `60s` | Circuit open duration |

All fields are optional (`#[serde(default)]`); omitted keys use the defaults above.

### Retry policy

**Transient (retry):** network errors; HTTP `429`; HTTP `5xx`; `Retry-After` on `429`
when parseable.

**Permanent (no retry):** `400`, `401`, `403`, `404`; malformed responses;
search DSL errors.

### Circuit breaker

When open, `poll()` skips the search, logs a warning, sleeps `polling_interval`,
and returns an empty batch with `state: None` so the runtime does not persist a
new cursor (same pattern as InfluxDB source).

Consecutive poll failures (after retries exhausted) increment the breaker.
Successful search resets it.

## At-least-once delivery

### Contract

The connector provides **at-least-once** delivery toward Iggy. Duplicates can
occur when:

- The process crashes after `finalize_poll` advances the in-memory cursor but
  before the runtime persists `ConnectorState`.
- The connector restarts from a cursor behind the last published batch.

The runtime saves `ConnectorState` only after a successful Iggy send.

### Mitigations (future phases)

| Tier | Action | Status |
| ---- | ------ | ------ |
| A | Document contract + recommend consumer dedup on `_id` | Partial (README) |
| B | Set `ProducedMessage.id` from OpenSearch `_id` | Planned |
| C | Runtime ack before cursor advance | Deferred (FFI change) |

### Consumer guidance

- Dedup on OpenSearch `_id` plus index name, or a business key in `_source`.
- Treat replays as normal for log pipelines; use upsert sinks where needed.

## Backfill gap

### Contract

Pagination is forward-only on `(timestamp_field asc, _id asc)` via `search_after`.
After catch-up, documents indexed with `timestamp_field` **older** than the
current cursor are not read until connector state is reset.

### Operator modes

| Mode | Use case |
| ---- | -------- |
| Forward tail (default) | Live streaming; timestamps track ingest order |
| Bounded backfill | One-time load via time-window `query` |
| Full rescan | Reset runtime `ConnectorState` (duplicates possible) |

### Future phases

| Tier | Feature |
| ---- | ------- |
| B | `backfill.mode = warn` — log when index min timestamp < cursor |
| C | `backfill.mode = window` — explicit time-window scan |
| E | `min_timestamp` on fresh start only |

## Implementation phases

| Phase | Scope | Status |
| ----- | ----- | ------ |
| 1 | HTTP retry + circuit breaker | **Implemented** |
| 2 | At-least-once + backfill runbook in README | Partial |
| 3 | `ProducedMessage.id` from `_id`; backfill warn mode | Planned |
| 4 | Backfill window mode | Planned |
| 5 | Exactly-once / runtime ack | Deferred |

## Testing retry and circuit breaker

Two layers cover resilience behavior. Use both: plugin tests are fast and precise;
integration tests exercise the full connectors runtime + FFI poll loop.

### Plugin HTTP tests (fast, no Docker)

In-process axum mock server in `src/http_tests.rs`. Run:

```bash
cargo test -p iggy_connector_opensearch_source given_transient_search_errors_when_poll_should_retry_and_succeed
cargo test -p iggy_connector_opensearch_source given_circuit_breaker_open_when_poll_should_return_empty_without_error
```

These tests inject `503`/`500` responses and assert retry counts and circuit-breaker
skip behavior at the plugin `poll()` boundary.

### Integration tests (runtime + wiremock)

Fixtures in `core/integration/tests/connectors/fixtures/opensearch/resilience.rs`
start a [wiremock](https://github.com/LukeMathWalker/wiremock-rs) `MockServer` (no
OpenSearch container) and point the connector URL at it via runtime env overrides:

| Fixture | Behavior |
| ------- | -------- |
| `OpenSearchSourceTransientErrorFixture` | Two `503` search responses, then one hit |
| `OpenSearchSourceCircuitBreakerFixture` | Persistent `500` with fast retry settings |

Circuit-breaker **open/skip** semantics (empty poll, no cursor advance) are asserted in
plugin `http_tests.rs` because they are precise to time and poll count. The integration
fixture verifies the runtime keeps the source `Running`, performs HTTP retries, and does
not publish messages under sustained failures.

Run:

```bash
cargo test -p integration -- connectors::opensearch::opensearch_source_resilience
```

Tests live in `opensearch_source_resilience.rs` and verify message production after
retry, plus runtime `Running` status with no messages under sustained failures.

### Manual / staging checks

Point `url` at a proxy or fault-injection layer in front of a real cluster. Tune
`max_retries`, `retry_delay`, `circuit_breaker_threshold`, and `circuit_breaker_cool_down`
in `plugin_config`. Watch connector logs for `OpenSearch request failed; retrying` and
`circuit breaker is OPEN`.

## Non-goals

- Exactly-once delivery in the connector alone
- Parallel slice / PIT export (separate design)
- Automatic full-index rescan without operator intent
- Replacing `opensearch-rs` with raw `reqwest` unless plugin retry proves insufficient
