# Template source connector

Starting point for a new Apache Iggy **source** connector. Everything except
talking to your actual external system is already implemented and follows
the project's required resilience/security patterns — see the module-level
doc comment at the top of `src/lib.rs` for the full rationale, and the
`connector-pr-review` skill and `core/connectors/docs/authoring-sinks-and-sources.md`
for the checklist this template is built against.

## Delivery Semantics

The framework around `fetch_records()` gives you retry (Path A: `max_retries`
total attempts), a circuit breaker, and the ACK/NACK cursor handshake described
below. Everything past "what fetches" depends on what you write in
`fetch_records()` — fill in the blanks with your real behavior before
shipping:

1. **Transient failure**: on Path A the HTTP client retries up to `max_retries`
   total attempts, then `poll()` returns the error and the circuit breaker counts
   it. On Path B the error is returned at once. Either way the runtime polls
   again after `poll_interval`.
2. **Permanent failure** (auth rejected, schema gone): return
   `Error::PermanentHttpError` from `fetch_records()` — `poll()` does not
   trip the circuit breaker on it, since a bad config/auth state isn't a
   connectivity signal the breaker should react to.
   A record that cannot be converted is different: set
   `FetchedRecord::payload` to `Err(Error::InvalidRecordValue(..))`. `poll()`
   sends the records before it and stops, and never moves the cursor past it. A
   bad record at the front of a batch fails the poll every `poll_interval` until
   the cause is fixed, so the problem stays visible instead of the record being
   lost. Skipping a record is a policy to choose on purpose, with a log line and
   a counter, never a silent `continue`.
3. **Duplication window**: at-least-once. `poll()` stages the cursor as
   *candidate* state; the runtime persists it only after the batch is sent,
   then reports `Ack`/`Nack`. A NACK (or a crash between send and the
   persisted-state write) redelivers the same range on the next poll.
4. **Dedup key**: set `FetchedRecord::natural_id` from a true per-record
   identity (row PK, document `_id`, event UUID) whenever your source has
   one — it becomes `ProducedMessage.id`, which a downstream sink can key an
   idempotent upsert on (see `mongodb_sink`) so the redelivery above doesn't
   produce a duplicate at the destination. Leave it `None` only when records
   genuinely have no natural identity (a pure generator). See
   `connector-pr-review` B5.

## What's already done for you

- Config parsing with `#[serde(deny_unknown_fields)]` so a typo in a TOML
  file fails loudly instead of silently doing nothing.
- Config validation in `open()` (not `new()`, which has no way to return an
  error).
- `connection_string` and the optional `auth_token` field both typed as
  `SecretString`, since either can carry credentials.
- A retry-wrapped HTTP client (`iggy_connector_sdk::retry::build_retry_client`)
  and a startup connectivity probe with its own backoff
  (`check_connectivity_with_retry`).
- A `CircuitBreaker` that's actually consulted before polling and updated
  after every attempt — not just constructed and forgotten.
- Cursor staging: `poll()` never commits its progress directly; it stages a
  candidate and `on_batch_result()` commits it only on `Ack`, discarding it
  on `Nack` so a failed delivery gets re-polled instead of silently lost.
- The `source_connector!` FFI macro invocation and a `Cargo.toml` with the
  right `crate-type`, workspace-pinned dependencies, and license header.
- `verbose_logging: Option<bool>` upgrading the per-poll log line from
  `debug!` to `info!`, mirroring the runtime's own `verbose` flag.
- The six canonical state/Ack-Nack tests, plus config validation and the
  `verbose_logging` flag.

## Choose your backend path

Pick exactly one path. They do not mix. Both keep the circuit breaker, the
sleep-first `poll()`, `stage_batch()`, the Ack/Nack cursor handshake and the
state handling, because none of that depends on the transport.

### Path A: HTTP backend

Keep the crate as it is. `connection_string` is the base URL, `self.client` is
a `reqwest` client wrapped by the SDK retry middleware (it retries a transient
failure up to `max_retries` total attempts), and `open()` probes
`health_check_path` with `check_connectivity_with_retry`.

### Path B: non-HTTP backend (database, queue, filesystem)

Replace the HTTP client and nothing else:

1. `Cargo.toml`: remove `reqwest` and `reqwest-middleware`, add your driver.
2. `src/lib.rs`:
   - Change the type of the `client` field from `Option<ClientWithMiddleware>` to
     your driver's client, which must implement `Debug`, and change the `client`
     parameter of `fetch_records()` to match.
   - Make `build_raw_client()` return that client and store it in `self.client`
     in `open()`.
   - In `open()` delete the `health_check_path` block (`Url::parse`,
     `RetryPolicy`, `check_connectivity_with_retry`) and the `build_retry_client`
     call. Probe the backend with your driver instead (a ping or a test query)
     and map a failure to `Error::Connection`.
   - Delete the imports that are left unused (`Url`, `ClientWithMiddleware`,
     `RetryPolicy`, `build_retry_client`, `check_connectivity_with_retry`).
3. Config: delete `health_check_path`, `max_retries`, `retry_delay`,
   `retry_max_delay`, `max_open_retries` and `open_retry_max_delay` everywhere
   they appear: the config field, the `retry_delay` field on `TemplateSource`,
   the `DEFAULT_*` consts, `config.toml`, the copy under
   `runtime/example_config/connectors/`, the table in this README, `test_config()`
   and the JSON in `given_numeric_knobs_as_json_numbers_should_deserialize`.
   Delete `timeout` the same way unless you apply it to your driver. Keep
   `poll_interval`, `batch_size` and `circuit_breaker_*`.
4. A failed fetch is retried by the next poll, paced by `poll_interval` and the
   circuit breaker. Add a retry loop inside `fetch_records()` only if your
   driver needs one.
5. Map driver errors onto the SDK variants: `Connection` or `HttpRequestFailed`
   for a transient failure, and an `Error::is_permanent()` variant
   (`SchemaMismatch`, `PermanentHttpError`) for one that will never succeed. The
   breaker is not touched by a permanent error.

See `core/connectors/sources/postgres_source` for a finished non-HTTP source.

## What you need to fill in

Search for `TODO(ConnectorDeveloper)` in `src/lib.rs`. One spot requires code
and one is documentation:

1. **`fetch_records()`** (required) - fetch up to `self.batch_size` new
   records from your system, ordered after `cursor` (`None` = start from the
   beginning, or from "now" - whichever fits your source). Map each result
   to a `FetchedRecord { cursor_value, natural_id, payload }`, using something
   monotonically increasing as `cursor_value` (a timestamp, an ID, a page
   token) - that's what lets the cursor-staging logic advance correctly.
2. **`connection_string`'s doc comment** (documentation only) - describe
   the exact shape your connector expects instead of the generic example.

## Using it

1. Copy this directory, rename it and the package in `Cargo.toml`
   (`iggy_connector_<yourname>_source`), and add it to the `members` list in
   the workspace root `Cargo.toml`.
2. Choose a backend path above, then fill in `fetch_records()`.
3. Update `config.toml` with your real `connection_string` and any
   settings specific to your system; delete `auth_token` if you don't need
   it, or add fields of your own the same way (see `TemplateSourceConfig`).
4. `cargo build --release -p iggy_connector_<yourname>_source`, point a
   runtime connector config file's `path` at the built `.so`/`.dylib`/`.dll`,
   and run the connector runtime — see `core/connectors/README.md` in this
   repo for the full runtime quick-start.
5. Before opening a PR: `cargo test`, `cargo clippy --all-targets`,
   `cargo fmt --check`, and re-read the connector-review checklist once more
   with fresh eyes — most review round-trips come from one of the items in
   that list, not from the connector-specific logic in `fetch_records()`.
