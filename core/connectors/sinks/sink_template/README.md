# Template sink connector

Starting point for a new Apache Iggy **sink** connector. Everything except
pushing data to your actual destination is already implemented and follows
the project's required resilience/security patterns — see the module-level
doc comment at the top of `src/lib.rs` for the full rationale, and the
`iggy-connector-review` skill / the "Building Connectors That Pass Review"
blog post for the checklist this template is built against.

## What's already done for you

- Config parsing with `#[serde(deny_unknown_fields)]` so a typo in a TOML
  file fails loudly instead of silently doing nothing.
- Config validation in `open()` (not `new()`, which has no way to return an
  error) — including validating `target` (the destination table/index/
  collection name) against an allowlist pattern *before* it can ever reach a
  query, path, or URL.
- `connection_string` and the optional `auth_token` field both typed as
  `SecretString`, since either can carry credentials.
- A retry-wrapped HTTP client (`iggy_connector_sdk::retry::build_retry_client`)
  and a startup connectivity probe with its own backoff
  (`check_connectivity_with_retry`).
- A `CircuitBreaker` that's actually consulted before each `consume()` call
  and updated once per call based on the outcome — not just constructed and
  forgotten, and not reset mid-batch by a partial success.
- Batching: `consume()` chunks the incoming messages by a configurable
  `batch_size` instead of sending everything in one unbounded request.
- The `sink_connector!` FFI macro invocation and a `Cargo.toml` with the
  right `crate-type`, workspace-pinned dependencies, and license header.
- Tests for config/identifier validation and the circuit-breaker short-circuit
  path.

## What you need to fill in

Search for `TODO(Developer)` in `src/lib.rs` — there is exactly one spot:

**`push_batch()`** — build the request/write that actually sends one chunk
of messages to your destination, using `self.config.connection_string` (and
`self.config.target`, already validated by the time this runs) via
`self.client` (already retry-wrapped). Distinguish permanent failures (bad
schema, a destination that will reject this payload shape no matter how many
times you retry) from transient ones (network error, 5xx, timeout) by
returning `Error::PermanentHttpError` for the former — see the doc comment on
that variant for why the distinction matters to the circuit breaker.

If your destination isn't HTTP, also revisit **`build_raw_client()`**: swap
the `reqwest::Client` for your driver's connection/pool setup (see
`core/connectors/sinks/postgres_sink` or `core/connectors/sinks/s3_sink` for
non-HTTP examples), store it on `TemplateSink` in place of the HTTP-specific
`client` field, and adjust or remove the `check_connectivity_with_retry` call
in `open()` in favor of whatever connectivity check your driver offers.

## Using it

1. Copy this directory, rename it and the package in `Cargo.toml`
   (`iggy_connector_<yourname>_sink`), and add it to the `members` list in
   the workspace root `Cargo.toml`.
2. Fill in the `TODO(Developer)` section(s).
3. Update `config.toml` with your real `connection_string` and `target`, and
   any settings specific to your system; delete `auth_token` if you don't
   need it, or add fields of your own the same way (see
   `TemplateSinkConfig`).
4. `cargo build --release -p iggy_connector_<yourname>_sink`, point a
   runtime connector config file's `path` at the built `.so`/`.dylib`/`.dll`,
   and run the connector runtime — see `core/connectors/README.md` in this
   repo for the full runtime quick-start.
5. Before opening a PR: `cargo test`, `cargo clippy --all-targets`,
   `cargo fmt --check`, and re-read the connector-review checklist once more
   with fresh eyes — most review round-trips come from one of the items in
   that list, not from the connector-specific logic in `push_batch()`.
