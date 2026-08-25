# Template source connector

Starting point for a new Apache Iggy **source** connector. Everything except
talking to your actual external system is already implemented and follows
the project's required resilience/security patterns — see the module-level
doc comment at the top of `src/lib.rs` for the full rationale, and the
`iggy-connector-review` skill / the "Building Connectors That Pass Review"
blog post for the checklist this template is built against.

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
- Tests for config validation and the Ack/Nack state-commit behavior.

## What you need to fill in

Search for `TODO(ConnectorDeveloper)` in `src/lib.rs` — there are exactly two spots:

1. **`build_raw_client()`** — if your source isn't HTTP, replace the
   `reqwest::Client` construction with your driver's connection/pool setup
   (see `core/connectors/sources/postgres_source` for a real non-HTTP
   example), store it on `TemplateSource` (you'll need to add a field —
   `client: Option<ClientWithMiddleware>` here is HTTP-specific), and adjust
   or remove the `check_connectivity_with_retry` call in `open()` in favor
   of whatever connectivity check your driver offers.
2. **`fetch_records()`** — fetch up to `self.batch_size` new records from
   your system, ordered after `cursor` (`None` = start from the beginning,
   or from "now" — whichever fits your source). Map each result to a
   `FetchedRecord { cursor_value, payload }`, using something monotonically
   increasing as `cursor_value` (a timestamp, an ID, a page token) — that's
   what lets the cursor-staging logic advance correctly.

## Using it

1. Copy this directory, rename it and the package in `Cargo.toml`
   (`iggy_connector_<yourname>_source`), and add it to the `members` list in
   the workspace root `Cargo.toml`.
2. Fill in the two `TODO(ConnectorDeveloper)` sections.
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
