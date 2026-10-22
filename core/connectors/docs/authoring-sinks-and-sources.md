# Authoring Apache Iggy sink and source connectors

> Reference guide for `core/connectors/` docs. For the narrative version
> suitable for the Apache Iggy blog, see [`BLOG_POST.md`](../BLOG_POST.md).
> Companion materials:
>
> - Review checklist skill: `.claude/skills/connector-pr-review/SKILL.md`
> - Sink fill-in-the-blank kit: `.claude/skills/connector-sink/TEMPLATE.md`
> - Source fill-in-the-blank kit: `.claude/skills/connector-source/TEMPLATE.md`
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
`last_err` batch handling (sink), ACK/NACK cursor staging (source),
sleep-first poll, lock discipline, and unit-test stubs are already
wired up. Copy the crate, rename the package, and fill in the
`TODO(ConnectorDeveloper)` spots in `src/lib.rs` — see each crate's `README.md`.
The skills' `TEMPLATE.md` files describe the same pattern in prose, for
when copying a whole crate is more than you need.

**Your implementation surface is intentionally small:**

1. Build a client from `connection_string` (secret).
2. Probe connectivity in `open()`.
3. **Sink:** push one batch.
4. **Source:** fetch the next batch and compute the next cursor.

Everything else should match siblings.

## Hard rules (review blockers)

### Secrets

- Credentials are `secrecy::SecretString` with
  `iggy_common::serde_secret::serialize_secret`.
- Never log connection strings, API keys, or tokens.
- Never persist URL userinfo / bearers into source state files.

### Errors and retries

- `max_retries` means **total attempts** (default **3**). README must match.
- Map infra/auth/schema failures to permanent variants
  (`PermanentHttpError`, `InitError`, `SchemaMismatch`).
- Map network/5xx/retryable SQLSTATE to transient variants.
- Do **not** decide retryability by substring-matching `err.to_string()`.
- Cap backoff so a dead backend cannot stall shutdown.

### Delivery semantics (document in README)

Every plugin README needs a short paragraph that answers:

1. What happens on transient failure?
2. What happens on permanent failure?
3. What is the duplication window?
4. What is the dedup key (or “none”)?

Do not claim “no data loss” or “exactly once” unless the backend unique
key **and** the runtime path actually enforce it. Today’s runtime can
still advance consumer offsets in ways that punish swallowed `Ok(())`
after a failed sink batch — return `Err` for retryable failures.

### Source-specific

Since #3855, sources use a formal one-in-flight-batch ACK/NACK
handshake, not just a convention: `poll()` returns *candidate* state;
the runtime sends the batch, saves that state only on success, then
calls `on_batch_result(SourceBatchResult::Ack | Nack)`. Only
`on_batch_result` may commit staged work, and the SDK will not call
`poll()` again until it returns.

- `async fn poll(&self)` with state behind `tokio::sync::Mutex`.
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
- Process all batches; keep `last_err`; never convert a failed batch into
  bare `Ok(())`.
- Prefer stable dedup keys (`stream:topic:partition:message_id` or a
  natural backend key). No random UUIDs per emit.
- Use `payload.try_to_bytes()` — do not clone `Payload::Json`.

### Config canon

| Concept | Field name |
| ------- | ---------- |
| Timeout | `timeout` |
| Retry attempts | `max_retries` |
| Base backoff | `retry_delay` |
| Backoff ceiling | `retry_max_delay` |
| Source cadence | `poll_interval` |
| Verbosity | `verbose_logging` |

Use `Option<T>` + humantime strings for durations, apply defaults in
`new()` with `warn!` on parse failure, and prefer
`#[serde(deny_unknown_fields)]` so typo’d knobs fail loud.

Validate structure and connectivity in `new()` / `open()`, never on the
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
   path `../../target/release/lib…`.
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
[ ] consume/poll never returns Ok(()) after a failed batch that should retry
[ ] Transient vs permanent errors mapped (no Display substring matching)
[ ] Stable message / dedup IDs (no random UUID per emit)
[ ] README delivery semantics paragraph present and honest
[ ] README defaults match code consts
[ ] deny_unknown_fields on plugin config
[ ] Canonical knob names (timeout, max_retries, retry_delay, poll_interval)
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
