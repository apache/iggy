---
name: connector-pr-review
description: Review checklist for Apache Iggy connector sink/source PRs. Load when reviewing a connectors plugin PR, when authoring a new sink/source and wanting to pre-flight against common review blockers, or when diagnosing why a connectors PR is stuck in review. Encodes recurring review patterns mined from real apache/iggy connector PRs. NOT for runtime/SDK internals (use connector-runtime / connector-sdk).
---

# Connector PR review checklist

> Universal rules live in [connectors-overview](../connectors-overview/SKILL.md).
> Authoring skills: [connector-sink](../connector-sink/SKILL.md),
> [connector-source](../connector-source/SKILL.md),
> [connector-testing](../connector-testing/SKILL.md).
> Fill-in-the-blank kits: those skills' `TEMPLATE.md` files.

Use this skill to **catch the issues that repeatedly burn review cycles**
before asking for a human re-review. Cite symbols/paths, not stale line numbers.

## Contents

- [How to use](#how-to-use)
- [Blockers (must fix before merge)](#blockers-must-fix-before-merge)
- [High-frequency convention nits](#high-frequency-convention-nits)
- [Delivery semantics (document honestly)](#delivery-semantics-document-honestly)
- [PR / CI hygiene](#pr--ci-hygiene)
- [Pre-flight author checklist](#pre-flight-author-checklist)
- [Evidence base](#evidence-base)

## How to use

1. Load this skill for any PR under `core/connectors/sinks/` or `core/connectors/sources/`.
2. Walk **Blockers** first. Any hit is CHANGES_REQUESTED.
3. Then **Convention nits** and **Delivery semantics**.
4. End with **PR / CI hygiene** (cheap passes that still delay first review).
5. Prefer "copy the closest exemplar" over inventing new knobs.

## Blockers (must fix before merge)

### B1. Secrets

- [ ] Every credential field is `secrecy::SecretString` with
      `#[serde(serialize_with = "iggy_common::serde_secret::serialize_secret")]`.
- [ ] No connection string / API key / token in `info!`/`debug!`/`error!` /
      `format!` into SQL / file-state metadata.
- [ ] Source state must not persist URL userinfo or bearer tokens.

Plain `String` for a credential is a review-blocker. Pattern:
`PostgresSinkConfig::connection_string` in `sinks/postgres_sink`.

### B2. Swallowing `Err` while offsets advance (sinks)

- [ ] `consume()` must **not** catch a batch failure and return `Ok(())`.
- [ ] Prefer `last_err` pattern: process all batches, return the last transient
      error (see `connector-sink` Hard rules / TEMPLATE).
- [ ] README must not claim "no data loss" / strong idempotency unless the
      backend + runtime path actually enforce it.

Today the runtime can commit consumer offsets even when plugin errors are
poorly surfaced (#2927 / #2928 class issues). Authors must be honest about
loss windows instead of overselling.

### B3. Source cursor / side-effects staged for `on_batch_result`, not committed in `poll()`

Since #3855, this is an SDK-enforced contract, not just a convention:
`poll()` returns *candidate* state; the runtime sends the batch, saves
that state on success, then calls `on_batch_result(Ack | Nack)`. Only
`on_batch_result` may commit.

- [ ] `poll()` stages cursor changes and destructive work (delete-after-read /
      mark-processed / ACK upstream) - it does not mutate committed state or
      touch upstream rows directly. Committing in `poll()` leaves nothing to
      roll back on a Nack, defeating the point of the handshake.
- [ ] `on_batch_result(SourceBatchResult::Ack)` commits the staged work;
      `on_batch_result(SourceBatchResult::Nack)` discards it so the batch is
      redelivered against unchanged committed state. A source with no staged
      work (e.g. a pure generator) may rely on the SDK's default no-op impl.
- [ ] Returning `Err` from `on_batch_result` is deliberate - it stops the SDK
      from polling further rather than risk silently advancing past a failed
      rollback. Don't swallow a rollback failure into `Ok(())`.
- [ ] Always return `ConnectorState` in every `ProducedMessages` that made
      progress; return `state: None` for an empty poll that made none (avoids
      an unnecessary write and can't persist state left over from a failed
      batch).
- [ ] `poll()` sleeps **first**, then fetches (never sleep after holding a batch).

### B4. Transient vs permanent errors

- [ ] Infra/auth/schema-gone failures map to `Error::PermanentHttpError` /
      `Error::InitError` / `Error::SchemaMismatch` — not `InvalidRecord`.
- [ ] Retryable network/5xx/SQLSTATE map to transient variants
      (`HttpRequestFailed`, `Connection`, `CannotStoreData`).
- [ ] Do **not** classify retryability by substring-matching `err.to_string()`.
- [ ] `max_retries` means **total attempts** (default 3). README must match code.
- [ ] Cap retry budget so a dead backend cannot delay shutdown unboundedly.

### B5. Idempotency claims must be real

- [ ] Stable `ProducedMessage.id` / sink dedup key from natural IDs
      (table+PK, document `_id`, `stream:topic:partition:message_id`) —
      never random UUIDs per emit.
- [ ] If the backend PK / unique index is informational only (e.g. Redshift),
      do not advertise idempotency in README.
- [ ] External workflow IDs (Airflow `dag_run_id`, etc.) must be deterministic
      across retries.

### B6. Secrets / license policy for new SDKs

- [ ] New backend crates pass `scripts/ci/third-party-licenses.sh` (no BUSL /
      incompatible licenses pulled into the tree).
- [ ] Prefer workspace deps; avoid vendoring a license-hostile SDK just to wrap HTTP.

### B7. Tests that must exist

#### Sources

- [ ] Four canonical state tests (restore / no-state / invalid-state /
      round-trip), plus two ACK/NACK tests (`given_ack_when_batch_is_staged_
      should_commit_candidate_state`, `given_nack_when_batch_is_staged_
      should_keep_committed_state`) if `on_batch_result` is overridden - six
      total. Copy `sources/random_source/src/lib.rs::tests`. A source relying
      on the SDK's default no-op `on_batch_result` (no staged work) may skip
      the ACK/NACK pair.

#### Any external backend plugin

- [ ] At least one real-infra integration test under
      `core/integration/tests/connectors/<backend>/` with `#[iggy_harness]` +
      `testcontainers-modules` (or `wiremock` for pure HTTP).
- [ ] No false-green mocks that diverge from real backend semantics
      (Decimal, COPY, PK enforcement, etc.).

### B8. Config validation timing

- [ ] Structural validation + unknown enum rejection in `new()` /
      `open()` — not on first `poll()`/`consume()` after sleep.
- [ ] Connectivity check in `open()`; fail with `Error::InitError`.
- [ ] Invalid restored state: start fresh + `warn!`, but do **not** silently
      re-emit an entire index without calling that out in README.
- [ ] Config flag combos that no-op should `warn!` or `Err`, not silently ignore.

## High-frequency convention nits

These are "cheap" but burn full review rounds when missed.

### C1. Copy the closest exemplar

- [ ] File layout, log labels, error mapping, and test structure match the
      nearest sibling (`postgres_*`, `http_sink`, `elasticsearch_*`, …).
- [ ] Do not invent new names for existing knobs.

### C2. Config knob name canon

| Concept | Canonical field | Notes |
| ------- | --------------- | ----- |
| Request timeout | `timeout` | Not `request_timeout` |
| Retry attempts | `max_retries` | Total attempts, default 3 |
| Base backoff | `retry_delay` | humantime `Option<String>` |
| Backoff ceiling | `retry_max_delay` | Matches SDK's `ConnectivityConfig::open_retry_max_delay` |
| Poll cadence (sources) | `poll_interval` | humantime; sleep first |
| Plugin verbosity | `verbose_logging` | Mirror runtime `verbose` |
| Credentials | `connection_string` / `api_key` / … | Always `SecretString` |

- [ ] Durations: `Option<String>` + `humantime::Duration` in `new()`; fall back
      with `warn!`, never panic. Workspace `humantime` — not a pinned
      `humantime-serde`.
- [ ] New fields: `Option<T>` + `#[serde(default)]` where needed.
- [ ] Prefer `#[serde(deny_unknown_fields)]` on plugin config so typo’d knobs
      fail loud.

### C3. Crate / path / docs checklist

- [ ] `[lib] crate-type = ["cdylib", "lib"]`.
- [ ] Example TOML plugin path uses `../../target/release/lib…` like siblings.
- [ ] Row added to `sinks/README.md` or `sources/README.md`.
- [ ] Sample under `runtime/example_config/connectors/`.
- [ ] README defaults **byte-equal** to consts in code (diff them).
- [ ] No links to non-existent docs.

### C4. Hot path

- [ ] No `payload.clone().try_to_bytes()` — use `try_to_bytes(&self)`.
- [ ] `Vec::with_capacity(n)` for per-batch buffers.
- [ ] No `std::sync::Mutex` across `.await`; use `tokio::sync::Mutex`.
- [ ] `&self` on `consume` / `poll` (interior mutability only).
- [ ] No `tokio::spawn` inside plugin code.
- [ ] No `unwrap()`/`expect()` on external I/O outside tests.
- [ ] No eager `format!` around tracing args.

### C5. Containers / fixtures

- [ ] Testcontainers named `iggy-test-*` via `fixtures::unique_container_name`
      (or fixed `iggy-test-<svc>` for reuse fixtures).
- [ ] Custom Docker networks cleaned up.

## Delivery semantics (document honestly)

Every new connector README must answer in one short paragraph:

1. **What happens on transient failure?** (retry N times, then Err)
2. **What happens on permanent failure?** (drop/skip vs fail batch)
3. **What is the duplication window?** (at-least-once because state saves
   after Iggy send; or at-most-once if upstream ACK precedes send — say so)
4. **What is the dedup key?** (or "none — duplicates possible")

If the answer is hand-wavy, the PR is not ready.

## PR / CI hygiene

- [ ] Conventional commit: `feat(connectors): …` / `fix(connectors): …`.
- [ ] PR template filled (motivation, linked issue).
- [ ] `cargo fmt --all` + `cargo sort --no-format --workspace` +
      `cargo clippy -p <crate> --all-targets -- -D warnings` +
      `cargo test -p <crate>` green locally.
- [ ] Minimal `Cargo.lock` delta — no unrelated dependency churn.
- [ ] Do not modify unrelated Java/Python/foreign SDK trees in a connectors PR.
- [ ] Mark ready for review only after the above; stale-bot closes waiting PRs.

## Pre-flight author checklist

Paste into the PR description (or run mentally before `/ready`):

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

## Evidence base

Recurring comments mined from connector PRs including (non-exhaustive):
SurrealDB sink (#3453), Meilisearch sink/source (#3497/#3498), OpenSearch
source (#3515), Quickwit convention (#3523), MySQL source (#3568), JDBC
source (#3588), Doris retry (#3574), Redshift sink (#3654), Airflow trigger
(#3716), Fluss sink (#3782). Highest-density themes: delivery/offset
semantics, idempotency IDs, transient/permanent mapping, config-name drift,
secrets, README/code drift, false-green tests, CI/lockfile hygiene.

---

Discussion / help: see [AGENTS.md](../../../AGENTS.md#discussion-and-support).
