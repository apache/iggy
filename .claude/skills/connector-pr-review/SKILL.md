---
name: connector-pr-review
description: Review checklist for Apache Iggy connector sink/source PRs. Load when reviewing a connectors plugin PR, when authoring a new sink/source and wanting to pre-flight against common review blockers, or when diagnosing why a connectors PR is stuck in review. Encodes recurring review patterns mined from real apache/iggy connector PRs. NOT for runtime/SDK internals (use connector-runtime / connector-sdk).
---

# Connector PR review checklist

> Universal rules live in [connectors-overview](../connectors-overview/SKILL.md).
> Authoring skills: [connector-sink](../connector-sink/SKILL.md),
> [connector-source](../connector-source/SKILL.md),
> [connector-testing](../connector-testing/SKILL.md).
> Template crates: `sinks/sink_template`, `sources/source_template` (steps in those skills' `TEMPLATE.md`).

Use this skill to **catch the issues that repeatedly burn review cycles**
before asking for a human re-review. Cite symbols/paths, not stale line numbers.

## Contents

- [How to use](#how-to-use)
- [Blockers (must fix before merge)](#blockers-must-fix-before-merge)
- [High-frequency convention nits](#high-frequency-convention-nits)
- [Domain risks by connector family](#domain-risks-by-connector-family)
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

- [ ] Every credential field is `secrecy::SecretString`. `serialize_secret`
      (`iggy_common::serde_secret`) writes the plaintext, as the runtime needs it;
      `SecretString` hides the value in `Debug` output only. It has no `Display` impl. Use `serialize_redacted`
      for a field that must never leave the process in serialized form.
- [ ] No connection string / API key / token in `info!`/`debug!`/`error!` /
      `format!` into SQL / file-state metadata.
- [ ] Source state must not persist URL userinfo or bearer tokens.

Plain `String` for a credential is a review-blocker. Pattern:
`PostgresSinkConfig::connection_string` in `sinks/postgres_sink`.

### B2. Swallowing `Err` while offsets advance (sinks)

- [ ] `consume()` must **not** catch a batch failure and return `Ok(())`. The one exception is a permanent chunk skipped at most once, which is logged and counted in the plugin's own stats.
- [ ] Transient failures are retried inside the call within one time budget
      (template default `max_delivery_time = 20s`). When it runs out the sink
      stops, counts every undelivered message as failed, logs the stream, topic,
      partition and offset range, and returns the error (see `connector-sink`
      Delivery budget). At most once, permanent and unclassified errors only
      affect their own chunk. At least once, both stop the call at that chunk.
- [ ] An open circuit breaker is waited out inside the budget, never answered
      with an immediate error: by default the offset is already stored, so a
      refused batch is lost.
- [ ] A sink that accepts `[delivery] guarantee = "at_least_once"`
      (`enable_at_least_once()` returns `Ok`; the SDK default refuses) never
      skips a message. A permanent rejection fails the call so the runtime
      stops delivery of that topic at that offset, an unclassified error stops the call at
      its chunk so no later chunk is written before redelivery, and a sink that buffers across calls
      proves delivery in `flush()`.
- [ ] README must not claim "no data loss" / strong idempotency unless the
      backend + runtime path actually enforce it.

Since #4629 the server stores a sink's consumer offset when it answers the
poll, and the runtime logs and counts an `Err` from `consume()` without
redelivering the batch. Returning `Err` makes a failure visible; it does not
trigger a retry. Authors must be honest about that loss window instead of
overselling.

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
      `Error::SchemaMismatch` - not `InvalidRecord`. `Error::InitError` is not on
      `Error::is_permanent()`; use `Connection` for a failed connectivity check
      and `InvalidConfigValue` for bad config.
- [ ] Retryable network/5xx/SQLSTATE map to transient variants
      (`HttpRequestFailed`, `Connection`, `CannotStoreData`, `WriteFailure`,
      `Storage`, `TransientState`, `CatalogCommitError`).
- [ ] Do **not** classify retryability by substring-matching `err.to_string()`.
- [ ] `max_retries` means **total attempts** (default 3). README must match code.
- [ ] Cap retry budget so a dead backend cannot delay shutdown unboundedly. A
      sink's `consume()` blocks a runtime worker thread and a stop waits for
      it, so it needs one time budget for the whole call, well under a minute,
      and a breaker cool-down shorter than that budget.

### B5. Idempotency claims must be real

- [ ] Stable `ProducedMessage.id` / sink dedup key from natural IDs
      (table+PK, document `_id`, `stream:topic:partition:offset` - use
      `message_id` instead only if producers guarantee unique ids, because two
      messages sharing an id would overwrite each other) -
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

- [ ] Structural validation + unknown enum rejection in `open()` - not on first `poll()`/`consume()` after sleep.
- [ ] Connectivity check in `open()`; fail with `Error::Connection` (or `InitError`).
- [ ] Invalid restored state: start fresh + `warn!`, but do **not** silently
      re-emit an entire index without calling that out in README.
- [ ] Config flag combos that no-op should `warn!` or `Err`, not silently ignore.

### B9. A batch with any row not delivered is a failure, even if the call "succeeded"

A bulk/batch API call returning 200 does not mean every row in it landed.
This is about `push_batch()`. `consume()` itself returns `Ok` after a permanent skip
at most once, on purpose: the chunk is logged and counted in the plugin's own
stats, not hidden.
Recurring, independently-discovered bug across four different sinks:

- [ ] If your destination's write API can partially or fully reject a batch
      while the call itself succeeds (HTTP 200 with per-item errors, a bulk
      index response, a stream-load response), count accepted items and
      return `Err` unless **every** item of a non-empty input was accepted -
      never `Ok(())`. A partial reject is the case at-least-once cannot
      absorb: `Ok(())` lets the stored offset pass the rejected rows.
      BigQuery (#4394): "when every row of a non-empty batch
      is rejected, `consume` still returns `Ok`." Elasticsearch (#4154): bulk
      index knows the rejected count but still returns `Ok`. Doris (#3215):
      "if every message in the batch is non-JSON... `consume()` returns
      `Ok(())`" with the batch silently gone. S3 (#3103): a formatter failure
      produced an empty placeholder that was still counted as uploaded.
- [ ] A per-record encode/serialize failure must not silently substitute an
      empty/null/default value that then gets counted as delivered - skip
      and count it as lost (visibly), or fail the whole chunk.

### B10. A wrapping timeout does not cancel in-flight work

`tokio::time::timeout(dur, fut)` dropping `fut` on expiry does **not** mean
the operation didn't happen - for a blocking-executor call, an FFI call, or
anything already past the "no going back" point in the wire protocol, the
work keeps running after the timeout fires and can still complete
server-side. Confirmed independently on two unrelated backends:

- [ ] Treat an expired wrapping timeout as **unknown outcome**, not
      **didn't happen** - do not assume it's safe to retry the identical
      operation without a dedup/idempotency key. RabbitMQ (#3973): "Wrapping
      `basic_publish` in `timeout()` does not make it abortable... usually
      already delivered." Postgres (#3957): "`timeout` drops the client
      future, it does not abort the statement - a DELETE... can still commit
      server-side after this logs a failure."
- [ ] Prefer a protocol/transport-level timeout over (or in addition to) a
      wrapping `tokio::time::timeout`: `reqwest::ClientBuilder::timeout`,
      a connection-string timeout parameter (`?connection_timeout=` for AMQP),
      or a driver's own `statement_timeout`. A bare `TcpStream::connect` run
      on a blocking executor is not cancelled by an outer async timeout and
      leaks a pool thread until the kernel's own SYN timeout (RabbitMQ #3973).
- [ ] This is why `build_raw_client()` in the sink/source templates sets
      `.timeout()` on the `reqwest::Client` itself rather than wrapping calls
      - copy that pattern, don't wrap instead of it.
- [ ] The sink template does also wrap each attempt in `timeout(remaining
      budget)` in `deliver_chunk()`, in addition to the transport timeout, so
      one hung request cannot outlast `max_delivery_time` (while `push_batch()` only awaits). It treats the
      expiry as an unknown outcome, which is safe only because the write is
      idempotent on `dedup_key()`. A sink without that idempotency must not
      retry after a wrapping timeout.

### B11. Ack-path work has no SDK deadline, but it blocks every later poll

`on_batch_result` has no SDK-enforced time limit, and a NACK does **not**
come from a timeout - the SDK (since #4269) only warns past ~30s without a
runtime result and keeps waiting; it never synthesizes a Nack or moves on.
The real risk is different: the SDK allows exactly one batch in flight, so
`poll()` is not called again until `on_batch_result` returns. Real I/O in
there - not just an in-memory state swap - silently stalls the whole source
for as long as it takes, with nothing visibly "failing."

- [ ] A retry/timeout budget inside `on_batch_result` must account for
      **every operation it runs, not one op's worth** - a per-table or
      per-item budget multiplies with the item count. Postgres (#3957,
      critical): "10s is per operation, not per batch... N tables burn
      N\*10s" serially inside one `on_batch_result` call, stalling every
      later poll by that much.
- [ ] User-configurable `max_retries`/`retry_delay` can combine into a wait
      time nobody sized on purpose - validate the combination in `open()`
      and reject configs that blow an internal budget, rather than letting
      the ack path stall at runtime. Same PR: "`max_retries=3` with
      `retry_delay=\"2s\"`, both straight from the readme, needs 12s and
      fails startup" once a sane cap was added.
- [ ] A failed destructive/cleanup op inside `on_batch_result` must not be
      silently logged and ignored while the batch is still ACKed - count
      consecutive failures and return `Err` past a threshold so the SDK
      stops the source, rather than resending the same broken operation
      forever. Same PR: "a failed slot advance only logs. the batch is
      already ACKed, so nothing stops it."
- [ ] A source whose input genuinely cannot be replayed after a restart
      (so repeated-Nack-triggered stop would be the wrong failure mode) can
      override `Source::batch_policy()` and call
      `BatchPolicy::with_max_consecutive_nacks(None)` (SDK 0.5.1-edge.2+) -
      prefer that over working around the default 5-consecutive-Nack stop
      inside plugin code.

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
| Base backoff | `retry_delay` | humantime `Option<String>`; a bad value falls back to 1 s |
| Backoff ceiling | `retry_max_delay` | Matches SDK's `RetryPolicy::max_delay` |
| Poll cadence (sources) | `poll_interval` | humantime; sleep first |
| Plugin verbosity | `verbose_logging` | Mirror runtime `verbose` |
| Credentials | `connection_string` / `api_key` / … | Always `SecretString` |

- [ ] Durations: `Option<String>` + `humantime::Duration` in `new()`; fall back
      with `warn!`, never panic. Workspace `humantime` - not a pinned
      `humantime-serde`.
- [ ] New fields: `Option<T>` + `#[serde(default)]` where needed.
- [ ] Prefer `#[serde(deny_unknown_fields)]` on plugin config so typo’d knobs
      fail loud.
- [ ] Reject degenerate numeric values (`0`, empty string) for a field where
      zero makes a check always-true or always-false, rather than letting it
      silently misbehave - `batch_size`/`max_messages_per_file` zero
      triggered S3 (#3103) to rotate on every single message. If a sibling
      connector already clamps a field like this (`.max(1)`), clamp
      consistently rather than leaving one sibling to reject what another
      silently accepts.
- [ ] Never derive a value that must stay identical across a retry of the
      *same* logical batch (a destination key, filename, or dedup id) from
      wall-clock time (`Utc::now()`) - derive it from the batch's own fixed
      fields (an offset, a message id, `origin_timestamp`). S3 (#3103): a
      `{timestamp}` path placeholder rendered at upload time broke the
      README's own idempotent-retry claim.

### C3. Crate / path / docs checklist

- [ ] `[lib] crate-type = ["cdylib", "lib"]`.
- [ ] `publish = false` in `[package]` - every in-tree plugin needs it, and
      it's easy to forget on a new crate (missed on S3 #3103 and Doris
      #3215, both caught in review).
- [ ] Example TOML plugin path uses `target/release/lib…` like siblings.
- [ ] Row added to `sinks/README.md` or `sources/README.md`.
- [ ] Sample under `runtime/example_config/connectors/`.
- [ ] README defaults **byte-equal** to consts in code (diff them).
- [ ] No links to non-existent docs.
- [ ] Helper modules stay crate-private (no `pub mod`) unless genuinely part
      of the plugin's public API - S3 (#3103) exposed `buffer`/`client`/etc.
      as public on a cdylib plugin where every sibling keeps them private.
- [ ] Crate-level doc comment doesn't use `[module]` intra-doc links to a
      private module - rustdoc warns on the dead link and the doctest CI job
      (`-D warnings`) fails the build. BigQuery (#4394): the crate doc linked
      five private modules this way. Use a plain code span (`` `module` ``)
      instead when the target isn't `pub`.

### C4. Hot path

- [ ] No `payload.clone().try_to_bytes()` - use `try_to_bytes(&self)`.
- [ ] `Vec::with_capacity(n)` for per-batch buffers.
- [ ] No `std::sync::Mutex` across `.await`; use `tokio::sync::Mutex`.
- [ ] Even a `tokio::sync::Mutex` must not be **held across** a slow network
      call or a retry sleep - lock, clone/extract what you need, drop the
      guard, *then* do I/O. This isn't just a deadlock rule: holding the
      lock across the call serializes every concurrent caller sharing that
      plugin instance onto one upload/write at a time, killing the
      parallelism the runtime's per-partition tasks were meant to give you.
      S3 (#3103): one `buffers.lock().await` held through `put_object()`
      *and* the retry sleep capped the whole sink's throughput to one
      upload in flight regardless of partition count.
- [ ] `&self` on `consume` / `poll` (interior mutability only).
- [ ] No `tokio::spawn` inside plugin code.
- [ ] No `unwrap()`/`expect()` on external I/O outside tests.
- [ ] No eager `format!` around tracing args.
- [ ] Never `let _ = fallible_call()` on a side effect that matters (a
      cleanup op, a quoted identifier, a staged delete) - log or propagate
      the error. Postgres (#3957): `let _` on a staged-operation error made
      an always-failing delete invisible.
- [ ] If retrying raw HTTP status codes (not just SDK `Error` variants),
      classify by family - 5xx, 408, 429 retry; other 4xx fail fast. S3
      (#3103) retried `AccessDenied`/`NoSuchBucket`/`InvalidBucketName`
      three times each, burning the whole retry budget on failures no retry
      could fix.
- [ ] Never log a substring, prefix, or fragment of a credential "for
      diagnostics" - it still identifies the credential type/shape for zero
      diagnostic value. S3 (#3103) logged the first 3 characters of an AWS
      access key (`AKIA`/`ASIA`/`AROA` prefix).

### C5. Containers / fixtures

- [ ] Testcontainers named `iggy-test-*` via `fixtures::unique_container_name`
      (or fixed `iggy-test-<svc>` for reuse fixtures).
- [ ] Custom Docker networks cleaned up.

## Domain risks by connector family

The blockers above are generic, and they explain roughly one review comment in six. Most of the
rest is backend-specific but repeats inside a family. Before the first request for review, walk
the cross-family list and the row for your family. Each line names the test that proves it (see
`connector-testing`, Canonical tests beyond state).

### Cross-family

- [ ] **Bad records.** A record that cannot be converted never lets the cursor pass it: stop before
      it, or skip it on purpose with a log line and a counter. Test: the template's bad-record tests.
- [ ] **Cursor ties.** Rows that share a cursor value, equal values that differ as strings, and
      commits that land out of order must not be skipped at a batch boundary. Test: equal cursor
      values across a batch boundary.
- [ ] **State identity.** Persisted state names what it belongs to (endpoint, prefix, table, topic)
      and is validated against the config on restore. A re-created source topic or table must not be
      resumed with stale offsets.
- [ ] **Type fidelity.** No silent loss: precision, units, sign, reserved null encodings, NaN and
      infinity. An unsupported type fails loudly, never becomes `null`. Test: type matrix.
- [ ] **Built strings.** Queries, paths, object keys and connection strings are never assembled from
      config or message data without quoting or an allowlist, and a sanitizer is injective. Test:
      distinct inputs never collide.
- [ ] **Success that is not.** Say how the client reports partial failure (per-item errors in a 2xx,
      a `flush` that returns `Ok` after a rejection) and test it with a fake that does exactly that.
- [ ] **Third-party defaults.** Derived `Debug` on a client that holds credentials, TLS and
      panic-on-default configuration, unbounded response or object sizes, and fallbacks that mask a
      bad value. Test: no secret in `Debug`; every size has a limit set in `open()`.
- [ ] **Persistent failure.** A failing poll or flush backs off and counts toward the breaker. Test:
      persistent failure is paced.
- [ ] **Probes.** The startup probe needs no more privilege than the connector uses and has no side
      effect. Document the least privilege the connector needs.
- [ ] **Env overrides.** Numeric and boolean knobs are typed fields and survive the env-override path.
- [ ] **Tests that prove something.** A test that stays green with the behavior removed does not
      count: a broker reconnect with no proof of disconnect, a read that can land mid-write, a fixed
      sleep. Revert the fix and watch the test fail.

### By family

| Family | Also check |
| --- | --- |
| SQL and JDBC sources (MySQL, JDBC, MSSQL CDC, Postgres) | Keyset pagination with a tie-breaker, comparing cursors the way the database does. Quoted identifiers and placeholders (replace order, `$offset`, table and key quoting). A custom query without an order or `LIMIT`. Mark or delete staged and applied only on `Ack`. Driver defaults that differ per database (parameter metadata, timeouts). CDC: which capture instance a fresh start selects, operation ordering inside one transaction, and what `latest` means under capture lag. |
| Object stores (S3, OpenDAL, Azure) | Key encoding (`+` against space, percent-decoding) and collisions after sanitizing. Listing truncation and `StartAfter`. Persisted state includes endpoint and prefix. Atomic writes (a reader must not see a partial object). Credential chain and profile handling. A probe that needs list or delete permission. Object size bounds and rotation. |
| Brokers (RabbitMQ, MQTT, Iggy as a source) | Ack granularity (acknowledge the last delivery with `multiple`, not each one). Reconnect and session semantics (clean start, QoS, client id uniqueness, keep-alive between batches). Whether the exactly-once replay state survives a restart. Metadata fidelity (publisher timestamp, message type, headers). Poison messages. TLS errors classified as permanent. |
| Search and index (OpenSearch, Fluss, Elasticsearch) | A bulk 200 with per-item errors and an absent `items` array. Document id validation, and an overwrite when a retry reorders the same id. `Retry-After`. Mapping and schema conflicts, cache invalidation after `ALTER` or drop, and unbounded auto-created tables. Late-visible documents and a unique tie-breaker field. |
| HTTP and telemetry sinks (HTTP, OTLP) | A cap on response bytes. Partial-success bodies parsed whatever the media-type case. Retries that block a runtime worker (see `connector-sink`, Delivery budget). Header and payload size limits. |

## Delivery semantics (document honestly)

Every new connector README must answer in one short paragraph:

1. **What happens on transient failure?** (retry N times, then Err)
2. **What happens on permanent failure?** (drop/skip vs fail batch)
3. **What is the duplication window?** (sources: at-least-once because state
   saves after Iggy send. Sinks: at-most-once from Iggy by default, because the
   server stores the offset when it answers the poll and the runtime never
   redelivers a failed batch, so say what a failed batch costs and where your
   own retries can duplicate. With `[delivery] guarantee = "at_least_once"` the
   runtime redelivers a failed call, so say that duplicates are expected and
   what the write is idempotent on)
4. **What is the dedup key?** (or "none - duplicates possible")

If the answer is hand-wavy, the PR is not ready.

## PR / CI hygiene

- [ ] Conventional commit: `feat(connectors): …` / `fix(connectors): …`.
- [ ] PR template filled (motivation, linked issue).
- [ ] `cargo fmt --all` + `cargo sort --no-format --workspace` +
      `cargo clippy -p <crate> --all-targets -- -D warnings` +
      `cargo test -p <crate>` green locally.
- [ ] Minimal `Cargo.lock` delta - no unrelated dependency churn.
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
[ ] Canonical knob names (timeout, max_retries, retry_delay, max_delivery_time for sinks, poll_interval for sources)
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
source (#3588), Doris retry (#3574) and sink review (#3215), Redshift sink
(#3654), Airflow trigger (#3716), Fluss sink (#3782), S3 sink (#3103),
Google BigQuery sink (#4394), Delta sink production hardening (#4219),
RabbitMQ sink (#3973), Elasticsearch sink batch semantics (#4154),
Postgres source ACK/NACK + CDC hardening (#3957), the shared
`retry_async`/`RetryPolicy` SDK helper review (#4104). Highest-density
themes: delivery/offset semantics, idempotency IDs, transient/permanent
mapping, config-name drift, secrets, README/code drift, false-green tests,
CI/lockfile hygiene, undelivered-rows-still-reports-Ok, a wrapping timeout
that doesn't cancel in-flight work, ack-path work blowing its fixed budget,
and holding a lock across slow I/O.

The domain-risk section comes from classifying about 360 inline review comments on the open
connector PRs (MySQL, JDBC, MSSQL CDC, S3, OpenDAL, MQTT, RabbitMQ, OpenSearch, Fluss, QuestDB,
OTLP, ScopeDB and the Iggy source): about 17% matched a rule above, about 34% were
backend-specific, and about 38% were performance or style.

Re-derive or extend this list with `gh pr list --repo apache/iggy --search
"connectors in:title" --state all --limit 100` plus `gh api
repos/apache/iggy/pulls/<N>/comments --paginate` for PRs under
`core/connectors/`.

---

Discussion / help: see [AGENTS.md](../../../AGENTS.md#discussion-and-support).
