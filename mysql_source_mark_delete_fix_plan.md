# MySQL Source: mark/delete-before-publish data-loss fix

Working notes for the `mark_or_delete` ordering bug in
`core/connectors/sources/mysql_source/src/lib.rs`. Status: **implemented in the
working tree** (per-table isolated two-phase poll). The SDK-level question and
`postgres_source` parity remain open for maintainer discussion (see the end).

## Contents

- [The bug](#the-bug)
- [Why it happens (code trace)](#why-it-happens-code-trace)
- [Scope: this is a subsystem pattern](#scope-this-is-a-subsystem-pattern)
- [Design reasoning](#design-reasoning)
- [Rejected alternatives](#rejected-alternatives)
- [Chosen design: two-phase poll with per-table isolation](#chosen-design-two-phase-poll-with-per-table-isolation)
- [What was implemented](#what-was-implemented)
- [Residual gaps (out of scope)](#residual-gaps-out-of-scope)
- [Follow-ups](#follow-ups)
- [Open question for maintainer](#open-question-for-maintainer)

## The bug

`poll_tables` marked/deleted a table's rows **before** the connector returned its
messages, and it did so incrementally per table while the whole poll was an
all-or-nothing `Result`.

Scenario: 3 tables, 10 rows each. Tables 1 & 2 are fully polled, their 20 rows
marked/deleted, their messages pushed into the `messages` vec. While polling
table 3, `process_row` returns `Err`. The `?` propagates, the `messages` vec
(holding tables 1 & 2's 20 messages) is dropped, and `poll()` returns `Err`.

Result: **tables 1 & 2's rows are deleted/marked in MySQL but never published to
Iggy.** Permanent data loss for `delete_after_read`; for `processed_column` the
rows are flagged processed so the next poll's filter excludes them - also lost.

## Why it happens (code trace)

- `poll()` -> `poll_tables().await?` (lib.rs:158-162). The `?` discarded the
  partially-built `messages` vec on any error.
- Inside the old `poll_tables`, `mark_or_delete_processed_rows` ran **per table,
  inside the loop**, before the final `Ok(messages)`.
- `messages.push` happened during the row loop, i.e. before that table's own
  mark - so a partially-processed table was already in the vec when it failed.
- `mark_or_delete_processed_rows` issues a **single** SQL statement per table
  (`DELETE ... WHERE pk IN (...)` or `UPDATE ... SET processed = TRUE WHERE pk
  IN (...)`). Under MySQL autocommit that statement is atomic per table:
  all-or-nothing, no "5 of 10" partial state. This is what makes per-table
  isolation safe.

Runtime behavior on `poll()` Err (`core/connectors/sdk/src/source.rs:182-189`):
the SDK loop logs `error!(... Failed to poll ...)` and `continue`s - it does
**not** crash the connector, does not publish, does not save state. Next interval
it polls again.

## Scope: this is a subsystem pattern

`postgres_source` has the identical mark-delete-before-send structure in its
polling path (`core/connectors/sources/postgres_source/src/lib.rs:488-560`):
same per-table `process_row(...)?` + `messages.push` loop, same per-table
`mark_or_delete_processed_rows(...).await?`, same final `Ok(messages)`. (Its CDC
path has no mark/delete and is unaffected.) The *complete* fix - mark/delete only
after Iggy acknowledges the batch - needs SDK-level changes because the `Source`
trait has no post-send ack. What follows is a **plugin-local mitigation**, not a
full durability guarantee.

## Design reasoning

Two error sites, two different natures:

| Error site | Nature | Policy |
| --- | --- | --- |
| `process_row` | deterministic (`Error::InvalidRecord` - undecodable column / unserializable payload). Retrying the same row fails identically forever. | no retry (pointless). Isolate: log the failing table + its resume offset, skip it this cycle, keep polling the other tables. Lossless because nothing is marked in phase 1. |
| `mark_or_delete` | I/O, possibly transient (deadlock, lock-wait, connection drop) or permanent (permission revoked, table dropped). | retry transient via `with_retry` + `is_transient_error`; on permanent/exhausted failure, isolate that one table. |

The invariant that prevents loss: **a table's messages are returned IFF its rows
were marked.** Dropping an *unmarked* table's buffer loses nothing (rows stay in
MySQL, re-polled next cycle). Loss only ever happens marked-but-not-published,
which the invariant structurally forbids.

Why the two phases are not symmetric:

- **Phase 1 (process)** has **no side effects**. A failing table can be skipped
  freely - its rows are untouched in MySQL.
- **Phase 2 (mark)** has **irreversible side effects**. Once `mark(A)` commits,
  A's rows are gone/flagged; you cannot roll it back. So phase 2 must isolate
  per table and never `?` after a commit.

Why isolate (skip the failing table) instead of aborting the whole poll: a
`process_row` error is deterministic, so a global abort would block **every**
table until an operator fixes the one bad row. Isolating keeps the healthy tables
flowing and tells the operator exactly which table and offset to fix. Both are
lossless (nothing marked yet); isolate is strictly better for liveness and for
diagnosability.

## Rejected alternatives

1. **Global abort on `process_row` (`?` the whole poll).** Lossless, but one
   deterministic bad row in any table starves *all* tables until fixed, and the
   only signal is an opaque whole-poll failure. Isolating per table is strictly
   better.

2. **Defer all marks to the end, then `?` on any mark failure (naive
   all-or-nothing in phase 2).** Unsafe: `mark(A)` commits, `mark(B)` fails, the
   `?` drops A's messages - A is deleted from MySQL but never published. The
   original bug relocated, not fixed. You cannot roll back a committed `DELETE`.

3. **True all-or-nothing via a single cross-table transaction** (`BEGIN;
   DELETE/UPDATE per table; COMMIT`, `ROLLBACK` on any failure). Safe, but
   **overkill** here: a permanently-failing table blocks *all* tables every
   cycle, it holds row locks across the whole batch, and it buys an "all tables
   advance together" invariant a polling source does not need (tables are
   independent streams). It also does not close the mark-before-publish window.

## Chosen design: two-phase poll with per-table isolation

- **Phase 1 - fetch + process, zero side effects.** For each table, fetch and
  process its rows into a `TableBatch`. Any error for a table (bad custom query,
  permanent fetch error, `process_row` decode failure) is logged with the table
  name and resume offset and the table is **skipped** for this cycle. Nothing is
  marked, so zero loss; the table retries next cycle while the others proceed.
- **Phase 2 - commit, per-table isolated.** For each buffered table, call
  `mark_or_delete_processed_rows` (which now retries transient errors). On
  success, append its messages and record its offset. On permanent/exhausted
  failure, log and **skip** that table - never `?`, because earlier tables in the
  loop may already be committed.
- **State** updates apply only for tables that committed.
- Only a genuinely global failure (cannot acquire the pool) aborts the poll.

| Approach | Safe? | Liveness | Diagnosability |
| --- | --- | --- | --- |
| Mark each, `Err` if any fails | No - loses already-marked tables | n/a | n/a |
| Global abort on `process_row` | Yes | poison row blocks all tables | opaque whole-poll error |
| **Per-table isolation (chosen)** | Yes - no loss | best; healthy tables keep flowing | precise per-table + offset log |
| Single cross-table transaction | Yes | poison table blocks all; more locking | per-table |

## What was implemented

All in `core/connectors/sources/mysql_source/src/lib.rs`. 24 existing unit tests
pass; clippy clean.

### `TableBatch` struct (after `ProcessedRow`)

```rust
/// One table's fully processed but not-yet-committed work. Built in the
/// side-effect-free first phase of `poll_tables`, then marked/deleted and
/// published in the second phase so a table's messages are emitted only once
/// its rows are marked.
struct TableBatch {
    table: String,
    messages: Vec<ProducedMessage>,
    processed_ids: Vec<String>,
    max_offset: Option<String>,
}
```

### `mark_or_delete_processed_rows` - transient retry

Builds the `DELETE` / `UPDATE` SQL deterministically (quoting stays outside the
retry), then wraps only the `.execute` in the existing `with_retry`
(transient-aware via `is_transient_error`). The offset-tracking-only case
(neither `delete_after_read` nor `processed_column`) early-returns `Ok(())`.

```rust
let query = if self.config.delete_after_read.unwrap_or(false) {
    // ... log + DELETE ...
    format!("DELETE FROM {quoted_table} WHERE {quoted_pk} IN ({ids_list})")
} else if let Some(processed_col) = &self.config.processed_column {
    let quoted_processed = quote_identifier(processed_col)?;
    // ... log + UPDATE ...
    format!("UPDATE {quoted_table} SET {quoted_processed} = TRUE WHERE {quoted_pk} IN ({ids_list})")
} else {
    return Ok(()); // offset-tracking only
};

with_retry(
    || sqlx::query(sqlx::AssertSqlSafe(query.as_str())).execute(pool),
    self.get_max_retries(),
    self.retry_delay.as_millis() as u64,
)
.await
.map(|_| ())
```

### `fetch_table_batch` helper (phase-1 per-table work)

Looks up nothing from state (caller passes `last_offset`); builds the query,
fetches with retry, runs `process_row` per row with `?`, returns a `TableBatch`.
Returning `Err` here lets the caller log + skip the one table.

```rust
async fn fetch_table_batch(
    &self,
    table: &str,
    row_config: &RowProcessingConfig<'_>,
    tracking_column: &str,
    batch_size: u32,
    last_offset: &Option<String>,
) -> Result<TableBatch, Error> { /* build query, fetch, process_row loop */ }
```

### `poll_tables` - two phases

```rust
// Phase 1: build per-table buffers; skip a failing table with a precise log.
let mut batches: Vec<TableBatch> = Vec::with_capacity(self.config.tables.len());
for table in &self.config.tables {
    let last_offset = { let s = self.state.lock().await; s.tracking_offsets.get(table).cloned() };
    match self.fetch_table_batch(table, &row_config, tracking_column, batch_size, &last_offset).await {
        Ok(batch) => { /* log fetched */ batches.push(batch); }
        Err(error) => error!(
            "Failed to process table '{table}' at offset {}, skipping this cycle: {error}",
            last_offset.as_deref().unwrap_or("<start>")
        ),
    }
}

// Phase 2: commit each table independently; skip a failing mark/delete.
let mut messages = Vec::new();
let mut state_updates: Vec<(String, String)> = Vec::new();
let mut total_processed: u64 = 0;
for mut batch in batches {
    if !batch.processed_ids.is_empty()
        && let Err(error) = self
            .mark_or_delete_processed_rows(pool, &batch.table, pk_column, &batch.processed_ids)
            .await
    {
        error!("Failed to mark or delete processed rows for table '{}', skipping this cycle: {error}", batch.table);
        continue;
    }
    total_processed += batch.messages.len() as u64;
    messages.append(&mut batch.messages);
    if let Some(offset) = batch.max_offset { state_updates.push((batch.table, offset)); }
}
// apply state_updates under a single lock; Ok(messages)
```

### What changed vs. before

| Concern | Before | After |
| --- | --- | --- |
| `process_row` error | `?` dropped earlier tables' already-marked messages -> loss | phase 1, pre-mark; logs table + offset and skips that table only |
| `mark_or_delete` error | mapped to `InvalidRecord`, no retry; `?` dropped batch | `with_retry` transient-aware; permanent failure isolates that table |
| Marked-but-not-published | possible across tables | structurally impossible (commit <=> mark, per table) |
| One bad table | aborted the whole poll | isolated; healthy tables keep flowing |
| Offset state on error | never applied (early `?`) | applied for tables that committed |
| `processed_rows` count | counted fetched rows | counts published (committed) rows |

## Residual gaps (out of scope)

- **Monitoring visibility.** With per-table skip, `poll()` returns `Ok`, so a
  permanently-stuck table does not surface as a poll failure - the only signal is
  the per-table `error!` log. The plugin cannot touch runtime metrics
  (`context.sources.set_error` is runtime-side, on the send path). Acceptable
  tradeoff for keeping healthy tables flowing, but worth noting in the PR.
- **At-least-once across the publish boundary.** Mark still precedes the
  runtime's `producer.send` (`source.rs:456`). A send failure after a successful
  poll still loses that batch. Closing this needs an SDK-level post-send ack on
  the `Source` trait - the same gap `postgres_source` has.
- **Row-level dead-lettering.** A single poison row still stalls its own table's
  progress (the table is skipped wholesale, not advanced past the bad row).
  Skipping just the offending row + advancing the offset is a larger feature;
  deferred.

## Follow-ups

1. Tests: (a) a `process_row` failure in one table leaves zero rows
   marked/deleted in that table and does not drop another table's committed
   messages; (b) a mark/delete failure isolates one table while others publish;
   (c) offset advances only for committed tables; (d) transient mark error is
   retried. See `connector-testing` skill for source state round-trip patterns.
2. Inline reply on the review thread summarizing the two-phase + per-table-skip
   fix and naming the SDK ack gap as broader than this PR.
3. Keep this PR scoped to `mysql_source`; do not bundle `postgres_source`.

## Open question for maintainer

1. Is the plugin-local mitigation acceptable as an interim step, or does the
   project want the proper SDK-level fix (`Source` post-send ack) so
   `mysql_source` + `postgres_source` share one solution?
2. Is the per-table-skip behavior acceptable - a bad row stalls only its own
   table (logged with table + offset) while the rest flow, and `poll()` still
   returns `Ok` so monitoring sees no poll failure?
3. Parity: should `postgres_source` get the same treatment in this PR or
   separately? (Recommended: separately.)
