# Announcing sink and source connector templates

*Draft for the Apache Iggy project blog. Replace this header line with
the final publish date and author byline before posting.*

Apache Iggy's connectors subsystem has grown fast. In the past few
months alone, contributors have shipped or proposed sink and source
connectors for Postgres, MongoDB, Elasticsearch, Iceberg, Delta Lake,
S3, InfluxDB, Doris, ClickHouse, SurrealDB, Meilisearch, OpenSearch,
Redshift, and more — each one a plugin that moves real data between
Apache Iggy and an external system, often in production. That growth
is great news for the project. It also means new contributors keep
re-solving the same non-backend-specific problems from scratch before
their PR can even get to the interesting part: talking to their actual
system.

## What we found

Looking back across recent connector PR reviews, the same handful of
issues came up again and again, and none of them had anything to do
with the destination or source system being integrated:

- **Credentials typed as plain `String`.** Connection strings and API
  keys landing in `Debug`/log output because the field wasn't
  `secrecy::SecretString`.
- **Cursor commits that don't survive a failed delivery.** Since
  [#3855](https://github.com/apache/iggy/pull/3855), sources use a
  formal ACK/NACK handshake — `poll()` stages candidate state, and
  only `on_batch_result()` commits it — but that shape has to be
  learned and wired up correctly every time.
- **Errors that don't distinguish retry-worthy from permanent.**
  Network hiccups and "this payload will never be accepted" ending up
  in the same catch-all error variant.
- **Config knobs with drifting names.** `retry_max_delay` here,
  `max_retry_delay` there, `request_timeout` somewhere else, for the
  same concept.
- **Missing canonical tests.** State restore/round-trip and ACK/NACK
  commit/discard behavior left untested because nobody had a reference
  test suite to copy.

None of this is specific to any one backend. It's framework plumbing
that every sink and every source needs, and until now every author
either copied the closest existing plugin and stripped it down, or
started from a blank `lib.rs` and rediscovered each of these the hard
way, one review round at a time.

## The templates

[`core/connectors/sinks/sink_template`](sinks/sink_template) and
[`core/connectors/sources/source_template`](sources/source_template)
are compiling, tested crates you copy and fill in — not prose
describing a pattern, but the pattern itself, already wired up and
passing `cargo test`:

- Config parsing with `#[serde(deny_unknown_fields)]`, so a typo'd TOML
  key fails loudly instead of silently doing nothing.
- `SecretString` on every credential-shaped field
  (`connection_string`, `auth_token`), via
  `iggy_common::serde_secret::serialize_secret`.
- A retry-wrapped HTTP client plus a startup connectivity probe with
  its own backoff.
- A `CircuitBreaker` that's actually consulted before each call and
  updated once per `consume()`/`poll()`, not per chunk.
- Sink: batching by a configurable size, and a `last_err` pattern that
  never swallows a failed batch into `Ok(())`.
- Source: the full #3855 ACK/NACK contract — `poll()` stages a
  candidate cursor, `on_batch_result()` commits it on `Ack` or
  discards it on `Nack`, so a dropped batch gets re-polled instead of
  silently lost.
- The canonical test suites: six tests for the sink, eight for the
  source (four state tests — restore, no-state, invalid-state,
  round-trip — plus the two ACK/NACK tests, plus config validation and
  the circuit-breaker short-circuit path).

What's left is marked `TODO(Developer)` in each crate's `src/lib.rs`:
one spot for a sink (`push_batch()`), two for a source
(`build_raw_client()` if you're not talking HTTP, and
`fetch_records()`). Everything else — the parts that used to eat a
review round — is already done.

## Using one

Copy the crate, rename the package and the directory, add it to the
workspace `members` list, fill in the `TODO(Developer)` spots, and
update `config.toml` for your system. Each crate's own `README.md`
walks through the exact steps. Both templates already build, `clippy
--all-targets -- -D warnings` clean, and pass their tests as committed
— the only thing that should break when you fill in the TODOs is the
`Err(Error::InitError("not implemented yet"))` stub they start from.

## Why this matters beyond Apache Iggy's connectors

The pattern generalizes past this one subsystem: any plugin system
with a real framework contract — secrets, retries, staged
commit/rollback, canonical tests — benefits more from a working,
compiling example than from a checklist alone. A checklist tells you
what to verify; a template gives you the thing already verified, so
your own diff is just the part only you can write.

---

*Feedback and discussion: see the project's
[GitHub Discussions](https://github.com/apache/iggy/discussions) or
[Discord](https://discord.gg/apache-iggy).*
