<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# InfluxDB Source Connector — Direct Runtime Dependencies

This file lists every direct (non-dev) dependency declared in
`core/connectors/sources/influxdb_source/Cargo.toml`, together with
its workspace-pinned version, license, and the specific role it plays
in this connector. Transitive dependencies are not listed here; refer
to `cargo tree -p iggy_connector_influxdb_source` for the full graph.

> **New in this PR (`feat/influxdb_v2_v3_connector`):**
> `ahash 0.8.12` was added. It replaces the standard-library `HashMap`
> with `AHashMap` in `row.rs` to reduce hash-table overhead on the hot
> row-parsing path (see review comment `row.rs:41`).

---

## Runtime dependencies

| Crate | Version (workspace) | License | Role in this connector |
| --- | --- | --- | --- |
| `ahash` | `^0.8.12` | MIT / Apache-2.0 | Non-cryptographic hash map (`AHashMap`) used as the `Row` type in `row.rs`. Replaces `std::HashMap` to reduce hash-table overhead on the per-row parsing hot path. |
| `async-trait` | `^0.1.89` | MIT / Apache-2.0 | Proc-macro that enables `async fn` in trait definitions; required by the `Source` trait impl in `lib.rs`. |
| `base64` | `^0.22.1` | MIT / Apache-2.0 | Decodes base64-encoded payload column values when `payload_format = "raw"` is configured (`v2.rs`, `v3.rs`). |
| `chrono` | `^0.4.44` | MIT / Apache-2.0 | RFC 3339 timestamp parsing, comparison, and formatting for cursor tracking in both V2 and V3 poll loops (`common.rs`, `v2.rs`, `v3.rs`). |
| `csv` | `^1.4.0` | MIT / Apache-2.0 | Parses InfluxDB V2 annotated-CSV query responses in `row.rs::parse_csv_rows`. |
| `dashmap` | `^6.1.0` | MIT | Concurrent hash map; injected into this crate's namespace by the `source_connector!` macro expansion in the SDK. Not used directly in source files. |
| `iggy_common` | `^0.10.1-edge.1` | Apache-2.0 | Shared Iggy types: `DateTime`, `Utc`, and `serde_secret` used for safe token serialisation in config structs. |
| `iggy_connector_sdk` | `^0.3.1-edge.1` | Apache-2.0 | Core connector abstractions: `Source` trait, `ProducedMessage`, `ProducedMessages`, `ConnectorState`, `Schema`, `Error`, retry/circuit-breaker utilities, and the `source_connector!` registration macro. |
| `regex` | `^1.12.3` | MIT / Apache-2.0 | Compiles the RFC 3339 cursor-validation regex once via `OnceLock` in `common.rs::cursor_re()`. |
| `reqwest` | `^0.13.3` | MIT / Apache-2.0 | Async HTTP client used to issue Flux (V2) and SQL (V3) query requests to InfluxDB. |
| `reqwest-middleware` | `^0.5.1` | MIT | Middleware wrapper around `reqwest::Client` that attaches the retry and tracing layers built by `iggy_connector_sdk::retry::build_retry_client`. |
| `secrecy` | `^0.10` | MIT / Apache-2.0 | `SecretString` wrapper that prevents accidental logging of API tokens in config structs. |
| `serde` | `^1.0.228` | MIT / Apache-2.0 | Derive macros for config struct de/serialisation (`V2SourceConfig`, `V3SourceConfig`, persisted state types). |
| `serde_json` | `^1.0.149` | MIT / Apache-2.0 | Serialises parsed rows into JSON message payloads; deserialises V3 JSONL response bodies; used in the custom `InfluxDbSourceConfig` deserialiser. |
| `simd-json` | `^0.17.0` | MIT / Apache-2.0 | SIMD-accelerated JSONL parser for InfluxDB V3 responses in `row.rs::parse_jsonl_rows`. Provides `BorrowedValue` types that are immediately converted to `serde_json::Value`. |
| `tokio` | `^1.52.3` | MIT | Async runtime; `tokio::time::sleep` used for poll-interval delays and circuit-breaker cool-down waits. |
| `tracing` | `^0.1.44` | MIT | Structured logging macros (`info!`, `warn!`, `error!`, `debug!`) used throughout the poll loop and state-management paths. |
| `uuid` | `^1.23.1` | MIT / Apache-2.0 | Generates a random fallback UUID base in `v2.rs` and `v3.rs` for rows that lack a cursor-field value (deterministic IDs are preferred and derived from timestamp nanos when the cursor field is present). |

## Dev-only dependencies

| Crate | Version | License | Role |
| --- | --- | --- | --- |
| `axum` | `^0.8.9` | MIT | Minimal HTTP server used in `#[tokio::test]` http_tests to mock the InfluxDB V2 and V3 query endpoints without requiring a live server. |
| `toml` | `^1.1.2` | MIT / Apache-2.0 | Deserialises TOML config snippets in unit tests that validate backward-compatible config loading. |

## Dependency change log

| Version | Change | Reason |
| --- | --- | --- |
| 0.4.1-edge.1 | Added `ahash ^0.8.12` | Switched `Row` type alias from `std::HashMap` to `AHashMap` in `row.rs`. AHash's non-cryptographic algorithm reduces hash-table overhead on the per-row hot path. Crate was already present in the workspace. |
