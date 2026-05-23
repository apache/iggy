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

# InfluxDB Sink Connector — Direct Runtime Dependencies

This file lists every direct (non-dev) dependency declared in
`core/connectors/sinks/influxdb_sink/Cargo.toml`, together with
its workspace-pinned version, license, and the specific role it plays
in this connector. Transitive dependencies are not listed here; refer
to `cargo tree -p iggy_connector_influxdb_sink` for the full graph.

> **No new dependencies were added in this PR (`feat/influxdb_v2_v3_connector`).**  
> All changes are within existing dependencies. The `base64::Engine::encode_string`  
> API (already in `base64 ^0.22.1`) replaced `encode()` to write directly into  
> the output buffer without an intermediate `String` allocation (see review  
> comment `sink/lib.rs:562`).

---

## Runtime dependencies

| Crate | Version (workspace) | License | Role in this connector |
| --- | --- | --- | --- |
| `async-trait` | `^0.1.89` | MIT / Apache-2.0 | Proc-macro that enables `async fn` in trait definitions; required by the `Sink` trait impl in `lib.rs`. |
| `base64` | `^0.22.1` | MIT / Apache-2.0 | Encodes raw message payloads as base64 when `payload_format = "base64"` is configured. Uses `Engine::encode_string` to write directly into the line-protocol output buffer with no intermediate allocation. |
| `bytes` | `^1.11.1` | MIT | Zero-copy `Bytes::from(body.into_bytes())` converts the line-protocol string body into the request body sent to InfluxDB's write endpoint without an extra copy. |
| `iggy_common` | `^0.10.1-edge.1` | Apache-2.0 | Shared Iggy types: `serde_secret` for safe token serialisation in config structs. |
| `iggy_connector_sdk` | `^0.3.1-edge.1` | Apache-2.0 | Core connector abstractions: `Sink` trait, `ConsumedMessage`, `MessagesMetadata`, `TopicMetadata`, `Error`, retry/circuit-breaker utilities, and the `sink_connector!` registration macro. |
| `reqwest` | `^0.13.3` | MIT / Apache-2.0 | Async HTTP client used to POST line-protocol batches to `/api/v2/write` (V2) and `/api/v3/write_lp` (V3). |
| `reqwest-middleware` | `^0.5.1` | MIT | Middleware wrapper around `reqwest::Client` that attaches the retry and tracing layers built by `iggy_connector_sdk::retry::build_retry_client`. |
| `secrecy` | `^0.10` | MIT / Apache-2.0 | `SecretString` / `SecretBox` wrappers that prevent accidental logging of API tokens in config structs and the `auth_header` field. |
| `serde` | `^1.0.228` | MIT / Apache-2.0 | Derive macros for config struct de/serialisation (`V2SinkConfig`, `V3SinkConfig`). |
| `serde_json` | `^1.0.149` | MIT / Apache-2.0 | Serialises JSON message payloads to compact strings for the `payload_json` field; used in the custom `InfluxDbSinkConfig` deserialiser that handles the `version` discriminator. |
| `tokio` | `^1.52.3` | MIT | Async runtime; `SystemTime::now()` (via `std::time`) provides the wall-clock fallback for messages with `timestamp == 0`. Tokio itself is an indirect requirement through the connector SDK. |
| `tracing` | `^0.1.44` | MIT | Structured logging macros (`info!`, `warn!`, `error!`, `debug!`) used throughout the consume loop, circuit-breaker paths, and config-validation guards. |

## Dev-only dependencies

| Crate | Version | License | Role |
| --- | --- | --- | --- |
| `axum` | `^0.8.9` | MIT | Minimal HTTP server used in `#[tokio::test]` http_tests to mock the InfluxDB V2 (`/api/v2/write`) and V3 (`/api/v3/write_lp`) write endpoints and the shared `/health` probe endpoint. |
| `toml` | `^1.1.2` | MIT / Apache-2.0 | Deserialises TOML config snippets in unit tests that validate backward-compatible config loading. |

## Dependency change log

| Version | Change | Reason |
| --- | --- | --- |
| 0.4.1-edge.1 | No Cargo.toml changes | `base64 ^0.22.1` (already a dependency) gained additional usage: `Engine::encode_string` now writes base64 directly into the line-protocol string buffer, eliminating the intermediate `String` allocation that the previous `encode()` call produced. |
