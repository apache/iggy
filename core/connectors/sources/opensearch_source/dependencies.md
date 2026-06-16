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

# OpenSearch Source Connector — Direct Runtime Dependencies

This file lists every direct (non-dev) dependency declared in
`core/connectors/sources/opensearch_source/Cargo.toml`, together with
its workspace-pinned version, license, and the specific role it plays
in this connector. Transitive dependencies are not listed here; refer
to `cargo tree -p iggy_connector_opensearch_source` for the full graph.

---

## Runtime dependencies

| Crate | Version (workspace) | License | Role in this connector |
| --- | --- | --- | --- |
| `async-trait` | `^0.1.89` | MIT / Apache-2.0 | Proc-macro that enables `async fn` in trait definitions; required by the `Source` trait impl in `lib.rs`. |
| `dashmap` | `^6.1.0` | MIT | Concurrent hash map; injected into this crate's namespace by the `source_connector!` macro expansion in the SDK. Not used directly in source files. |
| `humantime` | `^2.3.0` | MIT / Apache-2.0 | Parses human-readable duration strings in optional file-state `auto_save_interval` config (reserved for future use). |
| `iggy_common` | workspace | Apache-2.0 | Shared Iggy types: `DateTime`, `Utc`, and `serde_secret` for optional basic-auth password serialisation. |
| `iggy_connector_sdk` | workspace | Apache-2.0 | Core connector abstractions: `Source` trait, `ProducedMessage`, `ProducedMessages`, `ConnectorState`, `Schema`, `Error`, `parse_duration`, and the `source_connector!` registration macro. |
| `once_cell` | `^1.21.4` | MIT / Apache-2.0 | `Lazy` global; injected by the `source_connector!` macro expansion in the SDK. Not used directly in source files. |
| `opensearch` | `2.4.0` | Apache-2.0 | Official OpenSearch Rust client for index existence checks and `search` requests with `search_after` pagination. |
| `rmp-serde` | workspace | MIT / Apache-2.0 | Serialises connector runtime state to MessagePack for the connectors runtime state file. |
| `secrecy` | `^0.10` | MIT / Apache-2.0 | `SecretString` wrapper that prevents accidental logging of passwords in config structs. |
| `serde` | workspace | MIT / Apache-2.0 | Derive macros for config and persisted-state de/serialisation. |
| `serde_json` | workspace | MIT / Apache-2.0 | Builds OpenSearch query bodies and serialises document payloads into produced messages. |
| `tokio` | workspace | MIT | Async runtime; `tokio::sync::Mutex` for connector state and `tokio::time::sleep` for poll-interval delays. |
| `tracing` | workspace | MIT | Structured logging macros (`info!`, `warn!`, `error!`) used throughout the poll loop and state paths. |
