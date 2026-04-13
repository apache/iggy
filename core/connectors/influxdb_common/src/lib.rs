/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Shared InfluxDB connector components for Iggy.
//!
//! This crate provides the version-abstraction layer that both the sink and
//! source connectors use to support InfluxDB V2 and V3 without duplicating
//! protocol details.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │        Sink / Source connector          │  (iggy_connector_influxdb_sink/source)
//! │  open() / consume() / poll() / close()  │
//! │  Batching · Retry · Circuit breaker     │
//! │  Metrics · Cursor state                 │
//! └────────────────┬────────────────────────┘
//!                  │ uses
//!                  ▼
//! ┌─────────────────────────────────────────┐
//! │       InfluxDbAdapter trait             │  (this crate)
//! │  auth_header_value()                    │
//! │  write_url()                            │
//! │  build_query()                          │
//! │  query_content_type()                   │
//! │  query_accept_header()                  │
//! │  parse_rows()                           │
//! │  health_url()                           │
//! └──────────┬──────────────────────────────┘
//!            │
//!     ┌──────┴──────┐
//!     ▼             ▼
//!  V2Adapter     V3Adapter
//!  Token auth    Bearer auth
//!  /api/v2/*     /api/v3/*
//!  Flux+CSV      SQL+JSONL
//! ```

pub mod adapter;
pub mod config;
pub mod protocol;
pub mod row;
mod v2;
mod v3;

pub use adapter::{InfluxDbAdapter, Row};
pub use config::ApiVersion;
pub use protocol::{write_field_string, write_measurement, write_tag_value};
pub use row::{parse_csv_rows, parse_jsonl_rows};
