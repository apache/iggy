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

//! The `InfluxDbAdapter` trait — the single seam between version-agnostic
//! orchestration logic and version-specific HTTP details.

pub use crate::row::Row;
use iggy_connector_sdk::Error;
use reqwest::Url;

/// Version-specific HTTP details for InfluxDB sink and source connectors.
///
/// There are two concrete implementations:
/// - [`crate::v2::V2Adapter`] — InfluxDB 2.x (Flux, `/api/v2/*`, `Token` auth)
/// - [`crate::v3::V3Adapter`] — InfluxDB 3.x (SQL/InfluxQL, `/api/v3/*`, `Bearer` auth)
///
/// The connector structs store a `Box<dyn InfluxDbAdapter>` (created at
/// construction time from `ApiVersion::make_adapter()`) and call these methods
/// inside `open()` / `consume()` / `poll()`.
pub trait InfluxDbAdapter: Send + Sync + std::fmt::Debug {
    // ── Authentication ───────────────────────────────────────────────────────

    /// Return the full value for the `Authorization` HTTP header.
    ///
    /// - V2: `"Token {token}"`
    /// - V3 native: `"Bearer {token}"`
    fn auth_header_value(&self, token: &str) -> String;

    // ── Sink ─────────────────────────────────────────────────────────────────

    /// Build the fully-qualified write URL including all required query params.
    ///
    /// - V2: `POST /api/v2/write?org={org}&bucket={bucket}&precision={p}`
    /// - V3: `POST /api/v3/write_lp?db={db}&precision={p}`
    ///
    /// # Parameters
    /// - `base`           — base URL, e.g. `"http://localhost:8086"` (no trailing slash)
    /// - `bucket_or_db`   — V2: bucket name; V3: database name
    /// - `org`            — V2: organisation name (`Some`); V3: ignored (`None` ok)
    /// - `precision`      — timestamp precision string (`"ns"`, `"us"`, `"ms"`, `"s"`)
    fn write_url(
        &self,
        base: &str,
        bucket_or_db: &str,
        org: Option<&str>,
        precision: &str,
    ) -> Result<Url, Error>;

    // ── Source ───────────────────────────────────────────────────────────────

    /// Build the query `(url, json_body)` pair ready to be POSTed.
    ///
    /// - V2: URL = `/api/v2/query?org={org}`, body = Flux dialect JSON wrapper
    /// - V3: URL = `/api/v3/query_sql`, body = `{"db":…,"q":…,"format":"jsonl"}`
    ///
    /// # Parameters
    /// - `base`         — base URL
    /// - `query`        — final query string (placeholders already substituted)
    /// - `bucket_or_db` — V2: not used in body; V3: database name for `"db"` key
    /// - `org`          — V2: appended as `?org=` query param; V3: ignored
    fn build_query(
        &self,
        base: &str,
        query: &str,
        bucket_or_db: &str,
        org: Option<&str>,
    ) -> Result<(Url, serde_json::Value), Error>;

    /// `Content-Type` header for query requests.
    ///
    /// - V2: `"application/json"`
    /// - V3: `"application/json"`
    fn query_content_type(&self) -> &'static str;

    /// `Accept` header for query requests.
    ///
    /// - V2: `"text/csv"` (annotated CSV)
    /// - V3: `"application/json"` (format is controlled by body `"format":"jsonl"`)
    fn query_accept_header(&self) -> &'static str;

    /// Parse a raw query response body into a list of field-maps.
    ///
    /// - V2: parse annotated CSV (skip `#`-annotation rows and header rows)
    /// - V3: parse JSONL (one JSON object per line, values stringified)
    fn parse_rows(&self, response_body: &str) -> Result<Vec<Row>, Error>;

    // ── Shared ───────────────────────────────────────────────────────────────

    /// Health-check URL used by `open()` to verify server reachability.
    ///
    /// Both V2 and V3 expose `GET /health` and `GET /ping`; this returns
    /// `/health` for both since it is always unauthenticated in default setups.
    fn health_url(&self, base: &str) -> Result<Url, Error>;
}
