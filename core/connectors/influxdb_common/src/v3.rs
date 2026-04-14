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

//! InfluxDB V3 adapter — SQL/InfluxQL, `/api/v3/*`, `Bearer` auth, JSONL.

use crate::adapter::{InfluxDbAdapter, Row};
use crate::row::parse_jsonl_rows;
use iggy_connector_sdk::Error;
use reqwest::Url;

/// Map a short precision string to InfluxDB 3's long-form equivalent.
///
/// InfluxDB 3 rejects the V2 short forms (`"ns"`, `"us"`, `"ms"`, `"s"`)
/// on the `/api/v3/write_lp` endpoint; it expects the full English words.
fn map_precision(p: &str) -> &'static str {
    match p {
        "ns" => "nanosecond",
        "ms" => "millisecond",
        "s" => "second",
        _ => "microsecond", // covers "us" and any unrecognised value
    }
}

/// Adapter for InfluxDB 3.x (Core / Enterprise).
///
/// | Aspect | Detail |
/// |---|---|
/// | Auth | `Authorization: Bearer {token}` |
/// | Write endpoint | `POST /api/v3/write_lp?db=X&precision=P` |
/// | Query endpoint | `POST /api/v3/query_sql` with JSON body `{"db":…,"q":…,"format":"jsonl"}` |
/// | Query language | SQL (default) or InfluxQL via `/api/v3/query_influxql` |
/// | Response format | JSONL (newline-delimited JSON objects) |
///
/// ## Backward-compatibility note
/// InfluxDB 3.x also accepts the V2 write endpoint (`/api/v2/write`) for
/// migration convenience. This adapter uses the native V3 endpoint by default.
/// If you need to target the V2-compat path you can switch `api_version = "v2"`
/// in your connector config — the `V2Adapter` will then be selected instead.
#[derive(Debug)]
pub struct V3Adapter;

impl InfluxDbAdapter for V3Adapter {
    fn auth_header_value(&self, token: &str) -> String {
        format!("Bearer {token}")
    }

    fn write_url(
        &self,
        base: &str,
        bucket_or_db: &str,
        _org: Option<&str>,
        precision: &str,
    ) -> Result<Url, Error> {
        let mut url = Url::parse(&format!("{base}/api/v3/write_lp"))
            .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))?;

        url.query_pairs_mut()
            .append_pair("db", bucket_or_db)
            .append_pair("precision", map_precision(precision));

        Ok(url)
    }

    fn build_query(
        &self,
        base: &str,
        query: &str,
        bucket_or_db: &str,
        _org: Option<&str>,
    ) -> Result<(Url, serde_json::Value), Error> {
        let url = Url::parse(&format!("{base}/api/v3/query_sql"))
            .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))?;

        let body = serde_json::json!({
            "db":     bucket_or_db,
            "q":      query,
            "format": "jsonl"
        });

        Ok((url, body))
    }

    fn query_content_type(&self) -> &'static str {
        "application/json"
    }

    fn query_accept_header(&self) -> &'static str {
        // InfluxDB 3.x rejects "application/jsonl" as an invalid MIME type.
        // The response format is controlled by the `"format":"jsonl"` field in
        // the request body, so the Accept header just needs to be valid JSON.
        "application/json"
    }

    fn parse_rows(&self, response_body: &str) -> Result<Vec<Row>, Error> {
        parse_jsonl_rows(response_body)
    }

    fn health_url(&self, base: &str) -> Result<Url, Error> {
        Url::parse(&format!("{base}/health"))
            .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "http://localhost:8181";

    #[test]
    fn map_precision_maps_all_short_forms() {
        assert_eq!(map_precision("ns"), "nanosecond");
        assert_eq!(map_precision("ms"), "millisecond");
        assert_eq!(map_precision("s"), "second");
        assert_eq!(map_precision("us"), "microsecond");
        assert_eq!(map_precision("xx"), "microsecond"); // unknown → microsecond
    }

    #[test]
    fn auth_uses_bearer_scheme() {
        let a = V3Adapter;
        assert_eq!(a.auth_header_value("secret"), "Bearer secret");
    }

    #[test]
    fn write_url_uses_db_param_not_bucket() {
        let a = V3Adapter;
        let url = a
            .write_url(BASE, "sensors", Some("ignored_org"), "ns")
            .unwrap();
        let q = url.query().unwrap_or("");
        assert!(q.contains("db=sensors"), "missing db: {q}");
        assert!(!q.contains("bucket="), "bucket should not appear: {q}");
        assert!(!q.contains("org="), "org should not appear: {q}");
        assert!(q.contains("precision=nanosecond"), "missing precision: {q}");
        assert!(
            url.path().ends_with("/api/v3/write_lp"),
            "wrong path: {}",
            url.path()
        );
    }

    #[test]
    fn build_query_url_no_org_param() {
        let a = V3Adapter;
        let (url, body) = a
            .build_query(
                BASE,
                "SELECT * FROM cpu LIMIT 10",
                "sensors",
                Some("ignored"),
            )
            .unwrap();
        assert!(
            url.path().ends_with("/api/v3/query_sql"),
            "wrong path: {}",
            url.path()
        );
        // org must NOT appear in URL
        assert!(
            url.query().unwrap_or("").is_empty() || !url.query().unwrap_or("").contains("org="),
            "org should not be in URL: {:?}",
            url.query()
        );
        assert_eq!(body["db"].as_str(), Some("sensors"));
        assert_eq!(body["format"].as_str(), Some("jsonl"));
        assert!(body["q"].as_str().unwrap().contains("SELECT"));
    }

    #[test]
    fn content_type_and_accept() {
        let a = V3Adapter;
        assert_eq!(a.query_content_type(), "application/json");
        assert_eq!(a.query_accept_header(), "application/json");
    }

    #[test]
    fn health_url_path() {
        let a = V3Adapter;
        let url = a.health_url(BASE).unwrap();
        assert!(
            url.path().ends_with("/health"),
            "wrong path: {}",
            url.path()
        );
    }

    #[test]
    fn parse_rows_delegates_to_jsonl_parser() {
        let a = V3Adapter;
        let jsonl = r#"{"_time":"2024-01-01T00:00:00Z","_value":"42","host":"s1"}"#;
        let rows = a.parse_rows(jsonl).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("host").map(String::as_str), Some("s1"));
        assert_eq!(rows[0].get("_value").map(String::as_str), Some("42"));
    }
}
