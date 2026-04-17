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

use crate::adapter::InfluxDbAdapter;
use crate::row::{Row, parse_jsonl_rows};
use iggy_connector_sdk::Error;
use reqwest::Url;

/// The format parameter value used in V3 query request bodies.
const JSONL_FORMAT: &str = "jsonl";

/// Map a short precision string to InfluxDB 3's long-form equivalent.
///
/// InfluxDB 3 rejects the V2 short forms (`"ns"`, `"us"`, `"ms"`, `"s"`)
/// on the `/api/v3/write_lp` endpoint; it expects the full English words.
/// Returns an error for unrecognised precision values rather than silently
/// defaulting, which would timestamp data at the wrong precision.
fn map_precision(p: &str) -> Result<&'static str, Error> {
    match p {
        "ns" => Ok("nanosecond"),
        "us" => Ok("microsecond"),
        "ms" => Ok("millisecond"),
        "s" => Ok("second"),
        other => Err(Error::InvalidConfigValue(format!(
            "unknown precision {other:?}; valid values are \"ns\", \"us\", \"ms\", \"s\""
        ))),
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
pub(crate) struct V3Adapter;

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
            .append_pair("precision", map_precision(precision)?);

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
            "format": JSONL_FORMAT
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
        Url::parse(&format!("{}/health", base.trim_end_matches('/')))
            .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "http://localhost:8181";

    #[test]
    fn map_precision_maps_all_short_forms() {
        assert_eq!(map_precision("ns").unwrap(), "nanosecond");
        assert_eq!(map_precision("ms").unwrap(), "millisecond");
        assert_eq!(map_precision("s").unwrap(), "second");
        assert_eq!(map_precision("us").unwrap(), "microsecond");
    }

    #[test]
    fn map_precision_rejects_unknown_values() {
        assert!(map_precision("xx").is_err());
        assert!(map_precision("").is_err());
        assert!(map_precision("nanosecond").is_err());
    }

    #[test]
    fn auth_uses_bearer_scheme() {
        let a = V3Adapter;
        assert_eq!(a.auth_header_value("secret"), "Bearer secret");
    }

    #[test]
    fn write_url_encodes_db_with_special_characters() {
        // Database names with spaces and slashes are valid in InfluxDB 3.
        // query_pairs_mut().append_pair() percent-encodes them; this test
        // confirms the round-trip: encoded in the wire URL, recoverable on decode.
        let a = V3Adapter;
        let url = a.write_url(BASE, "team/sensors v2", None, "ns").unwrap();
        let q = url.query().unwrap_or("");
        // Raw characters must not appear verbatim in the query string.
        assert!(
            !q.contains("team/sensors v2"),
            "special chars should be encoded: {q}"
        );
        // Decoding must recover the original value exactly.
        let pairs: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(
            pairs.get("db").map(String::as_str),
            Some("team/sensors v2"),
            "decoded db name mismatch"
        );
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
    fn health_url_handles_trailing_slash() {
        let a = V3Adapter;
        let url = a.health_url("http://localhost:8181/").unwrap();
        assert!(!url.path().contains("//"));
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

    #[test]
    fn write_url_invalid_base_returns_error() {
        let a = V3Adapter;
        assert!(a.write_url("not a url", "db", None, "ns").is_err());
        assert!(a.write_url("", "db", None, "ns").is_err());
    }

    #[test]
    fn build_query_invalid_base_returns_error() {
        let a = V3Adapter;
        assert!(a.build_query("not a url", "SELECT 1", "db", None).is_err());
    }

    #[test]
    fn health_url_invalid_base_returns_error() {
        let a = V3Adapter;
        assert!(a.health_url("not a url").is_err());
        assert!(a.health_url("").is_err());
    }

    #[test]
    fn build_query_format_is_jsonl() {
        let a = V3Adapter;
        let (_, body) = a.build_query(BASE, "SELECT 1", "db", None).unwrap();
        assert_eq!(body["format"].as_str(), Some(JSONL_FORMAT));
    }
}
