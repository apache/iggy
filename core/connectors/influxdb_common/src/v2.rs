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

//! InfluxDB V2 adapter — Flux, `/api/v2/*`, `Token` auth, annotated CSV.

use crate::adapter::{InfluxDbAdapter, Row};
use crate::row::parse_csv_rows;
use iggy_connector_sdk::Error;
use reqwest::Url;

/// Adapter for InfluxDB 2.x.
///
/// | Aspect | Detail |
/// |---|---|
/// | Auth | `Authorization: Token {token}` |
/// | Write endpoint | `POST /api/v2/write?org=X&bucket=Y&precision=P` |
/// | Query endpoint | `POST /api/v2/query?org=X` with Flux dialect body |
/// | Query language | Flux |
/// | Response format | Annotated CSV (RFC 4180) |
#[derive(Debug)]
pub struct V2Adapter;

impl InfluxDbAdapter for V2Adapter {
    fn auth_header_value(&self, token: &str) -> String {
        format!("Token {token}")
    }

    fn write_url(
        &self,
        base: &str,
        bucket_or_db: &str,
        org: Option<&str>,
        precision: &str,
    ) -> Result<Url, Error> {
        let mut url = Url::parse(&format!("{base}/api/v2/write"))
            .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))?;

        {
            let mut q = url.query_pairs_mut();
            if let Some(o) = org {
                q.append_pair("org", o);
            }
            q.append_pair("bucket", bucket_or_db);
            q.append_pair("precision", precision);
        }

        Ok(url)
    }

    fn build_query(
        &self,
        base: &str,
        query: &str,
        _bucket_or_db: &str,
        org: Option<&str>,
    ) -> Result<(Url, serde_json::Value), Error> {
        let mut url = Url::parse(&format!("{base}/api/v2/query"))
            .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))?;

        if let Some(o) = org {
            url.query_pairs_mut().append_pair("org", o);
        }

        let body = serde_json::json!({
            "query": query,
            "dialect": {
                "annotations": [],
                "delimiter": ",",
                "header": true,
                "commentPrefix": "#"
            }
        });

        Ok((url, body))
    }

    fn query_content_type(&self) -> &'static str {
        "application/json"
    }

    fn query_accept_header(&self) -> &'static str {
        "text/csv"
    }

    fn parse_rows(&self, response_body: &str) -> Result<Vec<Row>, Error> {
        parse_csv_rows(response_body)
    }

    fn health_url(&self, base: &str) -> Result<Url, Error> {
        Url::parse(&format!("{base}/health"))
            .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "http://localhost:8086";

    #[test]
    fn auth_uses_token_scheme() {
        let a = V2Adapter;
        assert_eq!(a.auth_header_value("secret"), "Token secret");
    }

    #[test]
    fn write_url_includes_org_bucket_precision() {
        let a = V2Adapter;
        let url = a
            .write_url(BASE, "my_bucket", Some("my_org"), "us")
            .unwrap();
        let q = url.query().unwrap_or("");
        assert!(q.contains("org=my_org"), "missing org: {q}");
        assert!(q.contains("bucket=my_bucket"), "missing bucket: {q}");
        assert!(q.contains("precision=us"), "missing precision: {q}");
        assert!(
            url.path().ends_with("/api/v2/write"),
            "wrong path: {}",
            url.path()
        );
    }

    #[test]
    fn write_url_without_org() {
        let a = V2Adapter;
        let url = a.write_url(BASE, "bkt", None, "ns").unwrap();
        let q = url.query().unwrap_or("");
        assert!(!q.contains("org="), "org should be absent: {q}");
        assert!(q.contains("bucket=bkt"));
    }

    #[test]
    fn build_query_url_has_org_param() {
        let a = V2Adapter;
        let (url, body) = a
            .build_query(
                BASE,
                "from(bucket:\"b\") |> range(start:-1h)",
                "b",
                Some("org"),
            )
            .unwrap();
        assert!(
            url.path().ends_with("/api/v2/query"),
            "wrong path: {}",
            url.path()
        );
        let q = url.query().unwrap_or("");
        assert!(q.contains("org=org"), "missing org: {q}");
        assert!(body["query"].is_string(), "query field missing");
        assert!(body["dialect"].is_object(), "dialect field missing");
    }

    #[test]
    fn content_type_and_accept() {
        let a = V2Adapter;
        assert_eq!(a.query_content_type(), "application/json");
        assert_eq!(a.query_accept_header(), "text/csv");
    }

    #[test]
    fn health_url_path() {
        let a = V2Adapter;
        let url = a.health_url(BASE).unwrap();
        assert!(
            url.path().ends_with("/health"),
            "wrong path: {}",
            url.path()
        );
    }

    #[test]
    fn parse_rows_delegates_to_csv_parser() {
        let a = V2Adapter;
        let csv = "_time,_value\n2024-01-01T00:00:00Z,99\n";
        let rows = a.parse_rows(csv).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("_value").map(String::as_str), Some("99"));
    }
}
