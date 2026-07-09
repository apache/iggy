// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Thin `reqwest`-based HTTP client for the ClickHouse HTTP interface.
//!
//! ClickHouse exposes its HTTP API at `http://host:port/`. Queries are sent
//! either as a URL query parameter (`?query=...`) or in the request body.
//! Authentication uses the `X-ClickHouse-User` and `X-ClickHouse-Key` headers.
//!
//! Insert format:
//!   POST /?database={db}&query=INSERT+INTO+{table}+FORMAT+{fmt}
//!   Body: row data in the chosen format

use crate::schema::{Column, parse_type};
use bytes::Bytes;
use iggy_connector_sdk::Error;
use rand::RngExt;
use reqwest::StatusCode;
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::Deserialize;
use std::time::Duration;
use tracing::{debug, error, info, warn};

const USER_HEADER: &str = "X-ClickHouse-User";
const KEY_HEADER: &str = "X-ClickHouse-Key";

/// Thin wrapper around `reqwest::Client` pre-configured for a ClickHouse
/// endpoint.
#[derive(Debug)]
pub(crate) struct ClickHouseClient {
    inner: reqwest::Client,
    base_url: String,
    database: String,
    table: String,
    format_name: String,
    insert_url: String,
    insert_query: String,
}

impl ClickHouseClient {
    /// Build a new client.
    pub fn new(
        base_url: String,
        database: String,
        table: String,
        format_name: String,
        username: &str,
        password: &str,
        timeout: Duration,
    ) -> Result<Self, Error> {
        let mut auth_headers = HeaderMap::new();
        auth_headers.insert(
            USER_HEADER,
            HeaderValue::from_str(username)
                .map_err(|e| Error::InitError(format!("Invalid username header value: {e}")))?,
        );
        let mut key_value = HeaderValue::from_str(password)
            .map_err(|e| Error::InitError(format!("Invalid password header value: {e}")))?;
        key_value.set_sensitive(true);
        auth_headers.insert(KEY_HEADER, key_value);

        let inner = reqwest::Client::builder()
            .timeout(timeout)
            .default_headers(auth_headers)
            .build()
            .map_err(|e| Error::InitError(format!("Failed to build HTTP client: {e}")))?;

        let insert_url = format!(
            "{}/?database={}&date_time_input_format=best_effort",
            base_url,
            urlencoded(&database),
        );
        let insert_query = format!(
            "INSERT INTO `{}`.`{}` FORMAT {}",
            escape_backtick(&database),
            escape_backtick(&table),
            format_name,
        );

        Ok(ClickHouseClient {
            inner,
            base_url,
            database,
            table,
            format_name,
            insert_url,
            insert_query,
        })
    }

    /// Send `SELECT 1` to verify the server is reachable.
    pub async fn ping(&self) -> Result<(), Error> {
        let url = format!("{}/ping", self.base_url);
        let response = self
            .inner
            .get(&url)
            .send()
            .await
            .map_err(|e| Error::InitError(format!("Ping failed: {e}")))?;

        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            error!("ClickHouse ping returned HTTP {status}: {body}");
            Err(Error::InitError(format!(
                "ClickHouse ping returned HTTP {status}: {body}"
            )))
        }
    }

    /// Fetch the column definitions for the configured table.
    /// Returns columns ordered by their position in the table definition.
    pub async fn fetch_schema(&self) -> Result<Vec<Column>, Error> {
        let query = format!(
            "SELECT name, type, default_kind FROM system.columns \
             WHERE database = '{}' AND table = '{}' \
             ORDER BY position \
             FORMAT JSONEachRow",
            escape_single_quote(&self.database),
            escape_single_quote(&self.table),
        );

        let body = self.run_query(&query).await?;
        let columns = parse_schema_body(&body)?;

        if columns.is_empty() {
            error!(
                "Table '{}' not found or has no columns in database '{}'",
                self.table, self.database
            );
            return Err(Error::InitError(format!(
                "Table '{}' not found in database '{}'",
                self.table, self.database
            )));
        }

        info!(
            "Fetched schema for table '{}': {} columns",
            self.table,
            columns.len()
        );
        Ok(columns)
    }

    /// Insert `body` into `table` using the given ClickHouse FORMAT string.
    ///
    /// Retries up to `max_retries` times on transient errors (network errors,
    /// HTTP 429, HTTP 5xx). Does not retry on HTTP 4xx (data errors).
    ///
    /// # At-least-once semantics
    ///
    /// Each retry resends the identical body with no `insert_deduplication_token`.
    /// If the server committed the batch but the response was lost, the retry
    /// produces duplicate rows. Callers must tolerate this or handle
    /// deduplication at read time. See the README for details.
    // TODO: accept `Bytes` instead of `Vec<u8>` so callers can build into a
    // `BytesMut`, freeze it zero-copy, and reuse a thread-local buffer across
    // batches. `insert` is `pub(crate)` so the change is fully contained.
    pub async fn insert(
        &self,
        body: Vec<u8>,
        max_retries: u32,
        retry_delay: Duration,
    ) -> Result<(), Error> {
        if body.is_empty() {
            debug!("insert called with empty body — skipping");
            return Ok(());
        }

        let body = Bytes::from(body);
        let mut attempts = 0u32;
        loop {
            let result = self
                .inner
                .post(&self.insert_url)
                .header(CONTENT_TYPE, "application/octet-stream")
                .query(&[("query", &self.insert_query)])
                .body(body.clone())
                .send()
                .await;

            match result {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        debug!(
                            "Inserted {} bytes into {}.{} FORMAT {}",
                            body.len(),
                            self.database,
                            self.table,
                            self.format_name
                        );
                        return Ok(());
                    }

                    let body_text = response.text().await.unwrap_or_default();

                    if is_retryable_status(status) {
                        attempts += 1;
                        if attempts >= max_retries {
                            error!(
                                "Insert failed after {attempts} attempts (HTTP {status}): {body_text}"
                            );
                            return Err(Error::CannotStoreData(format!(
                                "HTTP {status}: {body_text}"
                            )));
                        }
                        warn!(
                            "Retryable HTTP {status} on attempt {attempts}/{max_retries}: {body_text}"
                        );
                        tokio::time::sleep(jittered_backoff(retry_delay, attempts)).await;
                    } else {
                        // Non-retryable 4xx data error. PermanentHttpError keeps
                        // the runtime circuit breaker from tripping on bad data.
                        error!("ClickHouse insert error HTTP {status}: {body_text}");
                        return Err(Error::PermanentHttpError(format!(
                            "HTTP {status}: {body_text}"
                        )));
                    }
                }
                Err(e) => {
                    // Network / timeout error — retryable.
                    attempts += 1;
                    if attempts >= max_retries {
                        error!("Insert failed after {attempts} attempts: {e}");
                        return Err(Error::CannotStoreData(format!(
                            "Network error after {attempts} attempts: {e}"
                        )));
                    }
                    warn!("Network error on attempt {attempts}/{max_retries}: {e}. Retrying...");
                    tokio::time::sleep(jittered_backoff(retry_delay, attempts)).await;
                }
            }
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /// Run a read-only query and return the response body as a String.
    async fn run_query(&self, query: &str) -> Result<String, Error> {
        let url = format!("{}/?database={}", self.base_url, urlencoded(&self.database));
        let response = self
            .inner
            .post(&url)
            .body(query.to_owned())
            .send()
            .await
            .map_err(|e| Error::InitError(format!("Query failed: {e}")))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| Error::InitError(format!("Failed to read response: {e}")))?;

        if !status.is_success() {
            error!("Query returned HTTP {status}: {body}");
            return Err(Error::InitError(format!("HTTP {status}: {body}")));
        }
        Ok(body)
    }
}

// ─── Helper types ─────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct SchemaRow {
    name: String,
    r#type: String,
    default_kind: Option<String>,
}

/// Parse a `system.columns` JSONEachRow response into insertable columns.
///
/// MATERIALIZED / ALIAS / EPHEMERAL columns are not part of the
/// RowBinaryWithDefaults insert set: ClickHouse expects zero bytes for them,
/// not even the DEFAULT flag. Emitting a prefix byte would shift the whole row
/// stream by one, so they are dropped entirely. `has_default` is set only for
/// ordinary DEFAULT columns.
fn parse_schema_body(body: &str) -> Result<Vec<Column>, Error> {
    let mut columns = Vec::new();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let row: SchemaRow = serde_json::from_str(line).map_err(|e| {
            error!("Failed to parse schema row '{line}': {e}");
            Error::InitError(format!("Schema parse error: {e}"))
        })?;

        let default_kind = row.default_kind.as_deref().unwrap_or("");
        match default_kind {
            "MATERIALIZED" | "ALIAS" | "EPHEMERAL" => continue,
            _ => {}
        }

        let ch_type = parse_type(&row.r#type)?;
        columns.push(Column {
            name: row.name,
            ch_type,
            has_default: default_kind == "DEFAULT",
        });
    }
    Ok(columns)
}

fn is_retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS | StatusCode::REQUEST_TIMEOUT
    ) || status.is_server_error()
}

/// Exponential backoff with full jitter.
///
/// Cap grows as `base * 2^attempt`, clamped to 60 s. The actual sleep is
/// a uniform random value in `[0, cap]`, so concurrent instances spread
/// their retries instead of thundering back together.
pub(crate) fn jittered_backoff(base: Duration, attempt: u32) -> Duration {
    const MAX: Duration = Duration::from_secs(60);
    let cap = base.saturating_mul(2u32.saturating_pow(attempt)).min(MAX);
    let cap_ms = cap.as_millis() as u64;
    Duration::from_millis(rand::rng().random_range(0..=cap_ms))
}

fn escape_single_quote(s: &str) -> String {
    s.replace('\'', "''")
}

fn escape_backtick(s: &str) -> String {
    s.replace('`', "``")
}

fn urlencoded(s: &str) -> String {
    // Minimal percent-encoding for the database name query parameter.
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => out.push(ch),
            other => {
                let mut buf = [0u8; 4];
                for byte in other.encode_utf8(&mut buf).bytes() {
                    out.push_str(&format!("%{byte:02X}"));
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_single_quote_plain() {
        assert_eq!(escape_single_quote("hello"), "hello");
    }

    #[test]
    fn escape_single_quote_doubles_quote() {
        assert_eq!(escape_single_quote("it's"), "it''s");
    }

    #[test]
    fn escape_single_quote_trailing_backslash_cannot_break_out() {
        // A trailing backslash must not escape the closing quote in SQL.
        // With the old \' approach, "foo\" → 'foo\'' which keeps the string open.
        // With doubling, "foo\" → 'foo\'' is never produced; output is "foo\" untouched,
        // and no single quote is present so no doubling occurs — the backslash is inert
        // inside a single-quoted ClickHouse string when not followed by a special char.
        let result = escape_single_quote("foo\\");
        assert_eq!(result, "foo\\");
        // Critically, the result contains no single quote, so it cannot close+reopen the literal.
        assert!(!result.contains("\\'"));
    }

    #[test]
    fn escape_backtick_plain() {
        assert_eq!(escape_backtick("my_table"), "my_table");
    }

    #[test]
    fn escape_backtick_doubles_backtick() {
        assert_eq!(escape_backtick("ta`ble"), "ta``ble");
    }

    #[test]
    fn escape_backtick_trailing_backslash_cannot_break_out() {
        // With the old \` approach, "innocent\" → `innocent\`` where \` is an escaped
        // backtick, leaving the identifier unclosed — SQL injection vector.
        // With doubling, the backslash is passed through as-is and is inert because
        // no \` sequence is ever produced.
        let result = escape_backtick("innocent\\");
        assert_eq!(result, "innocent\\");
        assert!(!result.contains("\\`"));
    }

    fn test_client(database: &str, table: &str, format_name: &str) -> ClickHouseClient {
        ClickHouseClient::new(
            "http://localhost:8123".to_owned(),
            database.to_owned(),
            table.to_owned(),
            format_name.to_owned(),
            "user",
            "pass",
            std::time::Duration::from_secs(10),
        )
        .expect("client construction failed")
    }

    #[test]
    fn given_plain_config_new_should_precompute_insert_url() {
        let client = test_client("mydb", "events", "JSONEachRow");
        assert_eq!(
            client.insert_url,
            "http://localhost:8123/?database=mydb&date_time_input_format=best_effort"
        );
    }

    #[test]
    fn given_plain_config_new_should_precompute_insert_query() {
        let client = test_client("mydb", "events", "JSONEachRow");
        assert_eq!(
            client.insert_query,
            "INSERT INTO `mydb`.`events` FORMAT JSONEachRow"
        );
    }

    #[test]
    fn given_database_needing_encoding_new_should_percent_encode_insert_url() {
        let client = test_client("my db", "events", "JSONEachRow");
        assert!(
            client.insert_url.contains("database=my%20db"),
            "URL was: {}",
            client.insert_url
        );
    }

    #[test]
    fn given_materialized_alias_ephemeral_columns_parse_schema_body_should_drop_them() {
        // (id UInt64, m UInt64 MATERIALIZED id*2, a UInt64 ALIAS id, e UInt64 EPHEMERAL, name String)
        // Only id and name are insertable, so the connector must emit 2 slots per row.
        let body = concat!(
            r#"{"name":"id","type":"UInt64","default_kind":""}"#,
            "\n",
            r#"{"name":"m","type":"UInt64","default_kind":"MATERIALIZED"}"#,
            "\n",
            r#"{"name":"a","type":"UInt64","default_kind":"ALIAS"}"#,
            "\n",
            r#"{"name":"e","type":"UInt64","default_kind":"EPHEMERAL"}"#,
            "\n",
            r#"{"name":"name","type":"String","default_kind":""}"#,
        );
        let columns = parse_schema_body(body).unwrap();
        let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "name"]);
        assert!(columns.iter().all(|c| !c.has_default));
    }

    #[test]
    fn given_default_column_parse_schema_body_should_flag_has_default() {
        let body = concat!(
            r#"{"name":"id","type":"UInt64","default_kind":""}"#,
            "\n",
            r#"{"name":"created","type":"DateTime","default_kind":"DEFAULT"}"#,
        );
        let columns = parse_schema_body(body).unwrap();
        assert_eq!(columns.len(), 2);
        assert!(!columns[0].has_default);
        assert!(columns[1].has_default);
    }

    #[test]
    fn given_names_with_backticks_new_should_double_escape_insert_query() {
        let client = test_client("my`db", "ta`ble", "JSONEachRow");
        assert_eq!(
            client.insert_query,
            "INSERT INTO `my``db`.`ta``ble` FORMAT JSONEachRow"
        );
    }
}
