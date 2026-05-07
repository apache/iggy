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
}

impl ClickHouseClient {
    /// Build a new client.
    pub fn new(
        base_url: String,
        database: String,
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
        auth_headers.insert(
            KEY_HEADER,
            HeaderValue::from_str(password)
                .map_err(|e| Error::InitError(format!("Invalid password header value: {e}")))?,
        );

        let inner = reqwest::Client::builder()
            .timeout(timeout)
            .default_headers(auth_headers)
            .build()
            .map_err(|e| Error::InitError(format!("Failed to build HTTP client: {e}")))?;

        Ok(ClickHouseClient {
            inner,
            base_url,
            database,
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

    /// Fetch the column definitions for `table` in the configured database.
    /// Returns columns ordered by their position in the table definition.
    pub async fn fetch_schema(&self, table: &str) -> Result<Vec<Column>, Error> {
        let query = format!(
            "SELECT name, type, default_kind FROM system.columns \
             WHERE database = '{}' AND table = '{}' \
             ORDER BY position \
             FORMAT JSONEachRow",
            escape_single_quote(&self.database),
            escape_single_quote(table),
        );

        let body = self.run_query(&query).await?;
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

            let ch_type = parse_type(&row.r#type)?;
            let has_default = matches!(
                row.default_kind.as_deref(),
                Some("DEFAULT") | Some("MATERIALIZED") | Some("ALIAS")
            );
            columns.push(Column {
                name: row.name,
                ch_type,
                has_default,
            });
        }

        if columns.is_empty() {
            error!(
                "Table '{table}' not found or has no columns in database '{}'",
                self.database
            );
            return Err(Error::InitError(format!(
                "Table '{table}' not found in database '{}'",
                self.database
            )));
        }

        info!(
            "Fetched schema for table '{}': {} columns",
            table,
            columns.len()
        );
        Ok(columns)
    }

    /// Insert `body` into `table` using the given ClickHouse FORMAT string.
    ///
    /// Retries up to `max_retries` times on transient errors (network errors,
    /// HTTP 429, HTTP 5xx). Does not retry on HTTP 4xx (data errors).
    pub async fn insert(
        &self,
        table: &str,
        format: &str,
        body: Vec<u8>,
        max_retries: u32,
        retry_delay: Duration,
    ) -> Result<(), Error> {
        if body.is_empty() {
            debug!("insert called with empty body — skipping");
            return Ok(());
        }

        let query = format!(
            "INSERT INTO `{}`.`{}` FORMAT {}",
            escape_backtick(&self.database),
            escape_backtick(table),
            format,
        );
        let url = format!(
            "{}/?database={}&date_time_input_format=best_effort",
            self.base_url,
            urlencoded(&self.database),
        );

        let body = Bytes::from(body);
        let mut attempts = 0u32;
        loop {
            let result = self
                .inner
                .post(&url)
                .header(CONTENT_TYPE, "application/octet-stream")
                .query(&[("query", &query)])
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
                            table,
                            format
                        );
                        return Ok(());
                    }

                    let body_text = response.text().await.unwrap_or_default();

                    if is_retryable_status(status) {
                        attempts += 1;
                        if attempts > max_retries {
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
                        tokio::time::sleep(retry_delay * attempts).await;
                    } else {
                        // Non-retryable: 4xx data error — log and fail immediately.
                        error!("ClickHouse insert error HTTP {status}: {body_text}");
                        return Err(Error::CannotStoreData(format!(
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
                    tokio::time::sleep(retry_delay * attempts).await;
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

fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn escape_single_quote(s: &str) -> String {
    s.replace('\'', "\\'")
}

fn escape_backtick(s: &str) -> String {
    s.replace('`', "\\`")
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
