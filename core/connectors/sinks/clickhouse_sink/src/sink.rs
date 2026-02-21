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

use crate::{ClickHouseSink, InsertFormat, StringFormat, client::ClickHouseClient};
use async_trait::async_trait;
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata};
use tracing::{debug, error, info, warn};

#[async_trait]
impl Sink for ClickHouseSink {
    // ─── open ────────────────────────────────────────────────────────────────

    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening ClickHouse sink connector ID: {} → {}/{} (format: {:?})",
            self.id,
            self.config.url,
            self.config.table,
            self.insert_format,
        );

        let client = ClickHouseClient::new(
            self.config.url.clone(),
            self.database().to_owned(),
            self.username(),
            self.password(),
            self.timeout(),
        )?;

        client.ping().await?;
        info!("ClickHouse sink ID: {} — ping OK", self.id);

        // For RowBinary mode, fetch and validate the table schema at startup.
        // This fails fast if the table doesn't exist or contains unsupported types.
        if self.insert_format == InsertFormat::RowBinary {
            let schema = client.fetch_schema(&self.config.table).await?;
            info!(
                "ClickHouse sink ID: {} — loaded schema ({} columns) for table '{}'",
                self.id,
                schema.len(),
                self.config.table
            );
            self.table_schema = Some(schema);
        }

        self.client = Some(client);
        Ok(())
    }

    // ─── consume ─────────────────────────────────────────────────────────────

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        if messages.is_empty() {
            return Ok(());
        }

        debug!(
            "ClickHouse sink ID: {} received {} messages from {}/{} partition {} offset {}",
            self.id,
            messages.len(),
            topic_metadata.stream,
            topic_metadata.topic,
            messages_metadata.partition_id,
            messages_metadata.current_offset,
        );

        let client = self.get_client()?;
        let table = &self.config.table;
        let format_name = self
            .insert_format
            .clickhouse_format_name(self.string_format);

        let body = match self.insert_format {
            InsertFormat::JsonEachRow => build_json_body(&messages),
            InsertFormat::RowBinary => {
                let schema = self.table_schema.as_deref().ok_or_else(|| {
                    error!("RowBinary mode but table schema is not loaded");
                    Error::InitError("Table schema not loaded".into())
                })?;
                build_row_binary_body(&messages, schema)?
            }
            InsertFormat::StringPassthrough => {
                build_string_body(&messages, self.string_format)
            }
        };

        if body.is_empty() {
            warn!(
                "ClickHouse sink ID: {} — no serialisable messages in batch of {}",
                self.id,
                messages.len()
            );
            return Ok(());
        }

        client
            .insert(
                table,
                format_name,
                body,
                self.max_retries(),
                self.retry_delay,
            )
            .await?;

        let count = messages.len() as u64;
        let mut state = self.state.lock().await;
        state.messages_processed += count;

        if self.verbose() {
            info!(
                "ClickHouse sink ID: {} inserted {} messages into '{table}' FORMAT {format_name}",
                self.id, count
            );
        } else {
            debug!(
                "ClickHouse sink ID: {} inserted {} messages into '{table}' FORMAT {format_name}",
                self.id, count
            );
        }

        Ok(())
    }

    // ─── close ───────────────────────────────────────────────────────────────

    async fn close(&mut self) -> Result<(), Error> {
        let state = self.state.lock().await;
        info!(
            "ClickHouse sink ID: {} closed. Processed {} messages, {} errors.",
            self.id, state.messages_processed, state.errors_count,
        );
        self.client = None;
        Ok(())
    }
}

// ─── Body builders ────────────────────────────────────────────────────────────

/// Build a newline-delimited JSON body for `FORMAT JSONEachRow`.
/// Each `Payload::Json` message becomes one line. Other payload types are skipped.
fn build_json_body(messages: &[ConsumedMessage]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(messages.len() * 64);
    for msg in messages {
        match &msg.payload {
            Payload::Json(value) => {
                if let Ok(s) = simd_json::to_string(value) {
                    buf.extend_from_slice(s.as_bytes());
                    buf.push(b'\n');
                } else {
                    warn!("Failed to serialise JSON payload at offset {}", msg.offset);
                }
            }
            other => {
                warn!(
                    "JSONEachRow mode: skipping unsupported payload type {:?} at offset {}",
                    payload_type_name(other),
                    msg.offset
                );
            }
        }
    }
    buf
}

/// Build a RowBinaryWithDefaults body.
/// Each `Payload::Json` message is serialised to binary using the table schema.
fn build_row_binary_body(
    messages: &[ConsumedMessage],
    schema: &[crate::schema::Column],
) -> Result<Vec<u8>, Error> {
    let mut buf = Vec::with_capacity(messages.len() * 128);
    for msg in messages {
        match &msg.payload {
            Payload::Json(value) => {
                crate::binary::serialize_row(value, schema, &mut buf)?;
            }
            other => {
                warn!(
                    "RowBinary mode: skipping unsupported payload type {:?} at offset {}",
                    payload_type_name(other),
                    msg.offset
                );
            }
        }
    }
    Ok(buf)
}

/// Build a raw string body for CSV / TSV / JSONEachRow string passthrough.
/// Each `Payload::Text` message is written as-is with a trailing newline
/// appended for CSV/TSV if not already present.
fn build_string_body(messages: &[ConsumedMessage], string_format: StringFormat) -> Vec<u8> {
    let mut buf = Vec::with_capacity(messages.len() * 64);
    for msg in messages {
        match &msg.payload {
            Payload::Text(s) => {
                buf.extend_from_slice(s.as_bytes());
                if string_format.requires_newline() && !s.ends_with('\n') {
                    buf.push(b'\n');
                }
            }
            other => {
                warn!(
                    "String passthrough mode: skipping unsupported payload type {:?} at offset {}",
                    payload_type_name(other),
                    msg.offset
                );
            }
        }
    }
    buf
}

fn payload_type_name(p: &Payload) -> &'static str {
    match p {
        Payload::Json(_) => "Json",
        Payload::Raw(_) => "Raw",
        Payload::Text(_) => "Text",
        Payload::Proto(_) => "Proto",
        Payload::FlatBuffer(_) => "FlatBuffer",
    }
}
