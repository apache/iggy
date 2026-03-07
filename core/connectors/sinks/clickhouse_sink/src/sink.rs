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

use crate::body::{build_json_body, build_row_binary_body, build_string_body};
use crate::{ClickHouseSink, InsertFormat, client::ClickHouseClient};
use async_trait::async_trait;
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata};
use tracing::{debug, error, info, warn};

#[async_trait]
impl Sink for ClickHouseSink {
    // ─── open ────────────────────────────────────────────────────────────────

    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening ClickHouse sink connector ID: {} → {}/{} (format: {:?})",
            self.id, self.config.url, self.config.table, self.insert_format,
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
            InsertFormat::StringPassthrough => build_string_body(&messages, self.string_format),
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
        self.table_schema = None;
        Ok(())
    }
}
