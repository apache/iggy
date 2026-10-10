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

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use iggy_connector_sdk::retry::{RetryPolicy, retry_async};
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::client::{AppendOutcome, BigQueryClient};
use crate::encode::{self, Chunk, RunContext};
use crate::error::{AppendError, TableError};
use crate::schema::{TableLayout, parse_table_schema};
use crate::{BigQuerySink, OpenState};

#[async_trait]
impl Sink for BigQuerySink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening BigQuery sink connector ID: {} for table {} (mode: {:?})",
            self.id, self.target, self.settings.mode
        );
        // The SDK turns a failed open into a bare status code, so this is the
        // only place the reason reaches the runtime log.
        self.try_open().await.inspect_err(|e| {
            error!(
                "Failed to open BigQuery sink connector ID: {} for table {}: {e}",
                self.id, self.target
            );
        })
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        if messages.is_empty() {
            return Ok(());
        }
        let Some(state) = &self.state else {
            return Err(Error::InitError("BigQuery sink is not open".into()));
        };
        let received = messages.len();
        debug!(
            "BigQuery sink ID: {} received {received} messages from {}/{} partition {} current_offset {}",
            self.id,
            topic_metadata.stream,
            topic_metadata.topic,
            messages_metadata.partition_id,
            messages_metadata.current_offset
        );

        let ctx = RunContext {
            layout: &state.layout,
            topic: topic_metadata,
            messages: &messages_metadata,
            max_request_bytes: self.settings.max_request_bytes,
            write_stream: state.client.write_stream_name(),
            missing_value: self.settings.missing_value.as_proto(),
        };
        let encoded = encode::encode(&ctx, messages).map_err(|e| {
            self.counters
                .rows_failed
                .fetch_add(received as u64, Ordering::Relaxed);
            error!(
                "BigQuery sink ID: {} cannot encode {received} messages from {}/{} partition {}: {e}",
                self.id, topic_metadata.stream, topic_metadata.topic, messages_metadata.partition_id
            );
            Error::Serialization(e.to_string())
        })?;

        for rejected in &encoded.rejected {
            warn!(
                "BigQuery sink ID: {} dropped message stream={} topic={} partition={} offset={}: {}",
                self.id,
                topic_metadata.stream,
                topic_metadata.topic,
                messages_metadata.partition_id,
                rejected.offset,
                rejected.reason
            );
        }
        self.counters
            .rows_rejected
            .fetch_add(encoded.rejected.len() as u64, Ordering::Relaxed);
        if encoded.chunks.is_empty() {
            return Err(Error::InvalidRecordValue(format!(
                "all {received} messages were rejected before AppendRows"
            )));
        }

        let mut written = 0u64;
        let mut last_error = None;
        for chunk in &encoded.chunks {
            match self
                .write_chunk(&state.client, chunk, topic_metadata, &messages_metadata)
                .await
            {
                Ok(rows) => written += rows,
                Err(e) => last_error = Some(e),
            }
        }
        self.counters
            .rows_written
            .fetch_add(written, Ordering::Relaxed);

        if self.settings.verbose {
            info!(
                "BigQuery sink ID: {} wrote {written} of {received} messages from {}/{} partition {} to {}",
                self.id,
                topic_metadata.stream,
                topic_metadata.topic,
                messages_metadata.partition_id,
                self.target
            );
        } else {
            debug!(
                "BigQuery sink ID: {} wrote {written} of {received} messages from {}/{} partition {} to {}",
                self.id,
                topic_metadata.stream,
                topic_metadata.topic,
                messages_metadata.partition_id,
                self.target
            );
        }

        match last_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.state = None;
        info!(
            "Closed BigQuery sink connector ID: {}, rows written: {}, rows rejected: {}, rows failed: {}, rows unconfirmed: {}",
            self.id,
            self.counters.rows_written.load(Ordering::Relaxed),
            self.counters.rows_rejected.load(Ordering::Relaxed),
            self.counters.rows_failed.load(Ordering::Relaxed),
            self.counters.rows_unconfirmed.load(Ordering::Relaxed)
        );
        Ok(())
    }
}

impl BigQuerySink {
    async fn try_open(&mut self) -> Result<(), Error> {
        self.config.validate()?;
        let timeout_duration = self.settings.timeout;
        timeout(timeout_duration, self.try_open_inner())
            .await
            .map_err(|_| {
                Error::InitError(format!(
                    "opening BigQuery sink timed out after {timeout_duration:?}"
                ))
            })?
    }

    async fn try_open_inner(&mut self) -> Result<(), Error> {
        let mut client = BigQueryClient::connect(self.id, &self.config, &self.settings).await?;
        let policy = self.retry_policy();

        let context = format!("BigQuery sink ID: {} tables.get", self.id);
        let body = retry_async(policy, &context, TableError::is_retryable, || {
            client.fetch_table()
        })
        .await
        .map_err(|failure| Error::InitError(format!("cannot read the table schema: {failure}")))?;

        let columns = parse_table_schema(&body).map_err(|e| Error::InitError(e.to_string()))?;
        let layout = TableLayout::build(columns, &self.settings)
            .map_err(|e| Error::InitError(e.to_string()))?;

        let context = format!("BigQuery sink ID: {} write stream setup", self.id);
        let stream = retry_async(policy, &context, AppendError::is_retryable, || {
            client.resolve_write_stream()
        })
        .await
        .map_err(|failure| Error::InitError(format!("cannot open the write stream: {failure}")))?;
        client.set_write_stream(stream);

        info!(
            "Opened BigQuery sink connector ID: {} for table {}: {} payload column(s), {} metadata column(s)",
            self.id,
            self.target,
            layout.raw.as_ref().map_or(layout.columns.len(), |_| 1),
            layout.metadata.len()
        );
        self.state = Some(OpenState { client, layout });
        Ok(())
    }

    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy {
            max_attempts: self.settings.max_retries,
            base_delay: self.settings.retry_delay,
            max_delay: self.settings.max_retry_delay,
        }
    }

    /// Append one chunk. When BigQuery reports row errors, nothing was
    /// appended: drop the reported rows and append the rest once more.
    /// Returns the number of rows written.
    async fn write_chunk(
        &self,
        client: &BigQueryClient,
        chunk: &Chunk,
        topic: &TopicMetadata,
        messages: &MessagesMetadata,
    ) -> Result<u64, Error> {
        let row_errors = match self
            .append_with_retry(client, chunk, topic, messages)
            .await?
        {
            AppendOutcome::Appended => return Ok(chunk.offsets.len() as u64),
            AppendOutcome::RowErrors(row_errors) => row_errors,
        };

        let row_errors =
            validate_row_errors(row_errors, chunk.offsets.len()).inspect_err(|_| {
                self.counters
                    .rows_failed
                    .fetch_add(chunk.offsets.len() as u64, Ordering::Relaxed);
            })?;
        let mut dropped = Vec::with_capacity(row_errors.len());
        for (row, reason) in row_errors {
            let offset = chunk.offsets.get(row).copied();
            warn!(
                "BigQuery sink ID: {} dropped row rejected by BigQuery stream={} topic={} partition={} offset={}: {reason}",
                self.id,
                topic.stream,
                topic.topic,
                messages.partition_id,
                offset.map_or_else(|| format!("unknown (row {row})"), |o| o.to_string())
            );
            dropped.push(row);
        }
        self.counters
            .rows_rejected
            .fetch_add(dropped.len() as u64, Ordering::Relaxed);

        let Some(retry) = encode::without_rows(chunk, &dropped)
            .map_err(|e| Error::Serialization(e.to_string()))?
        else {
            return Err(Error::InvalidRecordValue(
                "BigQuery rejected every row in the AppendRows request".into(),
            ));
        };
        match self
            .append_with_retry(client, &retry, topic, messages)
            .await?
        {
            AppendOutcome::Appended => Ok(retry.offsets.len() as u64),
            AppendOutcome::RowErrors(again) => {
                let again = validate_row_errors(again, retry.offsets.len()).inspect_err(|_| {
                    self.counters
                        .rows_failed
                        .fetch_add(retry.offsets.len() as u64, Ordering::Relaxed);
                })?;
                for (row, reason) in &again {
                    error!(
                        "BigQuery sink ID: {} row remained rejected after retry stream={} topic={} partition={} offset={}: {reason}",
                        self.id,
                        topic.stream,
                        topic.topic,
                        messages.partition_id,
                        retry.offsets[*row]
                    );
                }
                self.counters
                    .rows_failed
                    .fetch_add(retry.offsets.len() as u64, Ordering::Relaxed);
                error!(
                    "BigQuery sink ID: {} dropped {} rows (offsets {}..={}) of {}/{} partition {}: BigQuery reported {} more row error(s) after the bad rows were removed",
                    self.id,
                    retry.offsets.len(),
                    retry.offsets.first().copied().unwrap_or_default(),
                    retry.offsets.last().copied().unwrap_or_default(),
                    topic.stream,
                    topic.topic,
                    messages.partition_id,
                    again.len()
                );
                Err(Error::PermanentHttpError(format!(
                    "{} row error(s) persisted after removing rejected rows",
                    again.len()
                )))
            }
        }
    }

    async fn append_with_retry(
        &self,
        client: &BigQueryClient,
        chunk: &Chunk,
        topic: &TopicMetadata,
        messages: &MessagesMetadata,
    ) -> Result<AppendOutcome, Error> {
        let context = format!(
            "BigQuery sink ID: {} AppendRows ({} rows)",
            self.id,
            chunk.offsets.len()
        );
        retry_async(
            self.retry_policy(),
            &context,
            AppendError::is_retryable,
            || client.append(chunk),
        )
        .await
        .map_err(|failure| {
            if failure.error.is_retryable() {
                self.counters
                    .rows_unconfirmed
                    .fetch_add(chunk.offsets.len() as u64, Ordering::Relaxed);
                error!(
                    "BigQuery sink ID: {} append outcome is unknown for {} rows (offsets {}..={}) of {}/{} partition {} after the retry budget was exhausted: {failure}",
                    self.id,
                    chunk.offsets.len(),
                    chunk.offsets.first().copied().unwrap_or_default(),
                    chunk.offsets.last().copied().unwrap_or_default(),
                    topic.stream,
                    topic.topic,
                    messages.partition_id
                );
            } else {
                self.counters
                    .rows_failed
                    .fetch_add(chunk.offsets.len() as u64, Ordering::Relaxed);
                error!(
                    "BigQuery sink ID: {} dropped {} rows (offsets {}..={}) of {}/{} partition {} after a permanent append failure: {failure}",
                    self.id,
                    chunk.offsets.len(),
                    chunk.offsets.first().copied().unwrap_or_default(),
                    chunk.offsets.last().copied().unwrap_or_default(),
                    topic.stream,
                    topic.topic,
                    messages.partition_id
                );
            }
            Error::from(failure.error)
        })
    }
}

fn validate_row_errors(
    row_errors: Vec<(usize, String)>,
    row_count: usize,
) -> Result<Vec<(usize, String)>, Error> {
    let mut unique = BTreeMap::new();
    for (row, reason) in row_errors {
        if row >= row_count {
            return Err(Error::PermanentHttpError(format!(
                "BigQuery returned row error index {row} for a request with {row_count} rows"
            )));
        }
        unique.entry(row).or_insert(reason);
    }
    Ok(unique.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_duplicate_row_errors_should_deduplicate_them() {
        let errors = validate_row_errors(
            vec![
                (2, "first".into()),
                (0, "zero".into()),
                (2, "second".into()),
            ],
            3,
        )
        .expect("indexes are valid");
        assert_eq!(errors, vec![(0, "zero".into()), (2, "first".into())]);
    }

    #[test]
    fn given_out_of_range_row_error_should_fail_validation() {
        assert!(matches!(
            validate_row_errors(vec![(3, "bad".into())], 3),
            Err(Error::PermanentHttpError(_))
        ));
    }
}
