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

use std::collections::BTreeSet;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use iggy_connector_sdk::retry::{RetryPolicy, retry_async};
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata};
use scopedb_client::{
    AppendFailurePolicy, AppendOutcome, AppendRequestResult, AppendWriteReport, Client,
    Error as ScopeDBError, reqwest,
};
use secrecy::ExposeSecret;
use serde_json::value::RawValue;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::schema::Layout;
use crate::{OpenState, ScopeDBSink, encode};

#[async_trait]
impl Sink for ScopeDBSink {
    async fn open(&mut self) -> Result<(), Error> {
        self.try_open().await.inspect_err(|error| {
            error!(
                "Failed to open ScopeDB sink connector ID: {}: {error}",
                self.id
            );
        })
    }

    async fn consume(
        &self,
        topic: &TopicMetadata,
        metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let total_messages = messages.len();
        if total_messages == 0 {
            return Ok(());
        }
        let state = self
            .state
            .as_ref()
            .ok_or_else(|| Error::InitError("ScopeDB sink is not open".into()))?;
        let mut rows = Vec::with_capacity(total_messages);
        let mut offsets = Vec::with_capacity(total_messages);
        for message in messages {
            let offset = message.offset;
            match encode::row(message, topic, &metadata, &state.layout, &self.settings) {
                Ok(row) => {
                    rows.push(row);
                    offsets.push(offset);
                }
                Err(error) => self.reject(topic, &metadata, offset, &error),
            }
        }
        if rows.is_empty() {
            return Err(Error::InvalidRecordValue(
                "all messages were rejected before append".into(),
            ));
        }
        let result = match state.writer.write(&rows).await {
            Ok(report) => {
                self.record_committed_report(&report, topic, &metadata);
                Ok(())
            }
            Err(failure) => {
                self.settle(state, &failure, &rows, &offsets, topic, &metadata)
                    .await
            }
        };
        if self.settings.verbose {
            info!(
                "ScopeDB sink connector ID: {} settled {} messages from {}/{} partition={} current_offset={}, counters={:?}",
                self.id,
                total_messages,
                topic.stream,
                topic.topic,
                metadata.partition_id,
                metadata.current_offset,
                self.stats()
            );
        } else {
            debug!(
                "ScopeDB sink connector ID: {} settled {} messages from {}/{} partition={} current_offset={}, counters={:?}",
                self.id,
                total_messages,
                topic.stream,
                topic.topic,
                metadata.partition_id,
                metadata.current_offset,
                self.stats()
            );
        }
        result
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.state = None;
        info!(
            "Closed ScopeDB sink connector ID: {}, counters={:?}",
            self.id,
            self.stats()
        );
        Ok(())
    }
}

impl ScopeDBSink {
    async fn try_open(&mut self) -> Result<(), Error> {
        self.config.validate()?;
        self.settings.validate()?;
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(self.settings.timeout)
            .build()
            .map_err(|error| Error::InitError(error.to_string()))?;
        let mut builder = Client::builder(&self.config.endpoint)
            .http_client(http)
            .write_timeout(self.settings.timeout);
        if let Some(key) = &self.config.api_key {
            builder = builder.api_key(key.expose_secret());
        }
        let client = builder
            .build()
            .map_err(|error| Error::InitError(error.to_string()))?;
        let table = client
            .table(&self.config.table)
            .with_database(self.config.database.as_deref().unwrap_or("scopedb"))
            .with_schema(self.config.schema.as_deref().unwrap_or("public"));
        let writer = table
            .append_writer()
            .target_batch_bytes(self.settings.max_request_bytes)
            .max_batch_rows(self.settings.batch_size)
            .max_concurrent_batches(self.settings.max_concurrent_requests)
            .max_retries(self.settings.max_attempts.saturating_sub(1) as usize)
            .initial_backoff(self.settings.retry_delay)
            .max_backoff(self.settings.max_retry_delay)
            .retry_ambiguous(self.settings.retry_ambiguous)
            .attempt_timeout(self.settings.timeout)
            .failure_policy(AppendFailurePolicy::Continue)
            .build()
            .map_err(|error| Error::InvalidConfigValue(error.to_string()))?;
        let catalog = table.catalog();
        let policy = RetryPolicy {
            max_attempts: self.settings.max_attempts,
            base_delay: self.settings.retry_delay,
            max_delay: self.settings.max_retry_delay,
        };
        let context = format!(
            "ScopeDB sink connector ID: {} read table definition",
            self.id
        );
        let snapshot = timeout(
            self.settings.timeout,
            retry_async(policy, &context, ScopeDBError::is_retryable, || {
                catalog.get()
            }),
        )
        .await
        .map_err(|_| Error::InitError("ScopeDB startup timed out".into()))?
        .map_err(|failure| Error::InitError(failure.to_string()))?;
        let layout = Layout::new(snapshot.definition, &self.settings)?;
        self.state = Some(OpenState { writer, layout });
        info!(
            "Opened ScopeDB sink connector ID: {}, table={}, mode={:?}",
            self.id, self.config.table, self.settings.mode
        );
        Ok(())
    }

    async fn settle(
        &self,
        state: &OpenState,
        failure: &ScopeDBError,
        rows: &[Box<RawValue>],
        offsets: &[u64],
        topic: &TopicMetadata,
        metadata: &MessagesMetadata,
    ) -> Result<(), Error> {
        let report = failure
            .append_write_report()
            .expect("finite write errors retain their progress report");
        let mut last_error = None;
        for batch in report.batches() {
            match batch.result() {
                Ok(_) => {
                    self.record_committed_batch(batch, topic, metadata);
                }
                Err(error) => {
                    let indexes = self.rejected_indexes(batch, error);
                    if let Some(indexes) = indexes {
                        for index in &indexes {
                            self.reject(topic, metadata, offsets[*index], error);
                        }
                        let mut remaining_rows =
                            Vec::with_capacity(batch.input_range().len() - indexes.len());
                        let mut remaining_offsets = Vec::with_capacity(remaining_rows.capacity());
                        for index in batch.input_range() {
                            if !indexes.contains(&index) {
                                remaining_rows.push(&rows[index]);
                                remaining_offsets.push(offsets[index]);
                            }
                        }
                        if remaining_rows.is_empty() {
                            remember_failure(
                                &mut last_error,
                                Error::InvalidRecordValue(
                                    "ScopeDB rejected every row in a request".into(),
                                ),
                            );
                            continue;
                        }
                        // A complete rejected result proves that this request committed
                        // nothing. Successful sibling requests must never be replayed.
                        match state.writer.write(&remaining_rows).await {
                            Ok(recovered) => {
                                self.record_committed_report(&recovered, topic, metadata);
                            }
                            Err(failure) => {
                                self.record_final_report(
                                    failure
                                        .append_write_report()
                                        .expect("finite write errors retain their progress report"),
                                    &remaining_offsets,
                                    topic,
                                    metadata,
                                    &failure,
                                );
                                remember_failure(&mut last_error, sink_error(&failure));
                            }
                        }
                    } else {
                        self.fail(topic, metadata, &offsets[batch.input_range()], error);
                        remember_failure(&mut last_error, sink_error(error));
                    }
                }
            }
        }
        if !report.unsubmitted().is_empty() {
            self.not_submitted(topic, metadata, &offsets[report.unsubmitted()], failure);
            remember_failure(&mut last_error, sink_error(failure));
        }
        last_error.map_or(Ok(()), Err)
    }

    fn record_committed_report(
        &self,
        report: &AppendWriteReport,
        topic: &TopicMetadata,
        metadata: &MessagesMetadata,
    ) {
        for batch in report.batches() {
            if batch.result().is_ok() {
                self.record_committed_batch(batch, topic, metadata);
            }
        }
    }

    fn record_committed_batch(
        &self,
        batch: &AppendRequestResult,
        topic: &TopicMetadata,
        metadata: &MessagesMetadata,
    ) {
        let rows = batch.input_range().len();
        self.counters
            .written
            .fetch_add(rows as u64, Ordering::Relaxed);
        if batch.retries() > 0 {
            debug!(
                "ScopeDB sink connector ID: {} committed {rows} row(s) after {} retry attempt(s) stream={} topic={} partition={}",
                self.id,
                batch.retries(),
                topic.stream,
                topic.topic,
                metadata.partition_id
            );
        }
    }

    fn rejected_indexes(
        &self,
        batch: &AppendRequestResult,
        error: &ScopeDBError,
    ) -> Option<BTreeSet<usize>> {
        let details = error.append_details()?;
        if details.status != AppendOutcome::Failed
            || details.row_errors_truncated
            || details.row_errors.is_empty()
            || !error
                .http_status()
                .is_some_and(|status| matches!(status.as_u16(), 400 | 422))
        {
            return None;
        }
        details
            .row_errors
            .iter()
            .map(|row| batch.input_index(row.row_index))
            .collect()
    }

    fn record_final_report(
        &self,
        report: &AppendWriteReport,
        offsets: &[u64],
        topic: &TopicMetadata,
        metadata: &MessagesMetadata,
        cause: &ScopeDBError,
    ) {
        for batch in report.batches() {
            match batch.result() {
                Ok(_) => {
                    self.record_committed_batch(batch, topic, metadata);
                }
                Err(error) => self.fail(topic, metadata, &offsets[batch.input_range()], error),
            }
        }
        self.not_submitted(topic, metadata, &offsets[report.unsubmitted()], cause);
    }

    fn reject(
        &self,
        topic: &TopicMetadata,
        metadata: &MessagesMetadata,
        offset: u64,
        reason: &impl std::fmt::Display,
    ) {
        self.counters.rejected.fetch_add(1, Ordering::Relaxed);
        warn!(
            "ScopeDB sink connector ID: {} dropped row stream={} topic={} partition={} offset={offset}: {reason}",
            self.id, topic.stream, topic.topic, metadata.partition_id
        );
    }

    fn fail(
        &self,
        topic: &TopicMetadata,
        metadata: &MessagesMetadata,
        offsets: &[u64],
        reason: &ScopeDBError,
    ) {
        let unknown = reason
            .append_details()
            .is_none_or(|details| details.status == AppendOutcome::Ambiguous);
        let counter = if unknown {
            &self.counters.unconfirmed
        } else {
            &self.counters.failed
        };
        counter.fetch_add(offsets.len() as u64, Ordering::Relaxed);
        let retries = reason.retries();
        for offset in offsets {
            error!(
                "ScopeDB sink connector ID: {} append failed stream={} topic={} partition={} offset={offset} retries={retries} unknown={unknown}: {reason}",
                self.id, topic.stream, topic.topic, metadata.partition_id
            );
        }
    }

    fn not_submitted(
        &self,
        topic: &TopicMetadata,
        metadata: &MessagesMetadata,
        offsets: &[u64],
        reason: &ScopeDBError,
    ) {
        self.counters
            .failed
            .fetch_add(offsets.len() as u64, Ordering::Relaxed);
        for offset in offsets {
            error!(
                "ScopeDB sink connector ID: {} row not submitted stream={} topic={} partition={} offset={offset}: {reason}",
                self.id, topic.stream, topic.topic, metadata.partition_id
            );
        }
    }
}

fn sink_error(error: &ScopeDBError) -> Error {
    if error
        .append_details()
        .is_none_or(|details| details.status == AppendOutcome::Ambiguous)
    {
        Error::WriteFailure(error.to_string())
    } else {
        Error::PermanentHttpError(error.to_string())
    }
}

fn remember_failure(current: &mut Option<Error>, failure: Error) {
    // A later rejected request cannot resolve an earlier unknown commit outcome.
    if !matches!(current, Some(Error::WriteFailure(_))) {
        *current = Some(failure);
    }
}
