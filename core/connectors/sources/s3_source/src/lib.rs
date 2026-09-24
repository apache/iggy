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

use std::time::Duration;

use async_trait::async_trait;
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessages, Schema, Source, source::SourceBatchResult,
    source_connector,
};
use tokio::{
    sync::Mutex,
    time::{sleep, timeout},
};
use tracing::{debug, error, info, warn};

use crate::{
    config::{ResolvedConfig, S3SourceConfig},
    framing::FramingError,
    reader::{ReadBatch, ReadError},
    state::{ActiveObjectState, ScanState, SourceState, StateTracker},
};

pub mod config;

mod batch;
mod client;
mod framing;
mod message;
mod reader;
mod state;

#[cfg(test)]
mod protocol_tests;

const CONNECTOR_NAME: &str = "S3 source";
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
const READ_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Default)]
struct RecoveryState {
    retry_delay: Duration,
    latched_error: Option<Error>,
}

#[derive(Debug)]
pub struct S3Source {
    id: u32,
    config: S3SourceConfig,
    initial_state: Option<ConnectorState>,
    resolved_config: Option<ResolvedConfig>,
    client: Option<aws_sdk_s3::Client>,
    state: Mutex<StateTracker>,
    scan: Mutex<ScanState>,
    recovery: Mutex<RecoveryState>,
}

impl S3Source {
    pub fn new(id: u32, config: S3SourceConfig, initial_state: Option<ConnectorState>) -> Self {
        Self {
            id,
            config,
            initial_state,
            resolved_config: None,
            client: None,
            state: Mutex::new(StateTracker::new(SourceState::default())),
            scan: Mutex::new(ScanState::default()),
            recovery: Mutex::new(RecoveryState::default()),
        }
    }

    async fn select_object(&self) -> Result<Option<ActiveObjectState>, Error> {
        let active_object = {
            let tracker = self.state.lock().await;
            tracker.active_object().cloned()
        };
        if let Some(object) = active_object {
            return Ok(Some(object));
        }
        let start_after = {
            let scan = self.scan.lock().await;
            if scan.is_exhausted() {
                return Ok(None);
            }
            scan.start_after().map(str::to_owned)
        };
        let config = self.resolved_config.as_ref().ok_or(Error::InvalidState)?;
        let client = self.client.as_ref().ok_or(Error::InvalidState)?;
        let object = client::list_next_object(client, config, start_after.as_deref()).await?;
        if object.is_none() {
            self.scan.lock().await.mark_exhausted();
        }
        Ok(object)
    }

    async fn fetch_batch(
        &self,
        config: &ResolvedConfig,
    ) -> Result<Option<(ActiveObjectState, ReadBatch)>, Error> {
        let Some(mut object) = self.select_object().await? else {
            return Ok(None);
        };
        let client = self.client.as_ref().ok_or(Error::InvalidState)?;
        let response = client::open_object(client, config, &mut object)
            .await
            .map_err(|error| match error {
                Error::HttpRequestFailed(message) => Error::HttpRequestFailed(format!(
                    "{message}; key {:?}, offset {}",
                    object.key, object.next_byte_offset
                )),
                Error::PermanentHttpError(message) => Error::PermanentHttpError(format!(
                    "{message}; key {:?}, offset {}",
                    object.key, object.next_byte_offset
                )),
                error => error,
            })?;
        let result = timeout(
            READ_TIMEOUT,
            reader::read_batch(response.body.into_async_read(), &object, config),
        )
        .await
        .map_err(|_| Error::HttpRequestFailed("S3 body read timed out".into()))?;
        let batch = match result {
            Ok(batch) => batch,
            Err(ReadError::Framing(FramingError::RecordTooLarge {
                start_offset,
                max_record_bytes,
            })) => {
                let failure = Error::PermanentHttpError(format!(
                    "S3 object {:?} has a record at byte {start_offset} exceeding {max_record_bytes} bytes; correct limits and restart",
                    object.key,
                ));
                error!(connector_id = self.id, key = %object.key, offset = start_offset, max_record_bytes,
                    "{CONNECTOR_NAME} record exceeds configured limit; reads are blocked until restart");
                self.recovery.lock().await.latched_error = Some(failure.clone());
                return Err(failure);
            }
            Err(ReadError::Io(error)) => {
                return Err(Error::HttpRequestFailed(format!(
                    "S3 body read failed for key {:?} at byte {} ({:?})",
                    object.key,
                    object.next_byte_offset,
                    error.kind()
                )));
            }
            Err(ReadError::InvalidLength) => {
                return Err(Error::HttpRequestFailed(format!(
                    "S3 body length mismatch for key {:?}",
                    object.key
                )));
            }
        };
        Ok(Some((object, batch)))
    }

    fn serialize_pending(&self, tracker: &mut StateTracker) -> Result<ConnectorState, Error> {
        let candidate = tracker.pending_checkpoint().ok_or(Error::InvalidState)?;
        match ConnectorState::serialize(candidate, CONNECTOR_NAME, self.id) {
            Some(checkpoint) => Ok(checkpoint),
            None => {
                tracker.discard_pending().map_err(|_| Error::InvalidState)?;
                Err(Error::Serialization(
                    "failed to serialize S3 checkpoint".into(),
                ))
            }
        }
    }
}

#[async_trait]
impl Source for S3Source {
    async fn open(&mut self) -> Result<(), Error> {
        let resolved_config = ResolvedConfig::try_from(&self.config).map_err(|error| {
            Error::InitError(format!("invalid S3 source configuration: {error:?}"))
        })?;
        let restored_state = match self.initial_state.as_ref() {
            Some(checkpoint) => ConnectorState(checkpoint.0.clone())
                .deserialize::<SourceState>(CONNECTOR_NAME, self.id)
                .ok_or_else(|| Error::InitError("invalid S3 source checkpoint".into()))?,
            None => SourceState::default(),
        };
        if let Some(active) = &restored_state.active_object {
            active.validate().map_err(|error| {
                Error::InitError(format!("invalid S3 source checkpoint: {error:?}"))
            })?;
        }
        let client = client::create_client(&resolved_config).await;
        // Restored objects must reach poll's retry loop even when access is unavailable.
        if restored_state.active_object.is_none() {
            client::validate_access(&client, &resolved_config)
                .await
                .map_err(|error| {
                    Error::InitError(format!("S3 access validation failed: {error}"))
                })?;
        }

        self.client = Some(client);
        self.resolved_config = Some(resolved_config);
        self.initial_state = None;
        *self.state.get_mut() = StateTracker::new(restored_state);
        *self.scan.get_mut() = ScanState::default();
        *self.recovery.get_mut() = RecoveryState::default();
        info!(connector_id = self.id, "{CONNECTOR_NAME} opened");
        Ok(())
    }

    async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
        let completed_key = {
            let mut tracker = self.state.lock().await;
            match result {
                SourceBatchResult::Ack => tracker.commit_pending(),
                SourceBatchResult::Nack => tracker.discard_pending().map(|()| None),
            }
        }
        .map_err(|_| Error::InvalidState)?;
        if let Some(key) = completed_key {
            self.scan.lock().await.advance_after(key);
        }
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        let config = self.resolved_config.as_ref().ok_or(Error::InvalidState)?;
        let delay = self
            .recovery
            .lock()
            .await
            .retry_delay
            .max(config.poll_interval);
        sleep(delay).await;

        if let Some(error) = &self.recovery.lock().await.latched_error {
            return Err(error.clone());
        }
        if self.state.lock().await.pending_checkpoint().is_some() {
            return Err(Error::InvalidState);
        }
        let fetched = self.fetch_batch(config).await;
        {
            let mut recovery = self.recovery.lock().await;
            match &fetched {
                Ok(_) => recovery.retry_delay = Duration::ZERO,
                Err(
                    Error::HttpRequestFailed(_)
                    | Error::Connection(_)
                    | Error::PermanentHttpError(_),
                ) if recovery.latched_error.is_none() => {
                    recovery.retry_delay = recovery
                        .retry_delay
                        .saturating_mul(2)
                        .max(Duration::from_secs(1))
                        .min(MAX_RETRY_DELAY);
                    warn!(
                        connector_id = self.id,
                        retry_delay_ms = recovery.retry_delay.as_millis(),
                        "{CONNECTOR_NAME} fetch failed; committed progress is unchanged"
                    );
                }
                Err(_) => {}
            }
        }
        let fetched = fetched?;
        let mut tracker = self.state.lock().await;
        // No await after staging: cancellation cannot leave an unreturned batch pending.
        match fetched {
            None => {
                tracker.stage_current();
                Ok(ProducedMessages {
                    schema: Schema::Raw,
                    messages: Vec::new(),
                    state: None,
                })
            }
            Some((object, batch)) => {
                if batch.complete {
                    tracker.stage_completion(object.key.clone());
                } else {
                    tracker
                        .stage_offset(object.clone(), batch.next_offset)
                        .map_err(|_| Error::InvalidState)?;
                }
                let checkpoint = self.serialize_pending(&mut tracker)?;
                if config.verbose_logging {
                    info!(connector_id = self.id, key = %object.key, offset = batch.next_offset,
                        messages_count = batch.records.len(), complete = batch.complete, "{CONNECTOR_NAME} batch staged");
                } else {
                    debug!(connector_id = self.id, key = %object.key, offset = batch.next_offset,
                        messages_count = batch.records.len(), complete = batch.complete, "{CONNECTOR_NAME} batch staged");
                }
                Ok(message::build_produced_messages(
                    batch.records,
                    config.endpoint.as_deref().unwrap_or("aws"),
                    &config.bucket,
                    &object.key,
                    &object.etag,
                    checkpoint,
                ))
            }
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.client = None;
        self.resolved_config = None;
        info!(connector_id = self.id, "{CONNECTOR_NAME} closed");
        Ok(())
    }
}

source_connector!(S3Source);

#[cfg(test)]
mod tests {
    use iggy_connector_sdk::{ConnectorState, Schema, Source, source::SourceBatchResult};

    use super::*;

    fn active_object(next_byte_offset: u64) -> state::ActiveObjectState {
        state::ActiveObjectState {
            key: "logs/events.jsonl".to_string(),
            etag: "abc123".to_string(),
            size: 100,
            next_byte_offset,
        }
    }

    fn checkpoint_at(next_byte_offset: u64) -> ConnectorState {
        ConnectorState::serialize(
            &state::SourceState {
                active_object: Some(active_object(next_byte_offset)),
            },
            CONNECTOR_NAME,
            7,
        )
        .expect("checkpoint should serialize")
    }

    fn test_config() -> S3SourceConfig {
        serde_json::from_str(r#"{"bucket":"events","poll_interval":"1ms","region":"us-east-1"}"#)
            .expect("configuration should deserialize")
    }

    #[test]
    fn given_active_object_when_selecting_should_resume_committed_offset_without_listing() {
        let mut source = S3Source::new(7, test_config(), None);
        *source.state.get_mut() = StateTracker::new(SourceState {
            active_object: Some(active_object(20)),
        });
        source.scan.get_mut().mark_exhausted();
        source
            .state
            .get_mut()
            .stage_offset(active_object(20), 40)
            .expect("offset should stage");
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");

        runtime.block_on(async {
            let selected = source
                .select_object()
                .await
                .expect("selection should succeed");

            assert_eq!(selected, Some(active_object(20)));
            assert_eq!(source.scan.lock().await.start_after(), None);
            source
                .state
                .lock()
                .await
                .discard_pending()
                .expect("selection should preserve pending state");
        });
    }

    #[test]
    fn given_exhausted_scan_when_selecting_should_return_none_without_listing() {
        let mut source = S3Source::new(7, test_config(), None);
        source
            .scan
            .get_mut()
            .advance_after("logs/last.txt".to_string());
        source.scan.get_mut().mark_exhausted();
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");

        runtime.block_on(async {
            assert_eq!(
                source
                    .select_object()
                    .await
                    .expect("selection should succeed"),
                None
            );
            assert_eq!(
                source.scan.lock().await.start_after(),
                Some("logs/last.txt")
            );
        });
    }

    #[test]
    fn given_unopened_source_when_selecting_should_return_error_without_exhausting_scan() {
        let source = S3Source::new(7, test_config(), None);
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");

        runtime.block_on(async {
            assert!(matches!(
                source.select_object().await,
                Err(Error::InvalidState)
            ));
            assert!(!source.scan.lock().await.is_exhausted());
        });
    }

    #[test]
    fn given_config_and_state_when_source_created_should_retain_inputs_for_open() {
        let config: config::S3SourceConfig = serde_json::from_str(r#"{"bucket":"events"}"#)
            .expect("configuration should deserialize");

        let source = S3Source::new(7, config, Some(ConnectorState(vec![1, 2, 3])));

        assert_eq!(source.id, 7);
        assert_eq!(source.config.bucket, "events");
        assert_eq!(
            source
                .initial_state
                .as_ref()
                .map(|state| state.0.as_slice()),
            Some([1, 2, 3].as_slice())
        );
    }

    #[test]
    fn given_s3_source_should_implement_source_trait() {
        fn assert_source<T: Source>() {}

        assert_source::<S3Source>();
    }

    fn source_with_state(offset: u64) -> S3Source {
        let mut source = S3Source::new(7, test_config(), None);
        source.resolved_config = Some(ResolvedConfig::try_from(&source.config).expect("config"));
        *source.state.get_mut() = StateTracker::new(SourceState {
            active_object: Some(active_object(offset)),
        });
        source
    }

    #[test]
    fn given_connection_settings_when_client_created_should_retain_region() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let resolved = ResolvedConfig::try_from(&test_config()).expect("config");
            let client = client::create_client(&resolved).await;
            assert_eq!(
                client.config().region().map(|region| region.as_ref()),
                Some("us-east-1")
            );
        });
    }

    #[test]
    fn given_exhausted_source_when_polled_should_omit_unchanged_checkpoint_and_accept_ack_or_nack()
    {
        let mut source = S3Source::new(7, test_config(), None);
        source.resolved_config = Some(ResolvedConfig::try_from(&source.config).expect("config"));
        source.scan.get_mut().mark_exhausted();
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            for result in [SourceBatchResult::Nack, SourceBatchResult::Ack] {
                let batch = source.poll().await.expect("idle poll");
                assert_eq!(batch.schema, Schema::Raw);
                assert!(batch.messages.is_empty());
                assert!(batch.state.is_none());
                assert!(matches!(source.poll().await, Err(Error::InvalidState)));
                source.on_batch_result(result).await.expect("idle result");
                let tracker = source.state.lock().await;
                assert!(tracker.active_object().is_none());
                assert!(tracker.pending_checkpoint().is_none());
            }
        });
    }

    #[test]
    fn given_pending_offset_when_batch_acked_should_commit_offset() {
        let mut source = source_with_state(10);
        source
            .state
            .get_mut()
            .stage_offset(active_object(10), 40)
            .expect("stage");
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            source
                .on_batch_result(SourceBatchResult::Ack)
                .await
                .expect("ack");
            assert_eq!(
                source.state.lock().await.active_object(),
                Some(&active_object(40))
            );
            assert_eq!(source.scan.lock().await.start_after(), None);
        });
    }

    #[test]
    fn given_pending_offset_when_batch_nacked_should_preserve_committed_offset() {
        let mut source = source_with_state(10);
        source
            .state
            .get_mut()
            .stage_offset(active_object(10), 40)
            .expect("stage");
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            source
                .on_batch_result(SourceBatchResult::Nack)
                .await
                .expect("nack");
            assert_eq!(
                source.state.lock().await.active_object(),
                Some(&active_object(10))
            );
            assert_eq!(source.scan.lock().await.start_after(), None);
        });
    }

    #[test]
    fn given_pending_completion_when_acked_should_advance_scan_but_nack_should_not() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            for result in [SourceBatchResult::Ack, SourceBatchResult::Nack] {
                let mut source = source_with_state(10);
                source
                    .state
                    .get_mut()
                    .stage_completion("logs/events.jsonl".into());
                source.on_batch_result(result).await.expect("result");
                let tracker = source.state.lock().await;
                let scan = source.scan.lock().await;
                match result {
                    SourceBatchResult::Ack => {
                        assert!(tracker.active_object().is_none());
                        assert_eq!(scan.start_after(), Some("logs/events.jsonl"));
                    }
                    SourceBatchResult::Nack => {
                        assert_eq!(tracker.active_object(), Some(&active_object(10)));
                        assert_eq!(scan.start_after(), None);
                    }
                }
            }
        });
    }

    #[test]
    fn given_invalid_checkpoint_when_opened_should_reject_before_s3_access() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            for checkpoint in [ConnectorState(vec![0xc1]), checkpoint_at(101)] {
                let mut source = S3Source::new(7, test_config(), Some(checkpoint));
                assert!(matches!(source.open().await, Err(Error::InitError(_))));
                assert!(source.initial_state.is_some());
                assert!(source.client.is_none());
            }
        });
    }

    #[test]
    fn given_latched_record_error_when_polled_should_not_contact_s3() {
        let mut source = source_with_state(10);
        let failure = Error::PermanentHttpError("oversized record".into());
        source.recovery.get_mut().latched_error = Some(failure.clone());
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            for _ in 0..2 {
                assert_eq!(source.poll().await.expect_err("latched error"), failure);
            }
            assert_eq!(
                source.state.lock().await.active_object(),
                Some(&active_object(10))
            );
        });
    }
}
