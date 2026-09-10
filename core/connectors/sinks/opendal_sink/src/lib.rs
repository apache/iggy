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

use std::{
    collections::BTreeMap,
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata,
    retry::{RetryPolicy, parse_duration, retry_async},
    sink_connector,
};
use opendal::{Buffer, Operator};
use serde::Deserialize;
use tracing::{debug, error, info};

use crate::path::{PathContext, object_path};

mod path;

sink_connector!(OpenDalSink);

const CONNECTOR_NAME: &str = "OpenDAL sink";
const DEFAULT_PATH_TEMPLATE: &str = "{stream}/{topic}/{date}/{hour}";
const DEFAULT_MAX_ATTEMPTS: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";
const MAX_BACKOFF: Duration = Duration::from_secs(60);

#[derive(Clone, Deserialize)]
pub struct OpenDalSinkConfig {
    pub service: String,
    #[serde(default)]
    pub path_prefix: Option<String>,
    #[serde(default = "default_path_template")]
    pub path_template: String,
    #[serde(default)]
    pub options: BTreeMap<String, String>,
    #[serde(default)]
    pub max_attempts: Option<u32>,
    #[serde(default)]
    pub retry_delay: Option<String>,
    #[serde(default)]
    pub verbose_logging: Option<bool>,
}

impl fmt::Debug for OpenDalSinkConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenDalSinkConfig")
            .field("service", &self.service)
            .field("path_prefix", &self.path_prefix)
            .field("path_template", &self.path_template)
            .field("option_keys", &self.options.keys().collect::<Vec<_>>())
            .field("max_attempts", &self.max_attempts)
            .field("retry_delay", &self.retry_delay)
            .field("verbose_logging", &self.verbose_logging)
            .finish()
    }
}

#[derive(Debug)]
pub struct OpenDalSink {
    id: u32,
    config: OpenDalSinkConfig,
    path_prefix: String,
    retry_policy: RetryPolicy,
    verbose: bool,
    operator: Option<Operator>,
    messages_processed: AtomicU64,
    write_errors: AtomicU64,
}

impl OpenDalSink {
    pub fn new(id: u32, config: OpenDalSinkConfig) -> Self {
        let max_attempts = config.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS);

        let retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);

        let path_prefix = config
            .path_prefix
            .as_deref()
            .unwrap_or_default()
            .trim_matches('/')
            .to_string();
        let verbose = config.verbose_logging.unwrap_or(false);

        Self {
            id,
            config,
            path_prefix,
            retry_policy: RetryPolicy {
                max_attempts,
                base_delay: retry_delay,
                max_delay: MAX_BACKOFF,
            },
            verbose,
            operator: None,
            messages_processed: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
        }
    }

    async fn write_message(
        &self,
        operator: &Operator,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        message: ConsumedMessage,
        retry_context: &str,
    ) -> Result<(), Error> {
        let context = PathContext {
            stream: &topic_metadata.stream,
            topic: &topic_metadata.topic,
            partition_id: messages_metadata.partition_id,
            first_timestamp_micros: message.timestamp,
        };
        let path = object_path(
            &self.path_prefix,
            &self.config.path_template,
            &context,
            message.offset,
            messages_metadata.schema,
        )?;
        let payload = message.payload.try_into_vec()?;
        let buffer = Buffer::from(payload);

        retry_async(
            self.retry_policy,
            retry_context,
            opendal::Error::is_temporary,
            || operator.write(&path, buffer.clone()),
        )
        .await
        .map_err(|failure| {
            Error::CannotStoreData(format!(
                "Failed to write OpenDAL object '{path}' after {} attempt(s): {}",
                failure.attempts, failure.error
            ))
        })?;

        Ok(())
    }
}

#[async_trait]
impl Sink for OpenDalSink {
    async fn open(&mut self) -> Result<(), Error> {
        let service = self.config.service.trim();
        if service.is_empty() {
            return Err(Error::InvalidConfigValue(
                "OpenDAL service cannot be empty".to_string(),
            ));
        }
        if self.config.path_template.is_empty() {
            return Err(Error::InvalidConfigValue(
                "OpenDAL path_template cannot be empty".to_string(),
            ));
        }

        opendal::init_default_registry();
        let options = self.config.options.clone();
        let operator = Operator::via_iter(service, options).map_err(|error| {
            Error::InitError(format!(
                "Failed to create OpenDAL service '{service}': {error}"
            ))
        })?;

        if !operator.info().capability().write {
            return Err(Error::InvalidConfigValue(format!(
                "OpenDAL service '{service}' does not support writes"
            )));
        }

        operator.check().await.map_err(|error| {
            Error::InitError(format!(
                "OpenDAL service '{service}' connectivity check failed: {error}"
            ))
        })?;

        info!(
            "Opened {CONNECTOR_NAME} connector ID: {}, service: {service}, root: {}",
            self.id,
            operator.info().root()
        );
        self.operator = Some(operator);
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let Some(operator) = self.operator.as_ref() else {
            return Err(Error::InitError(
                "OpenDAL operator is not initialized".to_string(),
            ));
        };

        if self.verbose {
            info!(
                "{CONNECTOR_NAME} connector ID: {} consuming {} messages, stream: {}, topic: {}, \
                 partition_id: {}, current_offset: {}",
                self.id,
                messages.len(),
                topic_metadata.stream,
                topic_metadata.topic,
                messages_metadata.partition_id,
                messages_metadata.current_offset
            );
        } else {
            debug!(
                "{CONNECTOR_NAME} connector ID: {} consuming {} messages",
                self.id,
                messages.len()
            );
        }

        let retry_context = format!("{CONNECTOR_NAME} connector ID {} write", self.id);
        let mut last_error = None;
        for message in messages {
            match self
                .write_message(
                    operator,
                    topic_metadata,
                    &messages_metadata,
                    message,
                    &retry_context,
                )
                .await
            {
                Ok(()) => {
                    self.messages_processed.fetch_add(1, Ordering::Relaxed);
                }
                Err(write_error) => {
                    self.write_errors.fetch_add(1, Ordering::Relaxed);
                    error!(
                        "Failed to write message with {CONNECTOR_NAME} connector ID: {}, error: \
                         {write_error}",
                        self.id
                    );
                    last_error = Some(write_error);
                }
            }
        }

        match last_error {
            Some(write_error) => Err(write_error),
            None => Ok(()),
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.operator.take();
        info!(
            "Closed {CONNECTOR_NAME} connector ID: {}, processed: {}, errors: {}",
            self.id,
            self.messages_processed.load(Ordering::Relaxed),
            self.write_errors.load(Ordering::Relaxed)
        );
        Ok(())
    }
}

fn default_path_template() -> String {
    DEFAULT_PATH_TEMPLATE.to_string()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use iggy_connector_sdk::{Payload, Schema, Sink};
    use tempfile::TempDir;

    use super::*;

    fn test_config(root: &str) -> OpenDalSinkConfig {
        OpenDalSinkConfig {
            service: "fs".to_string(),
            path_prefix: Some("iggy/messages".to_string()),
            path_template: default_path_template(),
            options: BTreeMap::from([("root".to_string(), root.to_string())]),
            max_attempts: Some(1),
            retry_delay: None,
            verbose_logging: None,
        }
    }

    fn test_message(offset: u64, payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            id: offset as u128,
            offset,
            checksum: 0,
            timestamp: 0,
            origin_timestamp: 0,
            headers: None,
            payload,
        }
    }

    #[test]
    fn given_config_when_debugged_should_redact_option_values() {
        let config = test_config("/secret/storage/path");

        let output = format!("{config:?}");

        assert!(output.contains("root"));
        assert!(!output.contains("/secret/storage/path"));
    }

    #[test]
    fn given_fs_service_when_consuming_batch_should_write_each_payload_object() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let temp_dir = TempDir::new().expect("temp directory should be created");
            let root = temp_dir.path().to_string_lossy();
            let mut sink = OpenDalSink::new(1, test_config(&root));
            sink.open().await.expect("sink should open");

            let topic_metadata = TopicMetadata {
                stream: "events".to_string(),
                topic: "orders".to_string(),
            };
            let messages_metadata = MessagesMetadata {
                partition_id: 2,
                current_offset: 4,
                schema: Schema::Json,
            };
            let mut first_json = br#"{"id":1}"#.to_vec();
            let first_payload =
                simd_json::to_owned_value(&mut first_json).expect("JSON payload should be valid");
            let mut second_json = br#"{"id":2}"#.to_vec();
            let second_payload =
                simd_json::to_owned_value(&mut second_json).expect("JSON payload should be valid");

            sink.consume(
                &topic_metadata,
                messages_metadata,
                vec![
                    test_message(4, Payload::Json(first_payload)),
                    test_message(5, Payload::Json(second_payload)),
                ],
            )
            .await
            .expect("messages should be written");
            sink.close().await.expect("sink should close");

            for (offset, expected) in [
                (4, br#"{"id":1}"#.as_slice()),
                (5, br#"{"id":2}"#.as_slice()),
            ] {
                let path = temp_dir.path().join(format!(
                    "iggy/messages/events/orders/1970-01-01/00/00002-{offset:020}.json"
                ));
                let stored = fs::read(path).expect("stored object should be readable");
                assert_eq!(stored, expected);
            }
        });
    }

    #[test]
    fn given_whitespace_service_when_opening_should_reject_config() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let mut config = test_config("/tmp");
            config.service = " \t".to_string();
            let mut sink = OpenDalSink::new(1, config);

            let error = sink
                .open()
                .await
                .expect_err("whitespace service should fail");

            assert!(matches!(error, Error::InvalidConfigValue(_)));
        });
    }
    #[test]
    fn given_empty_path_template_when_opening_should_reject_config() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let mut config = test_config("/tmp");
            config.path_template.clear();
            let mut sink = OpenDalSink::new(1, config);

            let error = sink
                .open()
                .await
                .expect_err("empty path template should fail");

            assert!(matches!(error, Error::InvalidConfigValue(_)));
        });
    }
}
