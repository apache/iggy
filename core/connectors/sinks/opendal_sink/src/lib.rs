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
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use tracing::{debug, error, info};

use crate::path::{PathContext, object_path};

mod formatter;
mod path;

sink_connector!(OpenDalSink);

const CONNECTOR_NAME: &str = "OpenDAL sink";
const DEFAULT_PATH_TEMPLATE: &str = "{stream}/{topic}/{date}/{hour}";
const DEFAULT_MAX_ATTEMPTS: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_OUTPUT_FORMAT: &str = "json_lines";
const MAX_BACKOFF: Duration = Duration::from_secs(60);

#[derive(Clone, Deserialize)]
pub struct OpenDalSinkConfig {
    pub service: String,
    #[serde(default)]
    pub path_prefix: Option<String>,
    #[serde(default = "default_path_template")]
    pub path_template: String,
    #[serde(default)]
    pub options: BTreeMap<String, SecretString>,
    #[serde(default = "default_output_format")]
    pub output_format: String,
    #[serde(default = "default_true")]
    pub include_metadata: bool,
    #[serde(default)]
    pub include_headers: bool,
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
            .field("output_format", &self.output_format)
            .field("include_metadata", &self.include_metadata)
            .field("include_headers", &self.include_headers)
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
    output_format: Option<OutputFormat>,
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
            output_format: None,
            operator: None,
            messages_processed: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
        }
    }

    async fn write_batch(
        &self,
        operator: &Operator,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
        output_format: OutputFormat,
    ) -> Result<(), Error> {
        let Some(first_message) = messages.first() else {
            return Ok(());
        };
        let last_offset = messages
            .last()
            .map_or(first_message.offset, |message| message.offset);
        let context = PathContext {
            stream: &topic_metadata.stream,
            topic: &topic_metadata.topic,
            partition_id: messages_metadata.partition_id,
            first_timestamp_micros: first_message.timestamp,
        };
        let path = object_path(
            &self.path_prefix,
            &self.config.path_template,
            &context,
            first_message.offset,
            last_offset,
            output_format,
        )?;
        let entries = messages
            .iter()
            .map(|message| {
                formatter::format_message(
                    message,
                    topic_metadata,
                    messages_metadata,
                    self.config.include_metadata,
                    self.config.include_headers,
                    output_format,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let data = formatter::finalize_buffer(entries.iter().map(Vec::as_slice), output_format);
        let buffer = Buffer::from(data);
        let retry_context = format!("{CONNECTOR_NAME} connector ID {} batch write", self.id);

        retry_async(
            self.retry_policy,
            &retry_context,
            opendal::Error::is_temporary,
            || operator.write(&path, buffer.clone()),
        )
        .await
        .map_err(|failure| {
            Error::CannotStoreData(format!(
                "Failed to write OpenDAL batch object '{path}' with offsets {}-{last_offset} \
                 after {} attempt(s): {}",
                first_message.offset, failure.attempts, failure.error
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

        let output_format = OutputFormat::try_from(self.config.output_format.as_str())?;
        opendal::install_default();
        let options = self
            .config
            .options
            .iter()
            .map(|(key, value)| (key.clone(), value.expose_secret().to_owned()));
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
        self.output_format = Some(output_format);
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
        let Some(output_format) = self.output_format else {
            return Err(Error::InitError(
                "OpenDAL output format is not initialized".to_string(),
            ));
        };
        if messages.is_empty() {
            return Ok(());
        }

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
        }

        let message_count = messages.len() as u64;
        match self
            .write_batch(
                operator,
                topic_metadata,
                &messages_metadata,
                &messages,
                output_format,
            )
            .await
        {
            Ok(()) => {
                self.messages_processed
                    .fetch_add(message_count, Ordering::Relaxed);
                debug!(
                    "{CONNECTOR_NAME} connector ID: {} uploaded one object with {} messages",
                    self.id, message_count
                );
                Ok(())
            }
            Err(write_error) => {
                self.write_errors
                    .fetch_add(message_count, Ordering::Relaxed);
                error!(
                    "{CONNECTOR_NAME} connector ID: {} failed to upload {} messages: {write_error}",
                    self.id, message_count
                );
                Err(write_error)
            }
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.operator.take();
        self.output_format.take();
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

fn default_output_format() -> String {
    DEFAULT_OUTPUT_FORMAT.to_string()
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutputFormat {
    JsonLines,
    JsonArray,
    Raw,
}

impl TryFrom<&str> for OutputFormat {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "json_lines" | "jsonl" | "jsonlines" => Ok(Self::JsonLines),
            "json_array" => Ok(Self::JsonArray),
            "raw" => Ok(Self::Raw),
            other => Err(Error::InvalidConfigValue(format!(
                "Unknown output format: '{other}'. Expected: json_lines, json_array, or raw"
            ))),
        }
    }
}

impl OutputFormat {
    fn file_extension(self) -> &'static str {
        match self {
            Self::JsonLines => "jsonl",
            Self::JsonArray => "json",
            Self::Raw => "bin",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use iggy_connector_sdk::{Payload, Schema, Sink};
    use tempfile::TempDir;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        time::timeout,
    };

    use super::*;

    fn test_config(root: &str) -> OpenDalSinkConfig {
        OpenDalSinkConfig {
            service: "fs".to_string(),
            path_prefix: Some("iggy/messages".to_string()),
            path_template: default_path_template(),
            options: BTreeMap::from([("root".to_string(), SecretString::from(root.to_string()))]),
            output_format: default_output_format(),
            include_metadata: false,
            include_headers: false,
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
    fn given_temporary_s3_failure_when_consuming_should_retry_write() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("test server should bind");
            let endpoint = format!(
                "http://{}",
                listener
                    .local_addr()
                    .expect("test server address should be available")
            );
            let mut config = test_config("/tmp");
            config.service = "s3".to_string();
            config.max_attempts = Some(2);
            config.retry_delay = Some("1ms".to_string());
            config.options = BTreeMap::from([
                (
                    "bucket".to_string(),
                    SecretString::from("test-bucket".to_string()),
                ),
                (
                    "region".to_string(),
                    SecretString::from("us-east-1".to_string()),
                ),
                ("endpoint".to_string(), SecretString::from(endpoint)),
                (
                    "access_key_id".to_string(),
                    SecretString::from("access-key".to_string()),
                ),
                (
                    "secret_access_key".to_string(),
                    SecretString::from("secret-key".to_string()),
                ),
            ]);
            let mut sink = OpenDalSink::new(1, config);

            let server = async {
                for request_index in 0..3 {
                    let (mut connection, _) = listener
                        .accept()
                        .await
                        .expect("OpenDAL should connect to the test server");
                    let mut request = [0; 4096];
                    let bytes_read = connection
                        .read(&mut request)
                        .await
                        .expect("test server should read the request");
                    let request = String::from_utf8_lossy(&request[..bytes_read]);

                    let (status, headers, body) = match request_index {
                        0 => {
                            assert!(request.starts_with("GET "));
                            (
                                "200 OK",
                                "",
                                concat!(
                                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                                    "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
                                    "<Name>test-bucket</Name><Prefix></Prefix><KeyCount>0</KeyCount>",
                                    "<MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated>",
                                    "</ListBucketResult>"
                                ),
                            )
                        }
                        1 => {
                            assert!(request.starts_with("PUT "));
                            (
                                "500 Internal Server Error",
                                "",
                                concat!(
                                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                                    "<Error><Code>InternalError</Code>",
                                    "<Message>temporary failure</Message></Error>"
                                ),
                            )
                        }
                        2 => {
                            assert!(request.starts_with("PUT "));
                            ("200 OK", "ETag: \"test-etag\"\r\n", "")
                        }
                        _ => unreachable!(),
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/xml\r\n{headers}\
                         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    connection
                        .write_all(response.as_bytes())
                        .await
                        .expect("test server should write the response");
                }
            };
            let client = async {
                sink.open().await.expect("S3 service should open");
                let topic_metadata = TopicMetadata {
                    stream: "events".to_string(),
                    topic: "orders".to_string(),
                };
                let messages_metadata = MessagesMetadata {
                    partition_id: 2,
                    current_offset: 5,
                    schema: Schema::Raw,
                };
                sink.consume(
                    &topic_metadata,
                    messages_metadata,
                    vec![
                        test_message(4, Payload::Raw(vec![1, 2, 3])),
                        test_message(5, Payload::Raw(vec![4, 5, 6])),
                    ],
                )
                .await
                .expect("temporary S3 error should be retried");
                assert_eq!(sink.messages_processed.load(Ordering::Relaxed), 2);
            };

            let (server_result, client_result) = tokio::join!(
                timeout(Duration::from_secs(5), server),
                timeout(Duration::from_secs(5), client)
            );
            server_result.expect("OpenDAL should complete three requests");
            client_result.expect("OpenDAL operations should finish");
        });
    }

    #[test]
    fn given_unopened_sink_when_consuming_should_return_init_error() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let sink = OpenDalSink::new(1, test_config("/tmp"));
            let topic_metadata = TopicMetadata {
                stream: "events".to_string(),
                topic: "orders".to_string(),
            };
            let messages_metadata = MessagesMetadata {
                partition_id: 2,
                current_offset: 4,
                schema: Schema::Raw,
            };

            let error = sink
                .consume(&topic_metadata, messages_metadata, Vec::new())
                .await
                .expect_err("unopened sink should reject messages");

            assert!(matches!(error, Error::InitError(_)));
        });
    }

    #[test]
    fn given_unwritable_fs_service_when_consuming_should_return_storage_error() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let temp_dir = TempDir::new().expect("temp directory should be created");
            let root = temp_dir.path().to_path_buf();
            let config = test_config(&root.to_string_lossy());
            let mut sink = OpenDalSink::new(1, config);
            sink.open().await.expect("sink should open");
            fs::remove_dir_all(&root).expect("storage root should be removed");
            fs::write(&root, b"not a directory").expect("storage root should become a file");

            let topic_metadata = TopicMetadata {
                stream: "events".to_string(),
                topic: "orders".to_string(),
            };
            let messages_metadata = MessagesMetadata {
                partition_id: 2,
                current_offset: 4,
                schema: Schema::Raw,
            };
            let error = sink
                .consume(
                    &topic_metadata,
                    messages_metadata,
                    vec![
                        test_message(4, Payload::Raw(vec![1, 2, 3])),
                        test_message(5, Payload::Raw(vec![4, 5, 6])),
                    ],
                )
                .await
                .expect_err("unwritable storage should reject the message");

            assert!(matches!(error, Error::CannotStoreData(_)));
            assert_eq!(sink.messages_processed.load(Ordering::Relaxed), 0);
            assert_eq!(sink.write_errors.load(Ordering::Relaxed), 2);
        });
    }

    #[test]
    fn given_fs_service_when_consuming_batch_should_write_single_json_lines_object() {
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
                current_offset: 5,
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
            .expect("batch should be written");
            sink.close().await.expect("sink should close");

            let path = temp_dir.path().join(
                "iggy/messages/events/orders/1970-01-01/00/\
                 00002-00000000000000000004-00000000000000000005.jsonl",
            );
            let stored = fs::read(path).expect("stored batch object should be readable");
            assert_eq!(
                stored,
                br#"{"payload":{"id":1}}
{"payload":{"id":2}}
"#
            );
            assert_eq!(sink.messages_processed.load(Ordering::Relaxed), 2);
        });
    }

    #[test]
    fn given_json_array_batch_when_consumed_should_write_single_object() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let temp_dir = TempDir::new().expect("temp directory should be created");
            let root = temp_dir.path().to_string_lossy();
            let mut config = test_config(&root);
            config.output_format = "json_array".to_string();
            let mut sink = OpenDalSink::new(1, config);
            sink.open().await.expect("sink should open");
            let topic_metadata = TopicMetadata {
                stream: "events".to_string(),
                topic: "orders".to_string(),
            };
            let mut messages = Vec::with_capacity(2);
            for (offset, id) in [(4, 1), (5, 2)] {
                let mut json = format!(r#"{{"id":{id}}}"#).into_bytes();
                let payload =
                    simd_json::to_owned_value(&mut json).expect("JSON payload should be valid");
                messages.push(test_message(offset, Payload::Json(payload)));
            }

            sink.consume(
                &topic_metadata,
                MessagesMetadata {
                    partition_id: 2,
                    current_offset: 4,
                    schema: Schema::Json,
                },
                messages,
            )
            .await
            .expect("batch should be uploaded");

            let path = temp_dir.path().join(
                "iggy/messages/events/orders/1970-01-01/00/\
                 00002-00000000000000000004-00000000000000000005.json",
            );
            let stored = fs::read(path).expect("stored batch object should be readable");
            assert_eq!(stored, br#"[{"payload":{"id":1}},{"payload":{"id":2}}]"#);
            assert_eq!(sink.messages_processed.load(Ordering::Relaxed), 2);
            sink.close().await.expect("sink should close");
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

    #[test]
    fn given_unknown_output_format_when_opening_should_reject_config() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let mut config = test_config("/tmp");
            config.output_format = "csv".to_string();
            let mut sink = OpenDalSink::new(1, config);

            let error = sink
                .open()
                .await
                .expect_err("unknown output format should fail");

            assert!(matches!(error, Error::InvalidConfigValue(_)));
        });
    }
}
