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
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, retry::retry_async,
    sink_connector,
};
use opendal::{Buffer, Operator};
use tokio::time::{Instant, timeout, timeout_at};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    config::{OpenDalSinkConfig, ValidatedConfig, Validation},
    path::{PathContext, object_path},
};

mod config;
mod formatter;
mod path;

sink_connector!(OpenDalSink);

const CONNECTOR_NAME: &str = "OpenDAL sink";
const ABORT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct OpenDalSink {
    id: u32,
    state: Option<SinkState>,
    messages_processed: AtomicU64,
    write_errors: AtomicU64,
}

#[derive(Debug)]
enum SinkState {
    Config(OpenDalSinkConfig),
    Running(RunningSink),
}

#[derive(Debug)]
struct RunningSink {
    config: ValidatedConfig,
    operator: Operator,
    formatter: formatter::Formatter,
}

impl OpenDalSink {
    pub fn new(id: u32, config: OpenDalSinkConfig) -> Self {
        Self {
            id,
            state: Some(SinkState::Config(config)),
            messages_processed: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
        }
    }
}

impl RunningSink {
    async fn write_object(&self, id: u32, path: &str, buffer: Buffer) -> Result<(), Error> {
        let retry_context = format!("{CONNECTOR_NAME} connector ID {id} object write");
        retry_async(
            self.config.retry_policy,
            &retry_context,
            opendal::Error::is_temporary,
            || self.write_attempt(id, path, buffer.clone()),
        )
        .await
        .map_err(|failure| {
            Error::CannotStoreData(format!(
                "Failed to write OpenDAL object '{path}' after {} attempt(s): {}",
                failure.attempts, failure.error
            ))
        })
    }

    async fn write_attempt(&self, id: u32, path: &str, buffer: Buffer) -> opendal::Result<()> {
        let duration = self.write_timeout_for(buffer.len());
        let deadline = Instant::now() + duration;
        let mut writer = timeout_at(
            deadline,
            self.operator
                .writer_with(path)
                .if_not_exists(self.config.if_not_exists),
        )
        .await
        .map_err(|_| write_timeout_error())??;
        let result = timeout_at(deadline, async {
            writer.write(buffer).await?;
            writer.close().await?;
            Ok(())
        })
        .await
        .unwrap_or_else(|_| Err(write_timeout_error()));

        if result.is_err() {
            match timeout(ABORT_TIMEOUT, writer.abort()).await {
                Ok(Ok(())) => {}
                Ok(Err(abort_error)) => warn!(
                    "{CONNECTOR_NAME} connector ID: {id} could not abort object '{path}': {abort_error}"
                ),
                Err(_) => {
                    warn!("{CONNECTOR_NAME} connector ID: {id} timed out aborting object '{path}'")
                }
            }
        }
        result
    }

    /// Adds one second per MiB to the base timeout, scaled to the buffer size.
    fn write_timeout_for(&self, buffer_size: usize) -> Duration {
        self.config
            .write_timeout
            .saturating_add(Duration::from_micros(
                (buffer_size as u64).saturating_mul(1_000_000) / (1024 * 1024),
            ))
    }

    async fn check_access(&self, id: u32) -> Result<(), Error> {
        let probe_name = format!(".iggy-write-probe-{}", Uuid::new_v4());
        let path_prefix = &self.config.path_prefix;
        let path = if path_prefix.is_empty() {
            probe_name
        } else {
            format!("{path_prefix}/{probe_name}")
        };
        let write_result = self.write_object(id, &path, Buffer::from("iggy")).await;
        let delete_result = timeout(self.config.write_timeout, self.operator.delete(&path)).await;
        write_result
            .map_err(|error| Error::InitError(format!("OpenDAL write probe failed: {error}")))?;
        match delete_result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(Error::InitError(format!(
                "OpenDAL write probe could not be deleted: {error}"
            ))),
            Err(_) => Err(Error::InitError(
                "OpenDAL write probe deletion timed out".to_string(),
            )),
        }
    }
}

#[async_trait]
impl Sink for OpenDalSink {
    async fn open(&mut self) -> Result<(), Error> {
        let config = match self.state.take() {
            Some(SinkState::Config(config)) => config,
            Some(running @ SinkState::Running(_)) => {
                self.state = Some(running);
                return Ok(());
            }
            None => {
                return Err(Error::InitError(
                    "OpenDAL sink was closed or failed to initialize".to_string(),
                ));
            }
        };
        let config = match config.validate() {
            Validation::Valid(config) => config,
            Validation::Invalid { config, error } => {
                self.state = Some(SinkState::Config(config));
                return Err(error);
            }
        };
        let formatter = config.build_formatter();
        let operator = config.build_operator().await?;
        let sink = RunningSink {
            config,
            operator,
            formatter,
        };
        sink.check_access(self.id).await?;

        info!(
            "Opened {CONNECTOR_NAME} connector ID: {}, service: {}, root: {}",
            self.id,
            sink.config.service,
            sink.operator.info().root()
        );
        self.state = Some(SinkState::Running(sink));
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let Some(SinkState::Running(state)) = &self.state else {
            return Err(Error::InitError(
                "OpenDAL sink is not initialized".to_string(),
            ));
        };
        let formatter = &state.formatter;
        if messages.is_empty() {
            return Ok(());
        }

        if state.config.verbose_logging {
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

        let mut remaining = messages.as_slice();
        let mut last_error = None;
        while !remaining.is_empty() {
            let (data, count) = match formatter.format_chunk(
                remaining,
                topic_metadata,
                &messages_metadata,
                state.config.max_file_size,
            ) {
                Ok(chunk) => chunk,
                Err(format_error) => {
                    self.write_errors.fetch_add(1, Ordering::Relaxed);
                    error!(
                        "{CONNECTOR_NAME} connector ID: {} cannot format message at offset {}: {format_error}",
                        self.id, remaining[0].offset
                    );
                    last_error = Some(format_error);
                    remaining = &remaining[1..];
                    continue;
                }
            };
            let chunk = &remaining[..count];
            let context = PathContext {
                stream: &topic_metadata.stream,
                topic: &topic_metadata.topic,
                partition_id: messages_metadata.partition_id,
                first_timestamp_micros: chunk[0].timestamp,
            };
            let result = async {
                let path = object_path(
                    &state.config.path_prefix,
                    &state.config.path_template,
                    &context,
                    chunk[0].offset,
                    chunk[count - 1].offset,
                    formatter.output_format(),
                )?;
                state.write_object(self.id, &path, Buffer::from(data)).await
            }
            .await;
            let message_count = count as u64;
            match result {
                Ok(()) => {
                    self.messages_processed
                        .fetch_add(message_count, Ordering::Relaxed);
                    debug!(
                        "{CONNECTOR_NAME} connector ID: {} uploaded one object with {} messages",
                        self.id, message_count
                    );
                }
                Err(write_error) => {
                    self.write_errors
                        .fetch_add(message_count, Ordering::Relaxed);
                    error!(
                        "{CONNECTOR_NAME} connector ID: {} failed to upload {} messages: {write_error}",
                        self.id, message_count
                    );
                    last_error = Some(write_error);
                }
            }
            remaining = &remaining[count..];
        }
        last_error.map_or(Ok(()), Err)
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.state = None;
        info!(
            "Closed {CONNECTOR_NAME} connector ID: {}, processed: {}, failed_messages: {}",
            self.id,
            self.messages_processed.load(Ordering::Relaxed),
            self.write_errors.load(Ordering::Relaxed)
        );
        Ok(())
    }
}

fn write_timeout_error() -> opendal::Error {
    opendal::Error::new(
        opendal::ErrorKind::Unexpected,
        "OpenDAL write attempt timed out",
    )
    .set_temporary()
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    use std::os::unix::fs::MetadataExt;
    use std::{
        fs,
        future::pending,
        sync::{Arc, atomic::AtomicBool},
    };

    use iggy_connector_sdk::{Payload, Schema, Sink};
    use opendal::{
        Capability, ErrorKind, Metadata, OperationContext,
        raw::{
            OpCopy, OpCreateDir, OpList, OpPresign, OpRead, OpRename, OpStat, OpWrite, RpCreateDir,
            RpPresign, RpRename, RpStat, Service, ServiceInfo, oio,
        },
    };
    use secrecy::SecretString;
    use tempfile::TempDir;

    use super::*;
    use crate::config::tests::test_config;

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

    #[cfg(target_os = "linux")]
    #[test]
    fn given_atomic_directory_on_another_device_when_opening_should_reject_config() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let root = TempDir::new().expect("root should be created");
            let Ok(atomic_directory) = TempDir::new_in("/dev/shm") else {
                return;
            };
            if fs::metadata(root.path())
                .expect("root metadata should exist")
                .dev()
                == fs::metadata(atomic_directory.path())
                    .expect("atomic metadata should exist")
                    .dev()
            {
                return;
            }
            let mut config = test_config(&root.path().to_string_lossy());
            config.options.insert(
                "atomic_write_dir".to_string(),
                SecretString::from(atomic_directory.path().to_string_lossy().into_owned()),
            );
            let error = OpenDalSink::new(1, config)
                .open()
                .await
                .expect_err("different devices cannot provide atomic rename");
            assert!(matches!(error, Error::InvalidConfigValue(_)));
            assert_eq!(
                fs::read_dir(atomic_directory.path())
                    .expect("atomic directory should exist")
                    .count(),
                0,
            );
        });
    }

    #[test]
    fn given_pending_abort_when_cleaning_up_should_preserve_write_error_and_drop_writer() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("paused runtime should start");
        runtime.block_on(async {
            let Validation::Valid(config) = test_config("/tmp").validate() else {
                panic!("write test configuration should be valid");
            };
            let dropped = Arc::new(AtomicBool::new(false));
            let operator = Operator::from_parts(
                OperationContext::default(),
                Arc::new(StalledAbortService {
                    dropped: dropped.clone(),
                }),
            );
            let formatter = config.build_formatter();
            let sink = RunningSink {
                config,
                operator,
                formatter,
            };
            let started = Instant::now();

            let error = timeout(
                ABORT_TIMEOUT + Duration::from_secs(1),
                sink.write_attempt(1, "events/orders/object.bin", Buffer::from("payload")),
            )
            .await
            .expect("an abort that never completes should still be bounded")
            .expect_err("the original write should fail");

            assert_eq!(error.kind(), ErrorKind::PermissionDenied);
            assert!(!error.is_temporary());
            assert!(started.elapsed() >= ABORT_TIMEOUT);
            assert!(started.elapsed() <= ABORT_TIMEOUT + Duration::from_millis(1));
            assert!(dropped.load(Ordering::Relaxed));
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
    fn given_running_sink_when_closed_should_reject_more_messages() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let root = TempDir::new().expect("root should be created");
            let mut config = test_config(&root.path().to_string_lossy());
            config.output_format = "raw".to_string();
            config.path_prefix = Some("/iggy/messages/".to_string());
            let mut sink = OpenDalSink::new(1, config);
            sink.open().await.expect("sink should open");
            sink.open().await.expect("running sink should stay open");
            let topic = TopicMetadata {
                stream: "events".to_string(),
                topic: "orders".to_string(),
            };
            let metadata = || MessagesMetadata {
                partition_id: 2,
                current_offset: 4,
                schema: Schema::Raw,
            };
            sink.consume(
                &topic,
                metadata(),
                vec![test_message(4, Payload::Raw(vec![1, 2]))],
            )
            .await
            .expect("running sink should store messages");
            sink.close().await.expect("sink should close");

            let error = sink
                .consume(
                    &topic,
                    metadata(),
                    vec![test_message(4, Payload::Raw(vec![3, 4]))],
                )
                .await
                .expect_err("closed sink should reject messages");
            assert!(matches!(error, Error::InitError(_)));
            assert_eq!(
                fs::read(root.path().join(
                    "iggy/messages/events/orders/1970-01-01/00/00002-00000000000000000004-00000000000000000004.bin",
                ))
                .expect("stored data should remain"),
                [1, 2],
            );
        });
    }

    #[test]
    fn given_closed_sink_when_opening_should_return_init_error() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        let mut sink = OpenDalSink::new(1, test_config(""));
        runtime.block_on(sink.close()).expect("sink should close");

        let error = runtime
            .block_on(sink.open())
            .expect_err("closed sink should not open");
        assert!(matches!(error, Error::InitError(_)));
    }

    #[test]
    fn given_invalid_config_when_opening_again_should_return_same_error() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let root = TempDir::new().expect("root should be created");
            for case in [
                "service",
                "path_template",
                "max_file_size",
                "write_timeout",
                "output_format",
                "duplicate_option",
                "missing_root",
                "empty_root",
            ] {
                let mut config = test_config(&root.path().to_string_lossy());
                match case {
                    "service" => config.service.clear(),
                    "path_template" => config.path_template = "{stream}".to_string(),
                    "max_file_size" => config.max_file_size = Some("invalid".to_string()),
                    "write_timeout" => config.write_timeout = Some("0s".to_string()),
                    "output_format" => config.output_format = "invalid".to_string(),
                    "duplicate_option" => {
                        config
                            .options
                            .insert("ROOT".to_string(), SecretString::from("other"));
                    }
                    "missing_root" => config.options.clear(),
                    "empty_root" => {
                        config
                            .options
                            .insert("root".to_string(), SecretString::from(""));
                    }
                    _ => unreachable!(),
                }
                let mut sink = OpenDalSink::new(1, config);
                let first = sink.open().await.expect_err(case);
                assert!(matches!(first, Error::InvalidConfigValue(_)), "{case}");
                let second = sink.open().await.expect_err(case);
                assert_eq!(first, second, "{case}");
            }
        });
    }

    #[test]
    fn given_failed_open_when_consuming_should_return_init_error() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let root = TempDir::new().expect("root should be created");
            let blocked_root = root.path().join("file");
            fs::write(&blocked_root, b"not a directory").expect("file should be created");
            let mut sink = OpenDalSink::new(1, test_config(&blocked_root.to_string_lossy()));
            assert!(matches!(sink.open().await, Err(Error::InitError(_))));

            let error = sink
                .consume(
                    &TopicMetadata {
                        stream: "events".to_string(),
                        topic: "orders".to_string(),
                    },
                    MessagesMetadata {
                        partition_id: 2,
                        current_offset: 4,
                        schema: Schema::Raw,
                    },
                    vec![test_message(4, Payload::Raw(vec![1, 2]))],
                )
                .await
                .expect_err("sink should reject messages after open fails");
            assert!(matches!(error, Error::InitError(_)));
            assert_eq!(
                fs::read(blocked_root).expect("file should remain"),
                b"not a directory",
            );
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
            assert!(
                temp_dir.path().join(".iggy-opendal-tmp").is_dir(),
                "filesystem writes should use the default atomic directory"
            );

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
    fn given_empty_atomic_directory_when_consuming_should_write_without_temporary_files() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let root = TempDir::new().expect("root should be created");
            let mut config = test_config(&root.path().to_string_lossy());
            config.output_format = "raw".to_string();
            config
                .options
                .insert("atomic_write_dir".to_string(), SecretString::from(""));
            let mut sink = OpenDalSink::new(1, config);
            sink.open()
                .await
                .expect("non-atomic filesystem storage should open");

            sink.consume(
                &TopicMetadata {
                    stream: "events".to_string(),
                    topic: "orders".to_string(),
                },
                MessagesMetadata {
                    partition_id: 2,
                    current_offset: 4,
                    schema: Schema::Raw,
                },
                vec![test_message(4, Payload::Raw(vec![1, 2, 3]))],
            )
            .await
            .expect("message should be stored");

            let object_path = root.path().join(
                "iggy/messages/events/orders/1970-01-01/00/\
                 00002-00000000000000000004-00000000000000000004.bin",
            );
            assert_eq!(
                fs::read(object_path).expect("object should be readable"),
                [1, 2, 3]
            );
            assert!(!root.path().join(".iggy-opendal-tmp").exists());
        });
    }

    #[test]
    fn given_small_object_limit_when_consuming_should_split_at_message_offsets() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let root = TempDir::new().expect("root should be created");
            let mut config = test_config(&root.path().to_string_lossy());
            config.max_file_size = Some("3B".to_string());
            config.output_format = "raw".to_string();
            let mut sink = OpenDalSink::new(1, config);
            sink.open().await.expect("sink should open");
            sink.consume(
                &TopicMetadata {
                    stream: "events".to_string(),
                    topic: "orders".to_string(),
                },
                MessagesMetadata {
                    partition_id: 2,
                    current_offset: 20,
                    schema: Schema::Raw,
                },
                vec![
                    test_message(4, Payload::Raw(vec![1, 2])),
                    test_message(9, Payload::Raw(vec![3])),
                    test_message(20, Payload::Raw(vec![4, 5])),
                ],
            )
            .await
            .expect("chunks should be written");
            let directory = root
                .path()
                .join("iggy/messages/events/orders/1970-01-01/00");
            assert_eq!(
                fs::read(directory.join("00002-00000000000000000004-00000000000000000009.bin"))
                    .expect("first chunk should exist"),
                [1, 2, 3],
            );
            assert_eq!(
                fs::read(directory.join("00002-00000000000000000020-00000000000000000020.bin"))
                    .expect("second chunk should exist"),
                [4, 5],
            );
            assert_eq!(
                fs::read_dir(directory)
                    .expect("object directory should exist")
                    .count(),
                2
            );
            assert_eq!(sink.messages_processed.load(Ordering::Relaxed), 3);
            assert_eq!(sink.write_errors.load(Ordering::Relaxed), 0);
        });
    }

    #[test]
    fn given_oversized_message_when_consuming_should_keep_other_messages() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let root = TempDir::new().expect("root should be created");
            let mut config = test_config(&root.path().to_string_lossy());
            config.max_file_size = Some("2B".to_string());
            config.output_format = "raw".to_string();
            let mut sink = OpenDalSink::new(1, config);
            sink.open().await.expect("sink should open");
            let error = sink
                .consume(
                    &TopicMetadata {
                        stream: "events".to_string(),
                        topic: "orders".to_string(),
                    },
                    MessagesMetadata {
                        partition_id: 2,
                        current_offset: 20,
                        schema: Schema::Raw,
                    },
                    vec![
                        test_message(4, Payload::Raw(vec![1])),
                        test_message(9, Payload::Raw(vec![2, 3, 4])),
                        test_message(20, Payload::Raw(vec![5])),
                    ],
                )
                .await
                .expect_err("oversized message should be rejected");
            assert!(matches!(error, Error::InvalidRecordValue(_)));
            let directory = root
                .path()
                .join("iggy/messages/events/orders/1970-01-01/00");
            assert_eq!(
                fs::read(directory.join("00002-00000000000000000004-00000000000000000004.bin"))
                    .expect("first valid message should exist"),
                [1],
            );
            assert_eq!(
                fs::read(directory.join("00002-00000000000000000020-00000000000000000020.bin"))
                    .expect("last valid message should exist"),
                [5],
            );
            assert_eq!(sink.messages_processed.load(Ordering::Relaxed), 2);
            assert_eq!(sink.write_errors.load(Ordering::Relaxed), 1);
        });
    }

    #[test]
    fn given_failed_atomic_rename_when_consuming_should_remove_temporary_file() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let root = TempDir::new().expect("root should be created");
            let mut sink = OpenDalSink::new(1, test_config(&root.path().to_string_lossy()));
            sink.open().await.expect("sink should open");
            let target = root.path().join(
                "iggy/messages/events/orders/1970-01-01/00/00002-00000000000000000004-00000000000000000004.jsonl",
            );
            fs::create_dir_all(&target).expect("target should be blocked by a directory");
            let error = sink.consume(
                &TopicMetadata { stream: "events".to_string(), topic: "orders".to_string() },
                MessagesMetadata { partition_id: 2, current_offset: 4, schema: Schema::Raw },
                vec![test_message(4, Payload::Raw(vec![1, 2]))],
            ).await.expect_err("rename onto a directory should fail");
            assert!(matches!(error, Error::CannotStoreData(_)));
            assert!(target.is_dir());
            assert_eq!(
                fs::read_dir(root.path().join(".iggy-opendal-tmp")).expect("atomic directory should exist").count(),
                0,
            );
        });
    }

    #[test]
    fn given_existing_object_when_replacement_is_disabled_should_keep_original_data() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let root = TempDir::new().expect("root should be created");
            let mut config = test_config(&root.path().to_string_lossy());
            config.output_format = "raw".to_string();
            config.if_not_exists = Some(true);
            let mut sink = OpenDalSink::new(1, config);
            sink.open().await.expect("conditional write probe should succeed");
            let topic = TopicMetadata { stream: "events".to_string(), topic: "orders".to_string() };
            for (payload, should_succeed) in [(vec![1, 2], true), (vec![3, 4], false)] {
                let result = sink.consume(
                    &topic,
                    MessagesMetadata { partition_id: 2, current_offset: 4, schema: Schema::Raw },
                    vec![test_message(4, Payload::Raw(payload))],
                ).await;
                if should_succeed {
                    result.expect("first write should succeed");
                } else {
                    assert!(matches!(result, Err(Error::CannotStoreData(_))));
                }
            }
            assert_eq!(
                fs::read(root.path().join(
                    "iggy/messages/events/orders/1970-01-01/00/00002-00000000000000000004-00000000000000000004.bin",
                )).expect("original object should remain"),
                [1, 2],
            );
        });
    }

    #[derive(Debug)]
    struct StalledAbortService {
        dropped: Arc<AtomicBool>,
    }

    impl Service for StalledAbortService {
        type Reader = ();
        type Writer = StalledAbortWriter;
        type Lister = ();
        type Deleter = ();
        type Copier = ();
        type Composer = ();

        fn info(&self) -> ServiceInfo {
            ServiceInfo::with_scheme("stalled-abort-test")
        }

        fn capability(&self) -> Capability {
            Capability {
                write: true,
                write_can_multi: true,
                ..Default::default()
            }
        }

        fn write(
            &self,
            _: &OperationContext,
            _: &str,
            _: OpWrite,
        ) -> opendal::Result<Self::Writer> {
            Ok(StalledAbortWriter {
                dropped: self.dropped.clone(),
            })
        }

        async fn create_dir(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpCreateDir,
        ) -> opendal::Result<RpCreateDir> {
            ().create_dir(ctx, path, args).await
        }

        async fn stat(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpStat,
        ) -> opendal::Result<RpStat> {
            ().stat(ctx, path, args).await
        }

        fn read(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpRead,
        ) -> opendal::Result<Self::Reader> {
            ().read(ctx, path, args)
        }

        fn delete(&self, ctx: &OperationContext) -> opendal::Result<Self::Deleter> {
            ().delete(ctx)
        }

        fn list(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpList,
        ) -> opendal::Result<Self::Lister> {
            ().list(ctx, path, args)
        }

        fn copy(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpCopy,
        ) -> opendal::Result<Self::Copier> {
            ().copy(ctx, from, to, args)
        }

        async fn rename(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpRename,
        ) -> opendal::Result<RpRename> {
            ().rename(ctx, from, to, args).await
        }

        async fn presign(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpPresign,
        ) -> opendal::Result<RpPresign> {
            ().presign(ctx, path, args).await
        }
    }

    #[derive(Debug)]
    struct StalledAbortWriter {
        dropped: Arc<AtomicBool>,
    }

    impl oio::Write for StalledAbortWriter {
        async fn write(&mut self, _: Buffer) -> opendal::Result<()> {
            Err(opendal::Error::new(
                ErrorKind::PermissionDenied,
                "backend write denied",
            ))
        }

        async fn close(&mut self) -> opendal::Result<Metadata> {
            oio::Write::close(&mut ()).await
        }

        async fn abort(&mut self) -> opendal::Result<()> {
            pending().await
        }
    }

    impl Drop for StalledAbortWriter {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Relaxed);
        }
    }
}
