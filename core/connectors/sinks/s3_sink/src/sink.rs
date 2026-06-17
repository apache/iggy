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

use crate::buffer::FileBuffer;
use crate::formatter;
use crate::path::{PathContext, render_s3_key};
use crate::{BufferKey, S3Sink};
use async_trait::async_trait;
use iggy_connector_sdk::retry::{exponential_backoff, jitter};
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

const MAX_BACKOFF: Duration = Duration::from_secs(60);

struct FlushPayload {
    data: Vec<u8>,
    s3_key: String,
    msg_count: u64,
    first_offset: u64,
    last_offset: u64,
}

#[async_trait]
impl Sink for S3Sink {
    async fn open(&mut self) -> Result<(), Error> {
        info!("Opening S3 sink connector with ID: {}", self.id);

        self.validate_and_parse_config()?;

        let bucket = crate::client::create_bucket(&self.config).await?;

        crate::client::verify_bucket(&bucket).await?;

        info!(
            "S3 sink ID: {} connected to bucket '{}' in region '{}'",
            self.id, self.config.bucket, self.config.region
        );

        self.bucket = Some(bucket);

        info!(
            "S3 sink ID: {} opened. format={}, rotation={}, max_file_size={}, template='{}'",
            self.id,
            self.config.output_format,
            self.config.file_rotation,
            self.config.max_file_size,
            self.config.path_template,
        );

        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let bucket = self
            .bucket
            .as_ref()
            .ok_or_else(|| Error::InitError("S3 client not initialized".to_string()))?;

        let key = BufferKey {
            stream: topic_metadata.stream.clone(),
            topic: topic_metadata.topic.clone(),
            partition_id: messages_metadata.partition_id,
        };

        let batch_size = messages.len() as u64;

        {
            let mut state = self.state.lock().await;
            state.messages_received += batch_size;
        }

        let buffer_arc = self
            .buffers
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(FileBuffer::new())))
            .clone();

        let mut processed = 0u64;
        let result = self
            .process_messages_inner(
                bucket,
                &key,
                &buffer_arc,
                topic_metadata,
                &messages_metadata,
                &messages,
                &mut processed,
            )
            .await;

        if let Err(ref e) = result {
            let lost = batch_size - processed;
            if lost > 0 {
                let mut state = self.state.lock().await;
                state.messages_lost += lost;
                error!(
                    "S3 sink ID: {} lost {lost} messages from batch of {batch_size} for {}/{}/{}: {e}",
                    self.id,
                    topic_metadata.stream,
                    topic_metadata.topic,
                    messages_metadata.partition_id,
                );
            }
        }

        debug!(
            "S3 sink ID: {} buffered {} messages for {}/{}/{}",
            self.id,
            batch_size,
            topic_metadata.stream,
            topic_metadata.topic,
            messages_metadata.partition_id,
        );

        result
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Closing S3 sink connector with ID: {}", self.id);

        if let Some(bucket) = &self.bucket {
            for entry in self.buffers.iter() {
                let key = entry.key().clone();
                let buffer_arc = entry.value().clone();
                let flush_payload = {
                    let mut buffer = buffer_arc.lock().await;
                    if buffer.is_empty() {
                        None
                    } else {
                        Some(self.extract_flush_payload(&key, &mut buffer))
                    }
                };
                if let Some(Ok(payload)) = flush_payload {
                    if let Err(e) = self.do_upload(bucket, payload).await {
                        error!(
                            "S3 sink ID: {} failed to flush on close for {}/{}/{}: {e}",
                            self.id, key.stream, key.topic, key.partition_id
                        );
                    }
                } else if let Some(Err(e)) = flush_payload {
                    error!(
                        "S3 sink ID: {} failed to prepare flush on close for {}/{}/{}: {e}",
                        self.id, key.stream, key.topic, key.partition_id
                    );
                }
            }
        } else {
            let pending: u64 = self
                .buffers
                .iter()
                .map(|e| e.value().try_lock().map(|b| b.message_count()).unwrap_or(0))
                .sum();
            if pending > 0 {
                warn!(
                    "S3 sink ID: {} closing without S3 client — {pending} buffered messages will be lost",
                    self.id,
                );
            }
        }

        let state = self.state.lock().await;
        info!(
            "S3 sink ID: {} closed. received={}, uploaded={}, lost={}",
            self.id, state.messages_received, state.messages_uploaded, state.messages_lost,
        );

        Ok(())
    }
}

impl S3Sink {
    #[allow(clippy::too_many_arguments)]
    async fn process_messages_inner(
        &self,
        bucket: &s3::Bucket,
        key: &BufferKey,
        buffer_arc: &Arc<tokio::sync::Mutex<FileBuffer>>,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
        processed: &mut u64,
    ) -> Result<(), Error> {
        for message in messages {
            let resolved = self.resolved();
            let formatted = formatter::format_message(
                message,
                topic_metadata,
                messages_metadata,
                self.config.include_metadata,
                self.config.include_headers,
                resolved.output_format,
            )?;

            let flush_payload = {
                let mut buffer = buffer_arc.lock().await;
                buffer.append(&formatted, message.offset, message.timestamp);

                if buffer.should_rotate(
                    self.config.file_rotation,
                    resolved.max_file_size_bytes,
                    resolved.max_messages,
                ) {
                    Some(self.extract_flush_payload(key, &mut buffer)?)
                } else {
                    None
                }
            };

            if let Some(payload) = flush_payload {
                self.do_upload(bucket, payload).await?;
            }

            *processed += 1;
        }
        Ok(())
    }

    fn extract_flush_payload(
        &self,
        key: &BufferKey,
        buffer: &mut FileBuffer,
    ) -> Result<FlushPayload, Error> {
        let resolved = self.resolved();
        let data = formatter::finalize_buffer(buffer.entries(), resolved.output_format);

        let ctx = PathContext {
            stream: &key.stream,
            topic: &key.topic,
            partition_id: key.partition_id,
            first_timestamp_micros: buffer.first_timestamp_micros(),
        };

        let s3_key = render_s3_key(
            self.config.prefix.as_deref(),
            &self.config.path_template,
            &ctx,
            buffer.first_offset(),
            buffer.last_offset(),
            resolved.output_format,
        )?;

        let msg_count = buffer.message_count();
        let first_offset = buffer.first_offset();
        let last_offset = buffer.last_offset();

        buffer.reset();

        Ok(FlushPayload {
            data,
            s3_key,
            msg_count,
            first_offset,
            last_offset,
        })
    }

    async fn do_upload(&self, bucket: &s3::Bucket, payload: FlushPayload) -> Result<(), Error> {
        match self
            .upload_with_retry(bucket, &payload.s3_key, &payload.data)
            .await
        {
            Ok(()) => {
                debug!(
                    "S3 sink ID: {} uploaded {} ({} messages, {} bytes)",
                    self.id,
                    payload.s3_key,
                    payload.msg_count,
                    payload.data.len(),
                );
                let mut state = self.state.lock().await;
                state.messages_uploaded += payload.msg_count;
                Ok(())
            }
            Err(e) => {
                error!(
                    "S3 sink ID: {} failed to upload {} ({} messages, offsets {}-{} lost): {e}",
                    self.id,
                    payload.s3_key,
                    payload.msg_count,
                    payload.first_offset,
                    payload.last_offset,
                );
                let mut state = self.state.lock().await;
                state.messages_lost += payload.msg_count;

                self.write_lost_marker(
                    bucket,
                    &payload.s3_key,
                    payload.first_offset,
                    payload.last_offset,
                    payload.msg_count,
                    &e,
                )
                .await;

                Err(e)
            }
        }
    }

    async fn write_lost_marker(
        &self,
        bucket: &s3::Bucket,
        s3_key: &str,
        first_offset: u64,
        last_offset: u64,
        msg_count: u64,
        error: &Error,
    ) {
        let marker_key = format!("{s3_key}.lost");
        let body = format!(
            "offset_range: {first_offset}-{last_offset}\nmessage_count: {msg_count}\nerror: {error}\n"
        );
        if let Err(e) = self
            .upload_with_retry(bucket, &marker_key, body.as_bytes())
            .await
        {
            warn!(
                "S3 sink ID: {} failed to write .lost marker at {} after retries: {e}",
                self.id, marker_key
            );
        }
    }

    async fn upload_with_retry(
        &self,
        bucket: &s3::Bucket,
        s3_key: &str,
        data: &[u8],
    ) -> Result<(), Error> {
        let resolved = self.resolved();
        let max_attempts = resolved.max_attempts;
        let base_delay = resolved.retry_delay;
        let mut attempt = 0u32;

        loop {
            match bucket.put_object(s3_key, data).await {
                Ok(response) => {
                    let status = response.status_code();
                    if (200..300).contains(&status) {
                        return Ok(());
                    }

                    if !is_retriable_status(status) {
                        return Err(Error::CannotStoreData(format!(
                            "S3 PutObject returned non-retriable status {status} for key '{s3_key}'"
                        )));
                    }

                    attempt += 1;
                    if attempt >= max_attempts {
                        return Err(Error::CannotStoreData(format!(
                            "S3 PutObject returned status {status} after {max_attempts} attempts for key '{s3_key}'"
                        )));
                    }
                    warn!(
                        "S3 sink ID: {} PutObject status {status} (attempt {attempt}/{max_attempts}). Retrying...",
                        self.id
                    );
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= max_attempts {
                        return Err(Error::CannotStoreData(format!(
                            "S3 PutObject failed after {max_attempts} attempts for key '{s3_key}': {e}"
                        )));
                    }
                    warn!(
                        "S3 sink ID: {} PutObject error (attempt {attempt}/{max_attempts}): {e}. Retrying...",
                        self.id
                    );
                }
            }
            // exponential_backoff expects a 0-based retry index
            let retry_index = attempt - 1;
            let delay = jitter(exponential_backoff(base_delay, retry_index, MAX_BACKOFF));
            tokio::time::sleep(delay).await;
        }
    }
}

fn is_retriable_status(status: u16) -> bool {
    status >= 500 || status == 408 || status == 429
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DEFAULT_MAX_FILE_SIZE, DEFAULT_OUTPUT_FORMAT, DEFAULT_PATH_TEMPLATE, FileRotation, S3Sink,
        S3SinkConfig,
    };

    fn test_config() -> S3SinkConfig {
        S3SinkConfig {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            prefix: Some("data".to_string()),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            path_template: DEFAULT_PATH_TEMPLATE.to_string(),
            file_rotation: FileRotation::Size,
            max_file_size: DEFAULT_MAX_FILE_SIZE.to_string(),
            max_messages_per_file: None,
            output_format: DEFAULT_OUTPUT_FORMAT.to_string(),
            include_metadata: true,
            include_headers: false,
            max_attempts: None,
            retry_delay: None,
            path_style: None,
        }
    }

    #[test]
    fn is_retriable_5xx() {
        assert!(is_retriable_status(500));
        assert!(is_retriable_status(502));
        assert!(is_retriable_status(503));
        assert!(is_retriable_status(599));
    }

    #[test]
    fn is_retriable_408_429() {
        assert!(is_retriable_status(408));
        assert!(is_retriable_status(429));
    }

    #[test]
    fn not_retriable_4xx() {
        assert!(!is_retriable_status(400));
        assert!(!is_retriable_status(403));
        assert!(!is_retriable_status(404));
        assert!(!is_retriable_status(405));
    }

    #[test]
    fn not_retriable_2xx() {
        assert!(!is_retriable_status(200));
        assert!(!is_retriable_status(204));
    }

    #[test]
    fn extract_flush_payload_embeds_partition_id() {
        let config = test_config();
        let mut sink = S3Sink::new(1, config);
        sink.validate_and_parse_config().unwrap();

        let key = BufferKey {
            stream: "logs".to_string(),
            topic: "events".to_string(),
            partition_id: 7,
        };

        let mut buffer = FileBuffer::new();
        buffer.append(b"{\"a\":1}", 0, 1_710_597_600_000_000);
        buffer.append(b"{\"b\":2}", 1, 1_710_597_601_000_000);

        let payload = sink.extract_flush_payload(&key, &mut buffer).unwrap();
        assert!(
            payload.s3_key.contains("00007-"),
            "S3 key must contain partition_id: {}",
            payload.s3_key
        );
        assert_eq!(payload.msg_count, 2);
        assert_eq!(payload.first_offset, 0);
        assert_eq!(payload.last_offset, 1);
    }

    #[test]
    fn extract_flush_payload_offset_padding_lex_sort() {
        let config = test_config();
        let mut sink = S3Sink::new(1, config);
        sink.validate_and_parse_config().unwrap();

        let key = BufferKey {
            stream: "s".to_string(),
            topic: "t".to_string(),
            partition_id: 0,
        };

        let mut buf1 = FileBuffer::new();
        buf1.append(b"x", 999_999, 1_000_000);
        let p1 = sink.extract_flush_payload(&key, &mut buf1).unwrap();

        let mut buf2 = FileBuffer::new();
        buf2.append(b"y", 1_000_000, 1_000_000);
        let p2 = sink.extract_flush_payload(&key, &mut buf2).unwrap();

        assert!(
            p1.s3_key < p2.s3_key,
            "Lex sort must be correct: {} < {}",
            p1.s3_key,
            p2.s3_key
        );
    }

    #[test]
    fn validate_max_messages_set_for_rotation_by_messages() {
        let config = S3SinkConfig {
            file_rotation: FileRotation::Messages,
            max_messages_per_file: Some(500),
            ..test_config()
        };
        let mut sink = S3Sink::new(1, config);
        sink.validate_and_parse_config().unwrap();
        assert_eq!(sink.resolved().max_messages, 500);
    }

    #[test]
    fn validate_rejects_messages_rotation_without_max() {
        let config = S3SinkConfig {
            file_rotation: FileRotation::Messages,
            max_messages_per_file: None,
            ..test_config()
        };
        let mut sink = S3Sink::new(1, config);
        assert!(sink.validate_and_parse_config().is_err());
    }

    #[test]
    fn validate_rejects_zero_max_messages() {
        let config = S3SinkConfig {
            file_rotation: FileRotation::Messages,
            max_messages_per_file: Some(0),
            ..test_config()
        };
        let mut sink = S3Sink::new(1, config);
        assert!(sink.validate_and_parse_config().is_err());
    }

    #[test]
    fn validate_rejects_zero_max_file_size() {
        let config = S3SinkConfig {
            max_file_size: "0B".to_string(),
            ..test_config()
        };
        let mut sink = S3Sink::new(1, config);
        assert!(sink.validate_and_parse_config().is_err());
    }

    #[test]
    fn validate_rejects_over_5gib_file_size() {
        let config = S3SinkConfig {
            max_file_size: "10GiB".to_string(),
            ..test_config()
        };
        let mut sink = S3Sink::new(1, config);
        assert!(sink.validate_and_parse_config().is_err());
    }

    #[test]
    fn validate_rejects_empty_bucket() {
        let config = S3SinkConfig {
            bucket: "".to_string(),
            ..test_config()
        };
        let mut sink = S3Sink::new(1, config);
        assert!(sink.validate_and_parse_config().is_err());
    }

    #[test]
    fn validate_rejects_empty_region() {
        let config = S3SinkConfig {
            region: "".to_string(),
            ..test_config()
        };
        let mut sink = S3Sink::new(1, config);
        assert!(sink.validate_and_parse_config().is_err());
    }

    #[test]
    fn validate_rejects_empty_path_template() {
        let config = S3SinkConfig {
            path_template: "".to_string(),
            ..test_config()
        };
        let mut sink = S3Sink::new(1, config);
        assert!(sink.validate_and_parse_config().is_err());
    }

    #[test]
    fn validate_size_rotation_max_messages_defaults_to_max() {
        let config = test_config();
        let mut sink = S3Sink::new(1, config);
        sink.validate_and_parse_config().unwrap();
        assert_eq!(sink.resolved().max_messages, u64::MAX);
    }

    #[test]
    fn flush_payload_data_is_json_lines() {
        let config = test_config();
        let mut sink = S3Sink::new(1, config);
        sink.validate_and_parse_config().unwrap();

        let key = BufferKey {
            stream: "s".to_string(),
            topic: "t".to_string(),
            partition_id: 0,
        };

        let mut buffer = FileBuffer::new();
        buffer.append(b"{\"a\":1}", 0, 1_000_000);
        buffer.append(b"{\"b\":2}", 1, 2_000_000);

        let payload = sink.extract_flush_payload(&key, &mut buffer).unwrap();
        assert_eq!(payload.data, b"{\"a\":1}\n{\"b\":2}\n");
    }

    #[test]
    fn flush_payload_data_is_json_array() {
        let config = S3SinkConfig {
            output_format: "json_array".to_string(),
            ..test_config()
        };
        let mut sink = S3Sink::new(1, config);
        sink.validate_and_parse_config().unwrap();

        let key = BufferKey {
            stream: "s".to_string(),
            topic: "t".to_string(),
            partition_id: 0,
        };

        let mut buffer = FileBuffer::new();
        buffer.append(b"{\"a\":1}", 0, 1_000_000);
        buffer.append(b"{\"b\":2}", 1, 2_000_000);

        let payload = sink.extract_flush_payload(&key, &mut buffer).unwrap();
        assert_eq!(payload.data, b"[{\"a\":1},{\"b\":2}]");
    }

    #[test]
    fn close_without_bucket_does_not_panic() {
        let config = test_config();
        let mut sink = S3Sink::new(1, config);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(sink.close());
        assert!(result.is_ok());
    }
}
