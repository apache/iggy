/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

        let max_messages = match self.config.max_messages_per_file {
            Some(0) => {
                return Err(Error::InvalidConfigValue(
                    "max_messages_per_file must be greater than 0".to_owned(),
                ));
            }
            Some(n) => n,
            None if self.config.file_rotation == crate::FileRotation::Messages => {
                return Err(Error::InvalidConfigValue(
                    "file_rotation is 'messages' but max_messages_per_file is not set".to_owned(),
                ));
            }
            None => u64::MAX,
        };
        let batch_size = messages.len() as u64;

        let buffer_arc = self
            .buffers
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(FileBuffer::new())))
            .clone();

        for message in &messages {
            let formatted = formatter::format_message(
                message,
                topic_metadata,
                &messages_metadata,
                self.config.include_metadata,
                self.config.include_headers,
                self.output_format,
            )?;

            let flush_payload = {
                let mut buffer = buffer_arc.lock().await;
                buffer.append(&formatted, message.offset, message.timestamp);

                if buffer.should_rotate(
                    self.config.file_rotation,
                    self.max_file_size_bytes,
                    max_messages,
                ) {
                    Some(self.extract_flush_payload(&key, &mut buffer)?)
                } else {
                    None
                }
            };

            if let Some(payload) = flush_payload {
                self.do_upload(bucket, payload).await?;
            }
        }

        {
            let mut state = self.state.lock().await;
            state.messages_received += batch_size;
        }

        debug!(
            "S3 sink ID: {} buffered {} messages for {}/{}/{}",
            self.id,
            batch_size,
            topic_metadata.stream,
            topic_metadata.topic,
            messages_metadata.partition_id,
        );

        Ok(())
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
    fn extract_flush_payload(
        &self,
        key: &BufferKey,
        buffer: &mut FileBuffer,
    ) -> Result<FlushPayload, Error> {
        let data = formatter::finalize_buffer(buffer.entries(), self.output_format);

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
            self.output_format,
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
        if let Err(e) = bucket.put_object(&marker_key, body.as_bytes()).await {
            warn!(
                "S3 sink ID: {} failed to write .lost marker at {}: {e}",
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
        let max_attempts = self.max_attempts();
        let base_delay = self.retry_delay;
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
            let delay = jitter(exponential_backoff(base_delay, attempt - 1, MAX_BACKOFF));
            tokio::time::sleep(delay).await;
        }
    }
}

fn is_retriable_status(status: u16) -> bool {
    status >= 500 || status == 408 || status == 429
}
