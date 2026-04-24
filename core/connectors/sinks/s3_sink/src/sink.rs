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
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata};
use tracing::{debug, error, info, warn};

#[async_trait]
impl Sink for S3Sink {
    async fn open(&mut self) -> Result<(), Error> {
        info!("Opening S3 sink connector with ID: {}", self.id);

        self.validate_and_parse_config()?;

        let bucket = crate::client::create_bucket(&self.config).await?;

        info!(
            "S3 sink ID: {} connected to bucket '{}' in region '{}'",
            self.id, self.config.bucket, self.config.region
        );

        match crate::client::verify_bucket(&bucket).await {
            Ok(()) => {
                info!(
                    "S3 sink ID: {} bucket '{}' connectivity verified",
                    self.id, self.config.bucket
                );
            }
            Err(e) => {
                warn!(
                    "S3 sink ID: {} bucket verification returned an error (non-fatal, \
                     the bucket may still be accessible): {e}",
                    self.id
                );
            }
        }

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

        let max_messages = self.config.max_messages_per_file.unwrap_or(u64::MAX);

        let mut buffers = self.buffers.lock().await;
        let buffer = buffers.entry(key.clone()).or_insert_with(FileBuffer::new);

        for message in &messages {
            let formatted = formatter::format_message(
                message,
                topic_metadata,
                &messages_metadata,
                self.config.include_metadata,
                self.config.include_headers,
                self.output_format,
            );
            buffer.append(formatted, message.offset, message.timestamp);

            if buffer.should_rotate(
                self.config.file_rotation,
                self.max_file_size_bytes,
                max_messages,
            ) {
                self.flush_buffer(bucket, &key, buffer).await;
            }
        }

        let mut state = self.state.lock().await;
        state.messages_processed += messages.len() as u64;

        debug!(
            "S3 sink ID: {} processed {} messages for {}/{}/{}",
            self.id,
            messages.len(),
            topic_metadata.stream,
            topic_metadata.topic,
            messages_metadata.partition_id,
        );

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Closing S3 sink connector with ID: {}", self.id);

        if let Some(bucket) = &self.bucket {
            let mut buffers = self.buffers.lock().await;
            let keys: Vec<BufferKey> = buffers.keys().cloned().collect();
            for key in keys {
                if let Some(buffer) = buffers.get_mut(&key)
                    && !buffer.is_empty()
                {
                    self.flush_buffer(bucket, &key, buffer).await;
                }
            }
        } else {
            let buffers = self.buffers.lock().await;
            let pending: u64 = buffers.values().map(|b| b.message_count()).sum();
            if pending > 0 {
                warn!(
                    "S3 sink ID: {} closing without S3 client — {pending} buffered messages will be lost",
                    self.id,
                );
            }
        }

        let state = self.state.lock().await;
        info!(
            "S3 sink ID: {} closed. messages_processed={}, uploads_completed={}, upload_errors={}",
            self.id, state.messages_processed, state.uploads_completed, state.upload_errors,
        );

        Ok(())
    }
}

impl S3Sink {
    async fn flush_buffer(&self, bucket: &s3::Bucket, key: &BufferKey, buffer: &mut FileBuffer) {
        if buffer.is_empty() {
            return;
        }

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
        );

        let msg_count = buffer.message_count();

        match self.upload_with_retry(bucket, &s3_key, &data).await {
            Ok(()) => {
                debug!(
                    "S3 sink ID: {} uploaded {} ({} messages, {} bytes)",
                    self.id,
                    s3_key,
                    msg_count,
                    data.len(),
                );
                let mut state = self.state.lock().await;
                state.uploads_completed += 1;
                drop(state);
                buffer.reset();
            }
            Err(e) => {
                error!(
                    "S3 sink ID: {} failed to upload {} ({} messages lost): {e}",
                    self.id, s3_key, msg_count
                );
                let mut state = self.state.lock().await;
                state.upload_errors += 1;
                drop(state);
                // Reset buffer even on failure to prevent unbounded growth.
                // Messages are lost but offsets will be re-delivered by the
                // runtime on next poll since consume() returned Ok.
                buffer.reset();
            }
        }
    }

    async fn upload_with_retry(
        &self,
        bucket: &s3::Bucket,
        s3_key: &str,
        data: &[u8],
    ) -> Result<(), Error> {
        let max_retries = self.max_retries();
        let retry_delay = self.retry_delay;
        let mut attempts = 0u32;

        loop {
            match bucket.put_object(s3_key, data).await {
                Ok(response) => {
                    let status = response.status_code();
                    if (200..300).contains(&status) {
                        return Ok(());
                    }
                    attempts += 1;
                    if attempts >= max_retries {
                        return Err(Error::CannotStoreData(format!(
                            "S3 PutObject returned status {status} after {attempts} attempts for key '{s3_key}'"
                        )));
                    }
                    warn!(
                        "S3 sink ID: {} PutObject status {status} (attempt {attempts}/{max_retries}). Retrying...",
                        self.id
                    );
                }
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_retries {
                        return Err(Error::CannotStoreData(format!(
                            "S3 PutObject failed after {attempts} attempts for key '{s3_key}': {e}"
                        )));
                    }
                    warn!(
                        "S3 sink ID: {} PutObject error (attempt {attempts}/{max_retries}): {e}. Retrying...",
                        self.id
                    );
                }
            }
            tokio::time::sleep(retry_delay * attempts).await;
        }
    }
}
