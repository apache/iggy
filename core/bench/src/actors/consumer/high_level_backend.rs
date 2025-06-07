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

use super::backend::{BenchmarkConsumerBackend, ConsumedBatch, HighLevelBackend};
use futures_util::StreamExt;
use iggy::prelude::*;
use integration::test_server::login_root;
use tokio::time::Instant;
use tracing::{info, warn};

impl BenchmarkConsumerBackend for HighLevelBackend {
    type Consumer = IggyConsumer;

    async fn setup(&self) -> Result<Self::Consumer, IggyError> {
        let topic_id: u32 = 1;
        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;

        let stream_id_str = self.config.stream_id.to_string();
        let topic_id_str = topic_id.to_string();

        let mut iggy_consumer = if let Some(cg_id) = self.config.consumer_group_id {
            let consumer_group_name = format!("bench_cg_{cg_id}");
            // Consumer groups use auto-commit (matching PollingKind::Next behavior from low-level API)
            client
                .consumer_group(&consumer_group_name, &stream_id_str, &topic_id_str)?
                .batch_size(self.config.messages_per_batch.get())
                .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
                .auto_join_consumer_group()
                .build()
        } else {
            // TODO(hubcio): as of now, there is no way to mimic the behavior of
            // PollingKind::Offset, because high level API doesn't provide method
            // to commit local offset manually, only auto-commit on server.
            client
                .consumer(
                    &format!("bench_consumer_{}", self.config.consumer_id),
                    &stream_id_str,
                    &topic_id_str,
                    1,
                )?
                .batch_size(self.config.messages_per_batch.get())
                .auto_commit(AutoCommit::Disabled) // TODO@spetz
                .build()
        };

        iggy_consumer.init().await?;
        Ok(iggy_consumer)
    }

    async fn warmup(&self, consumer: &mut Self::Consumer) -> Result<(), IggyError> {
        let warmup_end = Instant::now() + self.config.warmup_time.get_duration();
        while Instant::now() < warmup_end {
            if let Some(message) = consumer.next().await {
                if message.is_err() {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn consume_batch(
        &self,
        consumer: &mut Self::Consumer,
    ) -> Result<ConsumedBatch, IggyError> {
        let batch_start = Instant::now();
        let mut batch_messages = 0;
        let mut batch_user_bytes = 0;
        let mut batch_total_bytes = 0;

        while batch_messages <= self.config.messages_per_batch.get() {
            tracing::warn!("Consuming batch of {} messages", batch_messages);
            if let Some(message_result) = consumer.next().await {
                match message_result {
                    Ok(received_message) => {
                        batch_messages += 1;
                        batch_user_bytes += received_message.message.payload.len() as u64;
                        batch_total_bytes +=
                            received_message.message.get_size_bytes().as_bytes_u64();

                        if batch_messages >= self.config.messages_per_batch.get() {
                            info!(
                                "Batch of {} messages consumed, last_offset: {}",
                                batch_messages, received_message.message.header.offset
                            );
                            break;
                        }
                    }
                    Err(err) => {
                        warn!("Error receiving message: {}", err);
                    }
                }
            } else {
                break;
            }
        }

        Ok(ConsumedBatch {
            messages: batch_messages,
            user_data_bytes: batch_user_bytes,
            total_bytes: batch_total_bytes,
            latency: batch_start.elapsed(),
        })
    }

    fn log_setup_info(&self) {
        if let Some(cg_id) = self.config.consumer_group_id {
            info!(
                "Consumer #{}, part of consumer group #{} → polling in {} messages per batch from stream {}, using high-level API...",
                self.config.consumer_id,
                cg_id,
                self.config.messages_per_batch,
                self.config.stream_id,
            );
        } else {
            info!(
                "Consumer #{} → polling in {} messages per batch from stream {}, using high-level API...",
                self.config.consumer_id, self.config.messages_per_batch, self.config.stream_id,
            );
        }
    }

    fn log_warmup_info(&self) {
        if let Some(cg_id) = self.config.consumer_group_id {
            info!(
                "Consumer #{}, part of consumer group #{}, → warming up for {}...",
                self.config.consumer_id, cg_id, self.config.warmup_time
            );
        } else {
            info!(
                "Consumer #{} → warming up for {}...",
                self.config.consumer_id, self.config.warmup_time
            );
        }
    }
}
