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

use super::backend::{BenchmarkConsumerBackend, ConsumedBatch, LowLevelBackend};
use crate::benchmarks::common::create_consumer;
use crate::utils::{batch_total_size_bytes, batch_user_size_bytes};
use iggy::prelude::*;
use integration::test_server::login_root;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{info, warn};

impl BenchmarkConsumerBackend for LowLevelBackend {
    type Consumer = (
        IggyClient,
        Identifier,
        Identifier,
        Option<u32>,
        Consumer,
        u64,
    );

    async fn setup(&self) -> Result<Self::Consumer, IggyError> {
        let topic_id: u32 = 1;
        let default_partition_id: u32 = 1;
        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;

        let stream_id = self.config.stream_id.try_into().unwrap();
        let topic_id = topic_id.try_into().unwrap();
        let partition_id = if self.config.consumer_group_id.is_some() {
            None
        } else {
            Some(default_partition_id)
        };
        let cg_id = self.config.consumer_group_id;
        let consumer = create_consumer(
            &client,
            cg_id.as_ref(),
            &stream_id,
            &topic_id,
            self.config.consumer_id,
        )
        .await;

        Ok((client, stream_id, topic_id, partition_id, consumer, 0))
    }

    async fn warmup(&self, consumer: &mut Self::Consumer) -> Result<(), IggyError> {
        let (client, stream_id, topic_id, partition_id, consumer_obj, messages_processed) =
            consumer;
        let warmup_end = Instant::now() + self.config.warmup_time.get_duration();
        let mut warmup_messages_processed = 0;
        let mut _last_batch_user_size_bytes = 0;

        while Instant::now() < warmup_end {
            let messages_to_receive = self.config.messages_per_batch.get();
            let offset = *messages_processed + warmup_messages_processed;
            let (strategy, auto_commit) = match self.config.polling_kind {
                PollingKind::Offset => (PollingStrategy::offset(offset), false),
                PollingKind::Next => (PollingStrategy::next(), true),
                _ => panic!(
                    "Unsupported polling kind for benchmark: {:?}",
                    self.config.polling_kind
                ),
            };
            let polled_messages = client
                .poll_messages(
                    stream_id,
                    topic_id,
                    *partition_id,
                    consumer_obj,
                    &strategy,
                    messages_to_receive,
                    auto_commit,
                )
                .await?;

            if polled_messages.messages.is_empty() {
                warn!(
                    "Consumer #{} → Messages are empty for offset: {}, retrying...",
                    self.config.consumer_id, offset
                );
                continue;
            }
            warmup_messages_processed += polled_messages.messages.len() as u64;
            _last_batch_user_size_bytes = batch_user_size_bytes(&polled_messages);
        }
        Ok(())
    }

    async fn consume_batch(
        &self,
        consumer: &mut Self::Consumer,
    ) -> Result<Option<ConsumedBatch>, IggyError> {
        let (client, stream_id, topic_id, partition_id, consumer_obj, messages_processed) =
            consumer;
        let messages_to_receive = self.config.messages_per_batch.get();
        let offset = *messages_processed;
        let (strategy, auto_commit) = match self.config.polling_kind {
            PollingKind::Offset => (PollingStrategy::offset(offset), false),
            PollingKind::Next => (PollingStrategy::next(), true),
            _ => panic!(
                "Unsupported polling kind for benchmark: {:?}",
                self.config.polling_kind
            ),
        };

        let before_poll = Instant::now();
        let polled_messages = client
            .poll_messages(
                stream_id,
                topic_id,
                *partition_id,
                consumer_obj,
                &strategy,
                messages_to_receive,
                auto_commit,
            )
            .await;

        let polled_messages = match polled_messages {
            Ok(messages) => messages,
            Err(e) => {
                if matches!(e, IggyError::TopicIdNotFound(_, _)) {
                    return Ok(None);
                }
                return Err(e);
            }
        };

        if polled_messages.messages.is_empty() {
            return Ok(None);
        }
        let latency = if self.config.origin_timestamp_latency_calculation {
            let now = IggyTimestamp::now().as_micros();
            Duration::from_micros(now - polled_messages.messages[0].header.origin_timestamp)
        } else {
            before_poll.elapsed()
        };

        let batch_user_size_bytes = batch_user_size_bytes(&polled_messages);
        let batch_total_size_bytes = batch_total_size_bytes(&polled_messages);

        *messages_processed += polled_messages.messages.len() as u64;

        Ok(Some(ConsumedBatch {
            messages: u32::try_from(polled_messages.messages.len()).unwrap(),
            user_data_bytes: batch_user_size_bytes,
            total_bytes: batch_total_size_bytes,
            latency,
        }))
    }

    fn log_setup_info(&self) {
        if let Some(cg_id) = self.config.consumer_group_id {
            info!(
                "Consumer #{}, part of consumer group #{} → polling in {} messages per batch from stream {}, using low-level API...",
                self.config.consumer_id,
                cg_id,
                self.config.messages_per_batch,
                self.config.stream_id,
            );
        } else {
            info!(
                "Consumer #{} → polling in {} messages per batch from stream {}, using low-level API...",
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
