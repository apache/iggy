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

use crate::actors::{
    ApiLabel, BatchMetrics, BenchmarkInit,
    consumer::client::{
        BenchmarkConsumerClient,
        interface::{BenchmarkConsumerConfig, ConsumerClient},
    },
};

use futures_util::StreamExt;
use iggy::prelude::*;
use integration::test_server::{ClientFactory, login_root};
use std::{sync::Arc, time::Duration};
use tokio::time::{Instant, timeout};
use tracing::{error, warn};

pub struct HighLevelConsumerClient {
    client_factory: Arc<dyn ClientFactory>,
    config: BenchmarkConsumerConfig,
    consumer: Option<IggyConsumer>,
}

impl HighLevelConsumerClient {
    pub fn new(client_factory: Arc<dyn ClientFactory>, config: BenchmarkConsumerConfig) -> Self {
        Self {
            client_factory,
            config,
            consumer: None,
        }
    }
}

impl ConsumerClient for HighLevelConsumerClient {
    async fn consume_batch(&mut self) -> Result<Option<BatchMetrics>, IggyError> {
        let consumer = self.consumer.as_mut().expect("Consumer not initialized");

        let batch_start = Instant::now();
        let mut batch_messages = 0;
        let mut batch_user_bytes = 0;
        let mut batch_total_bytes = 0;

        while batch_messages < self.config.messages_per_batch.get() {
            let timeout_result = timeout(Duration::from_secs(1), consumer.next()).await;

            match timeout_result {
                Ok(Some(message_result)) => match message_result {
                    Ok(received_message) => {
                        batch_messages += 1;
                        batch_user_bytes += received_message.message.payload.len() as u64;
                        batch_total_bytes +=
                            received_message.message.get_size_bytes().as_bytes_u64();

                        let offset = received_message.message.header.offset;

                        if batch_messages >= self.config.messages_per_batch.get() {
                            if let Err(error) = consumer.store_offset(offset, None).await {
                                error!("Failed to store offset: {offset}. {error}");
                            }
                            break;
                        }
                    }
                    Err(err) => {
                        warn!("Error receiving message: {}", err);
                    }
                },
                Ok(None) | Err(_) => {
                    break;
                }
            }
        }

        if batch_messages == 0 {
            Ok(None)
        } else {
            Ok(Some(BatchMetrics {
                messages: batch_messages,
                user_data_bytes: batch_user_bytes,
                total_bytes: batch_total_bytes,
                latency: batch_start.elapsed(),
            }))
        }
    }
}

impl BenchmarkInit for HighLevelConsumerClient {
    async fn setup(&mut self) -> Result<(), IggyError> {
        let topic_id_str = "topic-1";
        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;

        let stream_id_str = self.config.stream_id.to_string();

        let mut consumer = if let Some(cg_id) = self.config.consumer_group_id {
            let consumer_group_name = format!("cg_{cg_id}");
            client
                .consumer_group(&consumer_group_name, &stream_id_str, topic_id_str)?
                .batch_length(self.config.messages_per_batch.get())
                .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
                .create_consumer_group_if_not_exists()
                .auto_join_consumer_group()
                .build()
        } else {
            // TODO(hubcio): as of now, there is no way to mimic the behavior of
            // PollingKind::Offset, because high level API doesn't provide method
            // to commit local offset manually, only auto-commit on server.
            client
                .consumer(
                    &format!("hl_consumer_{}", self.config.consumer_id),
                    &stream_id_str,
                    topic_id_str,
                    0,
                )?
                .polling_strategy(PollingStrategy::offset(0))
                .batch_length(self.config.messages_per_batch.get())
                .auto_commit(AutoCommit::Disabled)
                .build()
        };

        consumer.init().await?;
        self.consumer = Some(consumer);
        Ok(())
    }
}

impl ApiLabel for HighLevelConsumerClient {
    const API_LABEL: &'static str = "high-level";
}

impl BenchmarkConsumerClient for HighLevelConsumerClient {}
