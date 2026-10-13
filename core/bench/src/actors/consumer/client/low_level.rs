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

use crate::actors::consumer::client::BenchmarkConsumerClient;
use crate::actors::consumer::client::interface::{BenchmarkConsumerConfig, ConsumerClient};
use crate::actors::{ApiLabel, BatchMetrics, BenchmarkInit};
use crate::benchmarks::common::create_consumer;
use crate::utils::ClientFactory;
use crate::utils::{batch_total_size_bytes, batch_user_size_bytes};
use iggy::prelude::*;
use std::collections::HashMap;
use std::future::ready;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

pub struct LowLevelConsumerClient {
    client_factory: Arc<dyn ClientFactory>,
    config: BenchmarkConsumerConfig,
    client: Option<IggyClient>,
    consumer: Option<Consumer>,
    stream_id: Identifier,
    topic_id: Identifier,
    partition_id: Option<u32>,
    polling_strategy: PollingStrategy,
    auto_commit: bool,
    /// Where offset polling continues in each partition. A group member polls its partitions
    /// round-robin, so one shared cursor would skip the start of every partition but the first.
    next_offsets: HashMap<u32, u64>,
}

impl LowLevelConsumerClient {
    pub fn new(client_factory: Arc<dyn ClientFactory>, config: BenchmarkConsumerConfig) -> Self {
        Self {
            client_factory,
            config,
            client: None,
            consumer: None,
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partition_id: None,
            polling_strategy: PollingStrategy::next(),
            auto_commit: true,
            next_offsets: HashMap::new(),
        }
    }
}

impl ConsumerClient for LowLevelConsumerClient {
    async fn consume_batch(&mut self) -> Result<Option<BatchMetrics>, IggyError> {
        let client = self.client.as_ref().expect("client not initialized");
        let consumer = self.consumer.as_ref().expect("consumer not initialized");
        let messages_to_receive = self.config.messages_per_batch.get();

        let polling_strategy = self.polling_strategy;
        let next_offsets = &self.next_offsets;
        let strategy_for = |partition_id: u32| {
            next_offsets
                .get(&partition_id)
                .map_or(polling_strategy, |offset| PollingStrategy::offset(*offset))
        };
        let before_poll = Instant::now();
        let polled = client
            .poll_messages_with_strategy_for(
                &self.stream_id,
                &self.topic_id,
                self.partition_id,
                consumer,
                &strategy_for,
                messages_to_receive,
                self.auto_commit,
            )
            .await;

        let polled = match polled {
            Ok(p) => p,
            Err(e) => {
                if matches!(
                    e,
                    IggyError::TopicIdNotFound(_, _) | IggyError::TransientNotAccepted
                ) {
                    return Ok(None);
                }
                return Err(e);
            }
        };

        if polled.messages.is_empty() {
            return Ok(None);
        }
        let messages_count = polled.messages.len() as u64;
        let latency = if self.config.origin_timestamp_latency_calculation {
            let now = IggyTimestamp::now().as_micros();
            Duration::from_micros(now - polled.messages[0].header.origin_timestamp)
        } else {
            before_poll.elapsed()
        };

        let user_bytes = batch_user_size_bytes(&polled);
        let total_bytes = batch_total_size_bytes(&polled);

        if self.polling_strategy.kind == PollingKind::Offset
            && let Some(last) = polled.messages.last()
        {
            self.next_offsets
                .insert(polled.partition_id, last.header.offset + 1);
        }

        Ok(Some(BatchMetrics {
            messages: messages_count.try_into().unwrap(),
            user_data_bytes: user_bytes,
            total_bytes,
            latency,
        }))
    }

    fn reset_offsets(&mut self) -> impl Future<Output = Result<(), IggyError>> {
        self.next_offsets.clear();
        self.polling_strategy = PollingStrategy::offset(0);
        ready(Ok(()))
    }
}

impl BenchmarkInit for LowLevelConsumerClient {
    async fn setup(&mut self) -> Result<(), IggyError> {
        let topic_id_str = "topic-1";
        let default_partition_id = 0u32;

        let client = self.client_factory.create_authenticated_client().await?;

        let stream_id: Identifier = self.config.stream_id.as_str().try_into().unwrap();
        let topic_id = topic_id_str.try_into().unwrap();
        let partition_id = if self.config.consumer_group_id.is_some() {
            None
        } else {
            Some(default_partition_id)
        };

        let consumer = create_consumer(
            &client,
            self.config.consumer_group_id.as_ref(),
            &stream_id,
            &topic_id,
            self.config.consumer_id,
        )
        .await;
        let (polling_strategy, auto_commit) = match self.config.polling_kind {
            PollingKind::Offset => (PollingStrategy::offset(0), false),
            PollingKind::Next => (PollingStrategy::next(), true),
            _ => panic!("Unsupported polling kind: {:?}", self.config.polling_kind),
        };

        self.client = Some(client);
        self.consumer = Some(consumer);

        self.stream_id = stream_id;
        self.topic_id = topic_id;
        self.partition_id = partition_id;
        self.polling_strategy = polling_strategy;
        self.auto_commit = auto_commit;

        Ok(())
    }
}

impl ApiLabel for LowLevelConsumerClient {
    const API_LABEL: &'static str = "low-level";
}

impl BenchmarkConsumerClient for LowLevelConsumerClient {}

#[cfg(test)]
mod tests {
    use super::{BenchmarkConsumerConfig, ConsumerClient, LowLevelConsumerClient};
    use crate::utils::client_factory::TcpClientFactory;
    use bench_report::numeric_parameter::BenchmarkNumericParameter;
    use iggy::prelude::{
        ClientWrapper, Consumer, Identifier, IggyClient, IggyDuration, PollingStrategy, TcpClient,
        TcpClientConfig,
    };
    use std::{collections::HashMap, sync::Arc};

    const GROUP_ID: u32 = 1;
    const WARMUP_OFFSETS: [(u32, u64); 2] = [(0, 12), (1, 34)];

    #[tokio::test]
    async fn given_disconnected_group_member_when_resetting_should_rewind_without_offset_requests()
    {
        for (polling_strategy, auto_commit) in [
            (PollingStrategy::next(), true),
            (PollingStrategy::offset(0), false),
        ] {
            let mut consumer = LowLevelConsumerClient::new(
                Arc::new(TcpClientFactory::default()),
                BenchmarkConsumerConfig {
                    consumer_id: 1,
                    consumer_group_id: Some(GROUP_ID),
                    stream_id: "bench-stream-1".to_owned(),
                    messages_per_batch: BenchmarkNumericParameter::Value(1),
                    warmup_time: IggyDuration::default(),
                    polling_kind: polling_strategy.kind,
                    origin_timestamp_latency_calculation: false,
                    pretty: false,
                },
            );
            consumer.client = Some(IggyClient::new(ClientWrapper::Tcp(
                TcpClient::create(Arc::new(TcpClientConfig::default())).unwrap(),
            )));
            consumer.consumer = Some(Consumer::group(Identifier::numeric(GROUP_ID).unwrap()));
            consumer.polling_strategy = polling_strategy;
            consumer.auto_commit = auto_commit;
            if !auto_commit {
                consumer.next_offsets = HashMap::from(WARMUP_OFFSETS);
            }

            consumer.reset_offsets().await.unwrap();

            assert_eq!(consumer.polling_strategy, PollingStrategy::offset(0));
            assert!(consumer.next_offsets.is_empty());
            assert_eq!(consumer.auto_commit, auto_commit);
        }
    }
}
