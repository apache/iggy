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
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, timeout};

const PARTITION_ID: u32 = 0;

pub struct LowLevelConsumerClient {
    client_factory: Arc<dyn ClientFactory>,
    config: BenchmarkConsumerConfig,
    client: Option<IggyClient>,
    consumer: Option<Consumer>,
    stream_id: Identifier,
    topic_id: Identifier,
    /// Where the next poll of a standalone consumer starts.
    polling_strategy: PollingStrategy,
    commit: CommitPolicy,
    /// A group member owns no fixed partition, so only a reader follows its assignment.
    reader: Option<PartitionReader>,
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
            polling_strategy: PollingStrategy::next(),
            commit: CommitPolicy::OnRead,
            reader: None,
        }
    }
}

impl ConsumerClient for LowLevelConsumerClient {
    async fn consume_batch(&mut self) -> Result<Option<BatchMetrics>, IggyError> {
        let before_poll = Instant::now();
        let polled = if let Some(reader) = self.reader.as_mut() {
            // `next()` has no time limit, so a member whose partitions are drained would hold up
            // the end of the run, which waits for every actor.
            let poll_timeout = self.config.poll_options.request_timeout().get_duration();
            match timeout(poll_timeout, reader.next()).await {
                Ok(polled) => polled.map_err(IggyError::from),
                Err(_) => return Ok(None),
            }
        } else {
            let client = self.client.as_ref().expect("client not initialized");
            let consumer = self.consumer.as_ref().expect("consumer not initialized");
            let options = PollOptions {
                count: self.config.messages_per_batch.get(),
                commit: self.commit,
                ..self.config.poll_options
            };
            client
                .poll_messages(
                    &self.stream_id,
                    &self.topic_id,
                    PARTITION_ID,
                    consumer,
                    &self.polling_strategy,
                    options,
                )
                .await
        };

        let polled = match polled {
            Ok(polled) => polled,
            Err(IggyError::TopicIdNotFound(_, _) | IggyError::TransientNotAccepted) => {
                return Ok(None);
            }
            Err(error) => return Err(error),
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

        if self.reader.is_none() && self.polling_strategy.kind == PollingKind::Offset {
            self.polling_strategy = polled.continuation();
        }

        Ok(Some(BatchMetrics {
            messages: messages_count.try_into().unwrap(),
            user_data_bytes: user_bytes,
            total_bytes,
            latency,
        }))
    }

    async fn reset_offsets(&mut self) -> Result<(), IggyError> {
        self.polling_strategy = PollingStrategy::offset(0);
        if let Some(reader) = self.reader.as_mut() {
            // A partition assigned after this starts at the start of the reader instead.
            for partition in reader.partitions() {
                reader.seek(partition, PollingStrategy::offset(0)).await?;
            }
        }
        Ok(())
    }
}

impl BenchmarkInit for LowLevelConsumerClient {
    async fn setup(&mut self) -> Result<(), IggyError> {
        let topic_id_str = "topic-1";

        let client = self.client_factory.create_authenticated_client().await?;

        let stream_id: Identifier = self.config.stream_id.as_str().try_into().unwrap();
        let topic_id = topic_id_str.try_into().unwrap();

        let consumer = create_consumer(
            &client,
            self.config.consumer_group_id.as_ref(),
            &stream_id,
            &topic_id,
            self.config.consumer_id,
        )
        .await;
        let (polling_strategy, commit) = match self.config.polling_kind {
            PollingKind::Offset => (PollingStrategy::offset(0), CommitPolicy::Manual),
            PollingKind::Next => (PollingStrategy::next(), CommitPolicy::OnRead),
            _ => panic!("Unsupported polling kind: {:?}", self.config.polling_kind),
        };
        if self.config.consumer_group_id.is_some() {
            // A reader asks for one count, so a range of batch sizes reads up to the largest.
            let options = PollOptions {
                count: self.config.messages_per_batch.max(),
                commit,
                ..self.config.poll_options
            };
            // Each timed `next()` measures one fetch round trip, so the reader holds one request
            // and no prefetch queue.
            self.reader = Some(
                client
                    .reader(&stream_id, &topic_id, &consumer)
                    .start(polling_strategy)
                    .options(options)
                    .prefetch_messages(options.count)
                    .prefetch_bytes(options.max_bytes)
                    .build()
                    .await?,
            );
        }

        self.client = Some(client);
        self.consumer = Some(consumer);

        self.stream_id = stream_id;
        self.topic_id = topic_id;
        self.polling_strategy = polling_strategy;
        self.commit = commit;

        Ok(())
    }
}

impl ApiLabel for LowLevelConsumerClient {
    const API_LABEL: &'static str = "low-level";
}

impl BenchmarkConsumerClient for LowLevelConsumerClient {}

#[cfg(test)]
mod tests {
    use super::{BenchmarkConsumerConfig, ConsumerClient, LowLevelConsumerClient, PARTITION_ID};
    use crate::utils::client_factory::TcpClientFactory;
    use bench_report::numeric_parameter::BenchmarkNumericParameter;
    use iggy::prelude::{
        ClientWrapper, CommitPolicy, Consumer, Identifier, IggyClient, IggyDuration,
        PartitionContext, PollOptions, PollingKind, PollingStrategy, TcpClient, TcpClientConfig,
    };
    use std::sync::Arc;

    const CONSUMER_ID: u32 = 1;
    const WARMUP_OFFSET: u64 = 12;
    const WARMUP_HISTORY: PartitionContext = PartitionContext {
        incarnation: 1,
        owner_generation: 0,
        metadata_op: 1,
    };

    #[tokio::test]
    async fn given_disconnected_standalone_consumer_when_resetting_should_rewind_without_offset_requests()
     {
        for (polling_strategy, commit) in [
            (PollingStrategy::next(), CommitPolicy::OnRead),
            (
                PollingStrategy::continuation(
                    PARTITION_ID,
                    PollingKind::Offset,
                    WARMUP_OFFSET,
                    WARMUP_HISTORY,
                ),
                CommitPolicy::Manual,
            ),
        ] {
            let mut consumer = LowLevelConsumerClient::new(
                Arc::new(TcpClientFactory::default()),
                BenchmarkConsumerConfig {
                    consumer_id: CONSUMER_ID,
                    consumer_group_id: None,
                    stream_id: "bench-stream-1".to_owned(),
                    messages_per_batch: BenchmarkNumericParameter::Value(1),
                    warmup_time: IggyDuration::default(),
                    polling_kind: polling_strategy.kind,
                    poll_options: PollOptions::default(),
                    origin_timestamp_latency_calculation: false,
                    pretty: false,
                },
            );
            consumer.client = Some(IggyClient::new(ClientWrapper::Tcp(
                TcpClient::create(Arc::new(TcpClientConfig::default())).unwrap(),
            )));
            consumer.consumer = Some(Consumer::new(Identifier::numeric(CONSUMER_ID).unwrap()));
            consumer.polling_strategy = polling_strategy;
            consumer.commit = commit;

            consumer.reset_offsets().await.unwrap();

            assert_eq!(consumer.polling_strategy, PollingStrategy::offset(0));
            assert_eq!(consumer.commit, commit);
        }
    }
}
