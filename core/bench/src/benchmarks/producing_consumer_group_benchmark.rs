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

use crate::actors::producing_consumer::ProducingConsumer;
use crate::args::common::IggyBenchArgs;
use crate::benchmarks::benchmark::Benchmarkable;
use crate::benchmarks::{CONSUMER_GROUP_BASE_ID, CONSUMER_GROUP_NAME_PREFIX};
use crate::rate_limiter::RateLimiter;
use async_trait::async_trait;
use iggy::client::ConsumerGroupClient;
use iggy::clients::client::IggyClient;
use iggy::error::IggyError;
use iggy::messages::PollingKind;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use iggy_bench_report::individual_metrics::BenchmarkIndividualMetrics;
use integration::test_server::{login_root, ClientFactory};
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::{error, info};

pub struct EndToEndProducingConsumerGroupBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl EndToEndProducingConsumerGroupBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Self {
        Self {
            args,
            client_factory,
        }
    }

    pub async fn init_consumer_groups(&self, consumer_groups_count: u32) -> Result<(), IggyError> {
        let start_stream_id = self.args().start_stream_id();
        let topic_id: u32 = 1;
        let client = self.client_factory().create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;
        for i in 1..=consumer_groups_count {
            let consumer_group_id = CONSUMER_GROUP_BASE_ID + i;
            let stream_id = start_stream_id + i;
            let consumer_group_name =
                format!("{}-{}", CONSUMER_GROUP_NAME_PREFIX, consumer_group_id);
            info!(
                "Creating test consumer group with name: {}, id: {}, stream id: {}, topic id: {}",
                consumer_group_name, consumer_group_id, stream_id, topic_id
            );

            let cg = client
                .create_consumer_group(
                    &stream_id.try_into().unwrap(),
                    &topic_id.try_into().unwrap(),
                    &consumer_group_name,
                    Some(consumer_group_id),
                )
                .await;
            if cg.is_err() {
                let error = cg.err().unwrap();
                match error {
                    IggyError::ConsumerGroupIdAlreadyExists(_, _) => {
                        continue;
                    }
                    _ => error!("Error when creating consumer group : {error}"),
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Benchmarkable for EndToEndProducingConsumerGroupBenchmark {
    async fn run(
        &mut self,
    ) -> Result<JoinSet<Result<BenchmarkIndividualMetrics, IggyError>>, IggyError> {
        self.init_streams().await.expect("Failed to init streams!");
        let consumer_groups_count = self.args.number_of_consumer_groups();
        self.init_consumer_groups(consumer_groups_count)
            .await
            .expect("Failed to init consumer groups!");

        let actors_count = self.args.producers();
        info!(
            "Creating {actors_count} producing consumer(s) which belong to {consumer_groups_count} consumer groups...",
        );
        let start_consumer_group_id = CONSUMER_GROUP_BASE_ID;
        let start_stream_id = self.args.start_stream_id();
        let messages_per_batch = self.args.messages_per_batch();
        let message_batches = self.args.message_batches();
        let message_size = self.args.message_size();
        let partitions_count = self.args.number_of_partitions();
        let warmup_time = self.args.warmup_time();
        let polling_kind = PollingKind::Next;
        let consumer_groups_count = self.args.number_of_consumer_groups();
        let total_message_batches =
            Arc::new(AtomicI64::new((message_batches * actors_count) as i64));

        let mut set = JoinSet::new();
        for actor_id in 1..=actors_count {
            let args = self.args.clone();
            let client_factory = self.client_factory.clone();
            info!(
                "Executing the benchmark on producing consumer #{}...",
                actor_id
            );
            let stream_id = start_stream_id + 1 + (actor_id % consumer_groups_count);
            let consumer_group_id =
                start_consumer_group_id + 1 + (actor_id % consumer_groups_count);

            let actor = ProducingConsumer::new(
                client_factory,
                args.kind(),
                actor_id,
                Some(consumer_group_id),
                stream_id,
                partitions_count,
                messages_per_batch,
                message_batches,
                message_size,
                total_message_batches.clone(),
                warmup_time,
                args.sampling_time(),
                args.moving_average_window(),
                args.rate_limit()
                    .map(|rl| RateLimiter::new(rl.as_bytes_u64())),
                polling_kind,
                false,
            );
            set.spawn(actor.run());
        }

        info!("Created {actors_count} producing consumer(s) which would be part of {consumer_groups_count} consumer groups");
        Ok(set)
    }

    fn kind(&self) -> BenchmarkKind {
        self.args.kind()
    }

    fn total_messages(&self) -> u64 {
        let messages_per_batch = self.args.messages_per_batch();
        let message_batches = self.args.message_batches();
        let streams = self.args.streams();
        (messages_per_batch * message_batches * streams) as u64
    }

    fn args(&self) -> &IggyBenchArgs {
        &self.args
    }

    fn client_factory(&self) -> &Arc<dyn ClientFactory> {
        &self.client_factory
    }
}
