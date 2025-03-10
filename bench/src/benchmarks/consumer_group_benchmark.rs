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

use super::benchmark::Benchmarkable;
use crate::{
    actors::consumer::Consumer,
    args::common::IggyBenchArgs,
    benchmarks::{CONSUMER_GROUP_BASE_ID, CONSUMER_GROUP_NAME_PREFIX},
    rate_limiter::RateLimiter,
};
use async_trait::async_trait;
use iggy::{
    client::ConsumerGroupClient, clients::client::IggyClient, error::IggyError,
    messages::poll_messages::PollingKind,
};
use iggy_bench_report::{
    benchmark_kind::BenchmarkKind, individual_metrics::BenchmarkIndividualMetrics,
};
use integration::test_server::{login_root, ClientFactory};
use std::sync::{atomic::AtomicI64, Arc};
use tokio::task::JoinSet;
use tracing::{error, info};

pub struct ConsumerGroupBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl ConsumerGroupBenchmark {
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
impl Benchmarkable for ConsumerGroupBenchmark {
    async fn run(
        &mut self,
    ) -> Result<JoinSet<Result<BenchmarkIndividualMetrics, IggyError>>, IggyError> {
        self.check_streams().await?;
        let consumer_groups_count = self.args.number_of_consumer_groups();
        self.init_consumer_groups(consumer_groups_count)
            .await
            .expect("Failed to init consumer group");

        let start_stream_id = self.args.start_stream_id();
        let start_consumer_group_id = CONSUMER_GROUP_BASE_ID;
        let consumers = self.args.consumers();
        let messages_per_batch = self.args.messages_per_batch();
        let warmup_time = self.args.warmup_time();
        let polling_kind = PollingKind::Next;
        let message_batches = self.args.message_batches();
        let total_message_batches = Arc::new(AtomicI64::new((message_batches * consumers) as i64));

        let mut set = JoinSet::new();
        for consumer_id in 1..=consumers {
            let consumer_group_id =
                start_consumer_group_id + 1 + (consumer_id % consumer_groups_count);
            let stream_id = start_stream_id + 1 + (consumer_id % consumer_groups_count);

            let consumer = Consumer::new(
                self.client_factory.clone(),
                self.args.kind(),
                consumer_id,
                Some(consumer_group_id),
                stream_id,
                messages_per_batch,
                message_batches,
                // In this test all consumers are polling 8000 messages in total, doesn't matter which one is fastest
                total_message_batches.clone(),
                warmup_time,
                self.args.sampling_time(),
                self.args.moving_average_window(),
                polling_kind,
                false, // TODO: Calculate latency from timestamp in first message, it should be an argument to iggy-bench
                self.args
                    .rate_limit()
                    .map(|rl| RateLimiter::new(rl.as_bytes_u64())),
            );
            set.spawn(consumer.run());
        }

        info!(
            "Starting consumer group benchmark with {} messages",
            self.total_messages()
        );
        Ok(set)
    }

    fn kind(&self) -> BenchmarkKind {
        self.args.kind()
    }

    fn args(&self) -> &IggyBenchArgs {
        &self.args
    }

    fn client_factory(&self) -> &Arc<dyn ClientFactory> {
        &self.client_factory
    }
}
