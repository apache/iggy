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

use crate::actors::consumer::Consumer;
use crate::args::common::IggyBenchArgs;
use crate::benchmarks::benchmark::Benchmarkable;
use crate::rate_limiter::RateLimiter;
use async_trait::async_trait;
use iggy::error::IggyError;
use iggy::messages::poll_messages::PollingKind;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use iggy_bench_report::individual_metrics::BenchmarkIndividualMetrics;
use integration::test_server::ClientFactory;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::info;

pub struct ConsumerBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl ConsumerBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Self {
        Self {
            args,
            client_factory,
        }
    }
}

#[async_trait]
impl Benchmarkable for ConsumerBenchmark {
    async fn run(
        &mut self,
    ) -> Result<JoinSet<Result<BenchmarkIndividualMetrics, IggyError>>, IggyError> {
        self.check_streams().await?;
        let consumers_count = self.args.consumers();
        info!("Creating {} consumer(s)...", consumers_count);
        let messages_per_batch = self.args.messages_per_batch();
        let message_batches = self.args.message_batches();

        let mut set = JoinSet::new();
        for consumer_id in 1..=consumers_count {
            let args = self.args.clone();
            let client_factory = self.client_factory.clone();
            info!("Executing the benchmark on consumer #{}...", consumer_id);
            let args = args.clone();
            let start_stream_id = args.start_stream_id();
            let client_factory = client_factory.clone();
            let streams_number = args.streams();

            let stream_id = match self.args.kind() {
                BenchmarkKind::BalancedConsumerGroup
                | BenchmarkKind::BalancedProducerAndConsumerGroup => {
                    args.start_stream_id() + 1 + (consumer_id % streams_number)
                }
                _ => start_stream_id + consumer_id,
            };

            let warmup_time = args.warmup_time();
            let polling_kind = match self.args.kind() {
                BenchmarkKind::BalancedConsumerGroup
                | BenchmarkKind::BalancedProducerAndConsumerGroup => PollingKind::Next,
                _ => PollingKind::Offset,
            };

            let consumer = Consumer::new(
                client_factory,
                self.args.kind(),
                consumer_id,
                None,
                stream_id,
                messages_per_batch,
                message_batches,
                // In this test all consumers are polling 8000 messages in total, doesn't matter which one is fastest
                Arc::new(AtomicI64::new(message_batches as i64)),
                warmup_time,
                args.sampling_time(),
                args.moving_average_window(),
                polling_kind,
                false, // TODO: Calculate latency from timestamp in first message, it should be an argument to iggy-bench
                args.rate_limit()
                    .map(|rl| RateLimiter::new(rl.as_bytes_u64())),
            );
            set.spawn(consumer.run());
        }

        info!("Created {} consumer(s).", consumers_count);
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
