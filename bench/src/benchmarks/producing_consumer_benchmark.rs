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

pub struct EndToEndProducingConsumerBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl EndToEndProducingConsumerBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Self {
        Self {
            args,
            client_factory,
        }
    }
}

#[async_trait]
impl Benchmarkable for EndToEndProducingConsumerBenchmark {
    async fn run(
        &mut self,
    ) -> Result<JoinSet<Result<BenchmarkIndividualMetrics, IggyError>>, IggyError> {
        self.init_streams().await.expect("Failed to init streams!");
        let actors_count = self.args.producers();
        info!("Creating {} producing consumer(s)...", actors_count);
        let streams_number = self.args.streams();
        let messages_per_batch = self.args.messages_per_batch();
        let message_batches = self.args.message_batches();
        let message_size = self.args.message_size();
        let warmup_time = self.args.warmup_time();
        let polling_kind = PollingKind::Offset;

        let mut set = JoinSet::new();
        for actor_id in 1..=actors_count {
            let args = self.args.clone();
            let client_factory = self.client_factory.clone();
            info!(
                "Executing the benchmark on producing consumer #{}...",
                actor_id
            );
            let stream_id = args.start_stream_id() + 1 + (actor_id % streams_number);

            let actor = ProducingConsumer::new(
                client_factory,
                args.kind(),
                actor_id,
                None,
                stream_id,
                1,
                messages_per_batch,
                message_batches,
                message_size,
                Arc::new(AtomicI64::new(message_batches as i64)),
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

        info!("Created {} producing consumer(s).", actors_count);
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
