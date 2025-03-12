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

use crate::actors::producer::Producer;
use crate::args::common::IggyBenchArgs;
use crate::benchmarks::benchmark::{BenchmarkFutures, Benchmarkable};
use crate::rate_limiter::RateLimiter;
use async_trait::async_trait;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tracing::info;

pub struct ProducerBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl ProducerBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Self {
        Self {
            args,
            client_factory,
        }
    }
}

#[async_trait]
impl Benchmarkable for ProducerBenchmark {
    async fn run(&mut self) -> BenchmarkFutures {
        self.init_streams().await.expect("Failed to init streams!");
        let producers_count = self.args.producers();
        info!("Creating {} producer(s)...", producers_count);
        let streams_number = self.args.streams();
        let messages_per_batch = self.args.messages_per_batch();
        let message_batches = self.args.message_batches();
        let message_size = self.args.message_size();
        let partitions_count = self.args.number_of_partitions();
        let warmup_time = self.args.warmup_time();

        let mut futures: BenchmarkFutures = Ok(Vec::with_capacity(producers_count as usize));
        for producer_id in 1..=producers_count {
            let args = self.args.clone();
            let client_factory = self.client_factory.clone();
            info!("Executing the benchmark on producer #{}...", producer_id);
            let stream_id = args.start_stream_id() + 1 + (producer_id % streams_number);

            let producer = Producer::new(
                client_factory,
                args.kind(),
                producer_id,
                stream_id,
                partitions_count,
                messages_per_batch,
                message_batches,
                message_size,
                warmup_time,
                args.sampling_time(),
                args.moving_average_window(),
                args.rate_limit()
                    .map(|rl| RateLimiter::new(rl.as_bytes_u64())),
                false, // TODO: Put timestamp in payload of first message, it should be an argument to iggy-bench
            );
            let future = Box::pin(async move { producer.run().await });
            futures.as_mut().unwrap().push(future);
        }
        info!("Created {} producer(s).", producers_count);
        futures
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
