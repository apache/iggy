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
use crate::actors::producer::Producer;
use crate::args::common::IggyBenchArgs;
use crate::benchmarks::benchmark::{BenchmarkFutures, Benchmarkable};
use crate::rate_limiter::RateLimiter;
use async_trait::async_trait;
use iggy::messages::poll_messages::PollingKind;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use integration::test_server::ClientFactory;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use tracing::info;

pub struct ProducerAndConsumerBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl ProducerAndConsumerBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Self {
        Self {
            args,
            client_factory,
        }
    }
}

#[async_trait]
impl Benchmarkable for ProducerAndConsumerBenchmark {
    async fn run(&mut self) -> BenchmarkFutures {
        self.init_streams().await.expect("Failed to init streams!");
        let start_stream_id = self.args.start_stream_id();
        let producers = self.args.producers();
        let consumers = self.args.consumers();
        let messages_per_batch = self.args.messages_per_batch();
        let message_batches = self.args.message_batches();
        let message_size = self.args.message_size();
        let partitions_count = self.args.number_of_partitions();
        let warmup_time = self.args.warmup_time();

        let mut futures: BenchmarkFutures =
            Ok(Vec::with_capacity((producers + consumers) as usize));
        for producer_id in 1..=producers {
            let stream_id = start_stream_id + producer_id;
            let producer = Producer::new(
                self.client_factory.clone(),
                self.args.kind(),
                producer_id,
                stream_id,
                partitions_count,
                messages_per_batch,
                message_batches,
                message_size,
                warmup_time,
                self.args.sampling_time(),
                self.args.moving_average_window(),
                self.args
                    .rate_limit()
                    .map(|rl| RateLimiter::new(rl.as_bytes_u64())),
                false, // TODO: put timestamp into first message, it should be an argument to iggy-bench
            );
            let future = Box::pin(async move { producer.run().await });
            futures.as_mut().unwrap().push(future);
        }

        let polling_kind = PollingKind::Offset;

        for consumer_id in 1..=consumers {
            let stream_id = start_stream_id + consumer_id;
            let consumer = Consumer::new(
                self.client_factory.clone(),
                self.args.kind(),
                consumer_id,
                None,
                stream_id,
                messages_per_batch,
                message_batches,
                Arc::new(AtomicI64::new(message_batches as i64)), // in this test each consumer should receive constant number of messages
                warmup_time,
                self.args.sampling_time(),
                self.args.moving_average_window(),
                polling_kind,
                false, // TODO: Calculate latency from timestamp in first message, it should be an argument to iggy-bench
                self.args
                    .rate_limit()
                    .map(|rl| RateLimiter::new(rl.as_bytes_u64())),
            );
            let future = Box::pin(async move { consumer.run().await });
            futures.as_mut().unwrap().push(future);
        }
        info!(
            "Starting to send and poll {} messages",
            self.total_messages()
        );
        futures
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
