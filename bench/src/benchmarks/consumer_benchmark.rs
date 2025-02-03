use crate::actors::consumer::Consumer;
use crate::args::common::IggyBenchArgs;
use crate::benchmarks::benchmark::{BenchmarkFutures, Benchmarkable};
use async_trait::async_trait;
use iggy::messages::poll_messages::PollingKind;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use integration::test_server::ClientFactory;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
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
    async fn run(&mut self) -> BenchmarkFutures {
        self.check_streams().await?;
        let consumers_count = self.args.consumers();
        info!("Creating {} consumer(s)...", consumers_count);
        let messages_per_batch = self.args.messages_per_batch();
        let message_batches = self.args.message_batches();

        let mut futures: BenchmarkFutures = Ok(Vec::with_capacity(consumers_count as usize));
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
            );

            let future = Box::pin(async move { consumer.run().await });
            futures.as_mut().unwrap().push(future);
        }
        info!("Created {} consumer(s).", consumers_count);
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
