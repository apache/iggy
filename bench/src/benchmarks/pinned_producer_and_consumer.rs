use crate::args::common::IggyBenchArgs;
use crate::benchmarks::benchmark::{BenchmarkFutures, Benchmarkable};
use crate::benchmarks::common::*;
use async_trait::async_trait;
use iggy::utils::topic_size::MaxTopicSize;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tracing::info;

pub struct PinnedProducerAndConsumerBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl PinnedProducerAndConsumerBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Box<Self> {
        Box::new(Self {
            args,
            client_factory,
        })
    }
}

#[async_trait]
impl Benchmarkable for PinnedProducerAndConsumerBenchmark {
    async fn run(&self) -> BenchmarkFutures {
        self.init_streams().await?;
        let cf = &self.client_factory;
        let args = self.args.clone();
        let actors = self.args.producers() + self.args.consumers();

        let mut producers_fut = build_producer_futures(cf, &args)?;
        let mut consumers_fut = build_consumer_futures(cf, &args)?;

        let mut all_futures = Vec::with_capacity((actors) as usize);

        all_futures.append(&mut producers_fut);
        all_futures.append(&mut consumers_fut);

        Ok(all_futures)
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

    fn print_info(&self) {
        let streams = format!("streams: {}", self.args.streams());
        let partitions = format!("partitions: {}", self.args.number_of_partitions());
        let producers = format!("producers: {}", self.args.producers());
        let consumers = format!("consumers: {}", self.args.consumers());
        let max_topic_size = match self.args.max_topic_size() {
            Some(size) => format!(" max topic size: {}", size),
            None => format!(" max topic size: {}", MaxTopicSize::ServerDefault),
        };
        let common_params = self.common_params_str();

        info!("Staring benchmark PinnedProducerAndConsumer, {streams}, {partitions}, {producers}, {consumers}, {max_topic_size}, {common_params}");
    }
}
