use super::benchmark::{BenchmarkFutures, Benchmarkable};
use crate::{args::common::IggyBenchArgs, benchmarks::common::*};
use async_trait::async_trait;
use iggy::utils::topic_size::MaxTopicSize;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tracing::info;

pub struct BalancedProducerAndConsumerGroupBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl BalancedProducerAndConsumerGroupBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Box<Self> {
        Box::new(Self {
            args,
            client_factory,
        })
    }
}

#[async_trait]
impl Benchmarkable for BalancedProducerAndConsumerGroupBenchmark {
    async fn run(&self) -> BenchmarkFutures {
        self.init_streams().await?;
        let cf = &self.client_factory;
        let args = self.args.clone();
        let actors = args.producers() + args.consumers();

        init_consumer_groups(cf, &args).await?;

        let mut all_futures = Vec::with_capacity(actors as usize);
        let mut producer_futures = build_producer_futures(cf, &args)?;
        all_futures.append(&mut producer_futures);
        let mut consumer_futures = build_consumer_futures(cf, &args)?;
        all_futures.append(&mut consumer_futures);

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
        let cg_count = format!("consumer groups: {}", self.args.number_of_consumer_groups());
        let max_topic_size = match self.args.max_topic_size() {
            Some(size) => format!(" max topic size: {}", size),
            None => format!(" max topic size: {}", MaxTopicSize::ServerDefault),
        };
        let common_params = self.common_params_str();

        info!("Staring benchmark BalancedProducerAndConsumerGroup, {streams}, {partitions}, {producers}, {consumers}, {cg_count}, {max_topic_size}, {common_params}");
    }
}
