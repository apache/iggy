use super::benchmark::Benchmarkable;
use crate::{args::common::IggyBenchArgs, benchmarks::common::*};
use async_trait::async_trait;
use iggy::prelude::*;
use iggy_bench_report::{
    benchmark_kind::BenchmarkKind, individual_metrics::BenchmarkIndividualMetrics,
};
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tokio::task::JoinSet;
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
    async fn run(
        &mut self,
    ) -> Result<JoinSet<Result<BenchmarkIndividualMetrics, IggyError>>, IggyError> {
        self.init_streams().await?;
        let cf = &self.client_factory;
        let args = self.args.clone();
        let mut tasks: JoinSet<_> = JoinSet::new();

        init_consumer_groups(cf, &args).await?;

        let producer_futures = build_producer_futures(cf, &args)?;
        let consumer_futures = build_consumer_futures(cf, &args)?;

        for fut in producer_futures {
            tasks.spawn(fut);
        }

        for fut in consumer_futures {
            tasks.spawn(fut);
        }

        Ok(tasks)
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
