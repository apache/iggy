use super::benchmark::{BenchmarkFutures, Benchmarkable};
use crate::{args::common::IggyBenchArgs, benchmarks::common::*};
use async_trait::async_trait;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tracing::info;

pub struct BalancedConsumerGroupBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl BalancedConsumerGroupBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Box<Self> {
        Box::new(Self {
            args,
            client_factory,
        })
    }
}

#[async_trait]
impl Benchmarkable for BalancedConsumerGroupBenchmark {
    async fn run(&self) -> BenchmarkFutures {
        self.check_streams().await?;
        let cf = &self.client_factory;
        let args = self.args.clone();

        init_consumer_groups(cf, &args).await?;

        build_consumer_futures(cf, &args)
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
        let consumers = format!("consumers: {}", self.args.consumers());
        let cg_count = format!("consumer groups: {}", self.args.number_of_consumer_groups());
        let common_params = self.common_params_str();

        info!("Staring benchmark BalancedConsumerGroup, {streams}, {consumers}, {cg_count}, {common_params}");
    }
}
