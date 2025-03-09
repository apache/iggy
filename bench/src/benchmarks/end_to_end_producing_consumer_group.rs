use crate::args::common::IggyBenchArgs;
use crate::benchmarks::benchmark::{BenchmarkFutures, Benchmarkable};
use crate::benchmarks::common::*;
use async_trait::async_trait;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tracing::info;

pub struct EndToEndProducingConsumerGroupBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl EndToEndProducingConsumerGroupBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Box<Self> {
        Box::new(Self {
            args,
            client_factory,
        })
    }
}

#[async_trait]
impl Benchmarkable for EndToEndProducingConsumerGroupBenchmark {
    async fn run(&self) -> BenchmarkFutures {
        self.init_streams().await?;
        let cf = self.client_factory.clone();
        let args = self.args.clone();

        init_consumer_groups(&cf, &args).await?;

        build_producing_consumers_futures(cf, args)
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
        let kind = self.kind();
        let producing_consumers = format!("producing_consumers: {}", self.args.producers());
        let partitions = format!("partitions: {}", self.args.number_of_partitions());
        let cg_count = format!("consumer groups: {}", self.args.number_of_consumer_groups());
        let streams = format!("streams: {}", self.args.streams());
        let common_params = self.common_params_str();

        info!(
            "Staring benchmark {kind}, {producing_consumers}, {partitions}, \
            {cg_count}, {streams}, {common_params}"
        );
    }
}
