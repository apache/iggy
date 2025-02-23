use crate::args::common::IggyBenchArgs;
use crate::benchmarks::benchmark::Benchmarkable;
use crate::benchmarks::common::*;
use async_trait::async_trait;
use iggy::error::IggyError;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use iggy_bench_report::individual_metrics::BenchmarkIndividualMetrics;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::info;

pub struct PinnedConsumerBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl PinnedConsumerBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Box<Self> {
        Box::new(Self {
            args,
            client_factory,
        })
    }
}

#[async_trait]
impl Benchmarkable for PinnedConsumerBenchmark {
    async fn run(
        &mut self,
    ) -> Result<JoinSet<Result<BenchmarkIndividualMetrics, IggyError>>, IggyError> {
        self.check_streams().await?;
        let cf = &self.client_factory;
        let args = self.args.clone();
        let mut tasks: JoinSet<_> = JoinSet::new();

        let futures = build_consumer_futures(cf, &args)?;
        for fut in futures {
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
        let consumers = format!("consumers: {}", self.args.consumers());
        let common_params = self.common_params_str();

        info!("Staring benchmark PinnedConsumer, {streams}, {consumers}, {common_params}");
    }
}
