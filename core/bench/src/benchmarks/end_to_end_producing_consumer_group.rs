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

use crate::args::common::IggyBenchArgs;
use crate::benchmarks::common::{build_producing_consumer_groups_futures, init_consumer_groups};
use async_trait::async_trait;
use bench_report::benchmark_kind::BenchmarkKind;
use bench_report::individual_metrics::BenchmarkIndividualMetrics;
use iggy::prelude::*;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::info;

use super::benchmark::Benchmarkable;

pub struct EndToEndProducingConsumerGroupBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl EndToEndProducingConsumerGroupBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Self {
        Self {
            args,
            client_factory,
        }
    }
}

#[async_trait]
impl Benchmarkable for EndToEndProducingConsumerGroupBenchmark {
    async fn run(
        &mut self,
    ) -> Result<JoinSet<Result<BenchmarkIndividualMetrics, IggyError>>, IggyError> {
        self.init_streams().await?;
        let cf = self.client_factory.clone();
        let args = self.args.clone();
        let mut tasks = JoinSet::new();

        init_consumer_groups(&cf, &args).await?;

        let futures = build_producing_consumer_groups_futures(cf, args);
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
