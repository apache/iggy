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

use super::benchmark::Benchmarkable;
use crate::{
    args::common::IggyBenchArgs,
    benchmarks::common::{build_consumer_futures, init_consumer_groups},
    telemetry::{MetricsHandle, TelemetryContext},
};
use async_trait::async_trait;
use bench_report::{benchmark_kind::BenchmarkKind, individual_metrics::BenchmarkIndividualMetrics};
use iggy::prelude::*;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::info;

pub struct BalancedConsumerGroupBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
    telemetry_handles: Vec<MetricsHandle>,
}

impl BalancedConsumerGroupBenchmark {
    pub fn new(
        args: Arc<IggyBenchArgs>,
        client_factory: Arc<dyn ClientFactory>,
        telemetry: Option<&TelemetryContext>,
    ) -> Self {
        let telemetry_handles = telemetry.map_or_else(Vec::new, |ctx| {
            let consumers = args.consumers();
            let cg_count = args.number_of_consumer_groups();
            let start_stream_id = args.start_stream_id();

            (1..=consumers)
                .map(|consumer_id| {
                    let stream_id = if cg_count > 0 {
                        start_stream_id + 1 + (consumer_id % cg_count)
                    } else {
                        start_stream_id + consumer_id
                    };
                    ctx.create_handle("consumer", consumer_id, stream_id)
                })
                .collect()
        });

        Self {
            args,
            client_factory,
            telemetry_handles,
        }
    }
}

#[async_trait]
impl Benchmarkable for BalancedConsumerGroupBenchmark {
    async fn run(
        &mut self,
    ) -> Result<JoinSet<Result<BenchmarkIndividualMetrics, IggyError>>, IggyError> {
        self.check_streams().await?;
        let cf = &self.client_factory;
        let args = self.args.clone();
        let mut tasks: JoinSet<_> = JoinSet::new();

        init_consumer_groups(cf, &args).await?;

        let consumer_futures = build_consumer_futures(cf, &args, &self.telemetry_handles);
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
        let consumers = format!("consumers: {}", self.args.consumers());
        let cg_count = format!("consumer groups: {}", self.args.number_of_consumer_groups());
        let common_params = self.common_params_str();

        info!(
            "Staring benchmark BalancedConsumerGroup, {streams}, {consumers}, {cg_count}, {common_params}"
        );
    }
}
