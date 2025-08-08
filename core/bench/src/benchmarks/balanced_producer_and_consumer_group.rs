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
    benchmarks::common::{build_consumer_futures, build_producer_futures, init_consumer_groups},
    telemetry::{MetricsHandle, TelemetryContext},
};
use async_trait::async_trait;
use bench_report::{benchmark_kind::BenchmarkKind, individual_metrics::BenchmarkIndividualMetrics};
use iggy::prelude::*;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::info;

pub struct BalancedProducerAndConsumerGroupBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
    producer_telemetry_handles: Vec<MetricsHandle>,
    // TODO: Use this field when consumer telemetry is implemented
    #[allow(dead_code)]
    consumer_telemetry_handles: Vec<MetricsHandle>,
}

impl BalancedProducerAndConsumerGroupBenchmark {
    pub fn new(
        args: Arc<IggyBenchArgs>,
        client_factory: Arc<dyn ClientFactory>,
        telemetry: Option<&TelemetryContext>,
    ) -> Self {
        let (producer_telemetry_handles, consumer_telemetry_handles) = telemetry.map_or_else(
            || (Vec::new(), Vec::new()),
            |ctx| {
                let producers = args.producers();
                let consumers = args.consumers();
                let streams = args.streams();
                let cg_count = args.number_of_consumer_groups();
                let start_stream_id = args.start_stream_id();

                let producer_handles = (1..=producers)
                    .map(|producer_id| {
                        let stream_id = start_stream_id + 1 + (producer_id % streams);
                        ctx.create_handle("producer", producer_id, stream_id)
                    })
                    .collect();

                let consumer_handles = (1..=consumers)
                    .map(|consumer_id| {
                        let stream_id = if cg_count > 0 {
                            start_stream_id + 1 + (consumer_id % cg_count)
                        } else {
                            start_stream_id + consumer_id
                        };
                        ctx.create_handle("consumer", consumer_id, stream_id)
                    })
                    .collect();

                (producer_handles, consumer_handles)
            },
        );

        Self {
            args,
            client_factory,
            producer_telemetry_handles,
            consumer_telemetry_handles,
        }
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

        let producer_futures = build_producer_futures(cf, &args, &self.producer_telemetry_handles);
        let consumer_futures = build_consumer_futures(cf, &args, &self.consumer_telemetry_handles);

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
        let max_topic_size = self.args.max_topic_size().map_or_else(
            || format!(" max topic size: {}", MaxTopicSize::ServerDefault),
            |size| format!(" max topic size: {size}"),
        );
        let common_params = self.common_params_str();

        info!(
            "Staring benchmark BalancedProducerAndConsumerGroup, {streams}, {partitions}, {producers}, {consumers}, {cg_count}, {max_topic_size}, {common_params}"
        );
    }
}
