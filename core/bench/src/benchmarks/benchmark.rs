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

use crate::args::kind::BenchmarkKindCommand;
use crate::{args::common::IggyBenchArgs, utils::client_factory::create_client_factory};
use async_trait::async_trait;
use bench_report::benchmark_kind::BenchmarkKind;
use bench_report::individual_metrics::BenchmarkIndividualMetrics;
use iggy::clients::client::IggyClient;
use iggy::prelude::*;
use integration::test_server::{ClientFactory, login_root};
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::info;

use super::balanced_consumer_group::BalancedConsumerGroupBenchmark;
use super::balanced_producer::BalancedProducerBenchmark;
use super::balanced_producer_and_consumer_group::BalancedProducerAndConsumerGroupBenchmark;
use super::end_to_end_producing_consumer::EndToEndProducingConsumerBenchmark;
use super::end_to_end_producing_consumer_group::EndToEndProducingConsumerGroupBenchmark;
use super::pinned_consumer::PinnedConsumerBenchmark;
use super::pinned_producer::PinnedProducerBenchmark;
use super::pinned_producer_and_consumer::PinnedProducerAndConsumerBenchmark;

impl From<IggyBenchArgs> for Box<dyn Benchmarkable> {
    fn from(args: IggyBenchArgs) -> Self {
        let client_factory = create_client_factory(&args);

        match args.benchmark_kind {
            BenchmarkKindCommand::PinnedProducer(_) => {
                Box::new(PinnedProducerBenchmark::new(Arc::new(args), client_factory))
            }

            BenchmarkKindCommand::PinnedConsumer(_) => {
                Box::new(PinnedConsumerBenchmark::new(Arc::new(args), client_factory))
            }

            BenchmarkKindCommand::PinnedProducerAndConsumer(_) => Box::new(
                PinnedProducerAndConsumerBenchmark::new(Arc::new(args), client_factory),
            ),

            BenchmarkKindCommand::BalancedProducer(_) => Box::new(BalancedProducerBenchmark::new(
                Arc::new(args),
                client_factory,
            )),

            BenchmarkKindCommand::BalancedConsumerGroup(_) => Box::new(
                BalancedConsumerGroupBenchmark::new(Arc::new(args), client_factory),
            ),

            BenchmarkKindCommand::BalancedProducerAndConsumerGroup(_) => Box::new(
                BalancedProducerAndConsumerGroupBenchmark::new(Arc::new(args), client_factory),
            ),

            BenchmarkKindCommand::EndToEndProducingConsumer(_) => Box::new(
                EndToEndProducingConsumerBenchmark::new(Arc::new(args), client_factory),
            ),

            BenchmarkKindCommand::EndToEndProducingConsumerGroup(_) => Box::new(
                EndToEndProducingConsumerGroupBenchmark::new(Arc::new(args), client_factory),
            ),
            BenchmarkKindCommand::Examples => {
                unreachable!("Examples should be handled before this point")
            }
        }
    }
}

#[async_trait]
pub trait Benchmarkable: Send {
    async fn run(
        &mut self,
    ) -> Result<JoinSet<Result<BenchmarkIndividualMetrics, IggyError>>, IggyError>;
    fn kind(&self) -> BenchmarkKind;
    fn args(&self) -> &IggyBenchArgs;
    fn client_factory(&self) -> &Arc<dyn ClientFactory>;
    fn print_info(&self);

    /// Below methods have common implementation for all benchmarks.
    /// Initializes the streams and topics for the benchmark.
    /// This method is called before the benchmark is executed.
    async fn init_streams(&self) -> Result<(), IggyError> {
        let number_of_streams = self.args().streams();
        let partitions_count: u32 = self.args().number_of_partitions();
        let client = self.client_factory().create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;
        let streams = client.get_streams().await?;
        for i in 1..=number_of_streams {
            let stream_name = format!("bench-stream-{i}");
            let stream_id: Identifier = stream_name.as_str().try_into()?;
            if streams.iter().all(|s| s.name != stream_name) {
                info!("Creating the test stream '{}'", stream_name);
                client.create_stream(&stream_name).await?;
                let topic_name = "topic-1".to_string();
                let max_topic_size = self
                    .args()
                    .max_topic_size()
                    .map_or(MaxTopicSize::Unlimited, MaxTopicSize::Custom);

                info!(
                    "Creating the test topic '{}' for stream '{}' with max topic size: {:?}",
                    topic_name, stream_name, max_topic_size
                );

                client
                    .create_topic(
                        &stream_id,
                        &topic_name,
                        partitions_count,
                        CompressionAlgorithm::default(),
                        None,
                        IggyExpiry::NeverExpire,
                        max_topic_size,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn check_streams(&self) -> Result<(), IggyError> {
        let number_of_streams = self.args().streams();
        let client = self.client_factory().create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;
        let streams = client.get_streams().await?;
        for i in 1..=number_of_streams {
            let stream_name = format!("bench-stream-{i}");
            if streams.iter().all(|s| s.name != stream_name) {
                return Err(IggyError::ResourceNotFound(format!(
                    "Streams for testing are not properly initialized. Stream '{stream_name}' is missing."
                )));
            }
        }
        Ok(())
    }

    fn common_params_str(&self) -> String {
        let message_size = format!("message size: {} b,", self.args().message_size());
        let messages_per_batch = format!(
            " messages per batch: {} b,",
            self.args().messages_per_batch()
        );
        let data = self.args().total_data().map_or_else(
            || {
                format!(
                    " total batches to send: {},",
                    self.args().message_batches().unwrap()
                )
            },
            |data| format!(" total data to send: {data},"),
        );
        let rate_limit = self
            .args()
            .rate_limit()
            .map(|rl| format!(" global rate limit: {rl}/s"))
            .unwrap_or_default();

        format!("{message_size}{messages_per_batch}{data}{rate_limit}",)
    }
}
