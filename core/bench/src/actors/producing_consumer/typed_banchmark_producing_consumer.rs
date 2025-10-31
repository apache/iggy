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

use std::sync::Arc;

use crate::{
    actors::{
        consumer::client::{
            high_level::HighLevelConsumerClient, interface::BenchmarkConsumerConfig,
            low_level::LowLevelConsumerClient,
        },
        producer::client::{
            high_level::HighLevelProducerClient, interface::BenchmarkProducerConfig,
            low_level::LowLevelProducerClient,
        },
        producing_consumer::BenchmarkProducingConsumer,
    },
    utils::finish_condition::BenchmarkFinishCondition,
};
use bench_report::{
    benchmark_kind::BenchmarkKind, individual_metrics::BenchmarkIndividualMetrics,
    numeric_parameter::BenchmarkNumericParameter,
};

use iggy::prelude::*;
use integration::test_server::ClientFactory;

pub enum TypedBenchmarkProducingConsumer {
    High(BenchmarkProducingConsumer<HighLevelProducerClient, HighLevelConsumerClient>),
    Low(BenchmarkProducingConsumer<LowLevelProducerClient, LowLevelConsumerClient>),
}
impl TypedBenchmarkProducingConsumer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        use_high_level_api: bool,
        client_factory: Arc<dyn ClientFactory>,
        benchmark_kind: BenchmarkKind,
        actor_id: u32,
        consumer_group_id: Option<u32>,
        stream_id: String,
        partitions_count: u32,
        messages_per_batch: BenchmarkNumericParameter,
        message_size: BenchmarkNumericParameter,
        send_finish_condition: Arc<BenchmarkFinishCondition>,
        poll_finish_condition: Arc<BenchmarkFinishCondition>,
        warmup_time: IggyDuration,
        sampling_time: IggyDuration,
        moving_average_window: u32,
        limit_bytes_per_second: Option<IggyByteSize>,
        polling_kind: PollingKind,
        origin_timestamp_latency_calculation: bool,
    ) -> Self {
        let producer_config = BenchmarkProducerConfig {
            producer_id: actor_id,
            stream_id: stream_id.clone(),
            partitions: partitions_count,
            messages_per_batch,
            message_size,
            warmup_time,
        };

        let consumer_config = BenchmarkConsumerConfig {
            consumer_id: actor_id,
            consumer_group_id,
            stream_id,
            messages_per_batch,
            warmup_time,
            polling_kind,
            origin_timestamp_latency_calculation,
        };

        if use_high_level_api {
            let producer =
                HighLevelProducerClient::new(client_factory.clone(), producer_config.clone());
            let consumer = HighLevelConsumerClient::new(client_factory, consumer_config.clone());
            Self::High(BenchmarkProducingConsumer::new(
                producer,
                consumer,
                benchmark_kind,
                send_finish_condition,
                poll_finish_condition,
                sampling_time,
                moving_average_window,
                limit_bytes_per_second,
                producer_config,
                consumer_config,
            ))
        } else {
            let producer =
                LowLevelProducerClient::new(client_factory.clone(), producer_config.clone());
            let consumer = LowLevelConsumerClient::new(client_factory, consumer_config.clone());
            Self::Low(BenchmarkProducingConsumer::new(
                producer,
                consumer,
                benchmark_kind,
                send_finish_condition,
                poll_finish_condition,
                sampling_time,
                moving_average_window,
                limit_bytes_per_second,
                producer_config,
                consumer_config,
            ))
        }
    }

    pub async fn run(self) -> Result<BenchmarkIndividualMetrics, IggyError> {
        match self {
            Self::High(p) => p.run().await,
            Self::Low(p) => p.run().await,
        }
    }
}
