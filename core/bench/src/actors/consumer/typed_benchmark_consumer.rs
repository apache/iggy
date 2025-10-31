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
    actors::consumer::{
        BenchmarkConsumer,
        client::{
            high_level::HighLevelConsumerClient, interface::BenchmarkConsumerConfig,
            low_level::LowLevelConsumerClient,
        },
    },
    utils::finish_condition::BenchmarkFinishCondition,
};
use bench_report::{
    benchmark_kind::BenchmarkKind, individual_metrics::BenchmarkIndividualMetrics,
    numeric_parameter::BenchmarkNumericParameter,
};
use iggy::prelude::*;
use integration::test_server::ClientFactory;

pub enum TypedBenchmarkConsumer {
    High(BenchmarkConsumer<HighLevelConsumerClient>),
    Low(BenchmarkConsumer<LowLevelConsumerClient>),
}

impl TypedBenchmarkConsumer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        use_high_level_api: bool,
        client_factory: Arc<dyn ClientFactory>,
        benchmark_kind: BenchmarkKind,
        consumer_id: u32,
        consumer_group_id: Option<u32>,
        stream_id: String,
        messages_per_batch: BenchmarkNumericParameter,
        finish_condition: Arc<BenchmarkFinishCondition>,
        warmup_time: IggyDuration,
        sampling_time: IggyDuration,
        moving_average_window: u32,
        polling_kind: PollingKind,
        limit_bytes_per_second: Option<IggyByteSize>,
        origin_timestamp_latency_calculation: bool,
    ) -> Self {
        let config = BenchmarkConsumerConfig {
            consumer_id,
            consumer_group_id,
            stream_id,
            messages_per_batch,
            warmup_time,
            polling_kind,
            origin_timestamp_latency_calculation,
        };

        if use_high_level_api {
            Self::High(BenchmarkConsumer::new(
                HighLevelConsumerClient::new(client_factory, config.clone()),
                benchmark_kind,
                finish_condition,
                sampling_time,
                moving_average_window,
                limit_bytes_per_second,
                config,
            ))
        } else {
            Self::Low(BenchmarkConsumer::new(
                LowLevelConsumerClient::new(client_factory, config.clone()),
                benchmark_kind,
                finish_condition,
                sampling_time,
                moving_average_window,
                limit_bytes_per_second,
                config,
            ))
        }
    }

    pub async fn run(self) -> Result<BenchmarkIndividualMetrics, IggyError> {
        match self {
            Self::High(c) => c.run().await,
            Self::Low(c) => c.run().await,
        }
    }
}
