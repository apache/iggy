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

use crate::{
    actors::producer::{
        BenchmarkProducer,
        client::{
            high_level::HighLevelProducerClient, interface::BenchmarkProducerConfig,
            low_level::LowLevelProducerClient,
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
use std::sync::Arc;

pub enum TypedBenchmarkProducer {
    High(BenchmarkProducer<HighLevelProducerClient>),
    Low(BenchmarkProducer<LowLevelProducerClient>),
}

impl TypedBenchmarkProducer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        use_high_level_api: bool,
        client_factory: Arc<dyn ClientFactory>,
        benchmark_kind: BenchmarkKind,
        producer_id: u32,
        stream_id: String,
        partitions: u32,
        messages_per_batch: BenchmarkNumericParameter,
        message_size: BenchmarkNumericParameter,
        finish_condition: Arc<BenchmarkFinishCondition>,
        warmup_time: IggyDuration,
        sampling_time: IggyDuration,
        moving_average_window: u32,
        limit_bytes_per_second: Option<IggyByteSize>,
    ) -> Self {
        let config = BenchmarkProducerConfig {
            producer_id,
            stream_id,
            partitions,
            messages_per_batch,
            message_size,
            warmup_time,
        };

        if use_high_level_api {
            let client = HighLevelProducerClient::new(client_factory, config.clone());
            Self::High(BenchmarkProducer::new(
                client,
                benchmark_kind,
                finish_condition,
                sampling_time,
                moving_average_window,
                limit_bytes_per_second,
                config,
            ))
        } else {
            let client = LowLevelProducerClient::new(client_factory, config.clone());
            Self::Low(BenchmarkProducer::new(
                client,
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
            Self::High(producer) => producer.run().await,
            Self::Low(producer) => producer.run().await,
        }
    }
}
