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
    actors::{ApiLabel, BatchMetrics, BenchmarkInit},
    utils::batch_generator::BenchmarkBatchGenerator,
};
use bench_report::numeric_parameter::BenchmarkNumericParameter;
use iggy::prelude::*;

#[derive(Debug, Clone)]
pub struct BenchmarkProducerConfig {
    pub producer_id: u32,
    pub stream_id: String,
    pub partitions: u32,
    pub messages_per_batch: BenchmarkNumericParameter,
    pub message_size: BenchmarkNumericParameter,
    pub warmup_time: IggyDuration,
}

pub trait ProducerClient: Send + Sync {
    async fn produce_batch(
        &mut self,
        batch_generator: &mut BenchmarkBatchGenerator,
    ) -> Result<Option<BatchMetrics>, IggyError>;
}

pub trait BenchmarkProducerClient: ProducerClient + BenchmarkInit + ApiLabel + Send + Sync {}
