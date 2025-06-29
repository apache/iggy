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
use crate::actors::{HighLevelApiMarker, LowLevelApiMarker};
use crate::utils::batch_generator::BenchmarkBatchGenerator;
use bench_report::numeric_parameter::BenchmarkNumericParameter;
use iggy::prelude::*;
use integration::test_server::ClientFactory;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct ProducerBackendImpl<T> {
    pub client_factory: Arc<dyn ClientFactory>,
    pub config: BenchmarkProducerConfig,
    _phantom: PhantomData<T>,
}

impl<T> ProducerBackendImpl<T> {
    pub fn new(client_factory: Arc<dyn ClientFactory>, config: BenchmarkProducerConfig) -> Self {
        Self {
            client_factory,
            config,
            _phantom: PhantomData,
        }
    }
}

pub type LowLevelBackend = ProducerBackendImpl<LowLevelApiMarker>;
pub type HighLevelBackend = ProducerBackendImpl<HighLevelApiMarker>;

pub enum ProducerBackend {
    LowLevel(LowLevelBackend),
    HighLevel(HighLevelBackend),
}

#[derive(Debug, Clone)]
pub struct ProducedBatch {
    pub messages: u32,
    pub user_data_bytes: u64,
    pub total_bytes: u64,
    pub latency: Duration,
}

#[derive(Debug, Clone)]
pub struct BenchmarkProducerConfig {
    pub producer_id: u32,
    pub stream_id: u32,
    pub partitions: u32,
    pub messages_per_batch: BenchmarkNumericParameter,
    pub message_size: BenchmarkNumericParameter,
    pub warmup_time: IggyDuration,
}

pub trait BenchmarkProducerBackend {
    type Producer;

    async fn setup(&self) -> Result<Self::Producer, IggyError>;

    async fn warmup(
        &self,
        producer: &mut Self::Producer,
        batch_generator: &mut BenchmarkBatchGenerator,
    ) -> Result<(), IggyError>;

    async fn produce_batch(
        &self,
        producer: &mut Self::Producer,
        batch_generator: &mut BenchmarkBatchGenerator,
    ) -> Result<Option<ProducedBatch>, IggyError>;

    fn log_setup_info(&self);
    fn log_warmup_info(&self);
}
