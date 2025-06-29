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

use std::{marker::PhantomData, sync::Arc};

use crate::actors::consumer::backend::ConsumedBatch;
use crate::actors::producer::backend::ProducedBatch;
use crate::actors::{HighLevelApiMarker, LowLevelApiMarker};
use crate::utils::batch_generator::BenchmarkBatchGenerator;
use bench_report::numeric_parameter::BenchmarkNumericParameter;
use iggy::prelude::*;
use integration::test_server::ClientFactory;

#[derive(Clone)]
pub struct ProducingConsumerBackendImpl<T> {
    pub client_factory: Arc<dyn ClientFactory>,
    pub config: BenchmarkProducingConsumerConfig,
    _phantom: PhantomData<T>,
}

impl<T> ProducingConsumerBackendImpl<T> {
    pub fn new(
        client_factory: Arc<dyn ClientFactory>,
        config: BenchmarkProducingConsumerConfig,
    ) -> Self {
        Self {
            client_factory,
            config,
            _phantom: PhantomData,
        }
    }
}
#[derive(Debug, Clone)]
pub struct BenchmarkProducingConsumerConfig {
    pub actor_id: u32,
    pub consumer_group_id: Option<u32>,
    pub stream_id: u32,
    pub partitions_count: u32,
    pub messages_per_batch: BenchmarkNumericParameter,
    pub message_size: BenchmarkNumericParameter,
    pub warmup_time: IggyDuration,
    pub polling_kind: PollingKind,
    pub origin_timestamp_latency_calculation: bool,
}

pub type LowLevelBackend = ProducingConsumerBackendImpl<LowLevelApiMarker>;
pub type HighLevelBackend = ProducingConsumerBackendImpl<HighLevelApiMarker>;

pub enum ProducingConsumerBackend {
    LowLevel(LowLevelBackend),
    HighLevel(HighLevelBackend),
}

pub trait BenchmarkProducingConsumerBackend {
    type MessagingContext;
    async fn setup(&self) -> Result<Self::MessagingContext, IggyError>;

    async fn warmup(
        &self,
        context: &mut Self::MessagingContext,
        batch_generator: &mut BenchmarkBatchGenerator,
    ) -> Result<(), IggyError>;

    async fn produce_batch(
        &self,
        context: &mut Self::MessagingContext,
        batch_generator: &mut BenchmarkBatchGenerator,
    ) -> Result<Option<ProducedBatch>, IggyError>;

    async fn consume_batch(
        &self,
        context: &mut Self::MessagingContext,
    ) -> Result<Option<ConsumedBatch>, IggyError>;

    fn log_setup_info(&self);
    fn log_warmup_info(&self);
}
