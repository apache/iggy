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

use bench_report::numeric_parameter::BenchmarkNumericParameter;
use iggy::prelude::*;
use integration::test_server::ClientFactory;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct ConsumerBackendImpl<T> {
    pub client_factory: Arc<dyn ClientFactory>,
    pub config: BenchmarkConsumerConfig,
    _phantom: PhantomData<T>,
}

impl<T> ConsumerBackendImpl<T> {
    pub fn new(client_factory: Arc<dyn ClientFactory>, config: BenchmarkConsumerConfig) -> Self {
        Self {
            client_factory,
            config,
            _phantom: PhantomData,
        }
    }
}

pub struct LowLevelApiMarker;
pub struct HighLevelApiMarker;

pub type LowLevelBackend = ConsumerBackendImpl<LowLevelApiMarker>;
pub type HighLevelBackend = ConsumerBackendImpl<HighLevelApiMarker>;

pub enum ConsumerBackend {
    LowLevel(LowLevelBackend),
    HighLevel(HighLevelBackend),
}

#[derive(Debug, Clone)]
pub struct ConsumedBatch {
    pub messages: u32,
    pub user_data_bytes: u64,
    pub total_bytes: u64,
    pub latency: Duration,
}

#[derive(Debug, Clone)]
pub struct BenchmarkConsumerConfig {
    pub consumer_id: u32,
    pub consumer_group_id: Option<u32>,
    pub stream_id: u32,
    pub messages_per_batch: BenchmarkNumericParameter,
    pub warmup_time: IggyDuration,
    pub polling_kind: PollingKind,
    pub origin_timestamp_latency_calculation: bool,
}

pub trait BenchmarkConsumerBackend {
    type Consumer;

    async fn setup(&self) -> Result<Self::Consumer, IggyError>;
    async fn warmup(&self, consumer: &mut Self::Consumer) -> Result<(), IggyError>;
    async fn consume_batch(
        &self,
        consumer: &mut Self::Consumer,
    ) -> Result<Option<ConsumedBatch>, IggyError>;
    fn log_setup_info(&self);
    fn log_warmup_info(&self);
}
