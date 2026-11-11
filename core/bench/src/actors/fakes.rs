// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::future::{pending, ready};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use iggy::prelude::IggyError;

use crate::actors::consumer::client::BenchmarkConsumerClient;
use crate::actors::consumer::client::interface::ConsumerClient;
use crate::actors::producer::client::BenchmarkProducerClient;
use crate::actors::producer::client::interface::ProducerClient;
use crate::actors::{ApiLabel, BatchMetrics, BenchmarkInit};
use crate::utils::batch_generator::BenchmarkBatchGenerator;

const BATCH_BYTES: u64 = 32;

pub struct FakeConsumer {
    pub resets: Arc<AtomicUsize>,
    // Stands for a group member with drained partitions, whose poll never returns.
    pub blocked: bool,
}

pub struct FakeProducer;

impl ConsumerClient for FakeConsumer {
    async fn consume_batch(&mut self) -> Result<Option<BatchMetrics>, IggyError> {
        if self.blocked {
            return pending().await;
        }
        Ok(Some(batch()))
    }

    fn reset_offsets(&mut self) -> impl Future<Output = Result<(), IggyError>> {
        self.resets.fetch_add(1, Ordering::Relaxed);
        ready(Ok(()))
    }
}

impl BenchmarkInit for FakeConsumer {
    fn setup(&mut self) -> impl Future<Output = Result<(), IggyError>> {
        ready(Ok(()))
    }
}

impl ApiLabel for FakeConsumer {
    const API_LABEL: &'static str = "fake";
}

impl BenchmarkConsumerClient for FakeConsumer {}

impl ProducerClient for FakeProducer {
    fn produce_batch(
        &mut self,
        _batch_generator: &mut BenchmarkBatchGenerator,
    ) -> impl Future<Output = Result<Option<BatchMetrics>, IggyError>> {
        ready(Ok(Some(batch())))
    }
}

impl BenchmarkInit for FakeProducer {
    fn setup(&mut self) -> impl Future<Output = Result<(), IggyError>> {
        ready(Ok(()))
    }
}

impl ApiLabel for FakeProducer {
    const API_LABEL: &'static str = "fake";
}

impl BenchmarkProducerClient for FakeProducer {}

const fn batch() -> BatchMetrics {
    BatchMetrics {
        messages: 1,
        user_data_bytes: BATCH_BYTES,
        total_bytes: BATCH_BYTES,
        latency: Duration::ZERO,
    }
}
