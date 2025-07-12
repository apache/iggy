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
        ApiLabel, BatchMetrics, BenchmarkInit,
        producer::client::{BenchmarkProducerClient, BenchmarkProducerConfig, ProducerClient},
    },
    utils::batch_generator::BenchmarkBatchGenerator,
};
use iggy::prelude::*;
use integration::test_server::{ClientFactory, login_root};
use tokio::time::Instant;

pub struct LowLevelProducerClient {
    client_factory: Arc<dyn ClientFactory>,
    config: BenchmarkProducerConfig,
    client: Option<IggyClient>,
    partitioning: Option<Partitioning>,
}

impl LowLevelProducerClient {
    pub fn new(client_factory: Arc<dyn ClientFactory>, config: BenchmarkProducerConfig) -> Self {
        Self {
            client_factory,
            config,
            client: None,
            partitioning: None,
        }
    }
}

#[async_trait::async_trait]
impl ProducerClient for LowLevelProducerClient {
    async fn produce_batch(
        &mut self,
        batch_generator: &mut BenchmarkBatchGenerator,
    ) -> Result<Option<BatchMetrics>, IggyError> {
        let client = self.client.as_mut().unwrap();
        let stream_id: Identifier = self.config.stream_id.try_into().unwrap();
        let topic_id: Identifier = 1.try_into().unwrap();
        let partitioning = self.partitioning.as_ref().unwrap();

        let batch = batch_generator.generate_batch();
        if batch.messages.is_empty() {
            return Ok(None);
        }

        let before_send = Instant::now();
        client
            .send_messages(&stream_id, &topic_id, partitioning, &mut batch.messages)
            .await?;
        let latency = before_send.elapsed();

        Ok(Some(BatchMetrics {
            messages: u32::try_from(batch.messages.len()).unwrap(),
            user_data_bytes: batch.user_data_bytes,
            total_bytes: batch.total_bytes,
            latency,
        }))
    }
}

#[async_trait::async_trait]
impl BenchmarkInit for LowLevelProducerClient {
    async fn setup(&mut self) -> Result<(), IggyError> {
        let default_partition_id = 1;
        let partitions = self.config.partitions;

        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;

        let partitioning = match partitions {
            0 => panic!("Partition count must be greater than 0"),
            1 => Partitioning::partition_id(default_partition_id),
            _ => Partitioning::balanced(),
        };

        self.client = Some(client);
        self.partitioning = Some(partitioning);
        Ok(())
    }
}
impl ApiLabel for LowLevelProducerClient {
    fn api_label(&self) -> &'static str {
        "low-level API"
    }
}
impl BenchmarkProducerClient for LowLevelProducerClient {}
