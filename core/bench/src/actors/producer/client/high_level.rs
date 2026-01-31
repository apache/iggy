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

use crate::utils::{ClientFactory, login_root};
use crate::{
    actors::{
        ApiLabel, BatchMetrics, BenchmarkInit,
        producer::client::{
            BenchmarkProducerClient,
            interface::{BenchmarkProducerConfig, ProducerClient},
        },
    },
    utils::batch_generator::BenchmarkBatchGenerator,
};
use iggy::prelude::*;
use std::sync::Arc;
use tokio::time::Instant;

pub struct HighLevelProducerClient {
    client_factory: Arc<dyn ClientFactory>,
    config: BenchmarkProducerConfig,
    producer: Option<IggyProducer>,
}

impl HighLevelProducerClient {
    pub fn new(client_factory: Arc<dyn ClientFactory>, config: BenchmarkProducerConfig) -> Self {
        Self {
            client_factory,
            config,
            producer: None,
        }
    }
}

impl ProducerClient for HighLevelProducerClient {
    async fn produce_batch(
        &mut self,
        batch_generator: &mut BenchmarkBatchGenerator,
    ) -> Result<Option<BatchMetrics>, IggyError> {
        let batch = batch_generator.generate_owned_batch();
        if batch.messages.is_empty() {
            return Ok(None);
        }
        let message_count = u32::try_from(batch.messages.len()).unwrap();
        let user_data_bytes = batch.user_data_bytes;
        let total_bytes = batch.total_bytes;

        let before_send = Instant::now();
        self.producer
            .as_mut()
            .expect("Producer not initialized")
            .send(batch.messages)
            .await?;
        let latency = before_send.elapsed();

        Ok(Some(BatchMetrics {
            messages: message_count,
            user_data_bytes,
            total_bytes,
            latency,
        }))
    }
}

impl BenchmarkInit for HighLevelProducerClient {
    async fn setup(&mut self) -> Result<(), IggyError> {
        let topic_id_str = "topic-1";
        let default_partition_id = 0u32;

        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;

        let stream_id_str = self.config.stream_id.clone();

        let partitioning = match self.config.partitions {
            0 => panic!("Partition count must be greater than 0"),
            1 => Partitioning::partition_id(default_partition_id),
            _ => Partitioning::balanced(),
        };

        self.producer = Some(
            client
                .producer(&stream_id_str, topic_id_str)?
                .partitioning(partitioning)
                .create_stream_if_not_exists()
                .create_topic_if_not_exists(
                    self.config.partitions,
                    Some(1),
                    IggyExpiry::NeverExpire,
                    MaxTopicSize::ServerDefault,
                )
                .build(),
        );
        self.producer.as_mut().unwrap().init().await?;
        Ok(())
    }
}
impl ApiLabel for HighLevelProducerClient {
    const API_LABEL: &'static str = "high-level";
}
impl BenchmarkProducerClient for HighLevelProducerClient {}
