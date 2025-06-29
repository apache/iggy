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
    actors::producer::backend::{BenchmarkProducerBackend, LowLevelBackend, ProducedBatch},
    utils::batch_generator::BenchmarkBatchGenerator,
};
use iggy::prelude::*;
use integration::test_server::login_root;
use tokio::time::Instant;
use tracing::info;

impl BenchmarkProducerBackend for LowLevelBackend {
    type Producer = (IggyClient, Identifier, Identifier, Partitioning);

    async fn setup(&self) -> Result<Self::Producer, iggy::prelude::IggyError> {
        let topic_id: u32 = 1;
        let default_partition_id: u32 = 1;
        let partitions = self.config.partitions;
        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;

        let stream_id = self.config.stream_id.try_into().unwrap();
        let topic_id = topic_id.try_into().unwrap();
        let partitioning = match partitions {
            0 => panic!("Partition count must be greater than 0"),
            1 => Partitioning::partition_id(default_partition_id),
            2.. => Partitioning::balanced(),
        };
        Ok((client, stream_id, topic_id, partitioning))
    }

    async fn warmup(
        &self,
        producer: &mut Self::Producer,
        batch_generator: &mut BenchmarkBatchGenerator,
    ) -> Result<(), iggy::prelude::IggyError> {
        let (client, stream_id, topic_id, partitioning) = producer;

        let warmup_end = Instant::now() + self.config.warmup_time.get_duration();

        while Instant::now() < warmup_end {
            let batch = batch_generator.generate_batch();
            client
                .send_messages(stream_id, topic_id, partitioning, &mut batch.messages)
                .await?;
        }
        Ok(())
    }

    async fn produce_batch(
        &self,
        producer: &mut Self::Producer,
        batch_generator: &mut BenchmarkBatchGenerator,
    ) -> Result<Option<super::backend::ProducedBatch>, iggy::prelude::IggyError> {
        let (client, stream_id, topic_id, partitioning) = producer;

        let batch = batch_generator.generate_batch();
        if batch.messages.is_empty() {
            return Ok(None);
        }
        let message_count = u32::try_from(batch.messages.len()).unwrap();
        let user_data_bytes = batch.user_data_bytes;
        let total_bytes = batch.total_bytes;

        let before_send = Instant::now();
        client
            .send_messages(stream_id, topic_id, partitioning, &mut batch.messages)
            .await?;

        let latency = before_send.elapsed();

        Ok(Some(ProducedBatch {
            messages: message_count,
            user_data_bytes,
            total_bytes,
            latency,
        }))
    }

    fn log_setup_info(&self) {
        info!(
            "Producer #{} → sending {} messages per batch (each {} bytes) to stream {} across {} partition(s), using low-level API...",
            self.config.producer_id,
            self.config.messages_per_batch,
            self.config.message_size,
            self.config.stream_id,
            self.config.partitions,
        );
    }

    fn log_warmup_info(&self) {
        info!(
            "Producer #{} → warming up for {}...",
            self.config.producer_id, self.config.warmup_time,
        );
    }
}
