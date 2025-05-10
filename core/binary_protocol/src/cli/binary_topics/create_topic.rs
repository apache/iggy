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

use crate::Client;
use crate::cli::cli_command::{CliCommand, PRINT_TARGET};
use anyhow::Context;
use async_trait::async_trait;
use core::fmt;
use iggy_common::create_topic::CreateTopic;
use iggy_common::{CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize};
use tracing::{Level, event};

pub struct CreateTopicCmd {
    create_topic: CreateTopic,
    message_expiry: IggyExpiry,
    max_topic_size: MaxTopicSize,
    replication_factor: u8,
}

impl CreateTopicCmd {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream_id: Identifier,
        topic_id: Option<u32>,
        partitions_count: u32,
        compression_algorithm: CompressionAlgorithm,
        name: String,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
        replication_factor: u8,
    ) -> Self {
        Self {
            create_topic: CreateTopic {
                stream_id,
                topic_id,
                partitions_count,
                compression_algorithm,
                name,
                message_expiry,
                max_topic_size,
                replication_factor: Some(replication_factor),
            },
            message_expiry,
            max_topic_size,
            replication_factor,
        }
    }

    fn get_topic_id_info(&self) -> String {
        match self.create_topic.topic_id {
            Some(topic_id) => format!("ID: {}", topic_id),
            None => "ID auto incremented".to_string(),
        }
    }
}

#[async_trait]
impl CliCommand for CreateTopicCmd {
    fn explain(&self) -> String {
        format!("{}", self)
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .create_topic(&self.create_topic.stream_id, &self.create_topic.name, self.create_topic.partitions_count, self.create_topic.compression_algorithm, self.create_topic.replication_factor, self.create_topic.topic_id, self.create_topic.message_expiry, self.create_topic.max_topic_size)
            .await
            .with_context(|| {
                format!(
                    "Problem creating topic (ID: {}, name: {}, partitions count: {}) in stream with ID: {}",
                    self.create_topic.topic_id.unwrap_or(0), self.create_topic.name, self.create_topic.partitions_count, self.create_topic.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Topic with name: {}, {}, partitions count: {}, compression algorithm: {}, message expiry: {}, max topic size: {}, replication factor: {} created in stream with ID: {}",
            self.create_topic.name,
            self.get_topic_id_info(),
            self.create_topic.partitions_count,
            self.create_topic.compression_algorithm,
            self.message_expiry,
            self.max_topic_size,
            self.replication_factor,
            self.create_topic.stream_id,
        );

        Ok(())
    }
}

impl fmt::Display for CreateTopicCmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let topic_id = self.get_topic_id_info();
        let topic_name = &self.create_topic.name;
        let compression_algorithm = &self.create_topic.compression_algorithm;
        let message_expiry = &self.message_expiry;
        let max_topic_size = &self.max_topic_size;
        let replication_factor = self.replication_factor;
        let stream_id = &self.create_topic.stream_id;

        write!(
            f,
            "create topic with name: {topic_name}, {topic_id}, message expiry: {message_expiry}, compression algorithm: {compression_algorithm}, \
            max topic size: {max_topic_size}, replication factor: {replication_factor} in stream with ID: {stream_id}",
        )
    }
}
