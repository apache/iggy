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
use iggy_common::update_topic::UpdateTopic;
use iggy_common::{CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize};
use tracing::{Level, event};

pub struct UpdateTopicCmd {
    update_topic: UpdateTopic,
    message_expiry: IggyExpiry,
    max_topic_size: MaxTopicSize,
    replication_factor: u8,
}

impl UpdateTopicCmd {
    pub fn new(
        stream_id: Identifier,
        topic_id: Identifier,
        compression_algorithm: CompressionAlgorithm,
        name: String,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
        replication_factor: u8,
    ) -> Self {
        Self {
            update_topic: UpdateTopic {
                stream_id,
                topic_id,
                name,
                compression_algorithm,
                message_expiry,
                max_topic_size,
                replication_factor: Some(replication_factor),
            },
            message_expiry,
            max_topic_size,
            replication_factor,
        }
    }
}

#[async_trait]
impl CliCommand for UpdateTopicCmd {
    fn explain(&self) -> String {
        format!("{self}")
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .update_topic(&self.update_topic.stream_id, &self.update_topic.topic_id, &self.update_topic.name, self.update_topic.compression_algorithm, self.replication_factor.into(), self.message_expiry, self.max_topic_size)
            .await
            .with_context(|| {
                format!(
                    "Problem updating topic (ID: {}, name: {}, message expiry: {}) in stream with ID: {}",
                    self.update_topic.topic_id,
                    self.update_topic.name,
                    self.message_expiry,
                    self.update_topic.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Topic with ID: {} updated name: {}, updated message expiry: {}, updated compression algorithm: {}, updated max topic size: {}, updated replication factor: {} in stream with ID: {}",
            self.update_topic.topic_id,
            self.update_topic.name,
            self.message_expiry,
            self.update_topic.compression_algorithm,
            self.max_topic_size,
            self.replication_factor,
            self.update_topic.stream_id,
        );

        Ok(())
    }
}

impl fmt::Display for UpdateTopicCmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let topic_id = &self.update_topic.topic_id;
        let topic_name = &self.update_topic.name;
        let compression_algorithm = &self.update_topic.compression_algorithm;
        let message_expiry = &self.message_expiry;
        let max_topic_size = &self.max_topic_size;
        let replication_factor = self.replication_factor;
        let stream_id = &self.update_topic.stream_id;

        write!(
            f,
            "update topic with ID: {topic_id}, name: {topic_name}, message expiry: \
            {message_expiry}, compression algorithm: {compression_algorithm}, max topic size: {max_topic_size}, replication \
            factor: {replication_factor}, in stream with ID: {stream_id}",
        )
    }
}
