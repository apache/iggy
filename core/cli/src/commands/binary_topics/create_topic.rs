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

use crate::commands::cli_command::{CliCommand, PRINT_TARGET};
use anyhow::Context;
use async_trait::async_trait;
use core::fmt;
use iggy_common::{
    Client, CompressionAlgorithm, Durability, Identifier, IggyExpiry, MaxTopicSize,
    TopicCreateOptions,
};
use std::collections::BTreeMap;
use tracing::{Level, event};

pub struct CreateTopicCmd {
    stream_id: Identifier,
    name: String,
    partitions_count: u32,
    compression_algorithm: CompressionAlgorithm,
    message_expiry: IggyExpiry,
    max_topic_size: MaxTopicSize,
    options: TopicCreateOptions,
}

impl CreateTopicCmd {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream_id: Identifier,
        partitions_count: u32,
        compression_algorithm: CompressionAlgorithm,
        name: String,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
        raw_options: BTreeMap<String, String>,
        durability: Durability,
        consumer_offset_durability: Durability,
    ) -> Self {
        let options = TopicCreateOptions {
            partitions_count: Some(partitions_count),
            compression_algorithm: (compression_algorithm != CompressionAlgorithm::default())
                .then_some(compression_algorithm),
            message_expiry: (message_expiry != IggyExpiry::ServerDefault).then_some(message_expiry),
            max_topic_size: (max_topic_size != MaxTopicSize::ServerDefault)
                .then_some(max_topic_size),
            raw: raw_options,
            durability,
            consumer_offset_durability,
            ..TopicCreateOptions::default()
        };
        Self {
            stream_id,
            name,
            partitions_count,
            compression_algorithm,
            message_expiry,
            max_topic_size,
            options,
        }
    }
}

#[async_trait]
impl CliCommand for CreateTopicCmd {
    fn explain(&self) -> String {
        format!("{self}")
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .create_topic(&self.stream_id, &self.name, &self.options)
            .await
            .with_context(|| {
                format!(
                    "Problem creating topic (name: {}, partitions count: {}) in stream with ID: {}",
                    self.name, self.partitions_count, self.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Topic with name: {}, partitions count: {}, compression algorithm: {}, message expiry: {}, max topic size: {} created in stream with ID: {}",
            self.name,
            self.partitions_count,
            self.compression_algorithm,
            self.message_expiry,
            self.max_topic_size,
            self.stream_id,
        );

        Ok(())
    }
}

impl fmt::Display for CreateTopicCmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let topic_name = &self.name;
        let compression_algorithm = &self.compression_algorithm;
        let message_expiry = &self.message_expiry;
        let max_topic_size = &self.max_topic_size;
        let stream_id = &self.stream_id;

        write!(
            f,
            "create topic with name: {topic_name}, message expiry: {message_expiry}, \
            compression algorithm: {compression_algorithm}, max topic size: {max_topic_size} \
            in stream with ID: {stream_id}",
        )
    }
}
