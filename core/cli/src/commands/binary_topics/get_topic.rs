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
use comfy_table::{Table, presets::ASCII_NO_BORDERS};
use iggy_common::Client;
use iggy_common::Identifier;
use iggy_common::IggyExpiry;
use tracing::{Level, event};

pub struct GetTopicCmd {
    stream_id: Identifier,
    topic_id: Identifier,
}

impl GetTopicCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier) -> Self {
        Self {
            stream_id,
            topic_id,
        }
    }
}

#[async_trait]
impl CliCommand for GetTopicCmd {
    fn explain(&self) -> String {
        format!(
            "get topic with ID: {} from stream with ID: {}",
            self.topic_id, self.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let topic = client
            .get_topic(&self.stream_id, &self.topic_id)
            .await
            .with_context(|| {
                format!(
                    "Problem getting topic with ID: {} in stream {}",
                    self.topic_id, self.stream_id
                )
            })?;

        if topic.is_none() {
            event!(target: PRINT_TARGET, Level::INFO, "Topic with ID: {} in stream {} was not found", self.topic_id, self.stream_id);
            return Ok(());
        }

        let topic = topic.unwrap();
        let mut table = Table::new();

        table.set_header(vec!["Property", "Value"]);
        table.add_row(vec!["Topic id", format!("{}", topic.id).as_str()]);
        table.add_row(vec![
            "Created",
            topic.created_at.to_utc_string("%Y-%m-%d %H:%M:%S").as_str(),
        ]);
        table.add_row(vec!["Topic name", topic.name.as_str()]);
        table.add_row(vec!["Topic size", format!("{}", topic.size).as_str()]);
        table.add_row(vec![
            "Compression",
            topic.compression_algorithm.to_string().as_str(),
        ]);
        table.add_row(vec![
            "Message expiry",
            match topic.message_expiry {
                IggyExpiry::NeverExpire => String::from("unlimited"),
                IggyExpiry::ServerDefault => String::from("server_default"),
                IggyExpiry::ExpireDuration(value) => format!("{value}"),
            }
            .as_str(),
        ]);
        table.add_row(vec![
            "Max topic size",
            format!("{}", topic.max_topic_size).as_str(),
        ]);
        table.add_row(vec![
            "Topic message count",
            format!("{}", topic.messages_count).as_str(),
        ]);
        table.add_row(vec![
            "Partitions count",
            format!("{}", topic.partitions_count).as_str(),
        ]);

        if !topic.partitions.is_empty() {
            let mut partitions_table = Table::new();
            partitions_table.load_style(ASCII_NO_BORDERS);
            partitions_table.set_header(vec![
                "Partition id",
                "Created",
                "Segments count",
                "Current offset",
                "Size",
                "Messages count",
            ]);

            for partition in &topic.partitions {
                partitions_table.add_row(vec![
                    partition.id.to_string(),
                    partition.created_at.to_utc_string("%Y-%m-%d %H:%M:%S"),
                    partition.segments_count.to_string(),
                    partition.current_offset.to_string(),
                    partition.size.to_string(),
                    partition.messages_count.to_string(),
                ]);
            }
            table.add_row(vec!["Partitions", partitions_table.to_string().as_str()]);
        }

        if !topic.options.is_empty() {
            let mut options_table = Table::new();
            options_table.load_style(ASCII_NO_BORDERS);
            options_table.set_header(vec!["Option", "Value"]);
            for (key, option) in &topic.options {
                options_table.add_row(vec![
                    String::from_utf8_lossy(key.as_bytes()).into_owned(),
                    option.value.to_string_value(),
                ]);
            }
            table.add_row(vec!["Options", options_table.to_string().as_str()]);
        }

        event!(target: PRINT_TARGET, Level::INFO,"{table}");

        Ok(())
    }
}
