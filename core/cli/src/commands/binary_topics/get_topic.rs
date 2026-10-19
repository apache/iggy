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
use iggy_common::TopicDetails;
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
        let table = topic_table(&topic);

        event!(target: PRINT_TARGET, Level::INFO,"{table}");

        Ok(())
    }
}

fn topic_table(topic: &TopicDetails) -> Table {
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
        options_table.set_header(vec!["Option", "Value", "Source"]);
        for (key, option) in &topic.options {
            options_table.add_row(vec![
                String::from_utf8_lossy(key.as_bytes()).into_owned(),
                option.value.to_string_value(),
                if option.explicit {
                    "explicit"
                } else {
                    "default"
                }
                .to_owned(),
            ]);
        }
        table.add_row(vec!["Options", options_table.to_string().as_str()]);
    }

    table
}

#[cfg(test)]
mod tests {
    use super::topic_table;
    use iggy_common::{
        CompressionAlgorithm, HeaderKey, HeaderValue, IggyByteSize, IggyExpiry, IggyTimestamp,
        MaxTopicSize, OptionValue, Partition, PartitionContext, ResourceOptions, TopicDetails,
    };

    fn topic() -> TopicDetails {
        TopicDetails {
            id: 7,
            created_at: IggyTimestamp::from(0_u64),
            name: "test-topic".to_owned(),
            size: IggyByteSize::from(0_u64),
            message_expiry: IggyExpiry::NeverExpire,
            compression_algorithm: CompressionAlgorithm::None,
            max_topic_size: MaxTopicSize::Unlimited,
            messages_count: 0,
            partitions_count: 0,
            partitions: Vec::new(),
            options: ResourceOptions::default(),
        }
    }

    #[test]
    fn empty_collections_keep_topic_properties_without_nested_tables() {
        let output = topic_table(&topic()).to_string();
        assert!(output.contains("Topic name          | test-topic"));
        assert!(output.contains("Partitions count    | 0"));
        assert!(!output.contains("Partition id"));
        assert!(!output.contains("Segments count"));
        assert!(!output.contains("Options"));
    }

    #[test]
    fn partition_rows_keep_distinct_fields_and_large_offsets() {
        let mut topic = topic();
        topic.partitions_count = 2;
        topic.partitions = vec![
            Partition {
                id: 9,
                created_at: IggyTimestamp::from(0_u64),
                segments_count: 3,
                current_offset: u64::MAX,
                size: IggyByteSize::from(1230_u64),
                messages_count: 5,
                context: PartitionContext::default(),
            },
            Partition {
                id: 2,
                created_at: IggyTimestamp::from(1_000_000_u64),
                segments_count: 7,
                current_offset: 11,
                size: IggyByteSize::from(0_u64),
                messages_count: 13,
                context: PartitionContext::default(),
            },
        ];
        let output = topic_table(&topic).to_string();
        let rows = output
            .lines()
            .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
            .collect::<Vec<_>>();
        assert!(
            rows.contains(&format!(
                "| | 9 | 1970-01-01 00:00:00 | 3 | {} | 1.23 KB | 5 |",
                u64::MAX
            )),
            "{output}"
        );
        assert!(rows.contains(&"| | 2 | 1970-01-01 00:00:01 | 7 | 11 | 0 B | 13 |".to_owned()));
    }

    #[test]
    fn options_keep_typed_values_provenance_and_unknown_keys() {
        let mut topic = topic();
        let long_value = "long value / café: ".repeat(10);
        topic.options = ResourceOptions::from([
            (
                HeaderKey::try_from("future_option").unwrap(),
                OptionValue::explicit(HeaderValue::try_from(long_value.as_str()).unwrap()),
            ),
            (
                HeaderKey::try_from("preallocate_segments").unwrap(),
                OptionValue::derived(HeaderValue::from(false)),
            ),
            (
                HeaderKey::try_from("messages_required_to_save").unwrap(),
                OptionValue::explicit(HeaderValue::from(17_u32)),
            ),
        ]);
        let output = topic_table(&topic).to_string();
        let rows = output
            .lines()
            .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
            .collect::<Vec<_>>();
        assert!(
            rows.contains(&"| Options | Option | Value | Source |".to_owned()),
            "{output}"
        );
        assert!(rows.contains(&format!(
            "| | future_option | {} | explicit |",
            long_value.trim()
        )));
        assert!(rows.contains(&"| | preallocate_segments | false | default |".to_owned()));
        assert!(rows.contains(&"| | messages_required_to_save | 17 | explicit |".to_owned()));
        let option_lines = output
            .lines()
            .filter(|line| {
                line.contains("future_option")
                    || line.contains("preallocate_segments")
                    || line.contains("messages_required_to_save")
            })
            .collect::<Vec<_>>();
        assert_eq!(option_lines.len(), 3);
        assert!(output.contains(&long_value));
        assert!(option_lines.iter().all(|line| {
            line.chars().filter(|&character| character == '|').count() == 5
                && line.chars().count() == option_lines[0].chars().count()
        }));
    }

    #[test]
    fn resolved_options_keep_full_width_values_and_sources() {
        let mut fixture = topic();
        fixture.options = ResourceOptions::from([
            (
                HeaderKey::try_from("max_topic_size").unwrap(),
                OptionValue::derived(HeaderValue::from(u64::MAX)),
            ),
            (
                HeaderKey::try_from("size_of_messages_required_to_save").unwrap(),
                OptionValue::derived(HeaderValue::from(1_048_576_u64)),
            ),
            (
                HeaderKey::try_from("messages_required_to_save").unwrap(),
                OptionValue::derived(HeaderValue::from(1024_u32)),
            ),
            (
                HeaderKey::try_from("preallocate_segments").unwrap(),
                OptionValue::explicit(HeaderValue::from(false)),
            ),
            (
                HeaderKey::try_from("durability").unwrap(),
                OptionValue::explicit(HeaderValue::try_from("persisted").unwrap()),
            ),
            (
                HeaderKey::try_from("consumer_offset_durability").unwrap(),
                OptionValue::explicit(HeaderValue::try_from("replicated").unwrap()),
            ),
        ]);
        let output = topic_table(&fixture).to_string();
        for expected in [
            "Option                            | Value",
            "preallocate_segments              | false                | explicit",
            "messages_required_to_save         | 1024                 | default",
            "|  durability                        | persisted            | explicit",
            "consumer_offset_durability        | replicated           | explicit",
        ] {
            assert!(output.contains(expected), "missing {expected}: {output}");
        }
        let rows = output
            .lines()
            .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
            .collect::<Vec<_>>();
        for expected in [
            "| | max_topic_size | 18446744073709551615 | default |",
            "| | size_of_messages_required_to_save | 1048576 | default |",
        ] {
            assert!(
                rows.iter().any(|row| row == expected),
                "missing {expected}: {output}"
            );
        }
    }
}
