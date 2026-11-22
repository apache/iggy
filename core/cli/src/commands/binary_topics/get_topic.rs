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

use crate::commands::binary_partitions::get_partitions::partitions_table;
use crate::commands::cli_command::{CliCommand, PRINT_TARGET, print_json};
use anyhow::{Context, bail};
use async_trait::async_trait;
use comfy_table::{Table, presets::ASCII_NO_BORDERS};
use iggy_common::Client;
use iggy_common::GeoTopicDurations;
use iggy_common::Identifier;
use iggy_common::IggyExpiry;
use iggy_common::OptionKey;
use iggy_common::TopicDetails;
use tracing::{Level, event};

pub enum GetTopicOutput {
    Table,
    Json,
}

pub struct GetTopicCmd {
    stream_id: Identifier,
    topic_id: Identifier,
    output: GetTopicOutput,
}

impl GetTopicCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier, output: GetTopicOutput) -> Self {
        Self {
            stream_id,
            topic_id,
            output,
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

    fn stdout_is_data(&self) -> bool {
        matches!(self.output, GetTopicOutput::Json)
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

        let Some(topic) = topic else {
            if self.stdout_is_data() {
                bail!(
                    "Topic with ID: {} in stream {} was not found",
                    self.topic_id,
                    self.stream_id
                );
            }
            event!(target: PRINT_TARGET, Level::INFO, "Topic with ID: {} in stream {} was not found", self.topic_id, self.stream_id);
            return Ok(());
        };

        if self.stdout_is_data() {
            return print_json(&topic);
        }

        // A topic in a shadow stream comes from the stream's geo origin.
        let origin = client
            .get_stream(&self.stream_id)
            .await
            .ok()
            .flatten()
            .and_then(|stream| stream.shadow_origin());
        let table = topic_table(&topic, origin.as_deref());

        event!(target: PRINT_TARGET, Level::INFO,"{table}");

        Ok(())
    }
}

/// The table of `topic`, with the geo `origin` of its stream when it is a
/// shadow stream.
fn topic_table(topic: &TopicDetails, origin: Option<&str>) -> Table {
    let mut table = Table::new();

    table.set_header(vec!["Property", "Value"]);
    let resolved = topic.resolved_options();
    table.add_row(vec!["Topic id", format!("{}", topic.id).as_str()]);
    table.add_row(vec![
        "Created",
        topic.created_at.to_utc_string("%Y-%m-%d %H:%M:%S").as_str(),
    ]);
    table.add_row(vec!["Topic name", topic.name.as_str()]);
    table.add_row(vec!["Topic type", resolved.topic_type.as_ref()]);
    if let Some(geo_mode) = resolved.geo_mode {
        table.add_row(vec!["Geo mode", geo_mode.as_ref()]);
    }
    if let Some(origin) = origin {
        table.add_row(vec!["Origin", origin]);
    }
    for (label, duration) in geo_duration_rows(&resolved.geo_durations) {
        table.add_row(vec![label, duration.as_str()]);
    }
    table.add_row(vec!["Topic size", format!("{}", topic.size).as_str()]);
    table.add_row(vec![
        "Compression",
        resolved.compression_algorithm.to_string().as_str(),
    ]);
    table.add_row(vec![
        "Message expiry",
        match resolved.message_expiry {
            IggyExpiry::NeverExpire => String::from("unlimited"),
            IggyExpiry::ServerDefault => String::from("server_default"),
            IggyExpiry::ExpireDuration(value) => format!("{value}"),
        }
        .as_str(),
    ]);
    table.add_row(vec![
        "Max topic size",
        format!("{}", resolved.max_topic_size).as_str(),
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
        let mut partitions = partitions_table(&topic.partitions);
        partitions.load_style(ASCII_NO_BORDERS);
        table.add_row(vec!["Partitions", partitions.to_string().as_str()]);
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

/// The durations that a geo topic sets for itself, each with its label. The
/// node's settings apply to the ones it does not set, so they have no row.
fn geo_duration_rows(durations: &GeoTopicDurations) -> Vec<(&'static str, String)> {
    GeoTopicDurations::KEYS
        .into_iter()
        .filter_map(|key| {
            let duration = durations.get(key)?;
            let label = match key {
                OptionKey::FreshUploadInterval => "Fresh upload interval",
                OptionKey::ConsumerOffsetUploadInterval => "Offset upload interval",
                OptionKey::PositionsPublishInterval => "Positions publish interval",
                OptionKey::TombstoneGrace => "Tombstone grace",
                OptionKey::PositionsResumeTimeout => "Positions resume timeout",
                _ => "Rewrite max interval",
            };
            let shown = match (key, duration.is_zero()) {
                (OptionKey::ConsumerOffsetUploadInterval, true) => "disabled".to_owned(),
                (_, true) => "0".to_owned(),
                (_, false) => duration.to_string(),
            };
            Some((label, shown))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::topic_table;
    use iggy_common::{
        HeaderKey, HeaderValue, IggyByteSize, IggyTimestamp, OptionValue, Partition,
        PartitionContext, ResourceOptions, TopicDetails,
    };

    fn topic() -> TopicDetails {
        TopicDetails {
            id: 7,
            created_at: IggyTimestamp::from(0_u64),
            name: "test-topic".to_owned(),
            size: IggyByteSize::from(0_u64),
            messages_count: 0,
            partitions_count: 0,
            partitions: Vec::new(),
            options: ResourceOptions::default(),
        }
    }

    #[test]
    fn empty_collections_keep_topic_properties_without_nested_tables() {
        let output = topic_table(&topic(), None).to_string();
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
                members: vec![0, 2, 4],
                in_sync_members: vec![0, 4],
            },
            Partition {
                id: 2,
                created_at: IggyTimestamp::from(1_000_000_u64),
                segments_count: 7,
                current_offset: 11,
                size: IggyByteSize::from(0_u64),
                messages_count: 13,
                context: PartitionContext::default(),
                members: vec![1],
                in_sync_members: Vec::new(),
            },
        ];
        let output = topic_table(&topic, None).to_string();
        let rows = output
            .lines()
            .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
            .collect::<Vec<_>>();
        assert!(
            rows.contains(&format!(
                "| | 9 | 1970-01-01 00:00:00 | 3 | {} | 1.23 KB | 5 | 0,2,4 | 0,4 |",
                u64::MAX
            )),
            "{output}"
        );
        assert!(
            rows.contains(&"| | 2 | 1970-01-01 00:00:01 | 7 | 11 | 0 B | 13 | 1 | - |".to_owned())
        );
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
        let output = topic_table(&topic, None).to_string();
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
    fn given_a_geo_topic_in_a_shadow_stream_should_show_its_type_mode_and_origin() {
        let mut fixture = topic();
        fixture.options = ResourceOptions::from([
            (
                HeaderKey::try_from("topic_type").unwrap(),
                OptionValue::explicit(HeaderValue::try_from("geo").unwrap()),
            ),
            (
                HeaderKey::try_from("geo_mode").unwrap(),
                OptionValue::derived(HeaderValue::try_from("geo").unwrap()),
            ),
        ]);

        let shadow = topic_table(&fixture, Some("dc-a")).to_string();
        let normal = topic_table(&topic(), None).to_string();

        for expected in [
            "Topic type          | geo",
            "Geo mode            | geo",
            "Origin              | dc-a",
        ] {
            assert!(shadow.contains(expected), "{expected:?} in {shadow}");
        }
        assert!(normal.contains("Topic type          | normal"));
        assert!(!normal.contains("Geo mode"));
        assert!(!normal.contains("Origin"));
    }

    #[test]
    fn given_a_geo_topic_with_its_own_durations_should_show_the_ones_it_sets() {
        let mut fixture = topic();
        fixture.options = ResourceOptions::from([
            (
                HeaderKey::try_from("topic_type").unwrap(),
                OptionValue::explicit(HeaderValue::try_from("geo").unwrap()),
            ),
            (
                HeaderKey::try_from("tombstone_grace").unwrap(),
                OptionValue::explicit(HeaderValue::from(3_600_000_000u64)),
            ),
            (
                HeaderKey::try_from("consumer_offset_upload_interval").unwrap(),
                OptionValue::explicit(HeaderValue::from(0u64)),
            ),
        ]);

        let table = topic_table(&fixture, None).to_string();
        let row = |label: &str| {
            table
                .lines()
                .find(|line| line.contains(label))
                .map(|line| line.split('|').nth(2).unwrap_or_default().trim().to_owned())
        };

        assert_eq!(row("Tombstone grace").as_deref(), Some("1h"));
        assert_eq!(row("Offset upload interval").as_deref(), Some("disabled"));
        for unset in [
            "Fresh upload interval",
            "Positions publish interval",
            "Positions resume timeout",
            "Rewrite max interval",
        ] {
            assert_eq!(row(unset), None, "{unset} in {table}");
        }
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
        let output = topic_table(&fixture, None).to_string();
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
