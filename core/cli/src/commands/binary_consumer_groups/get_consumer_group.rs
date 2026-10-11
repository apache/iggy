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
use iggy_common::{Consumer, ConsumerGroupMember, Identifier, Partition};
use std::collections::HashMap;
use tracing::{Level, event};

pub struct GetConsumerGroupCmd {
    stream_id: Identifier,
    topic_id: Identifier,
    group_id: Identifier,
}

impl GetConsumerGroupCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier, consumer_group_id: Identifier) -> Self {
        Self {
            stream_id,
            topic_id,
            group_id: consumer_group_id,
        }
    }
}

fn partition_owner(members: &[ConsumerGroupMember]) -> HashMap<u32, u32> {
    let mut partition_to_member = HashMap::new();
    for member in members {
        for partition_id in &member.partitions {
            partition_to_member.insert(*partition_id, member.id);
        }
    }
    partition_to_member
}

fn find_partition(partitions: &[Partition], partition_id: u32) -> Option<&Partition> {
    partitions
        .iter()
        .find(|partition| partition.id == partition_id)
}

#[async_trait]
impl CliCommand for GetConsumerGroupCmd {
    fn explain(&self) -> String {
        format!(
            "get consumer group with ID: {} for topic with ID: {} and stream with ID: {}",
            self.group_id, self.topic_id, self.stream_id,
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let consumer_group = client
            .get_consumer_group(&self.stream_id, &self.topic_id, &self.group_id)
            .await
            .with_context(|| {
                format!(
                    "Problem getting consumer group with ID: {} for topic with ID: {} and stream with ID: {}",
                    self.group_id, self.topic_id, self.stream_id
                )
            })?;

        if consumer_group.is_none() {
            event!(target: PRINT_TARGET, Level::INFO, "Consumer group with ID: {} was not found", self.group_id);
            return Ok(());
        }

        let consumer_group = consumer_group.unwrap();

        let topic = client
            .get_topic(&self.stream_id, &self.topic_id)
            .await
            .with_context(|| {
                format!(
                    "Problem getting topic with ID: {} in stream with ID: {}",
                    self.topic_id, self.stream_id
                )
            })?
            .with_context(|| {
                format!(
                    "Topic with ID: {} in stream with ID: {} was not found",
                    self.topic_id, self.stream_id
                )
            })?;

        let consumer = Consumer::group(self.group_id.clone());
        let partition_to_member = partition_owner(&consumer_group.members);

        let mut partitions_table = Table::new();
        partitions_table.load_style(ASCII_NO_BORDERS);
        partitions_table.set_header(vec![
            "Partition",
            "Member",
            "Current offset",
            "Stored offset",
            "Lag",
        ]);

        for partition_id in 0..consumer_group.partitions_count {
            let partition = find_partition(&topic.partitions, partition_id).with_context(|| {
                format!(
                    "Partition with ID: {} in topic with ID: {} was not found",
                    partition_id, self.topic_id
                )
            })?;

            let consumer_offset = client
                .get_consumer_offset(
                    &consumer,
                    &self.stream_id,
                    &self.topic_id,
                    Some(partition_id),
                )
                .await
                .with_context(|| {
                    format!(
                        "Problem getting consumer offset for consumer group with ID: {} for stream with ID: {} and topic with ID: {} and partition with ID: {}",
                        self.group_id, self.stream_id, self.topic_id, partition_id
                    )
                })?;

            let member = partition_to_member
                .get(&partition_id)
                .map(|member_id| format!("{}", member_id))
                .unwrap_or_else(|| "-".to_string());

            let (current_offset, stored_offset, lag) = match consumer_offset {
                Some(offset_info) => {
                    let lag = offset_info.current_offset - offset_info.stored_offset;
                    (
                        format!("{}", offset_info.current_offset),
                        format!("{}", offset_info.stored_offset),
                        format!("{}", lag),
                    )
                }
                None => (
                    format!("{}", partition.current_offset),
                    "-".to_string(),
                    format!("{}", partition.messages_count),
                ),
            };

            partitions_table.add_row(vec![
                format!("{}", partition_id),
                member,
                current_offset,
                stored_offset,
                lag,
            ]);
        }

        let mut table = Table::new();

        table.set_header(vec!["Property", "Value"]);
        table.add_row(vec![
            "Consumer group id",
            format!("{}", consumer_group.id).as_str(),
        ]);
        table.add_row(vec!["Consumer group name", consumer_group.name.as_str()]);
        table.add_row(vec![
            "Partitions count",
            format!("{}", consumer_group.partitions_count).as_str(),
        ]);
        table.add_row(vec![
            "Members count",
            format!("{}", consumer_group.members_count).as_str(),
        ]);

        if consumer_group.members_count > 0 {
            let mut members_table = Table::new();
            members_table.load_style(ASCII_NO_BORDERS);
            members_table.set_header(vec!["Member id", "Partitions count", "Partitions"]);
            for member in consumer_group.members {
                members_table.add_row(vec![
                    format!("{}", member.id).as_str(),
                    format!("{}", member.partitions_count).as_str(),
                    member
                        .partitions
                        .iter()
                        .map(|i| format!("{i}"))
                        .collect::<Vec<String>>()
                        .join(", ")
                        .as_str(),
                ]);
            }
            table.add_row(vec!["Members", members_table.to_string().as_str()]);
        }

        table.add_row(vec!["Partitions", partitions_table.to_string().as_str()]);

        event!(target: PRINT_TARGET, Level::INFO,"{table}");

        Ok(())
    }
}
