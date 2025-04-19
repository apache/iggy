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

use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::consumer_groups::get_consumer_group::GetConsumerGroup;
use crate::identifier::Identifier;
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::{presets::ASCII_NO_BORDERS, Table};
use tracing::{event, Level};

pub struct GetConsumerGroupCmd {
    get_consumer_group: GetConsumerGroup,
}

impl GetConsumerGroupCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier, consumer_group_id: Identifier) -> Self {
        Self {
            get_consumer_group: GetConsumerGroup {
                stream_id,
                topic_id,
                group_id: consumer_group_id,
            },
        }
    }
}

#[async_trait]
impl CliCommand for GetConsumerGroupCmd {
    fn explain(&self) -> String {
        format!(
            "get consumer group with ID: {} for topic with ID: {} and stream with ID: {}",
            self.get_consumer_group.group_id,
            self.get_consumer_group.topic_id,
            self.get_consumer_group.stream_id,
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let consumer_group = client
            .get_consumer_group(&self.get_consumer_group.stream_id, &self.get_consumer_group.topic_id, &self.get_consumer_group.group_id)
            .await
            .with_context(|| {
                format!(
                    "Problem getting consumer group with ID: {} for topic with ID: {} and stream with ID: {}",
                    self.get_consumer_group.group_id, self.get_consumer_group.topic_id, self.get_consumer_group.stream_id
                )
            })?;

        if consumer_group.is_none() {
            event!(target: PRINT_TARGET, Level::INFO, "Consumer group with ID: {} was not found", self.get_consumer_group.group_id);
            return Ok(());
        }

        let consumer_group = consumer_group.unwrap();
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
            members_table.load_preset(ASCII_NO_BORDERS);
            members_table.set_header(vec!["Member id", "Partitions count", "Partitions"]);
            for member in consumer_group.members {
                members_table.add_row(vec![
                    format!("{}", member.id).as_str(),
                    format!("{}", member.partitions_count).as_str(),
                    member
                        .partitions
                        .iter()
                        .map(|i| format!("{}", i))
                        .collect::<Vec<String>>()
                        .join(", ")
                        .as_str(),
                ]);
            }
            table.add_row(vec!["Members", members_table.to_string().as_str()]);
        }

        event!(target: PRINT_TARGET, Level::INFO,"{table}");

        Ok(())
    }
}
