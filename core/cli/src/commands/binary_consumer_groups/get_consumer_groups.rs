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
use crate::commands::utils::list_rows::{ListRows, print_next};
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use iggy_common::Client;
use iggy_common::Identifier;
use std::fmt::{self, Display, Formatter};
use tracing::{Level, event};

pub enum GetConsumerGroupsOutput {
    Table,
    List,
}

impl Display for GetConsumerGroupsOutput {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            GetConsumerGroupsOutput::Table => write!(f, "table"),
            GetConsumerGroupsOutput::List => write!(f, "list"),
        }?;

        Ok(())
    }
}

pub struct GetConsumerGroupsCmd {
    stream_id: Identifier,
    topic_id: Identifier,
    output: GetConsumerGroupsOutput,
    rows: ListRows,
}

impl GetConsumerGroupsCmd {
    pub fn new(
        stream_id: Identifier,
        topic_id: Identifier,
        output: GetConsumerGroupsOutput,
        rows: ListRows,
    ) -> Self {
        Self {
            stream_id,
            topic_id,
            output,
            rows,
        }
    }
}

#[async_trait]
impl CliCommand for GetConsumerGroupsCmd {
    fn explain(&self) -> String {
        format!(
            "list consumer groups for stream with ID: {} and topic with ID: {} in {} mode",
            self.stream_id, self.topic_id, self.output
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let (stream_id, topic_id) = (&self.stream_id, &self.topic_id);
        let (consumer_groups, next) = self
            .rows
            .read(|query| async move {
                client
                    .get_consumer_groups_page(stream_id, topic_id, &query)
                    .await
            })
            .await
            .with_context(|| {
                format!(
                    "Problem getting consumer groups for stream with ID: {stream_id} and topic with ID: {topic_id}"
                )
            })?;

        match self.output {
            GetConsumerGroupsOutput::Table => {
                let mut table = Table::new();
                table.set_header(vec!["ID", "Name", "Partitions Count", "Members Count"]);
                consumer_groups.iter().for_each(|group| {
                    table.add_row(vec![
                        format!("{}", group.id),
                        group.name.clone(),
                        format!("{}", group.partitions_count),
                        format!("{}", group.members_count),
                    ]);
                });

                event!(target: PRINT_TARGET, Level::INFO, "{table}");
            }
            GetConsumerGroupsOutput::List => {
                consumer_groups.iter().for_each(|group| {
                    event!(target: PRINT_TARGET, Level::INFO,
                        "{}|{}|{}|{}",
                        group.id,
                        group.name,
                        group.partitions_count,
                        group.members_count,
                    );
                });
            }
        }
        print_next(next);

        Ok(())
    }
}
