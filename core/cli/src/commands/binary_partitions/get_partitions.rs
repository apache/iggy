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
use iggy_common::{Client, Identifier, Partition};
use std::fmt::{self, Display, Formatter};
use tracing::{Level, event};

pub enum GetPartitionsOutput {
    Table,
    List,
}

impl Display for GetPartitionsOutput {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            GetPartitionsOutput::Table => write!(f, "table"),
            GetPartitionsOutput::List => write!(f, "list"),
        }
    }
}

pub struct GetPartitionsCmd {
    stream_id: Identifier,
    topic_id: Identifier,
    output: GetPartitionsOutput,
    rows: ListRows,
}

impl GetPartitionsCmd {
    pub fn new(
        stream_id: Identifier,
        topic_id: Identifier,
        output: GetPartitionsOutput,
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
impl CliCommand for GetPartitionsCmd {
    fn explain(&self) -> String {
        format!(
            "list partitions for stream with ID: {} and topic with ID: {} in {} mode",
            self.stream_id, self.topic_id, self.output
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let (stream_id, topic_id) = (&self.stream_id, &self.topic_id);
        let (partitions, next) = self
            .rows
            // The partitions flags have no name filter, so the name is always
            // `None`.
            .read(|query| async move {
                client
                    .get_partitions_page(stream_id, topic_id, &query.page)
                    .await
            })
            .await
            .with_context(|| {
                format!(
                    "Problem getting partitions for stream with ID: {stream_id} and topic with ID: {topic_id}"
                )
            })?;

        match self.output {
            GetPartitionsOutput::Table => {
                let table = partitions_table(&partitions);
                event!(target: PRINT_TARGET, Level::INFO, "{table}");
            }
            GetPartitionsOutput::List => {
                for partition in &partitions {
                    event!(target: PRINT_TARGET, Level::INFO, "{}", partition_row(partition).join("|"));
                }
            }
        }
        print_next(next);

        Ok(())
    }
}

/// The partition table of `partition list`, and the nested one of `topic get`.
pub(crate) fn partitions_table(partitions: &[Partition]) -> Table {
    let mut table = Table::new();
    table.set_header(vec![
        "Partition id",
        "Created",
        "Segments count",
        "Current offset",
        "Size",
        "Messages count",
        "Members",
        "In sync",
    ]);
    for partition in partitions {
        table.add_row(partition_row(partition));
    }
    table
}

fn partition_row(partition: &Partition) -> Vec<String> {
    vec![
        partition.id.to_string(),
        partition.created_at.to_utc_string("%Y-%m-%d %H:%M:%S"),
        partition.segments_count.to_string(),
        partition.current_offset.to_string(),
        partition.size.to_string(),
        partition.messages_count.to_string(),
        node_list(&partition.members),
        node_list(&partition.in_sync_members),
    ]
}

/// Node ids as `0,2,4`, or `-` for none.
fn node_list(nodes: &[u8]) -> String {
    if nodes.is_empty() {
        return "-".to_owned();
    }
    nodes
        .iter()
        .map(u8::to_string)
        .collect::<Vec<_>>()
        .join(",")
}
