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
use comfy_table::Table;
use iggy_common::{ClusterNodeRole, ClusterNodeStatus};
use tracing::{Level, event};

pub enum GetClusterMetadataOutput {
    Table,
    List,
}

pub struct GetClusterMetadataCmd {
    output: GetClusterMetadataOutput,
}

impl GetClusterMetadataCmd {
    pub fn new(output: GetClusterMetadataOutput) -> Self {
        GetClusterMetadataCmd { output }
    }
}

impl Default for GetClusterMetadataCmd {
    fn default() -> Self {
        GetClusterMetadataCmd {
            output: GetClusterMetadataOutput::Table,
        }
    }
}

#[async_trait]
impl CliCommand for GetClusterMetadataCmd {
    fn explain(&self) -> String {
        let mode = match self.output {
            GetClusterMetadataOutput::Table => "table",
            GetClusterMetadataOutput::List => "list",
        };
        format!("get cluster metadata in {mode} mode")
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let cluster_metadata = client
            .get_cluster_metadata()
            .await
            .with_context(|| String::from("Problem getting cluster metadata"))?;

        if cluster_metadata.nodes.is_empty() {
            event!(target: PRINT_TARGET, Level::INFO, "No cluster nodes found!");
            return Ok(());
        }

        event!(target: PRINT_TARGET, Level::INFO, "Cluster name: {}", cluster_metadata.name);

        match self.output {
            GetClusterMetadataOutput::Table => {
                let mut table = Table::new();

                table.set_header(vec![
                    "Name", "IP", "TCP", "QUIC", "HTTP", "WebSocket", "Role", "Status",
                ]);

                cluster_metadata.nodes.iter().for_each(|node| {
                    table.add_row(vec![
                        node.name.clone(),
                        node.ip.clone(),
                        format!("{}", node.endpoints.tcp),
                        format!("{}", node.endpoints.quic),
                        format!("{}", node.endpoints.http),
                        format!("{}", node.endpoints.websocket),
                        node.role.to_string(),
                        node.status.to_string(),
                    ]);
                });

                event!(target: PRINT_TARGET, Level::INFO, "{table}");
            }
            GetClusterMetadataOutput::List => {
                cluster_metadata.nodes.iter().for_each(|node| {
                    event!(target: PRINT_TARGET, Level::INFO,
                        "{}|{}|{}|{}|{}|{}|{}|{}",
                        node.name,
                        node.ip,
                        node.endpoints.tcp,
                        node.endpoints.quic,
                        node.endpoints.http,
                        node.endpoints.websocket,
                        node.role,
                        node.status
                    );
                });
            }
        }

        Ok(())
    }
}
