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
use iggy_common::get_stats::GetStats;
use std::fmt::Display;
use std::time::SystemTime;
use tracing::{Level, event};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum GetStatsOutput {
    Table,
    List,
    Json,
    Toml,
}

impl Display for GetStatsOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GetStatsOutput::Table => write!(f, "table"),
            GetStatsOutput::List => write!(f, "list"),
            GetStatsOutput::Json => write!(f, "json"),
            GetStatsOutput::Toml => write!(f, "toml"),
        }
    }
}

pub struct GetStatsCmd {
    quiet_mode: bool,
    output: GetStatsOutput,
    _get_stats: GetStats,
}

impl GetStatsCmd {
    pub fn new(quiet_mode: bool, output: GetStatsOutput) -> Self {
        Self {
            quiet_mode,
            output,
            _get_stats: GetStats {},
        }
    }
}

#[async_trait]
impl CliCommand for GetStatsCmd {
    fn explain(&self) -> String {
        "stats command".to_owned()
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let stats = client
            .get_stats()
            .await
            .with_context(|| "Problem sending get_stats command".to_owned())?;

        let output = match self.output {
            GetStatsOutput::Table => {
                let mut table = Table::new();

                table.set_header(vec!["Server property", "Value"]);
                table.add_row(vec![
                    "Iggy Server PID",
                    format!("{}", stats.process_id).as_str(),
                ]);
                table.add_row(vec![
                    "Iggy Server CPU Usage",
                    format!("{:.4} %", stats.cpu_usage).as_str(),
                ]);
                table.add_row(vec![
                    "Total CPU Usage",
                    format!("{:.4} %", stats.total_cpu_usage).as_str(),
                ]);
                table.add_row(vec![
                    "Iggy Server Memory Usage",
                    stats.memory_usage.as_bytes_u64().to_string().as_str(),
                ]);

                table.add_row(vec![
                    "Total Memory (RAM)",
                    stats.total_memory.as_bytes_u64().to_string().as_str(),
                ]);
                table.add_row(vec![
                    "Available Memory (RAM)",
                    stats.available_memory.as_bytes_u64().to_string().as_str(),
                ]);
                table.add_row(vec![
                    "Iggy Server Run Time",
                    stats.run_time.as_secs().to_string().as_str(),
                ]);

                let start_time_utc = stats.start_time + SystemTime::UNIX_EPOCH;
                table.add_row(vec![
                    "Start Time (UTC)",
                    start_time_utc.to_string().as_str(),
                ]);

                table.add_row(vec![
                    "Read Bytes",
                    stats.read_bytes.as_bytes_u64().to_string().as_str(),
                ]);
                table.add_row(vec![
                    "Written Bytes",
                    stats.written_bytes.as_bytes_u64().to_string().as_str(),
                ]);
                table.add_row(vec![
                    "Messages Size Bytes",
                    stats
                        .messages_size_bytes
                        .as_bytes_u64()
                        .to_string()
                        .as_str(),
                ]);
                table.add_row(vec![
                    "Streams Count",
                    format!("{}", stats.streams_count).as_str(),
                ]);
                table.add_row(vec![
                    "Topics Count",
                    format!("{}", stats.topics_count).as_str(),
                ]);
                table.add_row(vec![
                    "Partitions Count",
                    format!("{}", stats.partitions_count).as_str(),
                ]);
                table.add_row(vec![
                    "Segments Count",
                    format!("{}", stats.segments_count).as_str(),
                ]);
                table.add_row(vec![
                    "Message Count",
                    format!("{}", stats.messages_count).as_str(),
                ]);
                table.add_row(vec![
                    "Clients Count",
                    format!("{}", stats.clients_count).as_str(),
                ]);
                table.add_row(vec![
                    "Consumer Groups Count",
                    format!("{}", stats.consumer_groups_count).as_str(),
                ]);

                table.add_row(vec!["OS Name", stats.os_name.as_str()]);
                table.add_row(vec!["OS Version", stats.os_version.as_str()]);
                table.add_row(vec!["Kernel Version", stats.kernel_version.as_str()]);

                table.to_string()
            }
            GetStatsOutput::List => {
                let mut list = Vec::new();

                list.push(format!("Iggy Server PID|{}", stats.process_id));
                list.push(format!("Iggy Server CPU Usage|{:.4} %", stats.cpu_usage));
                list.push(format!("Total CPU Usage|{:.4} %", stats.total_cpu_usage));
                list.push(format!(
                    "Iggy Server Memory Usage|{}",
                    stats.memory_usage.as_bytes_u64()
                ));
                list.push(format!(
                    "Total Memory (RAM)|{}",
                    stats.total_memory.as_bytes_u64()
                ));
                list.push(format!(
                    "Available Memory (RAM)|{}",
                    stats.available_memory.as_bytes_u64()
                ));
                list.push(format!("Iggy Server Run Time|{}", stats.run_time.as_secs()));

                let start_time_utc = stats.start_time + SystemTime::UNIX_EPOCH;
                list.push(format!("Start Time (UTC)|{start_time_utc}"));

                list.push(format!("Read Bytes|{}", stats.read_bytes.as_bytes_u64()));
                list.push(format!(
                    "Written Bytes|{}",
                    stats.written_bytes.as_bytes_u64()
                ));
                list.push(format!(
                    "Messages Size Bytes|{}",
                    stats.messages_size_bytes.as_bytes_u64()
                ));
                list.push(format!("Streams Count|{}", stats.streams_count));
                list.push(format!("Topics Count|{}", stats.topics_count));
                list.push(format!("Partitions Count|{}", stats.partitions_count));
                list.push(format!("Segments Count|{}", stats.segments_count));
                list.push(format!("Message Count|{}", stats.messages_count));
                list.push(format!("Clients Count|{}", stats.clients_count));
                list.push(format!(
                    "Consumer Groups Count|{}",
                    stats.consumer_groups_count
                ));

                list.push(format!("OS Name|{}", stats.os_name));
                list.push(format!("OS Version|{}", stats.os_version));
                list.push(format!("Kernel Version|{}", stats.kernel_version));

                list.join("\n")
            }
            GetStatsOutput::Toml => toml::to_string(&stats)?,
            GetStatsOutput::Json => serde_json::to_string_pretty(&stats)?,
        };

        if self.quiet_mode {
            println!("{output}");
        } else {
            event!(target: PRINT_TARGET, Level::INFO,"{output}");
        }

        Ok(())
    }
}
