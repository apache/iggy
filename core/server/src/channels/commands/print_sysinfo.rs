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

// TODO: Fixme
/*
use crate::{
    channels::server_command::BackgroundServerCommand,
    configs::server::ServerConfig,
    streaming::{systems::system::SharedSystem, utils::memory_pool},
};
use flume::{Receiver, Sender};
use human_repr::HumanCount;
use iggy_common::IggyDuration;
use tokio::time::{self};
use tracing::{error, info, warn};

#[derive(Debug, Default, Clone)]
pub struct SysInfoPrintCommand;

pub struct SysInfoPrinter {
    interval: IggyDuration,
    sender: Sender<SysInfoPrintCommand>,
}

pub struct SysInfoPrintExecutor;

impl SysInfoPrinter {
    pub fn new(interval: IggyDuration, sender: Sender<SysInfoPrintCommand>) -> Self {
        Self { interval, sender }
    }

    pub fn start(&self) {
        let interval = self.interval;
        let sender = self.sender.clone();
        if interval.is_zero() {
            info!("SysInfoPrinter is disabled.");
            return;
        }

        info!("SysInfoPrinter is enabled, system information will be printed every {interval}.");
        tokio::spawn(async move {
            let mut interval_timer = time::interval(interval.get_duration());
            loop {
                interval_timer.tick().await;
                let command = SysInfoPrintCommand {};
                sender.send(command).unwrap_or_else(|e| {
                    error!("Failed to send SysInfoPrintCommand. Error: {e}");
                });
            }
        });
    }
}

impl BackgroundServerCommand<SysInfoPrintCommand> for SysInfoPrintExecutor {
    async fn execute(&mut self, system: &SharedSystem, _command: SysInfoPrintCommand) {
        let stats = match system.read().await.get_stats().await {
            Ok(stats) => stats,
            Err(e) => {
                error!("Failed to get system information. Error: {e}");
                return;
            }
        };

        let free_memory_percent = (stats.available_memory.as_bytes_u64() as f64
            / stats.total_memory.as_bytes_u64() as f64)
            * 100f64;

        info!(
            "CPU: {:.2}%/{:.2}% (IggyUsage/Total), Mem: {:.2}%/{}/{}/{} (Free/IggyUsage/TotalUsed/Total), Clients: {}, Messages processed: {}, Read: {}, Written: {}, Uptime: {}",
            stats.cpu_usage,
            stats.total_cpu_usage,
            free_memory_percent,
            stats.memory_usage,
            stats.total_memory - stats.available_memory,
            stats.total_memory,
            stats.clients_count,
            stats.messages_count.human_count_bare(),
            stats.read_bytes,
            stats.written_bytes,
            stats.run_time
        );

        memory_pool().log_stats();
    }

    fn start_command_sender(
        &mut self,
        _system: SharedSystem,
        config: &ServerConfig,
        sender: Sender<SysInfoPrintCommand>,
    ) {
        let printer = SysInfoPrinter::new(config.system.logging.sysinfo_print_interval, sender);
        printer.start();
    }

    fn start_command_consumer(
        mut self,
        system: SharedSystem,
        _config: &ServerConfig,
        receiver: Receiver<SysInfoPrintCommand>,
    ) {
        tokio::spawn(async move {
            let system = system.clone();
            while let Ok(command) = receiver.recv_async().await {
                self.execute(&system, command).await;
            }
            warn!("Sysinfo printer stopped receiving commands.");
        });
    }
}

*/
