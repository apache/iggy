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

//TODO: Fixme
/*
use crate::channels::server_command::BackgroundServerCommand;
use crate::configs::server::StateMaintenanceConfig;
use crate::streaming::systems::system::SharedSystem;
use flume::Sender;
use iggy_common::IggyDuration;
use iggy_common::IggyTimestamp;
use tokio::time;
use tracing::{error, info, instrument, warn};

pub struct StateArchiver {
    enabled: bool,
    overwrite: bool,
    interval: IggyDuration,
    sender: Sender<ArchiveStateCommand>,
}

#[derive(Debug, Default, Clone)]
pub struct ArchiveStateCommand {
    overwrite: bool,
}

#[derive(Debug, Default, Clone)]
pub struct ArchiveStateExecutor;

impl StateArchiver {
    pub fn new(config: &StateMaintenanceConfig, sender: Sender<ArchiveStateCommand>) -> Self {
        Self {
            enabled: config.archiver_enabled,
            overwrite: config.overwrite,
            interval: config.interval,
            sender,
        }
    }

    pub fn start(&self) {
        if !self.enabled {
            info!("State archiver is disabled.");
            return;
        }

        let overwrite = self.overwrite;
        let interval = self.interval;
        let sender = self.sender.clone();
        info!("State archiver is enabled, state will be archived every: {interval}.");
        tokio::spawn(async move {
            let mut interval_timer = time::interval(interval.get_duration());
            loop {
                interval_timer.tick().await;
                sender
                    .send(ArchiveStateCommand { overwrite })
                    .unwrap_or_else(|err| {
                        error!("Failed to send ArchiveStateCommand. Error: {}", err);
                    });
            }
        });
    }
}

impl BackgroundServerCommand<ArchiveStateCommand> for ArchiveStateExecutor {
    #[instrument(skip_all, name = "trace_archive_state")]
    async fn execute(&mut self, system: &SharedSystem, command: ArchiveStateCommand) {
        let system = system.read().await;
        if system.archiver.is_none() {
            warn!("Archiver is disabled, state will not be archived.");
            return;
        }

        let base_directory = if command.overwrite {
            None
        } else {
            Some(format!("{}_state", IggyTimestamp::now().as_micros()))
        };
        let state_messages_file_path = system.config.get_state_messages_file_path();
        let state_info_path = system.config.get_state_info_path();
        info!("Archiving state...");
        let archiver = system.archiver.as_ref().unwrap();
        let files = [state_info_path.as_ref(), state_messages_file_path.as_ref()];
        if let Err(error) = archiver.archive(&files, base_directory).await {
            error!("Failed to archive state. Error: {}", error);
            return;
        }
        info!("State archived successfully.");
    }

    fn start_command_sender(
        &mut self,
        _system: SharedSystem,
        config: &crate::configs::server::ServerConfig,
        sender: Sender<ArchiveStateCommand>,
    ) {
        if !config.data_maintenance.archiver.enabled
            || !config.data_maintenance.state.archiver_enabled
        {
            return;
        }

        let state_archiver = StateArchiver::new(&config.data_maintenance.state, sender);
        state_archiver.start();
    }

    fn start_command_consumer(
        mut self,
        system: SharedSystem,
        config: &crate::configs::server::ServerConfig,
        receiver: flume::Receiver<ArchiveStateCommand>,
    ) {
        if !config.data_maintenance.archiver.enabled
            || !config.data_maintenance.state.archiver_enabled
        {
            return;
        }

        tokio::spawn(async move {
            let system = system.clone();
            while let Ok(command) = receiver.recv_async().await {
                self.execute(&system, command).await;
            }
            info!("State archiver receiver stopped.");
        });
    }
}

*/
