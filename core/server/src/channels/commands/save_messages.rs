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

//Todo: Fixme
/*
use crate::channels::server_command::BackgroundServerCommand;
use crate::configs::server::MessageSaverConfig;
use crate::configs::server::ServerConfig;
use crate::streaming::systems::system::SharedSystem;
use flume::{Receiver, Sender};
use iggy_common::IggyDuration;
use tokio::time;
use tracing::{error, info, instrument, warn};

pub struct MessagesSaver {
    enabled: bool,
    enforce_fsync: bool,
    interval: IggyDuration,
    sender: Sender<SaveMessagesCommand>,
}

#[derive(Debug, Default, Clone)]
pub struct SaveMessagesCommand {
    pub enforce_fsync: bool,
}

#[derive(Debug, Default, Clone)]
pub struct SaveMessagesExecutor;

impl MessagesSaver {
    pub fn new(config: &MessageSaverConfig, sender: Sender<SaveMessagesCommand>) -> Self {
        Self {
            enabled: config.enabled,
            enforce_fsync: config.enforce_fsync,
            interval: config.interval,
            sender,
        }
    }

    pub fn start(&self) {
        if !self.enabled {
            info!("Message saver is disabled.");
            return;
        }

        let enforce_fsync = self.enforce_fsync;
        let interval = self.interval;
        let sender = self.sender.clone();
        info!(
            "Message saver is enabled, buffered messages will be automatically saved every: {interval}, enforce fsync: {enforce_fsync}."
        );
        tokio::spawn(async move {
            let mut interval_timer = time::interval(interval.get_duration());
            loop {
                interval_timer.tick().await;
                let command = SaveMessagesCommand { enforce_fsync };
                sender.send(command).unwrap_or_else(|e| {
                    error!("Failed to send SaveMessagesCommand. Error: {e}",);
                });
            }
        });
    }
}

impl BackgroundServerCommand<SaveMessagesCommand> for SaveMessagesExecutor {
    #[instrument(skip_all, name = "trace_save_messages")]
    async fn execute(&mut self, system: &SharedSystem, _command: SaveMessagesCommand) {
        let saved_messages_count = system.read().await.persist_messages().await;
        match saved_messages_count {
            Ok(n) => {
                if n > 0 {
                    info!("Saved {n} buffered messages on disk.");
                }
            }
            Err(e) => {
                error!("Couldn't save buffered messages on disk. Error: {e}");
            }
        }
    }

    fn start_command_sender(
        &mut self,
        _system: SharedSystem,
        config: &ServerConfig,
        sender: Sender<SaveMessagesCommand>,
    ) {
        let messages_saver = MessagesSaver::new(&config.message_saver, sender);
        messages_saver.start();
    }

    fn start_command_consumer(
        mut self,
        system: SharedSystem,
        _config: &ServerConfig,
        receiver: Receiver<SaveMessagesCommand>,
    ) {
        tokio::spawn(async move {
            let system = system.clone();
            while let Ok(command) = receiver.recv_async().await {
                self.execute(&system, command).await;
            }
            warn!("Server command handler stopped receiving commands.");
        });
    }
}

*/
