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

use std::{cell::Cell, rc::Rc, sync::Arc};

use iggy_common::{Aes256GcmEncryptor, EncryptorKind};
use tracing::info;

use crate::{configs::server::ServerConfig, map_toggle_str, shard::Shard, state::{file::FileState, StateKind}, streaming::{persistence::persister::{FilePersister, FileWithSyncPersister, PersisterKind}, storage::SystemStorage}, versioning::SemanticVersion};

use super::{connector::ShardConnector, frame::ShardFrame, IggyShard};

#[derive(Default)]
pub struct IggyShardBuilder {
    id: Option<u16>,
    connections: Option<Vec<ShardConnector<ShardFrame>>>,
    config: Option<ServerConfig>,
}

impl IggyShardBuilder {
    pub fn id(mut self, id: u16) -> Self {
        self.id = Some(id);
        self
    }

    pub fn connections(mut self, connections: Vec<ShardConnector<ShardFrame>>) -> Self {
        self.connections = Some(connections);
        self
    }

    pub fn server_config(mut self, config: ServerConfig) -> Self {
        self.config = Some(config);
        self
    }

    // TODO: Too much happens in there, some of those bootstrapping logic should be moved outside.
    pub async fn build(self) -> IggyShard {
        let id = self.id.unwrap();
        let config = self.config.unwrap();
        let connections = self.connections.unwrap();
        let (stop_sender, stop_receiver, frame_receiver) = connections
            .iter()
            .filter(|c| c.id == id)
            .map(|c| {
                (
                    c.stop_sender.clone(),
                    c.stop_receiver.clone(),
                    c.receiver.clone(),
                )
            })
            .next()
            .expect("Failed to find connection with the specified ID");
        let shards = connections.into_iter().map(Shard::new).collect();
        let version = SemanticVersion::current().expect("Invalid version");

        info!(
            "Server-side encryption is {}.",
            map_toggle_str(config.system.encryption.enabled)
        );
        let encryptor: Option<Arc<EncryptorKind>> = match config.system.encryption.enabled {
            true => Some(Arc::new(EncryptorKind::Aes256Gcm(
                Aes256GcmEncryptor::from_base64_key(&config.system.encryption.key).unwrap(),
            ))),
            false => None,
        };

        let state_persister = Self::resolve_persister(config.system.state.enforce_fsync);
        let state = Rc::new(StateKind::File(FileState::new(
            &config.system.get_state_messages_file_path(),
            &version,
            state_persister,
            encryptor.clone(),
        )));

        let partition_persister = Self::resolve_persister(config.system.partition.enforce_fsync);
        let storage = SystemStorage::new(config.system, partition_persister);

        IggyShard {
                id: id,
                shards: shards,
                shards_table: Default::default(),
                storage: storage,
                state: state,
                config: config,
                stop_receiver: stop_receiver,
                stop_sender: stop_sender,
                frame_receiver: Cell::new(Some(frame_receiver)),
            }
    }

    fn resolve_persister(enforce_fsync: bool) -> Arc<PersisterKind> {
        match enforce_fsync {
            true => Arc::new(PersisterKind::FileWithSync(FileWithSyncPersister)),
            false => Arc::new(PersisterKind::File(FilePersister)),
        }
    }
}
