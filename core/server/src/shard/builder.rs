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

use std::{cell::Cell, rc::Rc, sync::atomic::AtomicBool};

use iggy_common::{Aes256GcmEncryptor, EncryptorKind};
use tracing::info;

use crate::{
    archiver::{Archiver, ArchiverKind},
    bootstrap::resolve_persister,
    configs::server::ServerConfig,
    map_toggle_str,
    shard::{Shard, task_registry::TaskRegistry},
    state::{StateKind, file::FileState},
    streaming::{diagnostics::metrics::Metrics, storage::SystemStorage},
    versioning::SemanticVersion,
};

use super::{IggyShard, transmission::connector::ShardConnector, transmission::frame::ShardFrame};

#[derive(Default)]
pub struct IggyShardBuilder {
    id: Option<u16>,
    connections: Option<Vec<ShardConnector<ShardFrame>>>,
    config: Option<ServerConfig>,
    encryptor: Option<EncryptorKind>,
    version: Option<SemanticVersion>,
    archiver: Option<ArchiverKind>,
    state: Option<StateKind>,
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

    pub fn config(mut self, config: ServerConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn encryptor(mut self, encryptor: Option<EncryptorKind>) -> Self {
        self.encryptor = encryptor;
        self
    }

    pub fn version(mut self, version: SemanticVersion) -> Self {
        self.version = Some(version);
        self
    }

    pub fn state(mut self, state: StateKind) -> Self {
        self.state = Some(state);
        self
    }

    pub fn archiver(mut self, archiver: Option<ArchiverKind>) -> Self {
        self.archiver = archiver;
        self
    }

    // TODO: Too much happens in there, some of those bootstrapping logic should be moved outside.
    pub fn build(self) -> IggyShard {
        let id = self.id.unwrap();
        let config = self.config.unwrap();
        let connections = self.connections.unwrap();
        let state = self.state.unwrap();
        let encryptor = self.encryptor;
        let version = self.version.unwrap();
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
        //TODO: Eghhhh.......
        let partition_persister = resolve_persister(config.system.partition.enforce_fsync);
        let storage = Rc::new(SystemStorage::new(
            config.system.clone(),
            partition_persister,
        ));
        let archiver = self.archiver.map(Rc::new);

        IggyShard {
            id: id,
            shards: shards,
            shards_table: Default::default(),
            storage: storage,
            encryptor: encryptor,
            archiver: archiver,
            state: state,
            config: config,
            version: version,
            stop_receiver: stop_receiver,
            stop_sender: stop_sender,
            messages_receiver: Cell::new(Some(frame_receiver)),
            metrics: Metrics::init(),
            task_registry: TaskRegistry::new(),
            is_shutting_down: AtomicBool::new(false),
            tcp_bound_address: Cell::new(None),

            users: Default::default(),
            permissioner: Default::default(),
            streams: Default::default(),
            streams_ids: Default::default(),
            client_manager: Default::default(),
            active_sessions: Default::default(),
        }
    }
}
