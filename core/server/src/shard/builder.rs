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

use super::{
    IggyShard, TaskRegistry, shards_table::ShardsTable, transmission::connector::ShardConnector,
    transmission::frame::ShardFrame,
};
use crate::{
    configs::server::ServerConfig,
    metadata::{SharedConsumerOffsetsStore, SharedMetadata, SharedStatsStore},
    partition_store::PartitionDataStore,
    state::file::FileState,
    streaming::{
        clients::client_manager::ClientManager, diagnostics::metrics::Metrics,
        users::permissioner::Permissioner, utils::ptr::EternalPtr,
    },
    versioning::SemanticVersion,
};
use iggy_common::EncryptorKind;
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::atomic::AtomicBool,
};

#[derive(Default)]
pub struct IggyShardBuilder {
    id: Option<u16>,
    shards_table: Option<EternalPtr<ShardsTable>>,
    shared_metadata: Option<EternalPtr<SharedMetadata>>,
    shared_stats: Option<EternalPtr<SharedStatsStore>>,
    shared_consumer_offsets: Option<EternalPtr<SharedConsumerOffsetsStore>>,
    state: Option<FileState>,
    client_manager: Option<ClientManager>,
    connections: Option<Vec<ShardConnector<ShardFrame>>>,
    config: Option<ServerConfig>,
    encryptor: Option<EncryptorKind>,
    version: Option<SemanticVersion>,
    metrics: Option<Metrics>,
    is_follower: bool,
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

    pub fn shards_table(mut self, shards_table: EternalPtr<ShardsTable>) -> Self {
        self.shards_table = Some(shards_table);
        self
    }

    pub fn shared_metadata(mut self, shared_metadata: EternalPtr<SharedMetadata>) -> Self {
        self.shared_metadata = Some(shared_metadata);
        self
    }

    pub fn shared_stats(mut self, shared_stats: EternalPtr<SharedStatsStore>) -> Self {
        self.shared_stats = Some(shared_stats);
        self
    }

    pub fn shared_consumer_offsets(
        mut self,
        shared_consumer_offsets: EternalPtr<SharedConsumerOffsetsStore>,
    ) -> Self {
        self.shared_consumer_offsets = Some(shared_consumer_offsets);
        self
    }

    pub fn clients_manager(mut self, client_manager: ClientManager) -> Self {
        self.client_manager = Some(client_manager);
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

    pub fn state(mut self, state: FileState) -> Self {
        self.state = Some(state);
        self
    }

    pub fn metrics(mut self, metrics: Metrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn is_follower(mut self, is_follower: bool) -> Self {
        self.is_follower = is_follower;
        self
    }

    // TODO: Too much happens in there, some of those bootstrapping logic should be moved outside.
    pub fn build(self) -> IggyShard {
        let id = self.id.unwrap();
        let shards_table = self.shards_table.unwrap();
        let shared_metadata = self.shared_metadata.unwrap();
        let shared_stats = self.shared_stats.unwrap();
        let shared_consumer_offsets = self.shared_consumer_offsets.unwrap();
        let state = self.state.unwrap();
        let config = self.config.unwrap();
        let connections = self.connections.unwrap();
        let encryptor = self.encryptor;
        let client_manager = self.client_manager.unwrap();
        let version = self.version.unwrap();
        let (stop_receiver, frame_receiver) = connections
            .iter()
            .filter(|c| c.id == id)
            .map(|c| (c.stop_receiver.clone(), c.receiver.clone()))
            .next()
            .expect("Failed to find connection with the specified ID");

        // Collect all stop_senders for broadcasting shutdown to all shards
        let all_stop_senders: Vec<_> = connections.iter().map(|c| c.stop_sender.clone()).collect();
        let shards = connections;

        // Initialize metrics
        let metrics = self.metrics.unwrap_or_else(Metrics::init);

        // Create TaskRegistry with all stop_senders for critical task failures
        let task_registry = Rc::new(TaskRegistry::new(id, all_stop_senders));

        // Create notification channel for config writer
        let (config_writer_notify, config_writer_receiver) = async_channel::bounded(1);

        // Trigger initial check in case servers bind before task starts
        let _ = config_writer_notify.try_send(());

        // Create permissioner with shared_metadata reference
        let permissioner = Permissioner::new(shared_metadata.clone());

        IggyShard {
            id,
            shards,
            shards_table,
            shared_metadata,
            shared_stats,
            shared_consumer_offsets,
            partition_store: RefCell::new(PartitionDataStore::new()),
            cg_partition_indices: RefCell::new(std::collections::HashMap::new()),
            fs_locks: Default::default(),
            encryptor,
            config,
            _version: version,
            state,
            stop_receiver,
            messages_receiver: Cell::new(Some(frame_receiver)),
            metrics,
            is_follower: self.is_follower,
            is_shutting_down: AtomicBool::new(false),
            tcp_bound_address: Cell::new(None),
            quic_bound_address: Cell::new(None),
            websocket_bound_address: Cell::new(None),
            http_bound_address: Cell::new(None),
            config_writer_notify,
            config_writer_receiver,
            task_registry,
            permissioner,
            client_manager,
        }
    }
}
