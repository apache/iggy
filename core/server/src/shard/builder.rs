/*
 * Licensed to the Apache Software Foundation (ASF) under one
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
    IggyShard, TaskRegistry, transmission::connector::ShardConnector,
    transmission::frame::ShardFrame,
};
use crate::metadata::{Metadata, MetadataWriter};
use crate::streaming::partitions::local_partitions::LocalPartitions;
use crate::{
    configs::server::ServerConfig,
    state::file::FileState,
    streaming::{
        clients::client_manager::ClientManager, diagnostics::metrics::Metrics,
        utils::ptr::EternalPtr,
    },
};
use ahash::AHashSet;
use dashmap::DashMap;
use iggy_common::EncryptorKind;
use iggy_common::SemanticVersion;
use iggy_common::sharding::{IggyNamespace, PartitionLocation};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::atomic::AtomicBool,
};

#[derive(Default)]
pub struct IggyShardBuilder {
    id: Option<u16>,
    shards_table: Option<EternalPtr<DashMap<IggyNamespace, PartitionLocation>>>,
    state: Option<FileState>,
    client_manager: Option<ClientManager>,
    connections: Option<Vec<ShardConnector<ShardFrame>>>,
    config: Option<ServerConfig>,
    encryptor: Option<EncryptorKind>,
    version: Option<SemanticVersion>,
    metrics: Option<Metrics>,
    is_follower: bool,
    metadata: Option<Metadata>,
    metadata_writer: Option<MetadataWriter>,
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

    pub fn shards_table(
        mut self,
        shards_table: EternalPtr<DashMap<IggyNamespace, PartitionLocation>>,
    ) -> Self {
        self.shards_table = Some(shards_table);
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

    pub fn metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn metadata_writer(mut self, metadata_writer: MetadataWriter) -> Self {
        self.metadata_writer = Some(metadata_writer);
        self
    }

    // TODO: Too much happens in there, some of those bootstrapping logic should be moved outside.
    pub fn build(self) -> IggyShard {
        let id = self.id.unwrap();
        let shards_table = self.shards_table.unwrap();
        let state = self.state.unwrap();
        let config = self.config.unwrap();
        let connections = self.connections.unwrap();
        let encryptor = self.encryptor;
        let client_manager = self.client_manager.unwrap();
        let version = self.version.unwrap();
        let metadata = self.metadata.expect("metadata is required");
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

        // Create per-shard stores (wrapped in RefCell for interior mutability)
        let local_partitions = RefCell::new(LocalPartitions::new());

        IggyShard {
            id,
            shards,
            shards_table,
            metadata,
            metadata_writer: self.metadata_writer.map(RefCell::new),
            local_partitions,
            pending_partition_inits: RefCell::new(AHashSet::new()),
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
            client_manager,
        }
    }
}
