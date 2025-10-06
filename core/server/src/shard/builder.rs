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
    IggyShard, TaskRegistry, transmission::connector::ShardConnector,
    transmission::frame::ShardFrame,
};
use crate::{
    configs::server::ServerConfig,
    shard::{Shard, ShardInfo, namespace::IggyNamespace},
    slab::streams::Streams,
    state::StateKind,
    streaming::{diagnostics::metrics::Metrics, users::user::User, utils::ptr::EternalPtr},
    versioning::SemanticVersion,
};
use ahash::HashMap;
use dashmap::DashMap;
use iggy_common::{EncryptorKind, UserId};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::atomic::AtomicBool,
};

#[derive(Default)]
pub struct IggyShardBuilder {
    id: Option<u16>,
    streams: Option<Streams>,
    shards_table: Option<EternalPtr<DashMap<IggyNamespace, ShardInfo>>>,
    state: Option<StateKind>,
    users: Option<HashMap<UserId, User>>,
    connections: Option<Vec<ShardConnector<ShardFrame>>>,
    config: Option<ServerConfig>,
    encryptor: Option<EncryptorKind>,
    version: Option<SemanticVersion>,
    metrics: Option<Metrics>,
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
        shards_table: EternalPtr<DashMap<IggyNamespace, ShardInfo>>,
    ) -> Self {
        self.shards_table = Some(shards_table);
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

    pub fn streams(mut self, streams: Streams) -> Self {
        self.streams = Some(streams);
        self
    }

    pub fn state(mut self, state: StateKind) -> Self {
        self.state = Some(state);
        self
    }

    pub fn users(mut self, users: HashMap<UserId, User>) -> Self {
        self.users = Some(users);
        self
    }

    pub fn metrics(mut self, metrics: Metrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    // TODO: Too much happens in there, some of those bootstrapping logic should be moved outside.
    pub fn build(self) -> IggyShard {
        let id = self.id.unwrap();
        let streams = self.streams.unwrap();
        let shards_table = self.shards_table.unwrap();
        let state = self.state.unwrap();
        let users = self.users.unwrap();
        let config = self.config.unwrap();
        let connections = self.connections.unwrap();
        let encryptor = self.encryptor;
        let version = self.version.unwrap();
        let (_, stop_receiver, frame_receiver) = connections
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

        // Initialize metrics
        let metrics = self.metrics.unwrap_or_else(Metrics::init);

        // Create TaskRegistry for this shard
        let task_registry = Rc::new(TaskRegistry::new(id));

        // Create notification channel for config writer
        let (config_writer_notify, config_writer_receiver) = async_channel::bounded(1);

        // Trigger initial check in case servers bind before task starts
        let _ = config_writer_notify.try_send(());

        IggyShard {
            id,
            shards,
            shards_table,
            streams2: streams, // TODO: Fixme
            users: RefCell::new(users),
            encryptor,
            config,
            _version: version,
            state,
            stop_receiver,
            messages_receiver: Cell::new(Some(frame_receiver)),
            metrics,
            is_shutting_down: AtomicBool::new(false),
            tcp_bound_address: Cell::new(None),
            quic_bound_address: Cell::new(None),
            http_bound_address: Cell::new(None),
            config_writer_notify,
            config_writer_receiver,
            task_registry,
            permissioner: Default::default(),
            client_manager: Default::default(),
            active_sessions: Default::default(),
        }
    }
}
