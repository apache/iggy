// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use self::tasks::{continuous, periodic};
use crate::{
    bootstrap::load_segments,
    configs::server::ServerConfig,
    metadata::{Metadata, MetadataWriter},
    shard::{
        task_registry::TaskRegistry, transmission::frame::ShardFrame,
        waiters::PollWaiterRegistry,
    },
    state::file::FileState,
    streaming::{
        clients::client_manager::ClientManager,
        diagnostics::metrics::Metrics,
        partitions::{local_partition::LocalPartition, local_partitions::LocalPartitions},
        session::Session,
        utils::ptr::EternalPtr,
    },
};
use ahash::AHashSet;
use builder::IggyShardBuilder;
use dashmap::DashMap;
use iggy_common::SemanticVersion;
use iggy_common::{EncryptorKind, IggyByteSize, IggyError};
use server_common::sharding::{IggyNamespace, PartitionLocation};
use std::{
    cell::{Cell, RefCell},
    net::SocketAddr,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tracing::{debug, error, info, instrument, warn};
use transmission::connector::{Receiver, ShardConnector, StopReceiver};

pub mod builder;
pub mod execution;
pub mod handlers;
pub mod system;
pub mod task_registry;
pub mod tasks;
pub mod transmission;
pub mod waiters;

#[cfg(feature = "systemd")]
pub mod systemd;

mod communication;

pub use communication::calculate_shard_assignment;

pub const COMPONENT: &str = "SHARD";
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
pub const BROADCAST_TIMEOUT: Duration = Duration::from_secs(20);

pub struct IggyShard {
    pub id: u16,
    shards: Vec<ShardConnector<ShardFrame>>,
    _version: SemanticVersion,

    pub(crate) metadata: Metadata,
    pub(crate) metadata_writer: Option<RefCell<MetadataWriter>>,
    pub(crate) local_partitions: RefCell<LocalPartitions>,
    pub(crate) pending_partition_inits: RefCell<AHashSet<IggyNamespace>>,
    pub(crate) poll_waiters: RefCell<PollWaiterRegistry>,

    pub(crate) shards_table: EternalPtr<DashMap<IggyNamespace, PartitionLocation>>,
    pub(crate) state: FileState,

    pub(crate) encryptor: Option<EncryptorKind>,
    pub(crate) config: ServerConfig,
    pub(crate) client_manager: ClientManager,
    pub(crate) metrics: Metrics,
    pub(crate) is_follower: bool,
    /// Index into `config.cluster.nodes` that describes this running node.
    /// `Some` only when cluster mode is enabled; validated at bootstrap to
    /// match exactly one entry in the nodes list.
    pub(crate) current_replica_id: Option<u8>,
    pub messages_receiver: Cell<Option<Receiver<ShardFrame>>>,
    pub(crate) stop_receiver: StopReceiver,
    pub(crate) is_shutting_down: AtomicBool,
    pub(crate) tcp_bound_address: Cell<Option<SocketAddr>>,
    pub(crate) quic_bound_address: Cell<Option<SocketAddr>>,
    pub(crate) websocket_bound_address: Cell<Option<SocketAddr>>,
    pub(crate) http_bound_address: Cell<Option<SocketAddr>>,
    pub(crate) config_writer_notify: async_channel::Sender<()>,
    config_writer_receiver: async_channel::Receiver<()>,
    pub(crate) task_registry: Rc<TaskRegistry>,
}

impl IggyShard {
    pub fn builder() -> IggyShardBuilder {
        Default::default()
    }

    pub fn writer(&self) -> std::cell::RefMut<'_, crate::metadata::MetadataWriter> {
        self.metadata_writer
            .as_ref()
            .expect("MetadataWriter only available on shard 0")
            .borrow_mut()
    }

    pub async fn init(&self) -> Result<(), IggyError> {
        self.load_segments().await?;
        let _ = self.load_users().await;
        Ok(())
    }

    fn init_tasks(self: &Rc<Self>) {
        continuous::spawn_message_pump(self.clone());

        // Spawn config writer task on shard 0 if we need to wait for bound addresses
        if self.id == 0
            && (self.config.tcp.enabled
                || self.config.quic.enabled
                || self.config.http.enabled
                || self.config.websocket.enabled)
        {
            tasks::oneshot::spawn_config_writer_task(self);
        }

        if self.config.tcp.enabled {
            continuous::spawn_tcp_server(self.clone());
        }

        if self.config.http.enabled && self.id == 0 {
            continuous::spawn_http_server(self.clone());
        }

        // JWT token cleaner task is spawned inside HTTP server because it needs `AppState`.

        // TODO(hubcio): QUIC doesn't properly work on all shards, especially tests `concurrent` and `system_scenario`.
        // it's probably related to Endpoint not Cloned between shards, but all shards are creating its own instance.
        // This way packet CID is invalid. (crypto-related stuff)
        if self.config.quic.enabled && self.id == 0 {
            continuous::spawn_quic_server(self.clone());
        }
        if self.config.websocket.enabled {
            continuous::spawn_websocket_server(self.clone());
        }

        if self.config.message_saver.enabled {
            periodic::spawn_message_saver(self.clone());
        }

        if self.config.data_maintenance.messages.cleaner_enabled {
            periodic::spawn_message_cleaner(self.clone());
        }

        if self.config.heartbeat.enabled {
            periodic::spawn_heartbeat_verifier(self.clone());
        }

        if self.config.personal_access_token.cleaner.enabled {
            periodic::spawn_personal_access_token_cleaner(self.clone());
        }

        if self.id == 0 {
            periodic::spawn_revocation_timeout_checker(self.clone());
        }

        if !self.config.system.logging.sysinfo_print_interval.is_zero() && self.id == 0 {
            periodic::spawn_sysinfo_printer(self.clone());
        }

        #[cfg(feature = "systemd")]
        if self.id == 0 {
            periodic::spawn_systemd_watchdog(self.clone());
        }
    }

    pub async fn run(self: &Rc<Self>) -> Result<(), IggyError> {
        let now: Instant = Instant::now();

        info!("Starting...");
        self.init().await?;

        // TODO: Fixme
        //self.assert_init();

        self.init_tasks();
        let (shutdown_complete_tx, shutdown_complete_rx) = async_channel::bounded(1);
        let stop_receiver = self.get_stop_receiver();
        let shard_for_shutdown = self.clone();

        // Spawn shutdown handler
        compio::runtime::spawn(async move {
            let _ = stop_receiver.recv().await;
            #[cfg(feature = "systemd")]
            if shard_for_shutdown.id == 0 {
                systemd::notify_stopping();
            }
            let drained = shard_for_shutdown.trigger_shutdown().await;
            #[cfg(feature = "systemd")]
            if shard_for_shutdown.id == 0 && !drained {
                warn!("Graceful shutdown timed out; some tasks did not drain in time");
                systemd::notify_status("graceful shutdown timed out");
            }
            #[cfg(not(feature = "systemd"))]
            let _ = drained;
            let _ = shutdown_complete_tx.send(()).await;
        })
        .detach();

        let elapsed = now.elapsed();
        info!("Initialized in {} ms.", elapsed.as_millis());

        shutdown_complete_rx.recv().await.ok();
        Ok(())
    }

    async fn load_segments(&self) -> Result<(), IggyError> {
        for shard_entry in self.shards_table.iter() {
            let (namespace, location) = shard_entry.pair();

            if *location.shard_id == self.id {
                let stream_id = namespace.stream_id();
                let topic_id: usize = namespace.topic_id();
                let partition_id = namespace.partition_id();

                info!(
                    "Loading segments for stream: {}, topic: {}, partition: {}",
                    stream_id, topic_id, partition_id
                );

                let partition_path =
                    self.config
                        .system
                        .get_partition_path(stream_id, topic_id, partition_id);

                let init_info = self
                    .metadata
                    .get_partition_init_info(stream_id, topic_id, partition_id)
                    .expect("Partition must exist in SharedMetadata");
                let created_at = init_info.created_at;
                let stats = init_info.stats;

                use crate::streaming::partitions::helpers::create_message_deduplicator;
                use crate::streaming::partitions::storage::{
                    load_consumer_group_offsets, load_consumer_offsets,
                };

                let consumer_offset_path =
                    self.config
                        .system
                        .get_consumer_offsets_path(stream_id, topic_id, partition_id);
