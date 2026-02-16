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

use self::tasks::{continuous, periodic};
use crate::{
    bootstrap::load_segments,
    configs::server::ServerConfig,
    metadata::{Metadata, MetadataWriter},
    shard::{task_registry::TaskRegistry, transmission::frame::ShardFrame},
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
use iggy_common::sharding::{IggyNamespace, PartitionLocation};
use iggy_common::{EncryptorKind, IggyError};
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
use tracing::{debug, error, info, instrument};
use transmission::connector::{Receiver, ShardConnector, StopReceiver};

pub mod builder;
pub mod execution;
pub mod handlers;
pub mod system;
pub mod task_registry;
pub mod tasks;
pub mod transmission;

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

    pub(crate) shards_table: EternalPtr<DashMap<IggyNamespace, PartitionLocation>>,
    pub(crate) state: FileState,

    pub(crate) encryptor: Option<EncryptorKind>,
    pub(crate) config: ServerConfig,
    pub(crate) client_manager: ClientManager,
    pub(crate) metrics: Metrics,
    pub(crate) is_follower: bool,
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
            shard_for_shutdown.trigger_shutdown().await;
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
                let consumer_group_offsets_path = self
                    .config
                    .system
                    .get_consumer_group_offsets_path(stream_id, topic_id, partition_id);

                // Reuse metadata's Arcs so both metadata and local_partitions
                // reference the same allocation — writes via store_consumer_offset
                // (metadata path) are visible to delete_oldest_segments (local path).
                let consumer_offsets = init_info.consumer_offsets;
                let consumer_group_offsets = init_info.consumer_group_offsets;

                {
                    let guard = consumer_offsets.pin();
                    for co in load_consumer_offsets(&consumer_offset_path).unwrap_or_default() {
                        guard.insert(co.consumer_id as usize, co);
                    }
                }

                {
                    let guard = consumer_group_offsets.pin();
                    for (cg_id, co) in load_consumer_group_offsets(&consumer_group_offsets_path)
                        .unwrap_or_default()
                    {
                        guard.insert(cg_id, co);
                    }
                }

                let message_deduplicator =
                    create_message_deduplicator(&self.config.system).map(Arc::new);

                match load_segments(
                    &self.config.system,
                    stream_id,
                    topic_id,
                    partition_id,
                    partition_path,
                    stats.clone(),
                )
                .await
                {
                    Ok(mut loaded_log) => {
                        if !loaded_log.has_segments() {
                            info!(
                                "No segments found on disk for partition ID: {} for topic ID: {} for stream ID: {}, creating initial segment",
                                partition_id, topic_id, stream_id
                            );
                            let segment = crate::streaming::segments::Segment::new(
                                0,
                                self.config.system.segment.size,
                            );
                            let storage =
                                crate::streaming::segments::storage::create_segment_storage(
                                    &self.config.system,
                                    stream_id,
                                    topic_id,
                                    partition_id,
                                    0,
                                    0,
                                    0,
                                )
                                .await?;
                            loaded_log.add_persisted_segment(segment, storage);
                            stats.increment_segments_count(1);
                        }

                        let current_offset = loaded_log.active_segment().end_offset;
                        stats.set_current_offset(current_offset);

                        // Only increment offset if we have messages (current_offset > 0).
                        // When current_offset is 0 and we have no messages, first message
                        // should get offset 0.
                        let should_increment_offset = current_offset > 0;

                        let revision_id = init_info.revision_id;

                        let partition = LocalPartition::with_log(
                            loaded_log,
                            stats,
                            Arc::new(AtomicU64::new(current_offset)),
                            consumer_offsets,
                            consumer_group_offsets,
                            message_deduplicator,
                            created_at,
                            revision_id,
                            should_increment_offset,
                        );

                        self.local_partitions
                            .borrow_mut()
                            .insert(*namespace, partition);

                        info!(
                            "Successfully loaded segments for stream: {}, topic: {}, partition: {}",
                            stream_id, topic_id, partition_id
                        );
                    }
                    Err(e) => {
                        error!(
                            "Failed to load segments for stream: {}, topic: {}, partition: {}: {}",
                            stream_id, topic_id, partition_id, e
                        );
                        return Err(e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn load_users(&self) -> Result<(), IggyError> {
        let users_count = self.metadata.users_count();
        self.metrics.increment_users(users_count as u32);
        info!("Initialized {} user(s).", users_count);
        Ok(())
    }

    pub fn assert_init(&self) -> Result<(), IggyError> {
        Ok(())
    }

    pub fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::Relaxed)
    }

    pub fn get_stop_receiver(&self) -> StopReceiver {
        self.stop_receiver.clone()
    }

    #[instrument(skip_all, name = "trace_shutdown")]
    pub async fn trigger_shutdown(&self) -> bool {
        self.is_shutting_down.store(true, Ordering::SeqCst);
        debug!("Shard {} shutdown state set", self.id);
        self.task_registry.graceful_shutdown(SHUTDOWN_TIMEOUT).await
    }

    pub fn get_available_shards_count(&self) -> u32 {
        self.shards.len() as u32
    }

    pub fn ensure_authenticated(&self, session: &Session) -> Result<(), IggyError> {
        if !session.is_active() {
            error!("{COMPONENT} - session is inactive, session: {session}");
            return Err(IggyError::StaleClient);
        }

        if session.is_authenticated() {
            Ok(())
        } else {
            error!("{COMPONENT} - unauthenticated access attempt, session: {session}");
            Err(IggyError::Unauthenticated)
        }
    }
}
