/* Licensed to the Apache Software Foundation (ASF) under one
inner() * or more contributor license agreements.  See the NOTICE file
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

pub mod builder;
pub mod namespace;
pub mod shards_table;
pub mod system;
pub mod task_registry;
pub mod tasks;
pub mod transmission;

mod communication;
pub mod handlers;
mod lazy_init;

// Re-export for backwards compatibility
pub use communication::calculate_shard_assignment;

use self::tasks::{continuous, periodic};
use crate::{
    bootstrap::load_segments,
    configs::server::ServerConfig,
    io::fs_locks::FsLocks,
    metadata::{SharedConsumerOffsetsStore, SharedMetadata, SharedStatsStore},
    partition_store::{PartitionData, PartitionDataStore},
    shard::{
        shards_table::ShardsTable, task_registry::TaskRegistry, transmission::frame::ShardFrame,
    },
    state::file::FileState,
    streaming::{
        clients::client_manager::ClientManager,
        diagnostics::metrics::Metrics,
        partitions::{
            helpers::create_message_deduplicator,
            journal::{Inner as JournalInner, Journal},
            partition::{ConsumerGroupOffsets, ConsumerOffsets},
        },
        session::Session,
        users::permissioner::Permissioner,
        utils::ptr::EternalPtr,
    },
    versioning::SemanticVersion,
};
use builder::IggyShardBuilder;
use iggy_common::sharding::IggyNamespace;
use iggy_common::{EncryptorKind, IggyError, IggyTimestamp};
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

pub const COMPONENT: &str = "SHARD";
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
pub const BROADCAST_TIMEOUT: Duration = Duration::from_secs(500);

pub struct IggyShard {
    pub id: u16,
    shards: Vec<ShardConnector<ShardFrame>>,
    _version: SemanticVersion,

    /// Shard routing table (ArcSwap + imbl::HashMap).
    pub(crate) shards_table: EternalPtr<ShardsTable>,

    pub(crate) state: FileState,

    /// Shared metadata accessible by all shards (ArcSwap-based).
    pub(crate) shared_metadata: EternalPtr<SharedMetadata>,

    /// Shared stats store for cross-shard stats visibility.
    pub(crate) shared_stats: EternalPtr<SharedStatsStore>,

    /// Shared consumer offsets store for cross-shard offset visibility.
    pub(crate) shared_consumer_offsets: EternalPtr<SharedConsumerOffsetsStore>,

    /// Per-shard partition data storage (flat HashMap, single-threaded).
    /// Used for direct partition data access without slab traversal.
    pub(crate) partition_store: RefCell<PartitionDataStore>,

    /// Consumer group member partition indices for round-robin partition selection.
    /// Key: (stream_id, topic_id, group_id, client_id), Value: current partition index.
    pub(crate) cg_partition_indices:
        RefCell<std::collections::HashMap<(usize, usize, usize, u32), usize>>,

    pub(crate) fs_locks: FsLocks,
    pub(crate) encryptor: Option<EncryptorKind>,
    pub(crate) config: ServerConfig,
    pub(crate) client_manager: ClientManager,
    pub(crate) permissioner: Permissioner,
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

    pub async fn init(&self) -> Result<(), IggyError> {
        self.load_segments().await?;
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
        for (namespace, location) in self.shards_table.iter() {
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

                // Get shared stats from SharedStatsStore
                let stats = self
                    .shared_stats
                    .get_partition_stats(stream_id, topic_id, partition_id)
                    .unwrap_or_else(
                        || Arc::new(crate::streaming::stats::PartitionStats::default()),
                    );

                // Populate partition_store with loaded segments
                self.populate_partition_store(
                    &namespace,
                    stream_id,
                    topic_id,
                    partition_id,
                    &partition_path,
                    &stats,
                )
                .await?;

                info!(
                    "Successfully loaded segments for stream: {}, topic: {}, partition: {}",
                    stream_id, topic_id, partition_id
                );
            }
        }

        Ok(())
    }

    /// Populate partition_store with SegmentedLog loaded from disk.
    async fn populate_partition_store(
        &self,
        namespace: &IggyNamespace,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        partition_path: &str,
        stats: &Arc<crate::streaming::stats::PartitionStats>,
    ) -> Result<(), IggyError> {
        // Load segments for partition_store
        let mut log = load_segments(
            &self.config.system,
            stream_id,
            topic_id,
            partition_id,
            partition_path.to_string(),
            Arc::clone(stats),
        )
        .await?;

        let current_offset = log.active_segment().end_offset;

        // Determine should_increment_offset based on whether there are existing messages
        // If any segment has data (current_position > 0), we should increment from the current offset
        // Otherwise, for empty partitions, first message should get offset 0
        let should_increment_offset = log
            .segments()
            .iter()
            .any(|segment| segment.current_position > 0);

        // Initialize the journal's base_offset so new messages get correct offsets.
        // After loading segments with messages, journal base_offset must be current_offset + 1.
        if should_increment_offset {
            log.journal_mut().init(JournalInner {
                base_offset: current_offset + 1,
                current_offset,
                ..Default::default()
            });
        }

        // Create message deduplicator if configured
        let message_deduplicator = create_message_deduplicator(&self.config.system).map(Arc::new);

        // Get shared offset from SharedMetadata's PartitionMeta (source of truth)
        let offset = self
            .shared_metadata
            .get_partition(namespace)
            .map(|pm| Arc::clone(&pm.offset))
            .unwrap_or_else(|| Arc::new(AtomicU64::new(current_offset)));

        // Get or create consumer offset structures
        let consumer_offsets = Arc::new(ConsumerOffsets::with_capacity(10));
        let consumer_group_offsets = Arc::new(ConsumerGroupOffsets::with_capacity(10));

        // Register consumer offsets in SharedConsumerOffsetsStore for cross-shard visibility
        self.shared_consumer_offsets.register_consumer_offsets(
            stream_id,
            topic_id,
            partition_id,
            Arc::clone(&consumer_offsets),
        );
        self.shared_consumer_offsets
            .register_consumer_group_offsets(
                stream_id,
                topic_id,
                partition_id,
                Arc::clone(&consumer_group_offsets),
            );

        let partition_data = PartitionData::new(
            log,
            offset,
            consumer_offsets,
            consumer_group_offsets,
            message_deduplicator,
            Arc::clone(stats),
            IggyTimestamp::now(),
            should_increment_offset,
        );

        self.partition_store
            .borrow_mut()
            .insert(*namespace, partition_data);

        debug!(
            "Populated partition_store for {:?} on shard {}",
            namespace, self.id
        );

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
