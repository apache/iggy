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
pub mod system;
pub mod task_registry;
pub mod tasks;
pub mod transmission;

mod communication;
mod handlers;

// Re-export for backwards compatibility
pub use communication::calculate_shard_assignment;

use self::tasks::{continuous, periodic};
use crate::{
    configs::server::ServerConfig,
    io::fs_locks::FsLocks,
    shard::{
        namespace::IggyNamespace,
        task_registry::TaskRegistry,
        transmission::{
            event::ShardEvent,
            frame::{ShardFrame, ShardResponse},
            message::ShardMessage,
        },
    },
    slab::{streams::Streams, traits_ext::EntityMarker, users::Users},
    state::file::FileState,
    streaming::{
        clients::client_manager::ClientManager, diagnostics::metrics::Metrics, session::Session,
        users::permissioner::Permissioner, utils::ptr::EternalPtr,
    },
    versioning::SemanticVersion,
};
use builder::IggyShardBuilder;
use dashmap::DashMap;
use iggy_common::{EncryptorKind, Identifier, IggyError};
use std::{
    cell::{Cell, RefCell},
    net::SocketAddr,
    rc::Rc,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};
use tracing::{debug, error, info, instrument};
use transmission::{
    connector::{Receiver, ShardConnector, StopReceiver},
    id::ShardId,
};

pub const COMPONENT: &str = "SHARD";
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
pub const BROADCAST_TIMEOUT: Duration = Duration::from_secs(500);

pub struct IggyShard {
    pub id: u16,
    shards: Vec<ShardConnector<ShardFrame>>,
    _version: SemanticVersion,

    pub(crate) streams: Streams,
    pub(crate) shards_table: EternalPtr<DashMap<IggyNamespace, ShardId>>,
    pub(crate) state: FileState,

    pub(crate) fs_locks: FsLocks,
    pub(crate) encryptor: Option<EncryptorKind>,
    pub(crate) config: ServerConfig,
    pub(crate) client_manager: ClientManager,
    pub(crate) permissioner: RefCell<Permissioner>,
    pub(crate) users: Users,
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
            let shutdown_success = shard_for_shutdown.trigger_shutdown().await;
            if !shutdown_success {
                error!("shutdown timed out");
            }
            let _ = shutdown_complete_tx.send(()).await;
        })
        .detach();

        let elapsed = now.elapsed();
        info!("Initialized in {} ms.", elapsed.as_millis());

        shutdown_complete_rx.recv().await.ok();
        Ok(())
    }

    async fn load_segments(&self) -> Result<(), IggyError> {
        use crate::bootstrap::load_segments;
        for shard_entry in self.shards_table.iter() {
            let (namespace, shard_id) = shard_entry.pair();

            if **shard_id == self.id {
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
                let stats = self.streams.with_partition_by_id(
                    &Identifier::numeric(stream_id as u32).unwrap(),
                    &Identifier::numeric(topic_id as u32).unwrap(),
                    partition_id,
                    |(_, stats, ..)| stats.clone(),
                );
                match load_segments(
                    &self.config.system,
                    stream_id,
                    topic_id,
                    partition_id,
                    partition_path,
                    stats,
                )
                .await
                {
                    Ok(loaded_log) => {
                        self.streams.with_partition_by_id_mut(
                            &Identifier::numeric(stream_id as u32).unwrap(),
                            &Identifier::numeric(topic_id as u32).unwrap(),
                            partition_id,
                            |(_, _, _, offset, .., log)| {
                                *log = loaded_log;
                                let current_offset = log.active_segment().end_offset;
                                offset.store(current_offset, Ordering::Relaxed);
                            },
                        );
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
        let users_list = self.users.values();
        let users_count = users_list.len();
        self.permissioner
            .borrow_mut()
            .init(&users_list.iter().collect::<Vec<_>>());
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

    pub async fn handle_shard_message(&self, message: ShardMessage) -> Option<ShardResponse> {
        handlers::handle_shard_message(self, message).await
    }

    pub(crate) async fn handle_event(&self, event: ShardEvent) -> Result<(), IggyError> {
        handlers::handle_event(self, event).await
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
