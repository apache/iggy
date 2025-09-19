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
pub mod logging;
pub mod namespace;
pub mod stats;
pub mod system;
pub mod task_registry;
pub mod tasks;
pub mod transmission;

use ahash::{AHashMap, AHashSet, HashMap};
use builder::IggyShardBuilder;
use dashmap::DashMap;
use error_set::ErrContext;
use futures::future::try_join_all;
use hash32::{Hasher, Murmur3Hasher};
use iggy_common::{
    EncryptorKind, Identifier, IggyError, IggyTimestamp, Permissions, PollingKind, UserId,
    UserStatus,
    defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME},
    locking::IggyRwLockFn,
};
use std::hash::Hasher as _;
use std::{
    cell::{Cell, RefCell},
    future::Future,
    net::SocketAddr,
    ops::Deref,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};
use tracing::{error, info, instrument, trace, warn};
use transmission::connector::{Receiver, ShardConnector, StopReceiver, StopSender};

use crate::{
    binary::handlers::messages::poll_messages_handler::IggyPollMetadata,
    configs::server::ServerConfig,
    http::http_server,
    io::fs_utils,
    shard::{
        namespace::{IggyFullNamespace, IggyNamespace},
        task_registry::TaskRegistry,
        tasks::messages::spawn_shard_message_task,
        transmission::{
            event::ShardEvent,
            frame::{ShardFrame, ShardResponse},
            message::{ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult},
        },
    },
    shard_error, shard_info, shard_warn,
    slab::{
        streams::Streams,
        traits_ext::{EntityComponentSystem, EntityMarker, Insert},
    },
    state::{
        StateKind,
        file::FileState,
        system::{StreamState, SystemState, UserState},
    },
    streaming::{
        clients::client_manager::ClientManager,
        diagnostics::metrics::Metrics,
        partitions,
        personal_access_tokens::personal_access_token::PersonalAccessToken,
        polling_consumer::PollingConsumer,
        session::Session,
        storage::SystemStorage,
        streams, topics,
        traits::MainOps,
        users::{permissioner::Permissioner, user::User},
        utils::ptr::EternalPtr,
    },
    tcp::tcp_server::spawn_tcp_server,
    versioning::SemanticVersion,
};

pub const COMPONENT: &str = "SHARD";
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

static USER_ID: AtomicU32 = AtomicU32::new(1);

type Task = Pin<Box<dyn Future<Output = Result<(), IggyError>>>>;

pub(crate) struct Shard {
    id: u16,
    connection: ShardConnector<ShardFrame>,
}

impl Shard {
    pub fn new(connection: ShardConnector<ShardFrame>) -> Self {
        Self {
            id: connection.id,
            connection,
        }
    }

    pub async fn send_request(&self, message: ShardMessage) -> Result<ShardResponse, IggyError> {
        let (sender, receiver) = async_channel::bounded(1);
        self.connection
            .sender
            .send(ShardFrame::new(message, Some(sender.clone()))); // Apparently sender needs to be cloned, otherwise channel will close...
        //TODO: Fixme
        let response = receiver.recv().await.map_err(|err| {
            error!("Failed to receive response from shard: {err}");
            IggyError::ShardCommunicationError(self.id)
        })?;
        Ok(response)
    }
}

// TODO: Maybe pad to cache line size?
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ShardInfo {
    pub id: u16,
}

impl ShardInfo {
    pub fn new(id: u16) -> Self {
        Self { id }
    }

    pub fn id(&self) -> u16 {
        self.id
    }
}

pub struct IggyShard {
    pub id: u16,
    shards: Vec<Shard>,
    version: SemanticVersion,

    // Heart transplant of the old streams structure.
    pub(crate) streams2: Streams,
    pub(crate) shards_table: EternalPtr<DashMap<IggyNamespace, ShardInfo>>,
    // TODO: Refactor.
    pub(crate) storage: Rc<SystemStorage>,
    pub(crate) state: StateKind,

    // Temporal...
    pub(crate) encryptor: Option<EncryptorKind>,
    pub(crate) config: ServerConfig,
    //TODO: This could be shared.
    pub(crate) client_manager: RefCell<ClientManager>,
    pub(crate) active_sessions: RefCell<Vec<Rc<Session>>>,
    pub(crate) permissioner: RefCell<Permissioner>,
    pub(crate) users: RefCell<HashMap<UserId, User>>,
    pub(crate) metrics: Metrics,
    pub messages_receiver: Cell<Option<Receiver<ShardFrame>>>,
    pub(crate) stop_receiver: StopReceiver,
    pub(crate) stop_sender: StopSender,
    pub(crate) task_registry: TaskRegistry,
    pub(crate) is_shutting_down: AtomicBool,
    pub(crate) tcp_bound_address: Cell<Option<SocketAddr>>,
    pub(crate) quic_bound_address: Cell<Option<SocketAddr>>,
}

impl IggyShard {
    pub fn builder() -> IggyShardBuilder {
        Default::default()
    }

    pub fn default_from_config(server_config: ServerConfig) -> Self {
        use crate::bootstrap::resolve_persister;
        use crate::streaming::storage::SystemStorage;
        use crate::versioning::SemanticVersion;

        let version = SemanticVersion::current().expect("Invalid version");
        let persister = resolve_persister(server_config.system.partition.enforce_fsync);
        let storage = Rc::new(SystemStorage::new(
            server_config.system.clone(),
            persister.clone(),
        ));

        let (stop_sender, stop_receiver) = async_channel::unbounded();

        let state_path = server_config.system.get_state_messages_file_path();
        let file_state = FileState::new(&state_path, &version, persister, None);
        let state = crate::state::StateKind::File(file_state);
        let shards_table = Box::new(DashMap::new());
        let shards_table = Box::leak(shards_table);

        let shard = Self {
            id: 0,
            shards: Vec::new(),
            shards_table: shards_table.into(),
            version,
            streams2: Default::default(),
            state,
            storage,
            //TODO: Fix
            encryptor: None,
            config: server_config,
            client_manager: Default::default(),
            active_sessions: Default::default(),
            permissioner: Default::default(),
            users: Default::default(),
            metrics: Metrics::init(),
            messages_receiver: Cell::new(None),
            stop_receiver,
            stop_sender,
            task_registry: TaskRegistry::new(),
            is_shutting_down: AtomicBool::new(false),
            tcp_bound_address: Cell::new(None),
            quic_bound_address: Cell::new(None),
        };
        let user = User::root(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD);
        shard
            .create_user_bypass_auth(
                &user.username,
                &user.password,
                UserStatus::Active,
                Some(Permissions::root()),
            )
            .unwrap();
        shard
    }

    pub async fn init(&self) -> Result<(), IggyError> {
        self.load_segments().await?;
        let _ = self.load_users().await;
        Ok(())
    }

    pub async fn run(self: &Rc<Self>) -> Result<(), IggyError> {
        // Workaround to ensure that the statistics are initialized before the server
        // loads streams and starts accepting connections. This is necessary to
        // have the correct statistics when the server starts.
        let now = Instant::now();
        self.get_stats().await?;
        shard_info!(self.id, "Starting...");
        self.init().await?;
        // TODO: Fixme
        //self.assert_init();

        // Create all tasks (tcp listener, http listener, command processor, in the future also the background jobs).
        let mut tasks: Vec<Task> = vec![Box::pin(spawn_shard_message_task(self.clone()))];
        if self.config.tcp.enabled {
            tasks.push(Box::pin(spawn_tcp_server(self.clone())));
        }

        if self.config.http.enabled && self.id == 0 {
            println!("Starting HTTP server on shard: {}", self.id);
            tasks.push(Box::pin(http_server::start(
                self.config.http.clone(),
                self.clone(),
            )));
        }

        if self.config.quic.enabled {
            tasks.push(Box::pin(crate::quic::quic_server::span_quic_server(
                self.clone(),
            )));
        }

        tasks.push(Box::pin(
            crate::channels::commands::clean_personal_access_tokens::clear_personal_access_tokens(
                self.clone(),
            ),
        ));
        // TOOD: Fixme, not always id 0 is the first shard.
        if self.id == 0 {
            tasks.push(Box::pin(
                crate::channels::commands::print_sysinfo::print_sys_info(self.clone()),
            ));
        }

        tasks.push(Box::pin(
            crate::channels::commands::verify_heartbeats::verify_heartbeats(self.clone()),
        ));
        tasks.push(Box::pin(
            crate::channels::commands::save_messages::save_messages(self.clone()),
        ));

        let stop_receiver = self.get_stop_receiver();
        let shard_for_shutdown = self.clone();

        /*
        compio::runtime::spawn(async move {
            let _ = stop_receiver.recv().await;
            info!("Shard {} received shutdown signal", shard_for_shutdown.id);

            let shutdown_success = shard_for_shutdown.trigger_shutdown().await;
            if !shutdown_success {
                shard_error!(shard_for_shutdown.id, "shutdown timed out");
            } else {
                shard_info!(shard_for_shutdown.id, "shutdown completed successfully");
            }
        });
        */

        let elapsed = now.elapsed();
        shard_info!(self.id, "Initialized in {} ms.", elapsed.as_millis());
        let result = try_join_all(tasks).await;
        result?;
        Ok(())
    }

    async fn load_segments(&self) -> Result<(), IggyError> {
        use crate::bootstrap::load_segments;
        for shard_entry in self.shards_table.iter() {
            let (namespace, shard_info) = shard_entry.pair();

            if shard_info.id == self.id {
                let stream_id = namespace.stream_id();
                let topic_id = namespace.topic_id();
                let partition_id = namespace.partition_id();

                shard_info!(
                    self.id,
                    "Loading segments for stream: {}, topic: {}, partition: {}",
                    stream_id,
                    topic_id,
                    partition_id
                );

                let partition_path =
                    self.config
                        .system
                        .get_partition_path(stream_id, topic_id, partition_id);
                let stats = self.streams2.with_partition_by_id(
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
                        self.streams2.with_partition_by_id_mut(
                            &Identifier::numeric(stream_id as u32).unwrap(),
                            &Identifier::numeric(topic_id as u32).unwrap(),
                            partition_id,
                            |(_, _, _, offset, .., log)| {
                                *log = loaded_log;
                                let current_offset = log.active_segment().end_offset;
                                offset.store(current_offset, Ordering::Relaxed);
                            },
                        );
                        shard_info!(
                            self.id,
                            "Successfully loaded segments for stream: {}, topic: {}, partition: {}",
                            stream_id,
                            topic_id,
                            partition_id
                        );
                    }
                    Err(e) => {
                        shard_error!(
                            self.id,
                            "Failed to load segments for stream: {}, topic: {}, partition: {}: {}",
                            stream_id,
                            topic_id,
                            partition_id,
                            e
                        );
                        return Err(e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn load_users(&self) -> Result<(), IggyError> {
        let users = self.users.borrow();
        let users_count = users.len();
        let current_user_id = users.keys().max().unwrap_or(&1);
        USER_ID.store(current_user_id + 1, Ordering::SeqCst);
        self.permissioner
            .borrow_mut()
            .init(&users.values().collect::<Vec<_>>());
        self.metrics.increment_users(users_count as u32);
        shard_info!(self.id, "Initialized {} user(s).", users_count);
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
        info!("Shard {} shutdown state set", self.id);
        self.task_registry.shutdown_all(SHUTDOWN_TIMEOUT).await
    }

    pub fn get_available_shards_count(&self) -> u32 {
        self.shards.len() as u32
    }

    pub async fn handle_shard_message(&self, message: ShardMessage) -> Option<ShardResponse> {
        match message {
            ShardMessage::Request(request) => match self.handle_request(request).await {
                Ok(response) => Some(response),
                Err(err) => Some(ShardResponse::ErrorResponse(err)),
            },
            ShardMessage::Event(event) => match self.handle_event(event).await {
                Ok(_) => Some(ShardResponse::Event),
                Err(err) => Some(ShardResponse::ErrorResponse(err)),
            },
        }
    }

    async fn handle_request(&self, request: ShardRequest) -> Result<ShardResponse, IggyError> {
        let stream_id = request.stream_id;
        let topic_id = request.topic_id;
        let partition_id = request.partition_id;
        match request.payload {
            ShardRequestPayload::SendMessages { batch } => {
                let ns = IggyFullNamespace::new(stream_id, topic_id, partition_id);
                let batch = self.maybe_encrypt_messages(batch)?;
                let messages_count = batch.count();
                self.streams2
                    .append_messages(self.id, &self.config.system, &ns, batch)
                    .await?;
                self.metrics.increment_messages(messages_count as u64);
                Ok(ShardResponse::SendMessages)
            }
            ShardRequestPayload::PollMessages { args, consumer } => {
                let current_offset = self.streams2.with_partition_by_id(
                    &stream_id,
                    &topic_id,
                    partition_id,
                    |(_, _, _, offset, ..)| offset.load(Ordering::Relaxed),
                );
                let metadata = IggyPollMetadata::new(partition_id as u32, current_offset);
                let count = args.count;
                let strategy = args.strategy;
                let value = strategy.value;
                let batches = match strategy.kind {
                    PollingKind::Offset => {
                        let offset = value;
                        // We have to remember to keep the invariant from the if that is on line 496.
                        // Alternatively a better design would be to get rid of that if and move the validations here.
                        let batches = self
                            .streams2
                            .get_messages_by_offset(
                                &stream_id,
                                &topic_id,
                                partition_id,
                                offset,
                                count,
                            )
                            .await?;
                        Ok(batches)
                    }
                    PollingKind::Timestamp => {
                        let timestamp = IggyTimestamp::from(value);
                        let timestamp_ts = timestamp.as_micros();
                        trace!(
                            "Getting {count} messages by timestamp: {} for partition: {}...",
                            timestamp_ts, partition_id
                        );

                        let batches = self
                            .streams2
                            .get_messages_by_timestamp(
                                &stream_id,
                                &topic_id,
                                partition_id,
                                timestamp_ts,
                                count,
                            )
                            .await?;
                        Ok(batches)
                    }
                    PollingKind::First => {
                        let first_offset = self.streams2.with_partition_by_id(
                            &stream_id,
                            &topic_id,
                            partition_id,
                            |(_, _, _, _, _, _, log)| {
                                log.segments()
                                    .first()
                                    .map(|segment| segment.start_offset)
                                    .unwrap_or(0)
                            },
                        );

                        let batches = self
                            .streams2
                            .get_messages_by_offset(
                                &stream_id,
                                &topic_id,
                                partition_id,
                                first_offset,
                                count,
                            )
                            .await?;
                        Ok(batches)
                    }
                    PollingKind::Last => {
                        let (start_offset, actual_count) = self.streams2.with_partition_by_id(
                            &stream_id,
                            &topic_id,
                            partition_id,
                            |(_, _, _, offset, _, _, _)| {
                                let current_offset = offset.load(Ordering::Relaxed);
                                let mut requested_count = 0;
                                if requested_count > current_offset + 1 {
                                    requested_count = current_offset + 1
                                }
                                let start_offset = 1 + current_offset - requested_count;
                                (start_offset, requested_count as u32)
                            },
                        );

                        let batches = self
                            .streams2
                            .get_messages_by_offset(
                                &stream_id,
                                &topic_id,
                                partition_id,
                                start_offset,
                                actual_count,
                            )
                            .await?;
                        Ok(batches)
                    }
                    PollingKind::Next => {
                        let (consumer_offset, consumer_id) = match consumer {
                            PollingConsumer::Consumer(consumer_id, _) => (
                                self.streams2
                                    .with_partition_by_id(
                                        &stream_id,
                                        &topic_id,
                                        partition_id,
                                        partitions::helpers::get_consumer_offset(consumer_id),
                                    )
                                    .map(|c_offset| c_offset.stored_offset),
                                consumer_id,
                            ),
                            PollingConsumer::ConsumerGroup(cg_id, _) => (
                                self.streams2
                                    .with_partition_by_id(
                                        &stream_id,
                                        &topic_id,
                                        partition_id,
                                        partitions::helpers::get_consumer_group_member_offset(
                                            cg_id,
                                        ),
                                    )
                                    .map(|cg_offset| cg_offset.stored_offset),
                                cg_id,
                            ),
                        };

                        let Some(consumer_offset) = consumer_offset else {
                            return Err(IggyError::ConsumerOffsetNotFound(consumer_id));
                        };
                        let offset = consumer_offset + 1;
                        trace!(
                            "Getting next messages for consumer id: {} for partition: {} from offset: {}...",
                            consumer_id, partition_id, offset
                        );
                        let batches = self
                            .streams2
                            .get_messages_by_offset(
                                &stream_id,
                                &topic_id,
                                partition_id,
                                offset,
                                count,
                            )
                            .await?;
                        Ok(batches)
                    }
                }?;

                let numeric_stream_id = self.streams2.with_stream_by_id(
                    &stream_id,
                    crate::streaming::streams::helpers::get_stream_id(),
                );
                let numeric_topic_id = self.streams2.with_topic_by_id(
                    &stream_id,
                    &topic_id,
                    crate::streaming::topics::helpers::get_topic_id(),
                );

                if args.auto_commit && !batches.is_empty() {
                    let offset = batches
                        .last_offset()
                        .expect("Batch set should have at least one batch");
                    trace!(
                        "Last offset: {} will be automatically stored for {}, stream: {}, topic: {}, partition: {}",
                        offset, consumer, numeric_stream_id, numeric_topic_id, partition_id
                    );
                    match consumer {
                        PollingConsumer::Consumer(consumer_id, _) => {
                            self.streams2.with_partition_by_id(
                                &stream_id,
                                &topic_id,
                                partition_id,
                                partitions::helpers::store_consumer_offset(
                                    consumer_id,
                                    numeric_stream_id,
                                    numeric_topic_id,
                                    partition_id,
                                    offset,
                                    &self.config.system,
                                ),
                            );
                            self.streams2
                                .with_partition_by_id_async(
                                    &stream_id,
                                    &topic_id,
                                    partition_id,
                                    partitions::helpers::persist_consumer_offset_to_disk(
                                        self.id,
                                        consumer_id,
                                    ),
                                )
                                .await?;
                        }
                        PollingConsumer::ConsumerGroup(cg_id, _) => {
                            self.streams2.with_partition_by_id(
                                &stream_id,
                                &topic_id,
                                partition_id,
                                partitions::helpers::store_consumer_group_member_offset(
                                    cg_id,
                                    numeric_stream_id,
                                    numeric_topic_id,
                                    partition_id,
                                    offset,
                                    &self.config.system,
                                ),
                            );
                            self.streams2.with_partition_by_id_async(
                                    &stream_id,
                                    &topic_id,
                                    partition_id,
                                    partitions::helpers::persist_consumer_group_member_offset_to_disk(
                                        self.id,
                                        cg_id,
                                    ),
                                )
                                .await?;
                        }
                    }
                }
                Ok(ShardResponse::PollMessages((metadata, batches)))
            }
        }
    }

    async fn handle_event(&self, event: ShardEvent) -> Result<(), IggyError> {
        match event {
            ShardEvent::LoginUser {
                client_id,
                username,
                password,
            } => self.login_user_event(client_id, &username, &password),
            ShardEvent::NewSession { address, transport } => {
                let session = self.add_client(&address, transport);
                self.add_active_session(session);
                Ok(())
            }
            ShardEvent::DeletedPartitions2 {
                stream_id,
                topic_id,
                partitions_count,
                partition_ids,
            } => {
                self.delete_partitions2_bypass_auth(
                    &stream_id,
                    &topic_id,
                    partitions_count,
                    partition_ids,
                )?;
                Ok(())
            }
            ShardEvent::UpdatedStream2 { stream_id, name } => {
                self.update_stream2_bypass_auth(&stream_id, &name)?;
                Ok(())
            }
            ShardEvent::PurgedStream2 { stream_id } => {
                self.purge_stream2_bypass_auth(&stream_id)?;
                Ok(())
            }
            ShardEvent::PurgedTopic {
                stream_id,
                topic_id,
            } => {
                todo!();
            }
            ShardEvent::CreatedUser {
                username,
                password,
                status,
                permissions,
            } => {
                self.create_user_bypass_auth(&username, &password, status, permissions.clone())?;
                Ok(())
            }
            ShardEvent::DeletedUser { user_id } => {
                self.delete_user_bypass_auth(&user_id)?;
                Ok(())
            }
            ShardEvent::LogoutUser { client_id } => {
                let sessions = self.active_sessions.borrow();
                let session = sessions.iter().find(|s| s.client_id == client_id).unwrap();
                self.logout_user(session)?;
                self.remove_active_session(session.get_user_id());

                Ok(())
            }
            ShardEvent::ChangedPassword {
                user_id,
                current_password,
                new_password,
            } => {
                self.change_password_bypass_auth(&user_id, &current_password, &new_password)?;
                Ok(())
            }
            ShardEvent::CreatedPersonalAccessToken {
                personal_access_token,
            } => {
                self.create_personal_access_token_bypass_auth(personal_access_token.to_owned())?;
                Ok(())
            }
            ShardEvent::DeletedPersonalAccessToken { user_id, name } => {
                self.delete_personal_access_token_bypass_auth(user_id, &name)?;
                Ok(())
            }
            ShardEvent::LoginWithPersonalAccessToken { token: _ } => todo!(),
            ShardEvent::UpdatedUser {
                user_id,
                username,
                status,
            } => {
                self.update_user_bypass_auth(&user_id, username.to_owned(), status)?;
                Ok(())
            }
            ShardEvent::UpdatedPermissions {
                user_id,
                permissions,
            } => {
                self.update_permissions_bypass_auth(&user_id, permissions.to_owned())?;
                Ok(())
            }
            ShardEvent::TcpBound { address } => {
                info!("Received TcpBound event with address: {}", address);
                self.tcp_bound_address.set(Some(address));
                Ok(())
            }
            ShardEvent::CreatedStream2 { id, stream } => {
                let stream_id = self.create_stream2_bypass_auth(stream);
                assert_eq!(stream_id, id);
                Ok(())
            }
            ShardEvent::DeletedStream2 { id, stream_id } => {
                let stream = self.delete_stream2_bypass_auth(&stream_id);
                assert_eq!(stream.id(), id);
                Ok(())
            }
            ShardEvent::CreatedTopic2 { stream_id, topic } => {
                let _topic_id = self.create_topic2_bypass_auth(&stream_id, topic);
                Ok(())
            }
            ShardEvent::CreatedPartitions2 {
                stream_id,
                topic_id,
                partitions,
            } => {
                self.create_partitions2_bypass_auth(&stream_id, &topic_id, partitions)
                    .await?;
                Ok(())
            }
            ShardEvent::DeletedTopic2 {
                id,
                stream_id,
                topic_id,
            } => {
                let topic = self.delete_topic_bypass_auth2(&stream_id, &topic_id);
                assert_eq!(topic.id(), id);
                Ok(())
            }
            ShardEvent::UpdatedTopic2 {
                stream_id,
                topic_id,
                name,
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor,
            } => {
                self.update_topic_bypass_auth2(
                    &stream_id,
                    &topic_id,
                    name.clone(),
                    message_expiry,
                    compression_algorithm,
                    max_topic_size,
                    replication_factor,
                )?;
                Ok(())
            }
            ShardEvent::CreatedConsumerGroup2 {
                stream_id,
                topic_id,
                cg,
            } => {
                let cg_id = cg.id();
                let id = self.create_consumer_group_bypass_auth2(&stream_id, &topic_id, cg);
                assert_eq!(id, cg_id);
                Ok(())
            }
            ShardEvent::DeletedConsumerGroup2 {
                id,
                stream_id,
                topic_id,
                group_id,
            } => {
                let cg = self.delete_consumer_group_bypass_auth2(&stream_id, &topic_id, &group_id);
                assert_eq!(cg.id(), id);
                Ok(())
            }
            ShardEvent::StoredOffset {
                stream_id,
                topic_id,
                partition_id,
                polling_consumer,
                offset,
            } => {
                self.store_consumer_offset_bypass_auth(
                    &stream_id,
                    &topic_id,
                    &polling_consumer,
                    partition_id,
                    offset,
                );
                Ok(())
            }
            ShardEvent::DeletedOffset {
                stream_id,
                topic_id,
                partition_id,
                polling_consumer,
            } => {
                self.delete_consumer_offset_bypass_auth(
                    &stream_id,
                    &topic_id,
                    &polling_consumer,
                    partition_id,
                )?;
                Ok(())
            }
        }
    }

    pub async fn send_request_to_shard_or_recoil(
        &self,
        namespace: &IggyNamespace,
        message: ShardMessage,
    ) -> Result<ShardSendRequestResult, IggyError> {
        if let Some(shard) = self.find_shard(namespace) {
            if shard.id == self.id {
                return Ok(ShardSendRequestResult::Recoil(message));
            }

            let response = match shard.send_request(message).await {
                Ok(response) => response,
                Err(err) => {
                    error!(
                        "{COMPONENT} - failed to send request to shard with ID: {}, error: {err}",
                        shard.id
                    );
                    return Err(err);
                }
            };
            Ok(ShardSendRequestResult::Response(response))
        } else {
            Err(IggyError::ShardNotFound(
                namespace.stream_id(),
                namespace.topic_id(),
                namespace.partition_id(),
            ))
        }
    }

    pub async fn broadcast_event_to_all_shards(&self, event: ShardEvent) -> Vec<ShardResponse> {
        let mut responses = Vec::with_capacity(self.get_available_shards_count() as usize);
        for maybe_receiver in self
            .shards
            .iter()
            .filter_map(|shard| {
                if shard.id != self.id {
                    Some(shard.connection.clone())
                } else {
                    None
                }
            })
            .map(|conn| {
                // TODO: Fixme, maybe we should send response_sender
                // and propagate errors back.
                let event = event.clone();
                if matches!(
                    &event,
                    ShardEvent::CreatedStream2 { .. }
                        | ShardEvent::DeletedStream2 { .. }
                        | ShardEvent::CreatedTopic2 { .. }
                        | ShardEvent::DeletedTopic2 { .. }
                        | ShardEvent::UpdatedTopic2 { .. }
                        | ShardEvent::CreatedPartitions2 { .. }
                        | ShardEvent::DeletedPartitions2 { .. }
                        | ShardEvent::CreatedConsumerGroup2 { .. }
                        | ShardEvent::DeletedConsumerGroup2 { .. }
                ) {
                    let (sender, receiver) = async_channel::bounded(1);
                    conn.send(ShardFrame::new(event.into(), Some(sender.clone())));
                    Some(receiver.clone())
                } else {
                    conn.send(ShardFrame::new(event.into(), None));
                    None
                }
            })
        {
            match maybe_receiver {
                Some(receiver) => {
                    let response = receiver.recv().await.unwrap();
                    responses.push(response);
                }
                None => {
                    responses.push(ShardResponse::Event);
                }
            }
        }
        responses
    }

    pub fn add_active_session(&self, session: Rc<Session>) {
        self.active_sessions.borrow_mut().push(session);
    }

    fn find_shard(&self, namespace: &IggyNamespace) -> Option<&Shard> {
        self.shards_table.get(namespace).map(|shard_info| {
            self.shards
                .iter()
                .find(|shard| shard.id == shard_info.id)
                .expect("Shard not found in the shards table.")
        })
    }

    pub fn find_shard_table_record(&self, namespace: &IggyNamespace) -> Option<ShardInfo> {
        self.shards_table.get(namespace).map(|entry| *entry)
    }

    pub fn remove_shard_table_record(&self, namespace: &IggyNamespace) -> ShardInfo {
        self.shards_table
            .remove(namespace)
            .map(|(_, shard_info)| shard_info)
            .expect("remove_shard_table_record: namespace not found")
    }

    pub fn remove_shard_table_records(
        &self,
        namespaces: &[IggyNamespace],
    ) -> Vec<(IggyNamespace, ShardInfo)> {
        namespaces
            .iter()
            .map(|ns| {
                let (ns, shard_info) = self.shards_table.remove(ns).unwrap();
                (ns, shard_info)
            })
            .collect()
    }

    pub fn insert_shard_table_record(&self, ns: IggyNamespace, shard_info: ShardInfo) {
        self.shards_table.insert(ns, shard_info);
    }

    pub fn get_current_shard_namespaces(&self) -> Vec<IggyNamespace> {
        self.shards_table
            .iter()
            .filter_map(|entry| {
                let (ns, shard_info) = entry.pair();
                if shard_info.id == self.id {
                    Some(*ns)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn insert_shard_table_records(
        &self,
        records: impl IntoIterator<Item = (IggyNamespace, ShardInfo)>,
    ) {
        for (ns, shard_info) in records {
            self.shards_table.insert(ns, shard_info);
        }
    }

    pub fn remove_active_session(&self, user_id: u32) {
        let mut active_sessions = self.active_sessions.borrow_mut();
        let pos = active_sessions
            .iter()
            .position(|s| s.get_user_id() == user_id);
        if let Some(pos) = pos {
            active_sessions.remove(pos);
        } else {
            error!(
                "{COMPONENT} - failed to remove active session for user ID: {user_id}, session not found."
            );
        }
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

pub fn calculate_shard_assignment(ns: &IggyNamespace, upperbound: u32) -> u16 {
    let mut hasher = Murmur3Hasher::default();
    hasher.write_u64(ns.inner());
    (hasher.finish32() % upperbound) as u16
}
