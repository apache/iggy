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
use error_set::ErrContext;
use futures::future::try_join_all;
use iggy_common::{
    EncryptorKind, Identifier, IggyError, Permissions, UserId, UserStatus,
    defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME},
    locking::IggyRwLockFn,
};
use namespace::IggyNamespace;
use std::{
    cell::{Cell, RefCell},
    future::Future,
    net::SocketAddr,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};
use tracing::{error, info, instrument, warn};
use transmission::connector::{Receiver, ShardConnector, StopReceiver, StopSender};

use crate::{
    archiver::ArchiverKind,
    configs::server::ServerConfig,
    http::http_server,
    io::fs_utils,
    shard::{
        task_registry::TaskRegistry,
        tasks::{
            auxilary::maintain_messages::spawn_message_maintainance_task,
            messages::spawn_shard_message_task,
        },
        transmission::{
            event::ShardEvent,
            frame::{ShardFrame, ShardResponse},
            message::{ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult},
        },
    },
    shard_error, shard_info, shard_warn,
    slab::{streams::Streams, traits_ext::EntityMarker},
    state::{
        StateKind,
        system::{StreamState, SystemState, UserState},
    },
    streaming::{
        clients::client_manager::ClientManager,
        diagnostics::metrics::Metrics,
        personal_access_tokens::personal_access_token::PersonalAccessToken,
        session::Session,
        storage::SystemStorage,
        users::{permissioner::Permissioner, user::User},
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

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct ShardInfo {
    id: u16,
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
    shards_table: RefCell<HashMap<IggyNamespace, ShardInfo>>,
    version: SemanticVersion,

    // Heart transplant of the old streams structure.
    pub(crate) streams2: Streams,
    // TODO: Refactor.
    pub(crate) storage: Rc<SystemStorage>,

    pub(crate) state: StateKind,
    // Temporal...
    pub(crate) init_state: Option<SystemState>,
    pub(crate) encryptor: Option<EncryptorKind>,
    pub(crate) archiver: Option<Rc<ArchiverKind>>,
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
        use crate::state::file::FileState;
        use crate::streaming::storage::SystemStorage;
        use crate::versioning::SemanticVersion;

        let version = SemanticVersion::current().expect("Invalid version");
        let persister = resolve_persister(server_config.system.partition.enforce_fsync);
        let storage = Rc::new(SystemStorage::new(
            server_config.system.clone(),
            persister.clone(),
        ));

        let state_path = server_config.system.get_state_messages_file_path();
        let file_state = FileState::new(&state_path, &version, persister, None);
        let state = crate::state::StateKind::File(file_state);

        let (stop_sender, stop_receiver) = async_channel::unbounded();

        let shard = Self {
            id: 0,
            shards: Vec::new(),
            shards_table: Default::default(),
            version,
            streams2: Streams::init(),
            storage,
            state,
            //TODO: Fix
            init_state: None,
            encryptor: None,
            archiver: None,
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
        let system_state = self.init_state.as_ref().unwrap();
        let SystemState { users, streams } = system_state;
        let _ = self.load_users(users.values().cloned().collect()).await;
        let _ = self.load_streams(streams.values().cloned().collect()).await;

        if let Some(archiver) = self.archiver.as_ref() {
            archiver
                .init()
                .await
                .expect("Failed to initialize archiver");
        }
        Ok(())
    }

    pub async fn run(self: &Rc<Self>) -> Result<(), IggyError> {
        // Workaround to ensure that the statistics are initialized before the server
        // loads streams and starts accepting connections. This is necessary to
        // have the correct statistics when the server starts.
        let now = Instant::now();
        //self.get_stats().await?;
        shard_info!(self.id, "Starting...");
        self.init().await?;
        // TODO: Fixme
        //self.assert_init();

        // Create all tasks (tcp listener, http listener, command processor, in the future also the background jobs).
        let mut tasks: Vec<Task> = vec![Box::pin(spawn_shard_message_task(self.clone()))];
        tasks.push(Box::pin(spawn_message_maintainance_task(self.clone())));
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

    async fn load_users(&self, users: Vec<UserState>) -> Result<(), IggyError> {
        shard_info!(self.id, "Loading users...");
        for user_state in users.into_iter() {
            let mut user = User::with_password(
                user_state.id,
                &user_state.username,
                user_state.password_hash,
                user_state.status,
                user_state.permissions,
            );

            user.created_at = user_state.created_at;
            user.personal_access_tokens = user_state
                .personal_access_tokens
                .into_values()
                .map(|token| {
                    (
                        Arc::new(token.token_hash.clone()),
                        PersonalAccessToken::raw(
                            user_state.id,
                            &token.name,
                            &token.token_hash,
                            token.expiry_at,
                        ),
                    )
                })
                .collect();
            self.users.borrow_mut().insert(user_state.id, user);
        }

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

    async fn load_streams(&self, streams: Vec<StreamState>) -> Result<(), IggyError> {
        shard_info!(self.id, "Loading streams from disk...");
        let mut unloaded_streams = Vec::new();
        // Does compio have api for that ?
        let mut dir_entries =
            std::fs::read_dir(&self.config.system.get_streams_path()).map_err(|error| {
                shard_error!(self.id, "Cannot read streams directory: {error}");
                IggyError::CannotReadStreams
            })?;

        //TODO: User the dir walk impl from main function, once implemented.
        while let Some(dir_entry) = dir_entries.next() {
            let dir_entry = dir_entry.unwrap();
            let name = dir_entry.file_name().into_string().unwrap();
            let stream_id = name.parse::<u32>().map_err(|_| {
                shard_error!(self.id, "Invalid stream ID file with name: '{name}'.");
                IggyError::InvalidNumberValue
            })?;
            let stream_state = streams.iter().find(|s| s.id == stream_id);
            if stream_state.is_none() {
                shard_error!(
                    self.id,
                    "Stream with ID: '{stream_id}' was not found in state, but exists on disk and will be removed."
                );
                if let Err(error) = fs_utils::remove_dir_all(&dir_entry.path()).await {
                    shard_error!(self.id, "Cannot remove stream directory: {error}");
                } else {
                    shard_warn!(self.id, "Stream with ID: '{stream_id}' was removed.");
                }
                continue;
            }

            let stream_state = stream_state.unwrap();
            let mut stream = Stream::empty(
                stream_id,
                &stream_state.name,
                self.config.system.clone(),
                self.storage.clone(),
            );
            stream.created_at = stream_state.created_at;
            unloaded_streams.push(stream);
        }

        let state_stream_ids = streams
            .iter()
            .map(|stream| stream.id)
            .collect::<AHashSet<u32>>();
        let unloaded_stream_ids = unloaded_streams
            .iter()
            .map(|stream| stream.stream_id)
            .collect::<AHashSet<u32>>();
        let mut missing_ids = state_stream_ids
            .difference(&unloaded_stream_ids)
            .copied()
            .collect::<AHashSet<u32>>();
        if missing_ids.is_empty() {
            shard_info!(self.id, "All streams found on disk were found in state.");
        } else {
            warn!("Streams with IDs: '{missing_ids:?}' were not found on disk.");
            if self.config.system.recovery.recreate_missing_state {
                shard_info!(
                    self.id,
                    "Recreating missing state in recovery config is enabled, missing streams will be created."
                );
                for stream_id in missing_ids.iter() {
                    let stream_id = *stream_id;
                    let stream_state = streams.iter().find(|s| s.id == stream_id).unwrap();
                    let stream = Stream::create(
                        stream_id,
                        &stream_state.name,
                        self.config.system.clone(),
                        self.storage.clone(),
                    );
                    stream.persist().await?;
                    unloaded_streams.push(stream);
                    info!(
                        "Missing stream with ID: '{stream_id}', name: {} was recreated.",
                        stream_state.name
                    );
                }
                missing_ids.clear();
            } else {
                shard_warn!(
                    self.id,
                    "Recreating missing state in recovery config is disabled, missing streams will not be created."
                );
            }
        }

        //TODO: Refactor...
        let mut streams_states = streams
            .into_iter()
            .filter(|s| !missing_ids.contains(&s.id))
            .map(|s| (s.id, s))
            .collect::<AHashMap<_, _>>();
        let loaded_streams = RefCell::new(Vec::new());
        let load_stream_tasks = unloaded_streams.into_iter().map(|mut stream| {
            let state = streams_states.remove(&stream.stream_id).unwrap();

            async {
                stream.load(state).await?;
                loaded_streams.borrow_mut().push(stream);
                Result::<(), IggyError>::Ok(())
            }
        });
        try_join_all(load_stream_tasks).await?;

        for stream in loaded_streams.take() {
            if self.streams.borrow().contains_key(&stream.stream_id) {
                shard_error!(
                    self.id,
                    "Stream with ID: '{}' already exists.",
                    &stream.stream_id
                );
                continue;
            }

            if self.streams_ids.borrow().contains_key(&stream.name) {
                shard_error!(
                    self.id,
                    "Stream with name: '{}' already exists.",
                    &stream.name
                );
                continue;
            }

            self.metrics.increment_streams(1);
            self.metrics.increment_topics(stream.get_topics_count());
            self.metrics
                .increment_partitions(stream.get_partitions_count());
            self.metrics.increment_segments(stream.get_segments_count());
            self.metrics.increment_messages(stream.get_messages_count());

            self.streams_ids
                .borrow_mut()
                .insert(stream.name.clone(), stream.stream_id);
            self.streams.borrow_mut().insert(stream.stream_id, stream);
        }

        shard_info!(
            self.id,
            "Loaded {} stream(s) from disk.",
            self.streams.borrow().len()
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
        todo!();
        /*
        let stream = self.get_stream(&Identifier::numeric(request.stream_id)?)?;
        let topic = stream.get_topic(&Identifier::numeric(request.topic_id)?)?;
        let partition_id = request.partition_id;
        match request.payload {
            ShardRequestPayload::SendMessages { batch } => {
                topic.append_messages(partition_id, batch).await?;
                Ok(ShardResponse::SendMessages)
            }
            ShardRequestPayload::PollMessages { args, consumer } => {
                let (metadata, batch) = topic
                    .get_messages(consumer, partition_id, args.strategy, args.count)
                    .await?;
                Ok(ShardResponse::PollMessages((metadata, batch)))
            }
        }
        */
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
                self.create_partitions2_bypass_auth(&stream_id, &topic_id, partitions)?;
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
                );
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
                namespace.stream_id,
                namespace.topic_id,
                namespace.partition_id,
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

    fn find_shard(&self, namespace: &IggyNamespace) -> Option<&Shard> {
        let shards_table = self.shards_table.borrow();
        shards_table.get(namespace).map(|shard_info| {
            self.shards
                .iter()
                .find(|shard| shard.id == shard_info.id)
                .expect("Shard not found in the shards table.")
        })
    }

    pub fn find_shard_table_record(&self, namespace: &IggyNamespace) -> Option<ShardInfo> {
        let shards_table = self.shards_table.borrow();
        shards_table.get(namespace).cloned()
    }

    pub fn remove_shard_table_records(
        &self,
        namespaces: &[IggyNamespace],
    ) -> Vec<(IggyNamespace, ShardInfo)> {
        let mut shards_table = self.shards_table.borrow_mut();
        namespaces
            .iter()
            .map(|ns| {
                let shard_info = shards_table.remove(ns).unwrap();
                (*ns, shard_info)
            })
            .collect()
    }

    pub fn create_shard_table_records(
        &self,
        partition_ids: &[u32],
        stream_id: u32,
        topic_id: u32,
    ) -> impl Iterator<Item = (IggyNamespace, ShardInfo)> {
        let records = partition_ids.iter().map(move |partition_id| {
            let namespace = IggyNamespace::new(stream_id, topic_id, *partition_id);
            let hash = namespace.generate_hash();
            let shard_id = hash % self.get_available_shards_count();
            let shard_info = ShardInfo::new(shard_id as u16);
            (namespace, shard_info)
        });
        records
    }

    pub fn insert_shard_table_records(
        &self,
        records: impl IntoIterator<Item = (IggyNamespace, ShardInfo)>,
    ) {
        self.shards_table.borrow_mut().extend(records);
    }

    pub fn add_active_session(&self, session: Rc<Session>) {
        self.active_sessions.borrow_mut().push(session);
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
