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
pub mod gate;
pub mod namespace;
pub mod system;
pub mod task_registry;
pub mod tasks;
pub mod transmission;

use ahash::{AHashMap, AHashSet, HashMap};
use builder::IggyShardBuilder;
use error_set::ErrContext;
use futures::future::try_join_all;
use iggy_common::{EncryptorKind, Identifier, IggyError, UserId};
use namespace::IggyNamespace;
use std::{
    cell::{Cell, RefCell},
    pin::Pin,
    rc::Rc,
    str::FromStr,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};
use tracing::{error, info, instrument, trace, warn};
use transmission::connector::{Receiver, ShardConnector, StopReceiver, StopSender};

use crate::{
    configs::server::ServerConfig,
    io::fs_utils,
    shard::{
        system::info::SystemInfo,
        task_registry::TaskRegistry,
        tasks::messages::spawn_shard_message_task,
        transmission::{
            event::ShardEvent,
            frame::{ShardFrame, ShardResponse},
            message::{ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult},
        },
    },
    state::{
        StateKind,
        file::FileState,
        system::{StreamState, SystemState, UserState},
    },
    streaming::{
        clients::client_manager::ClientManager,
        diagnostics::metrics::Metrics,
        partitions::partition,
        personal_access_tokens::personal_access_token::PersonalAccessToken,
        session::Session,
        storage::SystemStorage,
        streams::stream::Stream,
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

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
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

    pub(crate) streams: RefCell<HashMap<u32, Stream>>,
    pub(crate) streams_ids: RefCell<HashMap<String, u32>>,
    // TODO: Refactor.
    pub(crate) storage: Rc<SystemStorage>,

    pub(crate) state: StateKind,
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
}

impl IggyShard {
    pub fn builder() -> IggyShardBuilder {
        Default::default()
    }

    pub async fn init(&self) -> Result<(), IggyError> {
        let now = Instant::now();

        self.load_version().await?;
        let SystemState { users, streams } = self.load_state().await?;
        self.load_users(users.into_values().collect()).await;
        self.load_streams(streams.into_values().collect()).await;

        //TODO: Fix the archiver.
        /*
        if let Some(archiver) = self.archiver.as_ref() {
            archiver
                .init()
                .await
                .expect("Failed to initialize archiver");
        }
        */
        info!("Initialized system in {} ms.", now.elapsed().as_millis());
        Ok(())
    }

    pub async fn run(self: &Rc<Self>) -> Result<(), IggyError> {
        // Workaround to ensure that the statistics are initialized before the server
        // loads streams and starts accepting connections. This is necessary to
        // have the correct statistics when the server starts.
        //self.get_stats().await?;
        self.init().await?;
        // TODO: Fixme
        //self.assert_init();
        info!("Initiated shard with ID: {}", self.id);

        // Create all tasks (tcp listener, http listener, command processor, in the future also the background jobs).
        let mut tasks: Vec<Task> = vec![Box::pin(spawn_shard_message_task(self.clone()))];
        if self.config.tcp.enabled {
            tasks.push(Box::pin(spawn_tcp_server(self.clone())));
        }

        let stop_receiver = self.get_stop_receiver();
        let shard_for_shutdown = self.clone();

        /*
        compio::runtime::spawn(async move {
            let _ = stop_receiver.recv().await;
            info!("Shard {} received shutdown signal", shard_for_shutdown.id);

            let shutdown_success = shard_for_shutdown.trigger_shutdown().await;
            if !shutdown_success {
                error!("Shard {} shutdown timed out", shard_for_shutdown.id);
            } else {
                info!(
                    "Shard {} shutdown completed successfully",
                    shard_for_shutdown.id
                );
            }
        });
        */

        let result = try_join_all(tasks).await;
        result?;

        Ok(())
    }

    async fn load_version(&self) -> Result<(), IggyError> {
        async fn update_system_info(
            storage: &Rc<SystemStorage>,
            system_info: &mut SystemInfo,
            version: &SemanticVersion,
        ) -> Result<(), IggyError> {
            system_info.update_version(version);
            storage.info.save(system_info).await?;
            Ok(())
        }

        let current_version = &self.version;
        let mut system_info;
        let load_system_info = self.storage.info.load().await;
        if load_system_info.is_err() {
            let error = load_system_info.err().unwrap();
            if let IggyError::ResourceNotFound(_) = error {
                info!("System info not found, creating...");
                system_info = SystemInfo::default();
                update_system_info(&self.storage, &mut system_info, current_version).await?;
            } else {
                return Err(error);
            }
        } else {
            system_info = load_system_info.unwrap();
        }

        info!("Loaded {system_info}.");
        let loaded_version = SemanticVersion::from_str(&system_info.version.version)?;
        if current_version.is_equal_to(&loaded_version) {
            info!("System version {current_version} is up to date.");
        } else if current_version.is_greater_than(&loaded_version) {
            info!(
                "System version {current_version} is greater than {loaded_version}, checking the available migrations..."
            );
            update_system_info(&self.storage, &mut system_info, current_version).await?;
        } else {
            info!(
                "System version {current_version} is lower than {loaded_version}, possible downgrade."
            );
            update_system_info(&self.storage, &mut system_info, current_version).await?;
        }

        Ok(())
    }

    async fn load_state(&self) -> Result<SystemState, IggyError> {
        let state_entries = self.state.init().await.with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to initialize state entries")
        })?;
        let system_state = SystemState::init(state_entries)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to initialize system state")
            })?;
        Ok(system_state)
    }

    async fn load_users(&self, users: Vec<UserState>) -> Result<(), IggyError> {
        info!("Loading users...");
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
        info!("Initialized {users_count} user(s).");
        Ok(())
    }

    async fn load_streams(&self, streams: Vec<StreamState>) -> Result<(), IggyError> {
        info!("Loading streams from disk...");
        let mut unloaded_streams = Vec::new();
        // Does mononio has api for that ?
        let mut dir_entries =
            std::fs::read_dir(&self.config.system.get_streams_path()).map_err(|error| {
                error!("Cannot read streams directory: {error}");
                IggyError::CannotReadStreams
            })?;

        //TODO: User the dir walk impl from main function, once implemented.
        while let Some(dir_entry) = dir_entries.next() {
            let dir_entry = dir_entry.unwrap();
            let name = dir_entry.file_name().into_string().unwrap();
            let stream_id = name.parse::<u32>().map_err(|_| {
                error!("Invalid stream ID file with name: '{name}'.");
                IggyError::InvalidNumberValue
            })?;
            let stream_state = streams.iter().find(|s| s.id == stream_id);
            if stream_state.is_none() {
                error!(
                    "Stream with ID: '{stream_id}' was not found in state, but exists on disk and will be removed."
                );
                if let Err(error) = fs_utils::remove_dir_all(&dir_entry.path()).await {
                    error!("Cannot remove stream directory: {error}");
                } else {
                    warn!("Stream with ID: '{stream_id}' was removed.");
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
            info!("All streams found on disk were found in state.");
        } else {
            warn!("Streams with IDs: '{missing_ids:?}' were not found on disk.");
            if self.config.system.recovery.recreate_missing_state {
                info!(
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
                warn!(
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
                error!("Stream with ID: '{}' already exists.", &stream.stream_id);
                continue;
            }

            if self.streams_ids.borrow().contains_key(&stream.name) {
                error!("Stream with name: '{}' already exists.", &stream.name);
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

        info!(
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
    }

    async fn handle_event(&self, event: Arc<ShardEvent>) -> Result<(), IggyError> {
        match &*event {
            ShardEvent::CreatedStream { stream_id, name } => {
                self.create_stream_bypass_auth(*stream_id, name)
            }
            ShardEvent::CreatedTopic {
                stream_id,
                topic_id,
                name,
                partitions_count,
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor,
                shards_assignment,
            } => {
                self.create_topic_bypass_auth(
                    stream_id,
                    *topic_id,
                    name,
                    *partitions_count,
                    *message_expiry,
                    *compression_algorithm,
                    *max_topic_size,
                    *replication_factor,
                    shards_assignment.clone(),
                )
                .await
            }
            ShardEvent::LoginUser {
                client_id,
                username,
                password,
            } => self.login_user_event(*client_id, username, password),
            ShardEvent::NewSession { address, transport } => {
                let session = self.add_client(address, *transport);
                self.add_active_session(session);
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

    pub fn broadcast_event_to_all_shards(&self, event: Arc<ShardEvent>) -> Vec<ShardResponse> {
        self.shards
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
                conn.send(ShardFrame::new(event.clone().into(), None));
                ShardResponse::Event
            })
            .collect()
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

    pub fn insert_shard_table_records(
        &self,
        records: impl IntoIterator<Item = (IggyNamespace, ShardInfo)>,
    ) {
        self.shards_table.borrow_mut().extend(records);
    }

    pub fn add_active_session(&self, session: Rc<Session>) {
        self.active_sessions.borrow_mut().push(session);
    }

    pub fn ensure_authenticated(&self, session: &Session) -> Result<u32, IggyError> {
        let active_sessions = self.active_sessions.borrow();
        let user_id = active_sessions
            .iter()
            .find(|s| s.get_user_id() == session.get_user_id())
            .ok_or_else(|| IggyError::Unauthenticated)
            .and_then(|session| {
                if session.is_authenticated() {
                    Ok(session.get_user_id())
                } else {
                    error!("{COMPONENT} - unauthenticated access attempt, session: {session}");
                    Err(IggyError::Unauthenticated)
                }
            })?;
        Ok(user_id)
    }
}
