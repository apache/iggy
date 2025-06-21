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
pub mod transmission;

use ahash::HashMap;
use builder::IggyShardBuilder;
use error_set::ErrContext;
use iggy_common::IggyError;
use namespace::IggyNamespace;
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    str::FromStr,
    sync::Arc,
    time::Instant,
};
use tracing::info;
use transmission::connector::{Receiver, ShardConnector, StopReceiver, StopSender};

use crate::{
    bootstrap::create_root_user,
    configs::server::ServerConfig,
    shard::transmission::frame::ShardFrame,
    state::{
        StateKind,
        file::FileState,
        system::{SystemState, UserState},
    },
    streaming::{storage::SystemStorage, systems::info::SystemInfo},
    versioning::SemanticVersion,
};

pub const COMPONENT: &str = "SHARD";

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
}

struct ShardInfo {
    id: u16,
}

pub struct IggyShard {
    pub id: u16,
    shards: Vec<Shard>,
    shards_table: RefCell<HashMap<IggyNamespace, ShardInfo>>,
    version: SemanticVersion,

    //pub(crate) permissioner: RefCell<Permissioner>,
    //pub(crate) streams: RwLock<HashMap<u32, Stream>>,
    //pub(crate) streams_ids: RefCell<HashMap<String, u32>>,
    //pub(crate) users: RefCell<HashMap<UserId, User>>,
    // TODO: Refactor.
    pub(crate) storage: Rc<SystemStorage>,

    pub(crate) state: StateKind,
    //pub(crate) encryptor: Option<Rc<dyn Encryptor>>,
    config: ServerConfig,
    //pub(crate) client_manager: RefCell<ClientManager>,
    //pub(crate) active_sessions: RefCell<Vec<Session>>,
    //pub(crate) metrics: Metrics,
    pub frame_receiver: Cell<Option<Receiver<ShardFrame>>>,
    stop_receiver: StopReceiver,
    stop_sender: StopSender,
}

impl IggyShard {
    pub fn builder() -> IggyShardBuilder {
        Default::default()
    }

    pub async fn init(&mut self) -> Result<(), IggyError> {
        let now = Instant::now();
        //TODO: Fix this either by moving it to main function, or by using `run_once` barrier.
        //let state_entries = self.state.init().await?;
        //let system_state = SystemState::init(state_entries).await?;
        //let user = create_root_user();
        self.load_version().await?;
        let SystemState { users, streams } = self.load_state().await?;
        self.load_users(users.into_values().collect()).await;
        // Add default root user.
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

    async fn load_users(&mut self, users: Vec<UserState>) -> Result<(), IggyError> {
        info!("Loading users...");
        /*

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
            self.users.insert(user_state.id, user);
        }

        let users_count = self.users.len();
        let current_user_id = self.users.keys().max().unwrap_or(&1);
        USER_ID.store(current_user_id + 1, Ordering::SeqCst);
        self.permissioner
            .init(&self.users.values().collect::<Vec<&User>>());
        self.metrics.increment_users(users_count as u32);
        info!("Initialized {users_count} user(s).");
        */
        Ok(())
    }

    async fn load_streams(&mut self, streams: Vec<StreamState>) -> Result<(), IggyError> {
        todo!();
        info!("Loading streams from disk...");
        let mut unloaded_streams = Vec::new();
        let mut dir_entries = read_dir(&self.config.get_streams_path())
            .await
            .map_err(|error| {
                error!("Cannot read streams directory: {error}");
                IggyError::CannotReadStreams
            })?;

        while let Some(dir_entry) = dir_entries.next_entry().await.unwrap_or(None) {
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
                if let Err(error) = fs::remove_dir_all(&dir_entry.path()).await {
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
                self.config.clone(),
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
            if self.config.recovery.recreate_missing_state {
                info!(
                    "Recreating missing state in recovery config is enabled, missing streams will be created."
                );
                for stream_id in missing_ids.iter() {
                    let stream_id = *stream_id;
                    let stream_state = streams.iter().find(|s| s.id == stream_id).unwrap();
                    let stream = Stream::create(
                        stream_id,
                        &stream_state.name,
                        self.config.clone(),
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
            if self.streams.contains_key(&stream.stream_id) {
                error!("Stream with ID: '{}' already exists.", &stream.stream_id);
                continue;
            }

            if self.streams_ids.contains_key(&stream.name) {
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
                .insert(stream.name.clone(), stream.stream_id);
            self.streams.insert(stream.stream_id, stream);
        }

        info!("Loaded {} stream(s) from disk.", self.streams.len());
        Ok(())
    }

    pub fn assert_init(&self) {}
}
