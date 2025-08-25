use crate::{
    IGGY_ROOT_PASSWORD_ENV, IGGY_ROOT_USERNAME_ENV,
    configs::{config_provider::ConfigProviderKind, server::ServerConfig, system::SystemConfig},
    io::fs_utils,
    server_error::ServerError,
    shard::{
        system::info::SystemInfo,
        transmission::{
            connector::{ShardConnector, StopSender},
            frame::ShardFrame,
        },
    },
    slab::{
        streams::Streams,
        traits_ext::{
            EntityComponentSystem, EntityComponentSystemMutCell, InsertCell, IntoComponents,
        },
    },
    state::system::{StreamState, TopicState, UserState},
    streaming::{
        deduplication::message_deduplicator,
        partitions::{
            helpers::create_message_deduplicator, partition2, storage2::load_consumer_offsets,
        },
        persistence::persister::{FilePersister, FileWithSyncPersister, PersisterKind},
        personal_access_tokens::personal_access_token::PersonalAccessToken,
        stats::stats::{PartitionStats, StreamStats, TopicStats},
        storage::SystemStorage,
        streams::stream2,
        topics::{consumer_group2, topic2},
        users::user::User,
        utils::file::overwrite,
    },
    versioning::SemanticVersion,
};
use ahash::HashMap;
use compio::{fs::create_dir_all, runtime::Runtime};
use iggy_common::{
    ConsumerKind, IggyError, UserId,
    defaults::{
        DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME, MAX_PASSWORD_LENGTH, MAX_USERNAME_LENGTH,
        MIN_PASSWORD_LENGTH, MIN_USERNAME_LENGTH,
    },
};
use std::{collections::HashSet, env, path::Path, sync::Arc};
use tracing::info;

pub fn load_streams(
    state: impl IntoIterator<Item = StreamState>,
    config: &SystemConfig,
) -> Result<Streams, IggyError> {
    let streams = Streams::default();
    for StreamState {
        name,
        created_at,
        id,
        topics,
    } in state
    {
        info!("Loading stream with ID: {}, name: {} from state...", id, name);
        let stream_id = id;
        let stats = Arc::new(Default::default());
        let stream = stream2::Stream::new(name.clone(), stats.clone(), created_at);
        let new_id = streams.insert(stream);
        assert_eq!(
            new_id, stream_id as usize,
            "load_streams: id mismatch when inserting stream, mismatch for stream with ID: {}, name: {}",
            stream_id, name
        );
        info!("Loaded stream with ID: {}, name: {} from state...", id, name);

        let topics = topics.into_values();
        for TopicState {
            id,
            name,
            created_at,
            compression_algorithm,
            message_expiry,
            max_topic_size,
            replication_factor,
            consumer_groups,
            partitions,
        } in topics
        {
            info!("Loading topic with ID: {}, name: {} from state...", id, name);
            let topic_id = id;
            let parent_stats = stats.clone();
            let stats = Arc::new(TopicStats::new(parent_stats));
            let topic_id = streams.with_components_by_id(stream_id, move |(root, ..)| {
                let topic = topic2::Topic::new(
                    name.clone(),
                    stats.clone(),
                    created_at,
                    replication_factor,
                    message_expiry,
                    compression_algorithm,
                    max_topic_size,
                );
                let new_id = root.topics_mut().insert(topic);
                assert_eq!(
                    new_id, topic_id as usize,
                    "load_streams: topic id mismatch when inserting topic, mismatch for topic with ID: {}, name: {}",
                    topic_id, name
                );
                new_id
            });
            info!("Loaded topic with ID: {}, name: {} from state...", id, name);

            let parent_stats = stats.clone();
            let cgs = consumer_groups.into_values();
            let partitions = partitions.into_values();
            for partition_state in partitions {
                info!("Loading partition with ID: {}, for topic with ID: {} from state...", partition_state.id, topic_id);
                streams.with_components_by_id(stream_id, move |(root, ..)| {
                    root.topics()
                        .with_components_by_id_mut(topic_id, move |(root, ..)| {
                            let stats = PartitionStats::new(parent_stats);
                            // TODO: This has to be sampled from segments, if there exists more than 0 segements and the segment has more than 0 messages, then we set it to true.
                            let should_increment_offset = todo!();
                            // TODO: This has to be sampled from segments and it's indexes, offset = segment.start_offset + last_index.offset;
                            let offset = todo!();
                            let message_deduplicator = create_message_deduplicator(config);
                            let id = partition_state.id;

                            let consumer_offset_path = config.get_consumer_offsets_path(stream_id, topic_id, id);
                            let consumer_group_offsets_path = config.get_consumer_group_offsets_path(stream_id, topic_id, id);
                            let consumer_offset = load_consumer_offsets(&consumer_offset_path, ConsumerKind::Consumer)?
                            .into_iter().map(|offset| {
                                (offset.consumer_id, offset)
                            }).collect().into();
                            let consumer_group_offset = load_consumer_offsets(&consumer_group_offsets_path, ConsumerKind::ConsumerGroup)?.into_iter().map(|offset| {
                                (offset.consumer_id, offset)
                            }).collect().into();

                            let partition = partition2::Partition::new(
                                partition_state.created_at,
                                should_increment_offset,
                                stats,
                                message_deduplicator,
                                offset,
                                consumer_offset,
                                consumer_group_offset,
                            );
                            let new_id = root.partitions_mut().insert(partition);
                            assert_eq!(
                                new_id, id as usize,
                                "load_streams: partition id mismatch when inserting partition, mismatch for partition with ID: {}, for topic with ID: {}, for stream with ID: {}",
                                id, topic_id, stream_id
                            );
                        })
                })?;
                info!("Loaded partition with ID: {}, for topic with ID: {} from state...", partition_state.id, topic_id);
            }
            let partition_ids = streams.with_components_by_id(stream_id, |(root, ..)| {
                root.topics().with_components_by_id(topic_id, |(root, ..)| {
                    root.partitions().with_components(|components| {
                        let (root, ..) = components.into_components();
                        root.iter().map(|(_, root)| root.id()).collect::<Vec<_>>()
                    })
                })
            });

            for cg_state in cgs {
                info!("Loading consumer group with ID: {}, name: {} for topic with ID: {} from state...", cg_state.id, cg_state.name, topic_id);
                streams.with_components_by_id(stream_id, move |(root, ..)| {
                    root.topics()
                        .with_components_by_id_mut(topic_id, move |(root, ..)| {
                            let id = cg_state.id;
                            let cg = consumer_group2::ConsumerGroup::new(cg_state.name.clone(), Default::default(), partition_ids);
                            let new_id = root.consumer_groups_mut().insert(cg);
                            assert_eq!(
                                new_id, id as usize,
                                "load_streams: consumer group id mismatch when inserting consumer group, mismatch for consumer group with ID: {}, name: {} for topic with ID: {}, for stream with ID: {}",
                                id, cg_state.name, topic_id, stream_id
                            );
                        });
                });
                info!("Loaded consumer group with ID: {}, name: {} for topic with ID: {} from state...", cg_state.id, cg_state.name, topic_id);
            }
        }
    }
    streams
}

pub fn load_users(state: impl IntoIterator<Item = UserState>) -> HashMap<UserId, User> {
    let mut users = Default::default();
    for user_state in state {
        let UserState {
            id,
            username,
            password_hash,
            status,
            created_at,
            permissions,
            personal_access_tokens,
        } = user_state;
        let mut user = User::with_password(id, &username, password_hash, status, permissions);
        user.created_at = created_at;
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
        users.insert(id, user);
    }
    users
}

pub fn create_shard_connections(
    shards_set: &HashSet<usize>,
) -> (Vec<ShardConnector<ShardFrame>>, Vec<(u16, StopSender)>) {
    let shards_count = shards_set.len();
    let mut shards_vec: Vec<usize> = shards_set.iter().cloned().collect();
    shards_vec.sort();

    let connectors: Vec<ShardConnector<ShardFrame>> = shards_vec
        .into_iter()
        .map(|id| ShardConnector::new(id as u16, shards_count))
        .collect();

    let shutdown_handles = connectors
        .iter()
        .map(|conn| (conn.id, conn.stop_sender.clone()))
        .collect();

    (connectors, shutdown_handles)
}

pub async fn load_config(
    config_provider: &ConfigProviderKind,
) -> Result<ServerConfig, ServerError> {
    let config = ServerConfig::load(config_provider).await?;
    Ok(config)
}

pub async fn create_directories(config: &SystemConfig) -> Result<(), IggyError> {
    let system_path = config.get_system_path();
    if !Path::new(&system_path).exists() && create_dir_all(&system_path).await.is_err() {
        return Err(IggyError::CannotCreateBaseDirectory(system_path));
    }

    let state_path = config.get_state_path();
    if !Path::new(&state_path).exists() && create_dir_all(&state_path).await.is_err() {
        return Err(IggyError::CannotCreateStateDirectory(state_path));
    }
    let state_log = config.get_state_messages_file_path();
    if !Path::new(&state_log).exists() && (overwrite(&state_log).await).is_err() {
        return Err(IggyError::CannotCreateStateDirectory(state_log));
    }

    let streams_path = config.get_streams_path();
    if !Path::new(&streams_path).exists() && create_dir_all(&streams_path).await.is_err() {
        return Err(IggyError::CannotCreateStreamsDirectory(streams_path));
    }

    let runtime_path = config.get_runtime_path();
    if Path::new(&runtime_path).exists() && fs_utils::remove_dir_all(&runtime_path).await.is_err() {
        return Err(IggyError::CannotRemoveRuntimeDirectory(runtime_path));
    }

    if create_dir_all(&runtime_path).await.is_err() {
        return Err(IggyError::CannotCreateRuntimeDirectory(runtime_path));
    }

    info!(
        "Initializing system, data will be stored at: {}",
        config.get_system_path()
    );
    Ok(())
}

pub fn create_root_user() -> User {
    info!("Creating root user...");
    let username = env::var(IGGY_ROOT_USERNAME_ENV);
    let password = env::var(IGGY_ROOT_PASSWORD_ENV);
    if (username.is_ok() && password.is_err()) || (username.is_err() && password.is_ok()) {
        panic!(
            "When providing the custom root user credentials, both username and password must be set."
        );
    }
    if username.is_ok() && password.is_ok() {
        info!("Using the custom root user credentials.");
    } else {
        info!("Using the default root user credentials.");
    }

    let username = username.unwrap_or(DEFAULT_ROOT_USERNAME.to_string());
    let password = password.unwrap_or(DEFAULT_ROOT_PASSWORD.to_string());
    if username.is_empty() || password.is_empty() {
        panic!("Root user credentials are not set.");
    }
    if username.len() < MIN_USERNAME_LENGTH {
        panic!("Root username is too short.");
    }
    if username.len() > MAX_USERNAME_LENGTH {
        panic!("Root username is too long.");
    }
    if password.len() < MIN_PASSWORD_LENGTH {
        panic!("Root password is too short.");
    }
    if password.len() > MAX_PASSWORD_LENGTH {
        panic!("Root password is too long.");
    }
    let user = User::root(&username, &password);
    user
}

pub fn create_shard_executor(cpu_set: HashSet<usize>) -> Runtime {
    // TODO: The event interval tick, could be configured based on the fact
    // How many clients we expect to have connected.
    // This roughly estimates the number of tasks we will create.

    let mut proactor = compio::driver::ProactorBuilder::new();

    proactor
        .capacity(4096)
        .coop_taskrun(true)
        .taskrun_flag(false); // TODO: Try enabling this.

    // FIXME(hubcio): Only set thread_pool_limit(0) on non-macOS platforms
    // This causes a freeze on macOS with compio fs operations
    // see https://github.com/compio-rs/compio/issues/446
    #[cfg(not(all(target_os = "macos", target_arch = "aarch64")))]
    proactor.thread_pool_limit(0);

    compio::runtime::RuntimeBuilder::new()
        .with_proactor(proactor.to_owned())
        .event_interval(69)
        .thread_affinity(cpu_set)
        .build()
        .unwrap()
}

pub fn resolve_persister(enforce_fsync: bool) -> Arc<PersisterKind> {
    match enforce_fsync {
        true => Arc::new(PersisterKind::FileWithSync(FileWithSyncPersister)),
        false => Arc::new(PersisterKind::File(FilePersister)),
    }
}

pub async fn update_system_info(
    storage: &SystemStorage,
    system_info: &mut SystemInfo,
    version: &SemanticVersion,
) -> Result<(), IggyError> {
    system_info.update_version(version);
    storage.info.save(system_info).await?;
    Ok(())
}
