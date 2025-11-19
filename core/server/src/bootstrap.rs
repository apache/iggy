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

use crate::{
    IGGY_ROOT_PASSWORD_ENV, IGGY_ROOT_USERNAME_ENV,
    compat::index_rebuilding::index_rebuilder::IndexRebuilder,
    configs::{
        cache_indexes::CacheIndexesConfig,
        config_provider::ConfigProviderKind,
        server::ServerConfig,
        system::{INDEX_EXTENSION, LOG_EXTENSION, SystemConfig},
    },
    io::fs_utils::{self, DirEntry},
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
            EntityComponentSystem, EntityComponentSystemMutCell, Insert, InsertCell, IntoComponents,
        },
        users::Users,
    },
    state::system::{StreamState, TopicState, UserState},
    streaming::{
        partitions::{
            consumer_offset::ConsumerOffset,
            helpers::create_message_deduplicator,
            journal::MemoryMessageJournal,
            log::SegmentedLog,
            partition,
            storage::{load_consumer_group_offsets, load_consumer_offsets},
        },
        persistence::persister::{FilePersister, FileWithSyncPersister, PersisterKind},
        personal_access_tokens::personal_access_token::PersonalAccessToken,
        polling_consumer::ConsumerGroupId,
        segments::{Segment, storage::Storage},
        stats::{PartitionStats, StreamStats, TopicStats},
        storage::SystemStorage,
        streams::stream,
        topics::{consumer_group, topic},
        users::user::User,
        utils::{crypto, file::overwrite},
    },
    versioning::SemanticVersion,
};
use ahash::HashMap;
use compio::{fs::create_dir_all, runtime::Runtime};
use err_trail::ErrContext;
use iggy_common::{
    IggyByteSize, IggyError,
    defaults::{
        DEFAULT_ROOT_USERNAME, MAX_PASSWORD_LENGTH, MAX_USERNAME_LENGTH, MIN_PASSWORD_LENGTH,
        MIN_USERNAME_LENGTH,
    },
};
use std::{collections::HashSet, env, path::Path, sync::Arc};
use tracing::{info, warn};

pub async fn load_streams(
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
        info!(
            "Loading stream with ID: {}, name: {} from state...",
            id, name
        );
        let stream_id = id;
        let stats = Arc::new(StreamStats::default());
        let stream = stream::Stream::new(name.clone(), stats.clone(), created_at);
        let new_id = streams.insert(stream);
        assert_eq!(
            new_id, stream_id as usize,
            "load_streams: id mismatch when inserting stream, mismatch for stream with ID: {}, name: {}",
            stream_id, name
        );
        info!(
            "Loaded stream with ID: {}, name: {} from state...",
            id, name
        );

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
            info!(
                "Loading topic with ID: {}, name: {} from state...",
                id, name
            );
            let topic_id = id;
            let parent_stats = stats.clone();
            let stats = Arc::new(TopicStats::new(parent_stats));
            let topic_id = streams.with_components_by_id_mut(stream_id as usize, |(mut root, ..)| {
                let topic = topic::Topic::new(
                    name.clone(),
                    stats.clone(),
                    created_at,
                    replication_factor.unwrap_or(1),
                    message_expiry,
                    compression_algorithm,
                    max_topic_size,
                );
                let new_id = root.topics_mut().insert(topic);
                assert_eq!(
                    new_id, topic_id as usize,
                    "load_streams: topic id mismatch when inserting topic, mismatch for topic with ID: {}, name: {}",
                    topic_id, &name
                );
                new_id
            });
            info!("Loaded topic with ID: {}, name: {} from state...", id, name);

            let parent_stats = stats.clone();
            let cgs = consumer_groups.into_values();
            let partitions = partitions.into_values();

            // Load each partition asynchronously and insert immediately
            for partition_state in partitions {
                info!(
                    "Loading partition with ID: {}, for topic with ID: {} from state...",
                    partition_state.id, topic_id
                );

                let partition_id = partition_state.id;
                let partition = load_partition(
                    config,
                    stream_id as usize,
                    topic_id,
                    partition_state,
                    parent_stats.clone(),
                )
                .await?;
                streams.with_components_by_id(stream_id as usize, |(root, ..)| {
                    root.topics()
                        .with_components_by_id_mut(topic_id, |(mut root, ..)| {
                            let new_id = root.partitions_mut().insert(partition);
                            assert_eq!(
                                new_id, partition_id as usize,
                                "load_streams: partition id mismatch when inserting partition, mismatch for partition with ID: {}, for topic with ID: {}, for stream with ID: {}",
                                partition_id, topic_id, stream_id
                            );
                        });
                });

                info!(
                    "Loaded partition with ID: {}, for topic with ID: {} from state...",
                    partition_id, topic_id
                );
            }
            let partition_ids = streams.with_components_by_id(stream_id as usize, |(root, ..)| {
                root.topics().with_components_by_id(topic_id, |(root, ..)| {
                    root.partitions().with_components(|components| {
                        let (root, ..) = components.into_components();
                        root.iter().map(|(_, root)| root.id()).collect::<Vec<_>>()
                    })
                })
            });

            for cg_state in cgs {
                info!(
                    "Loading consumer group with ID: {}, name: {} for topic with ID: {} from state...",
                    cg_state.id, cg_state.name, topic_id
                );
                streams.with_components_by_id(stream_id as usize, |(root, ..)| {
                    root.topics()
                        .with_components_by_id_mut(topic_id, |(mut root, ..)| {
                            let id = cg_state.id;
                            let cg = consumer_group::ConsumerGroup::new(cg_state.name.clone(), Default::default(), partition_ids.clone());
                            let new_id = root.consumer_groups_mut().insert(cg);
                            assert_eq!(
                                new_id, id as usize,
                                "load_streams: consumer group id mismatch when inserting consumer group, mismatch for consumer group with ID: {}, name: {} for topic with ID: {}, for stream with ID: {}",
                                id, cg_state.name, topic_id, stream_id
                            );
                        });
                });
                info!(
                    "Loaded consumer group with ID: {}, name: {} for topic with ID: {} from state...",
                    cg_state.id, cg_state.name, topic_id
                );
            }
        }
    }
    Ok(streams)
}

pub fn load_users(state: impl IntoIterator<Item = UserState>) -> Users {
    let users = Users::new();
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
        user.personal_access_tokens = personal_access_tokens
            .into_values()
            .map(|token| {
                (
                    Arc::new(token.token_hash.clone()),
                    PersonalAccessToken::raw(id, &token.name, &token.token_hash, token.expiry_at),
                )
            })
            .collect();
        users.insert(user);
    }
    users
}

pub fn create_shard_connections(
    shards_set: &HashSet<usize>,
) -> (Vec<ShardConnector<ShardFrame>>, Vec<(u16, StopSender)>) {
    let shards_count = shards_set.len();

    // Create connectors with sequential IDs (0, 1, 2, ...) regardless of CPU core numbers
    let connectors: Vec<ShardConnector<ShardFrame>> = (0..shards_count)
        .map(|idx| ShardConnector::new(idx as u16))
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
    let mut username = env::var(IGGY_ROOT_USERNAME_ENV);
    let mut password = env::var(IGGY_ROOT_PASSWORD_ENV);
    if (username.is_ok() && password.is_err()) || (username.is_err() && password.is_ok()) {
        panic!(
            "When providing the custom root user credentials, both username and password must be set."
        );
    }
    if username.is_ok() && password.is_ok() {
        info!("Using the custom root user credentials.");
    } else {
        info!("Using the default root user credentials...");
        username = Ok(DEFAULT_ROOT_USERNAME.to_string());
        let generated_password = crypto::generate_secret(20..40);
        println!("Generated root user password: {generated_password}");
        password = Ok(generated_password);
    }

    let username = username.expect("Root username is not set.");
    let password = password.expect("Root password is not set.");
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

    User::root(&username, &password)
}

pub fn create_shard_executor(_cpu_set: HashSet<usize>) -> Runtime {
    // TODO: The event interval tick, could be configured based on the fact
    // How many clients we expect to have connected.
    // This roughly estimates the number of tasks we will create.
    let mut proactor = compio::driver::ProactorBuilder::new();

    proactor
        .capacity(4096)
        .coop_taskrun(true)
        .taskrun_flag(true);

    // FIXME(hubcio): Only set thread_pool_limit(0) on non-macOS platforms
    // This causes a freeze on macOS with compio fs operations
    // see https://github.com/compio-rs/compio/issues/446
    #[cfg(not(all(target_os = "macos", target_arch = "aarch64")))]
    proactor.thread_pool_limit(0);

    compio::runtime::RuntimeBuilder::new()
        .with_proactor(proactor.to_owned())
        .event_interval(128)
        .thread_affinity(_cpu_set)
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

async fn collect_log_files(partition_path: &str) -> Result<Vec<DirEntry>, IggyError> {
    let dir_entries = fs_utils::walk_dir(&partition_path)
        .await
        .map_err(|_| IggyError::CannotReadPartitions)?;
    let mut log_files = Vec::new();
    for entry in dir_entries {
        if entry.is_dir {
            continue;
        }

        let extension = entry.path.extension();
        if extension.is_none() || extension.unwrap() != LOG_EXTENSION {
            continue;
        }

        log_files.push(entry);
    }

    Ok(log_files)
}

pub async fn load_segments(
    config: &SystemConfig,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    partition_path: String,
    stats: Arc<PartitionStats>,
) -> Result<SegmentedLog<MemoryMessageJournal>, IggyError> {
    let mut log_files = collect_log_files(&partition_path).await?;
    log_files.sort_by(|a, b| a.path.file_name().cmp(&b.path.file_name()));
    let mut log = SegmentedLog::<MemoryMessageJournal>::default();
    for entry in log_files {
        let log_file_name = entry
            .path
            .file_stem()
            .unwrap()
            .to_string_lossy()
            .to_string();

        let start_offset = log_file_name.parse::<u64>().unwrap();

        let messages_file_path = format!("{}/{}.{}", partition_path, log_file_name, LOG_EXTENSION);
        let index_file_path = format!("{}/{}.{}", partition_path, log_file_name, INDEX_EXTENSION);
        let time_index_path = index_file_path.replace(INDEX_EXTENSION, "timeindex");

        async fn try_exists(path: &str) -> Result<bool, std::io::Error> {
            match compio::fs::metadata(path).await {
                Ok(_) => Ok(true),
                Err(err) => match err.kind() {
                    std::io::ErrorKind::NotFound => Ok(false),
                    _ => Err(err),
                },
            }
        }

        let index_path_exists = try_exists(&index_file_path).await.unwrap();
        let time_index_path_exists = try_exists(&time_index_path).await.unwrap();
        let index_cache_enabled = matches!(
            config.segment.cache_indexes,
            CacheIndexesConfig::All | CacheIndexesConfig::OpenSegment
        );

        if index_cache_enabled && (!index_path_exists || time_index_path_exists) {
            warn!(
                "Index at path {} does not exist, rebuilding it based on {}...",
                index_file_path, messages_file_path
            );
            let now = std::time::Instant::now();
            let index_rebuilder = IndexRebuilder::new(
                messages_file_path.clone(),
                index_file_path.clone(),
                start_offset,
            );
            index_rebuilder.rebuild().await.unwrap_or_else(|e| {
                panic!(
                    "Failed to rebuild index for partition with ID: {} for stream with ID: {} and topic with ID: {}. Error: {e}",
                    partition_id, stream_id, topic_id,
                )
            });
            info!(
                "Rebuilding index for path {} finished, it took {} ms",
                index_file_path,
                now.elapsed().as_millis()
            );
        }

        if time_index_path_exists {
            compio::fs::remove_file(&time_index_path).await.unwrap();
        }

        let messages_metadata = compio::fs::metadata(&messages_file_path)
            .await
            .map_err(|_| IggyError::CannotReadPartitions)?;
        let messages_size = messages_metadata.len() as u32;

        let index_size = match compio::fs::metadata(&index_file_path).await {
            Ok(metadata) => metadata.len() as u32,
            Err(_) => 0, // Default to 0 if index file doesn't exist
        };

        let storage = Storage::new(
            &messages_file_path,
            &index_file_path,
            messages_size as u64,
            index_size as u64,
            config.partition.enforce_fsync,
            config.partition.enforce_fsync,
            true,
        )
        .await?;

        let loaded_indexes = {
            storage.
            index_reader
            .as_ref()
            .unwrap()
            .load_all_indexes_from_disk()
            .await
            .with_error(|error| format!("Failed to load indexes during startup for stream ID: {}, topic ID: {}, partition_id: {}, {error}", stream_id, topic_id, partition_id))
            .map_err(|_| IggyError::CannotReadFile)?
        };

        let end_offset = if loaded_indexes.count() == 0 {
            0
        } else {
            let last_index_offset = loaded_indexes.last().unwrap().offset() as u64;
            start_offset + last_index_offset
        };

        let (start_timestamp, end_timestamp) = if loaded_indexes.count() == 0 {
            (0, 0)
        } else {
            (
                loaded_indexes.get(0).unwrap().timestamp(),
                loaded_indexes.last().unwrap().timestamp(),
            )
        };

        let mut segment = Segment::new(
            start_offset,
            config.segment.size,
            config.segment.message_expiry,
        );

        segment.start_timestamp = start_timestamp;
        segment.end_timestamp = end_timestamp;
        segment.end_offset = end_offset;
        segment.size = IggyByteSize::from(messages_size as u64);
        // At segment load, set the current position to the size of the segment (No data is buffered yet).
        segment.current_position = segment.size.as_bytes_u32();
        segment.sealed = true; // Persisted segments are assumed to be sealed

        if config.partition.validate_checksum {
            info!(
                "Validating checksum for segment at offset {} in stream ID: {}, topic ID: {}, partition ID: {}",
                start_offset, stream_id, topic_id, partition_id
            );
            let messages_count = loaded_indexes.count() as u32;
            if messages_count > 0 {
                const BATCH_COUNT: u32 = 10000;
                let mut current_relative_offset = 0u32;
                let mut processed_count = 0u32;

                while processed_count < messages_count {
                    let remaining_count = messages_count - processed_count;
                    let batch_count = std::cmp::min(BATCH_COUNT, remaining_count);
                    let batch_indexes = loaded_indexes
                        .slice_by_offset(current_relative_offset, batch_count)
                        .unwrap();

                    let messages_reader = storage.messages_reader.as_ref().unwrap();
                    match messages_reader.load_messages_from_disk(batch_indexes).await {
                        Ok(messages_batch) => {
                            if let Err(e) = messages_batch.validate_checksums() {
                                return Err(IggyError::CannotReadPartitions).with_error(|_| {
                                    format!(
                                        "Failed to validate message checksum for segment at offset {} in stream ID: {}, topic ID: {}, partition ID: {}, error: {}",
                                        start_offset, stream_id, topic_id, partition_id, e
                                    )
                                });
                            }
                            processed_count += messages_batch.count();
                            current_relative_offset += batch_count;
                        }
                        Err(e) => {
                            return Err(e).with_error(|_| {
                                format!(
                                    "Failed to load messages from disk for checksum validation at offset {} in stream ID: {}, topic ID: {}, partition ID: {}",
                                    start_offset, stream_id, topic_id, partition_id
                                )
                            });
                        }
                    }
                }
                info!(
                    "Checksum validation completed for segment at offset {}",
                    start_offset
                );
            }
        }

        log.add_persisted_segment(segment, storage);

        stats.increment_segments_count(1);

        stats.increment_size_bytes(messages_size as u64);

        let messages_count = if end_offset > start_offset {
            (end_offset - start_offset + 1) as u64
        } else if messages_size > 0 {
            loaded_indexes.count() as u64
        } else {
            0
        };

        if messages_count > 0 {
            stats.increment_messages_count(messages_count);
        }

        let should_cache_indexes = match config.segment.cache_indexes {
            CacheIndexesConfig::All => true,
            CacheIndexesConfig::OpenSegment => false,
            CacheIndexesConfig::None => false,
        };

        if should_cache_indexes {
            let segment_index = log.segments().len() - 1;
            log.set_segment_indexes(segment_index, loaded_indexes);
        }
    }

    if matches!(
        config.segment.cache_indexes,
        CacheIndexesConfig::OpenSegment
    ) && log.has_segments()
    {
        let segments_count = log.segments().len();
        if segments_count > 0 {
            let last_storage = log.storages().last().unwrap();
            match last_storage.index_reader.as_ref() {
                Some(index_reader) => {
                    if let Ok(loaded_indexes) = index_reader.load_all_indexes_from_disk().await {
                        log.set_segment_indexes(segments_count - 1, loaded_indexes);
                    }
                }
                None => {
                    warn!("Index reader not available for last segment in OpenSegment mode");
                }
            }
        }
    }

    Ok(log)
}

async fn load_partition(
    config: &SystemConfig,
    stream_id: usize,
    topic_id: usize,
    partition_state: crate::state::system::PartitionState,
    parent_stats: Arc<TopicStats>,
) -> Result<partition::Partition, IggyError> {
    let stats = Arc::new(PartitionStats::new(parent_stats));
    let partition_id = partition_state.id;

    let partition_path = config.get_partition_path(stream_id, topic_id, partition_id as usize);
    let log_files = collect_log_files(&partition_path).await?;
    let should_increment_offset = !log_files.is_empty()
        && log_files
            .first()
            .map(|entry| {
                let log_file_name = entry
                    .path
                    .file_stem()
                    .unwrap()
                    .to_string_lossy()
                    .to_string();

                let start_offset = log_file_name.parse::<u64>().unwrap();

                let messages_file_path = config.get_messages_file_path(
                    stream_id,
                    topic_id,
                    partition_id as usize,
                    start_offset,
                );
                let metadata = std::fs::metadata(&messages_file_path)
                    .expect("failed to get metadata for first segment in log");
                metadata.len() > 0
            })
            .unwrap_or_else(|| false);

    info!(
        "Loading partition with ID: {} for stream with ID: {} and topic with ID: {}, for path: {} from disk...",
        partition_id, stream_id, topic_id, partition_path
    );

    // Load consumer offsets
    let message_deduplicator = create_message_deduplicator(config);
    let consumer_offset_path =
        config.get_consumer_offsets_path(stream_id, topic_id, partition_id as usize);
    let consumer_group_offsets_path =
        config.get_consumer_group_offsets_path(stream_id, topic_id, partition_id as usize);

    let consumer_offset = Arc::new(
        load_consumer_offsets(&consumer_offset_path)?
            .into_iter()
            .map(|offset| (offset.consumer_id as usize, offset))
            .collect::<HashMap<usize, ConsumerOffset>>()
            .into(),
    );

    let consumer_group_offset = Arc::new(
        load_consumer_group_offsets(&consumer_group_offsets_path)?
            .into_iter()
            .collect::<HashMap<ConsumerGroupId, ConsumerOffset>>()
            .into(),
    );

    let log = Default::default();
    let partition = partition::Partition::new(
        partition_state.created_at,
        should_increment_offset,
        stats,
        message_deduplicator,
        Arc::new(Default::default()),
        consumer_offset,
        consumer_group_offset,
        log,
    );

    info!(
        "Loaded partition with ID: {} for stream with ID: {} and topic with ID: {}",
        partition_id, stream_id, topic_id
    );

    Ok(partition)
}
