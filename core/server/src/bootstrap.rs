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

use crate::{
    IGGY_ROOT_PASSWORD_ENV, IGGY_ROOT_USERNAME_ENV,
    compat::index_rebuilding::index_rebuilder::IndexRebuilder,
    configs::{
        cache_indexes::CacheIndexesConfig,
        server::ServerConfig,
        sharding::ShardInfo,
        system::{INDEX_EXTENSION, LOG_EXTENSION, SystemConfig},
    },
    io::fs_utils::{self, DirEntry},
    metadata::{ConsumerGroupMeta, MetadataWriter, PartitionMeta, StreamMeta, TopicMeta, UserMeta},
    server_error::ServerError,
    shard::{
        system::info::SystemInfo,
        transmission::{
            connector::{ShardConnector, StopSender},
            frame::ShardFrame,
        },
    },
    state::system::{StreamState, TopicState, UserState},
    streaming::{
        partitions::{
            consumer_group_offsets::ConsumerGroupOffsets, consumer_offsets::ConsumerOffsets,
            journal::MemoryMessageJournal, log::SegmentedLog,
        },
        persistence::persister::{FilePersister, FileWithSyncPersister, PersisterKind},
        segments::{Segment, storage::Storage},
        stats::{PartitionStats, StreamStats, TopicStats},
        storage::SystemStorage,
        users::user::User,
        utils::{crypto, file::overwrite},
    },
    versioning::SemanticVersion,
};
use compio::{fs::create_dir_all, runtime::Runtime};
use err_trail::ErrContext;
use iggy_common::{
    IggyByteSize, IggyError, PersonalAccessToken,
    defaults::{
        DEFAULT_ROOT_USERNAME, MAX_PASSWORD_LENGTH, MAX_USERNAME_LENGTH, MIN_PASSWORD_LENGTH,
        MIN_USERNAME_LENGTH,
    },
};
use slab::Slab;
use std::{env, path::Path, sync::Arc};
use tracing::{info, warn};

pub fn create_shard_connections(
    shard_assignment: &[ShardInfo],
) -> (Vec<ShardConnector<ShardFrame>>, Vec<(u16, StopSender)>) {
    // Create connectors with sequential IDs (0, 1, 2, ...) regardless of CPU core numbers
    let connectors: Vec<ShardConnector<ShardFrame>> = shard_assignment
        .iter()
        .enumerate()
        .map(|(idx, _assignment)| {
            // let cpu_id = assignment.cpu_set.iter().next().unwrap_or(&idx);
            ShardConnector::new(idx as u16)
        })
        .collect();

    let shutdown_handles = connectors
        .iter()
        .map(|conn| (conn.id, conn.stop_sender.clone()))
        .collect();

    (connectors, shutdown_handles)
}

pub async fn load_config() -> Result<ServerConfig, ServerError> {
    let config = ServerConfig::load().await?;
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

pub fn create_shard_executor() -> Runtime {
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
            .error(|e: &IggyError| format!("Failed to load indexes during startup for stream ID: {}, topic ID: {}, partition_id: {}, {e}", stream_id, topic_id, partition_id))
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
                                return Err(IggyError::CannotReadPartitions).error(|_: &IggyError| {
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
                            return Err(e).error(|_: &IggyError| {
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

/// Builds `InnerMetadata` from persisted user and stream state.
pub fn build_inner_metadata(
    users_state: impl IntoIterator<Item = UserState>,
    streams_state: impl IntoIterator<Item = StreamState>,
) -> crate::metadata::InnerMetadata {
    use crate::metadata::InnerMetadata;
    use std::sync::atomic::AtomicUsize;

    let mut user_entries = Vec::new();
    let mut user_index = ahash::AHashMap::default();
    let mut personal_access_tokens: ahash::AHashMap<
        u32,
        ahash::AHashMap<Arc<str>, PersonalAccessToken>,
    > = ahash::AHashMap::default();
    let mut users_count = 0;
    let mut pats_count = 0;

    for UserState {
        id,
        username,
        password_hash,
        status,
        created_at,
        permissions,
        personal_access_tokens: user_pats,
    } in users_state
    {
        let username_arc: Arc<str> = Arc::from(username.as_str());
        let user_meta = UserMeta {
            id,
            username: username_arc.clone(),
            password_hash: Arc::from(password_hash.as_str()),
            status,
            permissions: permissions.map(Arc::new),
            created_at,
        };
        user_entries.push((id as usize, user_meta));
        user_index.insert(username_arc, id);

        if !user_pats.is_empty() {
            let user_pat_map: ahash::AHashMap<Arc<str>, PersonalAccessToken> = user_pats
                .into_values()
                .map(|token| {
                    let pat = PersonalAccessToken::raw(
                        id,
                        &token.name,
                        &token.token_hash,
                        token.expiry_at,
                    );
                    (Arc::from(token.token_hash.as_str()), pat)
                })
                .collect();
            pats_count += user_pat_map.len();
            personal_access_tokens.insert(id, user_pat_map);
        }

        users_count += 1;
    }
    info!(
        "Building metadata: {} users, {} personal access tokens",
        users_count, pats_count
    );

    let mut stream_entries = Vec::new();
    let mut stream_index = ahash::AHashMap::default();
    let mut streams_count = 0;
    let mut topics_count = 0;
    let mut partitions_count = 0;
    let mut consumer_groups_count = 0;

    for StreamState {
        name,
        created_at,
        id,
        topics,
    } in streams_state
    {
        info!(
            "Building stream with ID: {}, name: {} metadata...",
            id, name
        );
        let stream_id = id as usize;
        let stream_name: Arc<str> = Arc::from(name.as_str());

        let stream_stats = Arc::new(StreamStats::default());

        let mut topic_entries = Vec::new();
        let mut topic_index = ahash::AHashMap::default();

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
        } in topics.into_values()
        {
            info!("Building topic with ID: {}, name: {} metadata...", id, name);
            let topic_id = id as usize;
            let topic_name: Arc<str> = Arc::from(name.as_str());

            let topic_stats = Arc::new(TopicStats::new(stream_stats.clone()));

            let mut partition_entries = Vec::new();
            let mut partition_ids = Vec::new();

            for partition_state in partitions.into_values() {
                let partition_id = partition_state.id as usize;
                partition_ids.push(partition_id);

                let partition_stats = Arc::new(PartitionStats::new(topic_stats.clone()));
                let partition_meta = PartitionMeta {
                    id: partition_id,
                    created_at: partition_state.created_at,
                    revision_id: 0,
                    stats: partition_stats,
                    consumer_offsets: Arc::new(ConsumerOffsets::with_capacity(0)),
                    consumer_group_offsets: Arc::new(ConsumerGroupOffsets::with_capacity(0)),
                };
                partition_entries.push((partition_id, partition_meta));
                partitions_count += 1;
            }

            partition_ids.sort_unstable();

            let mut cg_entries = Vec::new();
            let mut cg_index = ahash::AHashMap::default();

            for cg_state in consumer_groups.into_values() {
                info!(
                    "Building consumer group with ID: {}, name: {} for topic with ID: {} metadata...",
                    cg_state.id, cg_state.name, topic_id
                );
                let group_id = cg_state.id as usize;
                let group_name: Arc<str> = Arc::from(cg_state.name.as_str());
                let cg_meta = ConsumerGroupMeta {
                    id: group_id,
                    name: group_name.clone(),
                    partitions: partition_ids.clone(),
                    members: Slab::new(),
                };
                cg_entries.push((group_id, cg_meta));
                cg_index.insert(group_name, group_id);
                consumer_groups_count += 1;
            }

            let topic_meta = TopicMeta {
                id: topic_id,
                name: topic_name.clone(),
                created_at,
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor: replication_factor.unwrap_or(1),
                stats: topic_stats,
                partitions: partition_entries.into_iter().map(|(_, p)| p).collect(),
                consumer_groups: cg_entries.into_iter().collect(),
                consumer_group_index: cg_index,
                round_robin_counter: Arc::new(AtomicUsize::new(0)),
            };
            topic_entries.push((topic_id, topic_meta));
            topic_index.insert(topic_name, topic_id);
            topics_count += 1;
        }

        let stream_meta = StreamMeta {
            id: stream_id,
            name: stream_name.clone(),
            created_at,
            stats: stream_stats,
            topics: topic_entries.into_iter().collect(),
            topic_index,
        };
        stream_entries.push((stream_id, stream_meta));
        stream_index.insert(stream_name, stream_id);
        streams_count += 1;
    }

    info!(
        "Built metadata: {} streams, {} topics, {} partitions, {} consumer groups",
        streams_count, topics_count, partitions_count, consumer_groups_count
    );

    InnerMetadata {
        streams: stream_entries.into_iter().collect(),
        users: user_entries.into_iter().collect(),
        stream_index,
        user_index,
        personal_access_tokens,
        users_global_permissions: Default::default(),
        users_stream_permissions: Default::default(),
        users_can_poll_all_streams: Default::default(),
        users_can_send_all_streams: Default::default(),
        users_can_poll_stream: Default::default(),
        users_can_send_stream: Default::default(),
    }
}

/// Loads all metadata from persisted state into metadata writer.
/// Single atomic initialization via `Initialize` operation.
pub fn load_metadata(
    users_state: impl IntoIterator<Item = UserState>,
    streams_state: impl IntoIterator<Item = StreamState>,
    writer: &mut MetadataWriter,
) {
    let inner = build_inner_metadata(users_state, streams_state);
    writer.initialize(inner);
}
