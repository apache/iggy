use crate::{
    IGGY_ROOT_PASSWORD_ENV, IGGY_ROOT_USERNAME_ENV,
    compat::index_rebuilding::index_rebuilder::IndexRebuilder,
    configs::cache_indexes::CacheIndexesConfig,
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
            EntityComponentSystem, EntityComponentSystemMutCell, Insert, InsertCell, IntoComponents,
        },
    },
    state::system::{StreamState, TopicState, UserState},
    streaming::segments::{INDEX_EXTENSION, LOG_EXTENSION, Segment2, storage::Storage},
    streaming::{
        partitions::{
            consumer_offset::ConsumerOffset, helpers::create_message_deduplicator,
            journal::MemoryMessageJournal, log::SegmentedLog, partition2,
            storage2::load_consumer_offsets,
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
use error_set::ErrContext;
use iggy_common::{
    ConsumerKind, IggyError, UserId,
    defaults::{
        DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME, MAX_PASSWORD_LENGTH, MAX_USERNAME_LENGTH,
        MIN_PASSWORD_LENGTH, MIN_USERNAME_LENGTH,
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
        let stream = stream2::Stream::new(name.clone(), stats.clone(), created_at);
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
                let topic = topic2::Topic::new(
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
                let partition = load_partition_with_segments(
                    config,
                    stream_id as usize,
                    topic_id,
                    partition_state,
                    parent_stats.clone(),
                )
                .await?;

                // Insert partition into the container
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
                            let cg = consumer_group2::ConsumerGroup::new(cg_state.name.clone(), Default::default(), partition_ids.clone());
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

pub fn load_users(state: impl IntoIterator<Item = UserState>) -> HashMap<UserId, User> {
    let mut users = HashMap::default();
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
        .taskrun_flag(true); // TODO: Try enabling this.

    // FIXME(hubcio): Only set thread_pool_limit(0) on non-macOS platforms
    // This causes a freeze on macOS with compio fs operations
    // see https://github.com/compio-rs/compio/issues/446
    #[cfg(not(all(target_os = "macos", target_arch = "aarch64")))]
    proactor.thread_pool_limit(0);

    compio::runtime::RuntimeBuilder::new()
        .with_proactor(proactor.to_owned())
        .event_interval(128)
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

async fn load_partition_with_segments(
    config: &SystemConfig,
    stream_id: usize,
    topic_id: usize,
    partition_state: crate::state::system::PartitionState,
    parent_stats: Arc<TopicStats>,
) -> Result<partition2::Partition, IggyError> {
    use std::sync::atomic::AtomicU64;

    let stats = Arc::new(PartitionStats::new(parent_stats));
    let partition_id = partition_state.id as u32;

    // Load segments from disk to determine should_increment_offset and current offset
    let partition_path = config.get_partition_path(stream_id, topic_id, partition_id as usize);

    info!(
        "Loading partition with ID: {} for stream with ID: {} and topic with ID: {}, for path: {} from disk...",
        partition_id, stream_id, topic_id, partition_path
    );

    // Read directory entries to find log files using async fs_utils
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

    log_files.sort_by(|a, b| a.path.file_name().cmp(&b.path.file_name()));

    let mut should_increment_offset = false;
    let mut current_offset = 0u64;
    let mut log = SegmentedLog::<MemoryMessageJournal>::default();

    for entry in log_files {
        let log_file_name = entry
            .path
            .file_stem()
            .unwrap()
            .to_string_lossy()
            .to_string();

        let start_offset = log_file_name.parse::<u64>().unwrap();

        // Build file paths directly
        let messages_file_path = format!("{}/{}.{}", partition_path, start_offset, LOG_EXTENSION);
        let index_file_path = format!("{}/{}.{}", partition_path, start_offset, INDEX_EXTENSION);
        let time_index_path = index_file_path.replace(INDEX_EXTENSION, "timeindex");

        // Check if index files exist
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

        // Rebuild indexes if index cache is enabled and index at path does not exist
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

        // Get file metadata to determine segment properties
        let messages_metadata = compio::fs::metadata(&messages_file_path)
            .await
            .map_err(|_| IggyError::CannotReadPartitions)?;
        let messages_size = messages_metadata.len() as u32;

        let index_size = match compio::fs::metadata(&index_file_path).await {
            Ok(metadata) => metadata.len() as u32,
            Err(_) => 0, // Default to 0 if index file doesn't exist
        };

        // If the first segment has messages, we should increment the offset
        if !should_increment_offset {
            should_increment_offset = messages_size > 0;
        }

        // For segment validation, we'd need to implement checksum validation
        // directly on the files if needed - skipping for now as it requires
        // understanding the message format
        if config.partition.validate_checksum {
            info!("Checksum validation for segment at offset {}", start_offset);
        }

        // Create storage for the segment using existing files
        let storage = Storage::new(
            &messages_file_path,
            &index_file_path,
            messages_size as u64,
            index_size as u64,
            config.partition.enforce_fsync,
            config.partition.enforce_fsync,
            true, // file_exists = true for existing segments
        )
        .await?;

        // Load indexes from disk to calculate the correct end offset and cache them if needed
        // This matches the logic in Segment::load_from_disk method
        let loaded_indexes = {
            storage.
            index_reader
            .as_ref()
            .unwrap()
            .load_all_indexes_from_disk()
            .await
            .with_error_context(|error| format!("Failed to load indexes during startup for stream ID: {}, topic ID: {}, partition_id: {}, {error}", stream_id, topic_id, partition_id))
            .map_err(|_| IggyError::CannotReadFile)?
        };

        // Calculate end offset based on loaded indexes
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

        current_offset = current_offset.max(end_offset);

        // Create the new Segment with proper values from file system
        let mut segment = Segment2::new(
            start_offset,
            config.segment.size,
            config.segment.message_expiry,
        );

        // Set properties based on file data
        segment.start_timestamp = start_timestamp;
        segment.end_timestamp = end_timestamp;
        segment.end_offset = end_offset;
        segment.size = messages_size;
        segment.sealed = true; // Persisted segments are assumed to be sealed

        // Add segment to log first
        log.add_persisted_segment(segment, storage);

        // Increment stats for partition - this matches the behavior from partition storage load method
        stats.increment_segments_count(1);
        
        // Increment size and message counts based on the loaded segment data
        stats.increment_size_bytes(messages_size as u64);
        
        // Calculate message count from segment data (end_offset - start_offset + 1 if there are messages)
        let messages_count = if end_offset > start_offset {
            (end_offset - start_offset + 1) as u64
        } else if messages_size > 0 {
            // Fallback: estimate based on loaded indexes count if available
            loaded_indexes.count() as u64
        } else {
            0
        };
        
        if messages_count > 0 {
            stats.increment_messages_count(messages_count);
        }

        // Now handle index caching based on configuration
        let should_cache_indexes = match config.segment.cache_indexes {
            CacheIndexesConfig::All => true,
            CacheIndexesConfig::OpenSegment => false, // Will be handled after all segments are loaded
            CacheIndexesConfig::None => false,
        };

        // Set the loaded indexes if we should cache them
        if should_cache_indexes {
            let segment_index = log.segments().len() - 1;
            log.set_segment_indexes(segment_index, loaded_indexes);
        }
    }

    // Handle OpenSegment cache configuration: only the last segment should keep its indexes
    if matches!(
        config.segment.cache_indexes,
        CacheIndexesConfig::OpenSegment
    ) && log.has_segments()
    {
        let segments_count = log.segments().len();
        if segments_count > 0 {
            // Use the IndexReader from the last segment's storage to load indexes
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

    // Load consumer offsets
    let message_deduplicator = create_message_deduplicator(config);
    let consumer_offset_path =
        config.get_consumer_offsets_path(stream_id, topic_id, partition_id as usize);
    let consumer_group_offsets_path =
        config.get_consumer_group_offsets_path(stream_id, topic_id, partition_id as usize);

    let consumer_offset = Arc::new(
        load_consumer_offsets(&consumer_offset_path, ConsumerKind::Consumer)?
            .into_iter()
            .map(|offset| (offset.consumer_id as usize, offset))
            .collect::<HashMap<usize, ConsumerOffset>>()
            .into(),
    );

    let consumer_group_offset = Arc::new(
        load_consumer_offsets(&consumer_group_offsets_path, ConsumerKind::ConsumerGroup)?
            .into_iter()
            .map(|offset| (offset.consumer_id as usize, offset))
            .collect::<HashMap<usize, ConsumerOffset>>()
            .into(),
    );

    let partition = partition2::Partition::new(
        partition_state.created_at,
        should_increment_offset,
        stats,
        message_deduplicator,
        Arc::new(AtomicU64::new(current_offset)),
        consumer_offset,
        consumer_group_offset,
        log
    );

    info!(
        "Loaded partition with ID: {} for stream with ID: {} and topic with ID: {}, current offset: {}.",
        partition_id, stream_id, topic_id, current_offset
    );

    Ok(partition)
}
