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

use crate::server_error::ServerNgError;
use configs::server::ServerConfig;
use consensus::{LocalPipeline, PartitionsHandle, Sequencer, VsrConsensus};
use iggy_common::sharding::{IggyNamespace, PartitionLocation, ShardId};
use iggy_common::{
    ConsumerGroupOffsets, ConsumerOffsets, IggyByteSize, PartitionStats, TopicStats,
    sharding::LocalIdx, variadic,
};
use journal::Journal;
use journal::prepare_journal::PrepareJournal;
use message_bus::IggyMessageBus;
use metadata::IggyMetadata;
use metadata::MuxStateMachine;
use metadata::impls::metadata::{IggySnapshot, StreamsFrontend};
use metadata::impls::recovery::recover;
use metadata::stm::consumer_group::ConsumerGroups;
use metadata::stm::snapshot::Snapshot;
use metadata::stm::stream::{Partition, Streams};
use metadata::stm::user::Users;
use partitions::{
    IggyIndexWriter, IggyPartition, IggyPartitions, MessagesWriter, PartitionsConfig, Segment,
};
// TODO: decouple bootstrap/storage helpers and logging from the `server` crate.
use server::bootstrap::create_directories;
use server::log::logger::Logging;
use server::streaming::partitions::storage::{load_consumer_group_offsets, load_consumer_offsets};
use server::streaming::segments::storage::create_segment_storage;
use shard::shards_table::PapayaShardsTable;
use shard::{IggyShard, PartitionConsensusConfig, ShardIdentity};
use std::future::pending;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{info, warn};

const CLUSTER_ID: u128 = 1;
const SHARD_ID: u16 = 0;
const SHARD_REPLICA_ID: u8 = 0;
const SHARD_NAME: &str = "server-ng-shard-0";
const DEFAULT_CONFIG_PATH: &str = "core/server-ng/config.toml";

type ServerNgMuxStateMachine = MuxStateMachine<variadic!(Users, Streams, ConsumerGroups)>;
type ServerNgMetadata = IggyMetadata<
    VsrConsensus<IggyMessageBus>,
    PrepareJournal,
    IggySnapshot,
    ServerNgMuxStateMachine,
>;
type ServerNgShard = IggyShard<
    IggyMessageBus,
    PrepareJournal,
    IggySnapshot,
    ServerNgMuxStateMachine,
    PapayaShardsTable,
>;

pub trait RunServerNg {
    fn run(&self) -> impl Future<Output = Result<(), ServerNgError>>;
}

impl RunServerNg for Rc<ServerNgShard> {
    async fn run(&self) -> Result<(), ServerNgError> {
        info!(
            shard = self.id,
            partitions = self.plane.partitions().len(),
            "server-ng shard initialized"
        );
        warn!("TODO: start listeners, timers, and runtime services once the new infra exists");
        pending::<()>().await;
        #[allow(unreachable_code)]
        Ok(())
    }
}

/// Bootstraps `server-ng` from config and on-disk metadata/partition state.
///
/// # Errors
///
/// Returns an error if config loading, directory preparation, logging setup,
/// metadata recovery, or partition hydration fails.
pub async fn bootstrap(logging: &mut Logging) -> Result<Rc<ServerNgShard>, ServerNgError> {
    let config = ServerConfig::load_with_path(DEFAULT_CONFIG_PATH, include_str!("../config.toml"))
        .await
        .map_err(ServerNgError::Config)?;
    // TODO: decouple directory bootstrap from the `server` crate.
    create_directories(&config.system)
        .await
        .map_err(ServerNgError::CreateDirectories)?;
    logging
        .late_init(
            config.system.get_system_path(),
            &config.system.logging,
            &config.telemetry,
        )
        .map_err(ServerNgError::Logging)?;

    iggy_common::MemoryPool::init_pool(&config.system.memory_pool.into_other());

    let recovered = recover::<ServerNgMuxStateMachine>(Path::new(&config.system.path))
        .await
        .map_err(ServerNgError::MetadataRecovery)?;
    let restored_op = recovered
        .last_applied_op
        .unwrap_or_else(|| recovered.snapshot.sequence_number());

    let metadata = ServerNgMetadata::new(
        Some(restore_metadata_consensus(&recovered.journal, restored_op)),
        Some(recovered.journal),
        Some(recovered.snapshot),
        recovered.mux_stm,
        Some(PathBuf::from(&config.system.path)),
    );
    let shard = Rc::new(build_single_shard(&config, metadata).await?);
    info!(shard = shard.id, "server-ng bootstrap complete");

    Ok(shard)
}

fn restore_metadata_consensus(
    journal: &PrepareJournal,
    restored_op: u64,
) -> VsrConsensus<IggyMessageBus> {
    let mut consensus = VsrConsensus::new(
        CLUSTER_ID,
        SHARD_REPLICA_ID,
        1,
        0,
        IggyMessageBus::new(1, SHARD_ID, 0),
        LocalPipeline::new(),
    );

    let last_header = journal
        .last_op()
        .and_then(|op| usize::try_from(op).ok())
        .and_then(|op| journal.header(op).map(|header| *header));
    if let Some(header) = last_header {
        consensus.set_view(header.view);
    }

    consensus.init();
    consensus.sequencer().set_sequence(restored_op);
    consensus.restore_commit_state(restored_op, restored_op);
    if let Some(header) = last_header {
        consensus.set_last_prepare_checksum(header.checksum);
        consensus.set_log_view(header.view);
    }

    consensus
}

async fn build_single_shard(
    config: &ServerConfig,
    metadata: ServerNgMetadata,
) -> Result<ServerNgShard, ServerNgError> {
    let shard_id = ShardId::new(SHARD_ID);
    let partition_count = metadata.mux_stm.streams().read(|inner| {
        inner
            .items
            .iter()
            .map(|(_, stream)| {
                stream
                    .topics
                    .iter()
                    .map(|(_, topic)| topic.partitions.len())
                    .sum::<usize>()
            })
            .sum()
    });
    let mut partitions = IggyPartitions::with_capacity(
        shard_id,
        PartitionsConfig {
            messages_required_to_save: config.system.partition.messages_required_to_save,
            size_of_messages_required_to_save: config
                .system
                .partition
                .size_of_messages_required_to_save,
            enforce_fsync: config.system.partition.enforce_fsync,
            segment_size: config.system.segment.size,
        },
        partition_count,
    );
    let shards_table = PapayaShardsTable::with_capacity(partition_count);

    let (topic_stats, namespaces) = metadata.mux_stm.streams().read(|inner| {
        let mut topic_stats = Vec::new();
        let mut namespaces = Vec::with_capacity(partition_count);
        for (_, stream) in &inner.items {
            for (topic_id, topic) in &stream.topics {
                topic_stats.push(topic.stats.clone());
                for partition in &topic.partitions {
                    namespaces.push((stream.id, topic_id, topic.stats.clone(), partition.clone()));
                }
            }
        }
        (topic_stats, namespaces)
    });

    for topic_stats in topic_stats {
        topic_stats.zero_out_all();
    }

    for (stream_id, topic_id, topic_stats, partition_metadata) in namespaces {
        validate_recovered_namespace(config, stream_id, topic_id, partition_metadata.id)?;
        let namespace = IggyNamespace::new(stream_id, topic_id, partition_metadata.id);
        let partition = load_partition(config, namespace, topic_stats, &partition_metadata).await?;
        let local_idx = partitions.insert(namespace, partition);
        shards_table.insert(
            namespace,
            PartitionLocation::new(shard_id, LocalIdx::new(*local_idx)),
        );
    }

    Ok(IggyShard::without_inbox(
        ShardIdentity::new(SHARD_ID, SHARD_NAME.to_string()),
        metadata,
        partitions,
        shards_table,
        PartitionConsensusConfig::new(CLUSTER_ID, 1, IggyMessageBus::new(1, SHARD_ID, 0)),
    ))
}

const fn validate_recovered_namespace(
    config: &ServerConfig,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> Result<(), ServerNgError> {
    let namespace = &config.extra.namespace;
    if stream_id < namespace.max_streams
        && topic_id < namespace.max_topics
        && partition_id < namespace.max_partitions
    {
        return Ok(());
    }

    Err(ServerNgError::RecoveredNamespaceOutOfBounds {
        stream_id,
        topic_id,
        partition_id,
        max_streams: namespace.max_streams,
        max_topics: namespace.max_topics,
        max_partitions: namespace.max_partitions,
    })
}

async fn load_partition(
    config: &ServerConfig,
    namespace: IggyNamespace,
    topic_stats: Arc<TopicStats>,
    partition_metadata: &Partition,
) -> Result<IggyPartition<IggyMessageBus>, ServerNgError> {
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
    let partition_id = namespace.partition_id();
    let stats = Arc::new(PartitionStats::new(topic_stats));
    let consensus = VsrConsensus::new(
        CLUSTER_ID,
        SHARD_REPLICA_ID,
        1,
        namespace.inner(),
        IggyMessageBus::new(1, SHARD_ID, namespace.inner()),
        LocalPipeline::new(),
    );
    consensus.init();

    // TODO: decouple the loading logic from the `server` crate and load directly
    // into the new `partitions` log/runtime types.
    let loaded_log = server::bootstrap::load_segments(
        &config.system,
        stream_id,
        topic_id,
        partition_id,
        config
            .system
            .get_partition_path(stream_id, topic_id, partition_id),
        stats.clone(),
    )
    .await
    .map_err(|source| ServerNgError::PartitionLogLoad {
        stream_id,
        topic_id,
        partition_id,
        source,
    })?;

    let mut partition = IggyPartition::new(stats.clone(), consensus);
    hydrate_partition_log(
        &mut partition,
        config,
        stream_id,
        topic_id,
        partition_id,
        loaded_log,
    )
    .await?;

    let current_offset = partition
        .log
        .segments()
        .iter()
        .filter(|segment| segment.size > IggyByteSize::default())
        .map(|segment| segment.end_offset)
        .max()
        .unwrap_or(0);
    partition.created_at = partition_metadata.created_at;
    partition.offset.store(current_offset, Ordering::Release);
    partition
        .dirty_offset
        .store(current_offset, Ordering::Relaxed);
    partition.should_increment_offset = partition
        .log
        .segments()
        .iter()
        .any(|segment| segment.size > IggyByteSize::default());
    partition.stats.set_current_offset(current_offset);

    configure_consumer_offsets(&mut partition, config, namespace, current_offset);
    ensure_initial_segment(&mut partition, config, stream_id, topic_id, partition_id).await?;

    Ok(partition)
}

async fn hydrate_partition_log(
    partition: &mut IggyPartition<IggyMessageBus>,
    config: &ServerConfig,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    loaded_log: server::streaming::partitions::log::SegmentedLog<
        server::streaming::partitions::journal::MemoryMessageJournal,
    >,
) -> Result<(), ServerNgError> {
    // TODO: decouple the loading logic from the `server` crate. This currently
    // adapts the old server segmented log into the new `partitions` log.
    for (segment, storage) in loaded_log
        .segments()
        .iter()
        .zip(loaded_log.storages().iter().cloned())
    {
        partition
            .log
            .add_persisted_segment(convert_segment(segment), storage, None, None);
    }

    if let Some(active_index) = partition.log.segments().len().checked_sub(1) {
        let storage = &partition.log.storages()[active_index];
        if let (Some(messages_reader), Some(index_reader)) = (
            storage.messages_reader.as_ref(),
            storage.index_reader.as_ref(),
        ) {
            let index_path = index_reader.path();
            let index_size = std::fs::metadata(&index_path)
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            partition.log.messages_writers_mut()[active_index] = Some(Rc::new(
                MessagesWriter::new(
                    &messages_reader.path(),
                    Rc::new(AtomicU64::new(u64::from(messages_reader.file_size()))),
                    config.system.partition.enforce_fsync,
                    true,
                )
                .await
                .map_err(|source| ServerNgError::MessagesWriterInit {
                    stream_id,
                    topic_id,
                    partition_id,
                    source,
                })?,
            ));
            partition.log.index_writers_mut()[active_index] = Some(Rc::new(
                IggyIndexWriter::new(
                    &index_path,
                    Rc::new(AtomicU64::new(index_size)),
                    config.system.partition.enforce_fsync,
                    true,
                )
                .await
                .map_err(|source| ServerNgError::IndexWriterInit {
                    stream_id,
                    topic_id,
                    partition_id,
                    source,
                })?,
            ));
        }
    }

    Ok(())
}

fn convert_segment(segment: &iggy_common::Segment) -> Segment {
    Segment {
        sealed: segment.sealed,
        start_timestamp: segment.start_timestamp,
        end_timestamp: segment.end_timestamp,
        max_timestamp: segment.end_timestamp,
        current_position: u64::from(segment.current_position),
        start_offset: segment.start_offset,
        end_offset: segment.end_offset,
        size: segment.size,
        max_size: segment.max_size,
    }
}

fn configure_consumer_offsets(
    partition: &mut IggyPartition<IggyMessageBus>,
    config: &ServerConfig,
    namespace: IggyNamespace,
    current_offset: u64,
) {
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
    let partition_id = namespace.partition_id();
    let consumer_offsets_path =
        config
            .system
            .get_consumer_offsets_path(stream_id, topic_id, partition_id);
    let consumer_group_offsets_path =
        config
            .system
            .get_consumer_group_offsets_path(stream_id, topic_id, partition_id);

    // TODO: decouple consumer offset loading from the `server` crate.
    let loaded_consumer_offsets = load_consumer_offsets(&consumer_offsets_path).unwrap_or_default();
    let consumer_offsets = ConsumerOffsets::with_capacity(loaded_consumer_offsets.len());
    {
        let guard = consumer_offsets.pin();
        for offset in loaded_consumer_offsets {
            if offset.offset.load(Ordering::Relaxed) > current_offset {
                offset.offset.store(current_offset, Ordering::Relaxed);
            }
            guard.insert(offset.consumer_id as usize, offset);
        }
    }

    // TODO: decouple consumer group offset loading from the `server` crate.
    let loaded_group_offsets =
        load_consumer_group_offsets(&consumer_group_offsets_path).unwrap_or_default();
    let consumer_group_offsets = ConsumerGroupOffsets::with_capacity(loaded_group_offsets.len());
    {
        let guard = consumer_group_offsets.pin();
        for (group_id, offset) in loaded_group_offsets {
            if offset.offset.load(Ordering::Relaxed) > current_offset {
                offset.offset.store(current_offset, Ordering::Relaxed);
            }
            guard.insert(group_id, offset);
        }
    }

    partition.configure_consumer_offset_storage(
        consumer_offsets_path,
        consumer_group_offsets_path,
        consumer_offsets,
        consumer_group_offsets,
        config.system.partition.enforce_fsync,
    );
}

async fn ensure_initial_segment(
    partition: &mut IggyPartition<IggyMessageBus>,
    config: &ServerConfig,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> Result<(), ServerNgError> {
    if partition.log.has_segments() {
        return Ok(());
    }

    // TODO: decouple segment storage creation from the `server` crate.
    let storage =
        create_segment_storage(&config.system, stream_id, topic_id, partition_id, 0, 0, 0)
            .await
            .map_err(|source| ServerNgError::InitialSegmentStorage {
                stream_id,
                topic_id,
                partition_id,
                source,
            })?;
    let messages_path = config
        .system
        .get_messages_file_path(stream_id, topic_id, partition_id, 0);
    let index_path = config
        .system
        .get_index_path(stream_id, topic_id, partition_id, 0);
    partition.log.add_persisted_segment(
        Segment::new(0, config.system.segment.size),
        storage,
        Some(Rc::new(
            MessagesWriter::new(
                &messages_path,
                Rc::new(AtomicU64::new(0)),
                config.system.partition.enforce_fsync,
                false,
            )
            .await
            .map_err(|source| ServerNgError::MessagesWriterInit {
                stream_id,
                topic_id,
                partition_id,
                source,
            })?,
        )),
        Some(Rc::new(
            IggyIndexWriter::new(
                &index_path,
                Rc::new(AtomicU64::new(0)),
                config.system.partition.enforce_fsync,
                false,
            )
            .await
            .map_err(|source| ServerNgError::IndexWriterInit {
                stream_id,
                topic_id,
                partition_id,
                source,
            })?,
        )),
    );
    partition.stats.increment_segments_count(1);

    Ok(())
}
