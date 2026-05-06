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

use crate::config_writer::write_current_config;
use crate::server_error::ServerNgError;
use configs::server_ng::ServerNgConfig;
use consensus::{LocalPipeline, PartitionsHandle, Sequencer, VsrConsensus};
use iggy_binary_protocol::RequestHeader;
use iggy_common::sharding::{IggyNamespace, PartitionLocation, ShardId};
use iggy_common::{
    ConsumerGroupOffsets, ConsumerOffsets, IggyByteSize, PartitionStats, TopicStats,
    sharding::LocalIdx, variadic,
};
use journal::Journal;
use journal::prepare_journal::PrepareJournal;
use message_bus::client_listener::{self, RequestHandler};
use message_bus::fd_transfer;
use message_bus::installer;
use message_bus::installer::ConnectionInstaller;
use message_bus::installer::conn_info::{ClientConnMeta, ClientTransportKind};
use message_bus::replica::io as replica_io;
use message_bus::replica::listener::{self as replica_listener, MessageHandler};
use message_bus::transports::quic::server_config_with_cert;
use message_bus::transports::tls::{
    TlsServerCredentials, install_default_crypto_provider, load_pem, self_signed_for_loopback,
};
use message_bus::{
    AcceptedClientFn, AcceptedQuicClientFn, AcceptedReplicaFn, AcceptedTlsClientFn,
    AcceptedWsClientFn, IggyMessageBus, connector,
};
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
use shard::builder::IggyShardBuilder;
use shard::shards_table::PapayaShardsTable;
use shard::{
    CoordinatorConfig, IggyShard, PartitionConsensusConfig, ShardIdentity, channel, shard_channel,
};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{info, warn};

const CLUSTER_ID: u128 = 1;
const SHARD_ID: u16 = 0;
const SHARD_REPLICA_ID: u8 = 0;
const SHARD_NAME: &str = "server-ng-shard-0";
const SHARD_INBOX_CAPACITY: usize = 1024;

type ServerNgMuxStateMachine = MuxStateMachine<variadic!(Users, Streams, ConsumerGroups)>;
type ServerNgMetadata = IggyMetadata<
    VsrConsensus<Rc<IggyMessageBus>>,
    PrepareJournal,
    IggySnapshot,
    ServerNgMuxStateMachine,
>;
type ServerNgShard = IggyShard<
    Rc<IggyMessageBus>,
    PrepareJournal,
    IggySnapshot,
    ServerNgMuxStateMachine,
    PapayaShardsTable,
>;

type ServerNgShardHandle = Rc<RefCell<Option<Weak<ServerNgShard>>>>;

struct TcpTopology {
    self_replica_id: u8,
    replica_count: u8,
    client_listen_addr: SocketAddr,
    replica_listen_addr: Option<SocketAddr>,
    ws_listen_addr: Option<SocketAddr>,
    quic_listen_addr: Option<SocketAddr>,
    tcp_tls_listen_addr: Option<SocketAddr>,
    peers: Vec<(u8, SocketAddr)>,
}

struct LocalClientAcceptFns {
    tcp: AcceptedClientFn,
    ws: AcceptedWsClientFn,
    quic: AcceptedQuicClientFn,
    tcp_tls: AcceptedTlsClientFn,
}

#[derive(Default)]
struct BoundClientListeners {
    tcp: Option<SocketAddr>,
    tcp_tls: Option<SocketAddr>,
    ws: Option<SocketAddr>,
    quic: Option<SocketAddr>,
}

pub trait RunServerNg {
    fn run(
        &self,
        config: &ServerNgConfig,
        current_replica_id: Option<u8>,
    ) -> impl Future<Output = Result<(), ServerNgError>>;
}

impl RunServerNg for Rc<ServerNgShard> {
    /// Run the fully bootstrapped `server-ng` shard.
    ///
    /// # Errors
    ///
    /// Returns an error if TCP listener bootstrap fails or cluster TCP
    /// addresses cannot be resolved from config.
    async fn run(
        &self,
        config: &ServerNgConfig,
        current_replica_id: Option<u8>,
    ) -> Result<(), ServerNgError> {
        let topology = resolve_tcp_topology(config, current_replica_id)?;
        let (stop_tx, stop_rx) = channel(1);
        let message_pump_shard = Self::clone(self);
        let message_pump_handle = compio::runtime::spawn(async move {
            message_pump_shard.run_message_pump(stop_rx).await;
        });
        self.bus.track_background(message_pump_handle);

        let on_replica_message = make_replica_message_handler(self);
        let on_client_request = make_client_request_handler(self);
        let accepted_replica = make_local_replica_accept_fn(&self.bus, on_replica_message);
        let accepted_client = make_local_client_accept_fns(&self.bus, on_client_request);

        info!(
            shard = self.id,
            partitions = self.plane.partitions().len(),
            "server-ng shard initialized"
        );

        if let Err(error) =
            start_tcp_runtime(self, config, &topology, accepted_replica, accepted_client).await
        {
            let _ = stop_tx.try_send(());
            return Err(error);
        }

        self.bus.token().wait().await;
        let _ = stop_tx.try_send(());
        Ok(())
    }
}

/// Load config, prepare directories, and complete late logging init.
///
/// # Errors
///
/// Returns an error if config loading, directory preparation, or logging
/// setup fails.
pub async fn load_config(logging: &mut Logging) -> Result<ServerNgConfig, ServerNgError> {
    let config = ServerNgConfig::load()
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

    Ok(config)
}

/// Bootstraps `server-ng` from config and on-disk metadata/partition state.
///
/// # Errors
///
/// Returns an error if metadata recovery, consensus restoration, or
/// partition hydration fails.
pub async fn bootstrap(
    config: &ServerNgConfig,
    current_replica_id: Option<u8>,
) -> Result<Rc<ServerNgShard>, ServerNgError> {
    let topology = resolve_tcp_topology(config, current_replica_id)?;
    let bus = Rc::new(IggyMessageBus::with_config(SHARD_ID, config));
    let recovered = recover::<ServerNgMuxStateMachine>(Path::new(&config.system.path))
        .await
        .map_err(ServerNgError::MetadataRecovery)?;
    let restored_op = recovered.last_applied_op.unwrap_or_else(|| {
        recovered
            .snapshot
            .as_ref()
            .map_or(0, IggySnapshot::sequence_number)
    });

    let metadata = ServerNgMetadata::new(
        Some(restore_metadata_consensus(
            &recovered.journal,
            restored_op,
            topology.self_replica_id,
            topology.replica_count,
            Rc::clone(&bus),
        )),
        Some(recovered.journal),
        recovered.snapshot,
        recovered.mux_stm,
        Some(PathBuf::from(&config.system.path)),
    );
    let shard = build_single_shard(config, &topology, metadata, bus).await?;
    info!(shard = shard.id, "server-ng bootstrap complete");

    Ok(shard)
}

fn restore_metadata_consensus(
    journal: &PrepareJournal,
    restored_op: u64,
    self_replica_id: u8,
    replica_count: u8,
    bus: Rc<IggyMessageBus>,
) -> VsrConsensus<Rc<IggyMessageBus>> {
    let mut consensus = VsrConsensus::new(
        CLUSTER_ID,
        self_replica_id,
        replica_count,
        0,
        bus,
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
    config: &ServerNgConfig,
    topology: &TcpTopology,
    metadata: ServerNgMetadata,
    bus: Rc<IggyMessageBus>,
) -> Result<Rc<ServerNgShard>, ServerNgError> {
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
        let partition = load_partition(
            config,
            namespace,
            topic_stats,
            &partition_metadata,
            topology.self_replica_id,
            topology.replica_count,
            Rc::clone(&bus),
        )
        .await?;
        let local_idx = partitions.insert(namespace, partition);
        shards_table.insert(
            namespace,
            PartitionLocation::new(shard_id, LocalIdx::new(*local_idx)),
        );
    }

    let (sender, inbox) = shard_channel::<()>(SHARD_ID, SHARD_INBOX_CAPACITY);
    let senders = vec![sender];
    let shard_handle = Rc::new(RefCell::new(None));
    let on_replica_message = make_deferred_replica_message_handler(&shard_handle);
    let on_client_request = make_deferred_client_request_handler(&shard_handle);
    let built = IggyShardBuilder::new(
        ShardIdentity::new(SHARD_ID, SHARD_NAME.to_string()),
        Rc::clone(&bus),
        on_replica_message,
        on_client_request,
        metadata,
        partitions,
        senders,
        inbox,
        shards_table,
        PartitionConsensusConfig::new(CLUSTER_ID, topology.replica_count, Rc::clone(&bus)),
        CoordinatorConfig::default(),
        bus.token(),
    )
    .build();
    if let Some(refresh_task) = built.refresh_task {
        bus.track_background(refresh_task);
    }

    let shard = Rc::new(built.shard);
    *shard_handle.borrow_mut() = Some(Rc::downgrade(&shard));
    Ok(shard)
}

const fn validate_recovered_namespace(
    config: &ServerNgConfig,
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
    config: &ServerNgConfig,
    namespace: IggyNamespace,
    topic_stats: Arc<TopicStats>,
    partition_metadata: &Partition,
    self_replica_id: u8,
    replica_count: u8,
    bus: Rc<IggyMessageBus>,
) -> Result<IggyPartition<Rc<IggyMessageBus>>, ServerNgError> {
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
    let partition_id = namespace.partition_id();
    let stats = Arc::new(PartitionStats::new(topic_stats));
    let consensus = VsrConsensus::new(
        CLUSTER_ID,
        self_replica_id,
        replica_count,
        namespace.inner(),
        bus,
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

    configure_consumer_offsets(&mut partition, config, namespace, current_offset)?;
    ensure_initial_segment(&mut partition, config, stream_id, topic_id, partition_id).await?;

    Ok(partition)
}

async fn hydrate_partition_log(
    partition: &mut IggyPartition<Rc<IggyMessageBus>>,
    config: &ServerNgConfig,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    loaded_log: server::streaming::partitions::log::SegmentedLog<
        server::streaming::partitions::journal::MemoryMessageJournal,
    >,
) -> Result<(), ServerNgError> {
    // TODO: decouple the loading logic from the `server` crate. This currently
    // adapts the old server segmented log into the new `partitions` log.
    for (segment_index, (segment, storage)) in loaded_log
        .segments()
        .iter()
        .zip(loaded_log.storages().iter().cloned())
        .enumerate()
    {
        validate_recovered_segment(
            stream_id,
            topic_id,
            partition_id,
            segment,
            &storage,
            loaded_log
                .indexes()
                .get(segment_index)
                .and_then(|indexes| indexes.as_ref()),
        )?;
        let max_timestamp = match loaded_log
            .indexes()
            .get(segment_index)
            .and_then(|indexes| indexes.as_ref())
        {
            Some(indexes) => indexes_max_timestamp(indexes),
            None => load_segment_max_timestamp(&storage, stream_id, topic_id, partition_id).await?,
        };
        partition.log.add_persisted_segment(
            convert_segment(segment, max_timestamp),
            storage,
            None,
            None,
        );
    }

    if let Some(active_index) = partition.log.segments().len().checked_sub(1) {
        let storage = &partition.log.storages()[active_index];
        if let (Some(messages_reader), Some(index_reader)) = (
            storage.messages_reader.as_ref(),
            storage.index_reader.as_ref(),
        ) {
            let index_path = index_reader.path();
            let index_size = std::fs::metadata(&index_path).map_or(0, |metadata| metadata.len());
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

fn validate_recovered_segment(
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    segment: &iggy_common::Segment,
    storage: &iggy_common::SegmentStorage,
    indexes: Option<&server::streaming::segments::IggyIndexesMut>,
) -> Result<(), ServerNgError> {
    let messages_size_bytes = storage
        .messages_reader
        .as_ref()
        .map_or(0, |reader| u64::from(reader.file_size()));
    let indexed_size_bytes = indexes.map_or(0, |indexes| u64::from(indexes.messages_size()));
    if messages_size_bytes == indexed_size_bytes {
        return Ok(());
    }

    Err(ServerNgError::RecoveredSegmentSizeDivergence {
        stream_id,
        topic_id,
        partition_id,
        start_offset: segment.start_offset,
        end_offset: segment.end_offset,
        messages_size_bytes,
        indexed_size_bytes,
    })
}

fn convert_segment(segment: &iggy_common::Segment, max_timestamp: u64) -> Segment {
    Segment {
        sealed: segment.sealed,
        start_timestamp: segment.start_timestamp,
        end_timestamp: segment.end_timestamp,
        max_timestamp,
        current_position: u64::from(segment.current_position),
        start_offset: segment.start_offset,
        end_offset: segment.end_offset,
        size: segment.size,
        max_size: segment.max_size,
    }
}

fn indexes_max_timestamp(indexes: &server::streaming::segments::IggyIndexesMut) -> u64 {
    let mut max_timestamp = 0;
    for index in 0..indexes.count() {
        if let Some(index_view) = indexes.get(index) {
            max_timestamp = max_timestamp.max(index_view.timestamp());
        }
    }

    max_timestamp
}

async fn load_segment_max_timestamp(
    storage: &iggy_common::SegmentStorage,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> Result<u64, ServerNgError> {
    let Some(index_reader) = storage.index_reader.as_ref() else {
        return Ok(0);
    };

    let indexes = index_reader
        .load_all_indexes_from_disk()
        .await
        .map_err(|source| ServerNgError::SegmentIndexesLoad {
            stream_id,
            topic_id,
            partition_id,
            source,
        })?;
    Ok(indexes_max_timestamp(&indexes))
}

fn configure_consumer_offsets(
    partition: &mut IggyPartition<Rc<IggyMessageBus>>,
    config: &ServerNgConfig,
    namespace: IggyNamespace,
    current_offset: u64,
) -> Result<(), ServerNgError> {
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
            let recovered_offset = offset.offset.load(Ordering::Relaxed);
            if recovered_offset > current_offset {
                return Err(ServerNgError::RecoveredConsumerOffsetOutOfBounds {
                    consumer_kind: "consumer",
                    consumer_id: offset.consumer_id as usize,
                    offset: recovered_offset,
                    current_offset,
                    stream_id,
                    topic_id,
                    partition_id,
                });
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
            let recovered_offset = offset.offset.load(Ordering::Relaxed);
            if recovered_offset > current_offset {
                return Err(ServerNgError::RecoveredConsumerOffsetOutOfBounds {
                    consumer_kind: "consumer group",
                    consumer_id: group_id.0,
                    offset: recovered_offset,
                    current_offset,
                    stream_id,
                    topic_id,
                    partition_id,
                });
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
    Ok(())
}

async fn ensure_initial_segment(
    partition: &mut IggyPartition<Rc<IggyMessageBus>>,
    config: &ServerNgConfig,
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

fn resolve_tcp_topology(
    config: &ServerNgConfig,
    current_replica_id: Option<u8>,
) -> Result<TcpTopology, ServerNgError> {
    let default_client_addr = parse_socket_addr("tcp.address", &config.tcp.address)?;
    let default_ws_addr = resolve_optional_listener_addr(
        config.websocket.enabled,
        "websocket.address",
        &config.websocket.address,
    )?;
    let default_quic_addr =
        resolve_optional_listener_addr(config.quic.enabled, "quic.address", &config.quic.address)?;
    if !config.cluster.enabled {
        if let Some(replica_id) = current_replica_id {
            warn!(
                replica_id,
                "cluster is disabled, ignoring --replica-id for single-node server-ng startup"
            );
        }
        return Ok(TcpTopology {
            // Keep parity with the current server binary and the integration
            // harness: `--replica-id` may be passed unconditionally, but in
            // single-node mode there is only replica 0.
            self_replica_id: SHARD_REPLICA_ID,
            replica_count: 1,
            client_listen_addr: default_client_addr,
            replica_listen_addr: Some(SocketAddr::new(default_client_addr.ip(), 0)),
            ws_listen_addr: default_ws_addr,
            quic_listen_addr: default_quic_addr,
            tcp_tls_listen_addr: config.tcp.tls.enabled.then_some(default_client_addr),
            peers: Vec::new(),
        });
    }

    let self_replica_id = current_replica_id.ok_or(ServerNgError::MissingReplicaId)?;

    let self_node = config
        .cluster
        .nodes
        .iter()
        .find(|node| node.replica_id == self_replica_id)
        .ok_or(ServerNgError::ClusterNodeNotFound {
            replica_id: self_replica_id,
        })?;
    let replica_count = u8::try_from(config.cluster.nodes.len()).map_err(|_| {
        ServerNgError::ClusterReplicaCountTooLarge {
            count: config.cluster.nodes.len(),
        }
    })?;
    let (client_listen_addr, ws_listen_addr, quic_listen_addr) = resolve_cluster_client_addrs(
        self_node,
        default_client_addr,
        default_ws_addr,
        default_quic_addr,
    )?;
    let replica_port =
        self_node
            .ports
            .tcp_replica
            .ok_or(ServerNgError::ClusterReplicaPortMissing {
                replica_id: self_node.replica_id,
            })?;
    let replica_listen_addr = Some(socket_addr_from_parts(
        "cluster.nodes[*].ports.tcp_replica",
        &self_node.ip,
        replica_port,
    )?);
    let peers = resolve_cluster_replica_peers(&config.cluster.nodes, self_replica_id)?;

    Ok(TcpTopology {
        self_replica_id,
        replica_count,
        client_listen_addr,
        replica_listen_addr,
        ws_listen_addr,
        quic_listen_addr,
        tcp_tls_listen_addr: config.tcp.tls.enabled.then_some(client_listen_addr),
        peers,
    })
}

fn resolve_optional_listener_addr(
    enabled: bool,
    context: &'static str,
    address: &str,
) -> Result<Option<SocketAddr>, ServerNgError> {
    if enabled {
        return Ok(Some(parse_socket_addr(context, address)?));
    }
    Ok(None)
}

fn resolve_cluster_client_addrs(
    self_node: &configs::cluster::ClusterNodeConfig,
    default_client_addr: SocketAddr,
    default_ws_addr: Option<SocketAddr>,
    default_quic_addr: Option<SocketAddr>,
) -> Result<(SocketAddr, Option<SocketAddr>, Option<SocketAddr>), ServerNgError> {
    let client_port = self_node
        .ports
        .tcp
        .unwrap_or_else(|| default_client_addr.port());
    let client_listen_addr =
        socket_addr_from_parts("cluster.nodes[*].ports.tcp", &self_node.ip, client_port)?;
    let ws_listen_addr = resolve_cluster_optional_addr(
        self_node,
        "cluster.nodes[*].ports.websocket",
        default_ws_addr,
        |ports| ports.websocket,
    )?;
    let quic_listen_addr = resolve_cluster_optional_addr(
        self_node,
        "cluster.nodes[*].ports.quic",
        default_quic_addr,
        |ports| ports.quic,
    )?;
    Ok((client_listen_addr, ws_listen_addr, quic_listen_addr))
}

fn resolve_cluster_optional_addr(
    self_node: &configs::cluster::ClusterNodeConfig,
    context: &'static str,
    default_addr: Option<SocketAddr>,
    port_selector: impl Fn(&configs::cluster::TransportPorts) -> Option<u16>,
) -> Result<Option<SocketAddr>, ServerNgError> {
    let Some(default_addr) = default_addr else {
        return Ok(None);
    };
    let port = port_selector(&self_node.ports).unwrap_or_else(|| default_addr.port());
    socket_addr_from_parts(context, &self_node.ip, port).map(Some)
}

fn resolve_cluster_replica_peers(
    nodes: &[configs::cluster::ClusterNodeConfig],
    self_replica_id: u8,
) -> Result<Vec<(u8, SocketAddr)>, ServerNgError> {
    let mut peers = Vec::with_capacity(nodes.len().saturating_sub(1));
    for node in nodes {
        if node.replica_id == self_replica_id {
            continue;
        }
        let replica_port =
            node.ports
                .tcp_replica
                .ok_or(ServerNgError::ClusterReplicaPortMissing {
                    replica_id: node.replica_id,
                })?;
        peers.push((
            node.replica_id,
            socket_addr_from_parts("cluster.nodes[*].ports.tcp_replica", &node.ip, replica_port)?,
        ));
    }
    Ok(peers)
}

async fn start_tcp_runtime(
    shard: &Rc<ServerNgShard>,
    config: &ServerNgConfig,
    topology: &TcpTopology,
    accepted_replica: AcceptedReplicaFn,
    accepted_clients: LocalClientAcceptFns,
) -> Result<(), ServerNgError> {
    if config.tcp.enabled && !config.tcp.tls.enabled {
        return start_via_replica_io(shard, config, topology, accepted_replica, accepted_clients)
            .await;
    }

    start_manual_runtime(shard, config, topology, accepted_replica, accepted_clients).await
}

async fn start_via_replica_io(
    shard: &Rc<ServerNgShard>,
    config: &ServerNgConfig,
    topology: &TcpTopology,
    accepted_replica: AcceptedReplicaFn,
    accepted_clients: LocalClientAcceptFns,
) -> Result<(), ServerNgError> {
    let replica_addr = topology
        .replica_listen_addr
        .expect("topology must include replica listener address");
    let quic_credentials = topology
        .quic_listen_addr
        .is_some()
        .then(|| load_quic_server_credentials(config))
        .transpose()?;
    let tcp_tls_credentials = topology
        .tcp_tls_listen_addr
        .is_some()
        .then(|| load_tcp_tls_server_credentials(config))
        .transpose()?;

    let LocalClientAcceptFns {
        tcp,
        ws,
        quic,
        tcp_tls,
    } = accepted_clients;

    let bound = replica_io::start_on_shard_zero(
        &shard.bus,
        replica_addr,
        topology.client_listen_addr,
        topology.ws_listen_addr,
        topology.quic_listen_addr,
        quic_credentials,
        topology.tcp_tls_listen_addr,
        tcp_tls_credentials,
        None,
        None,
        CLUSTER_ID,
        topology.self_replica_id,
        topology.replica_count,
        topology.peers.clone(),
        accepted_replica,
        tcp,
        topology.ws_listen_addr.map(|_| ws),
        topology.quic_listen_addr.map(|_| quic),
        topology.tcp_tls_listen_addr.map(|_| tcp_tls),
        None,
        shard.bus.config().reconnect_period,
    )
    .await
    .map_err(ServerNgError::StartListeners)?;
    let Some(bound) = bound else {
        return Ok(());
    };

    write_current_config(
        config,
        Some(topology.self_replica_id),
        Some(bound.client),
        config.cluster.enabled.then_some(bound.replica),
        bound.tcp_tls,
        bound.quic,
        bound.ws,
    )
    .await?;
    if config.cluster.enabled {
        info!(
            shard = shard.id,
            replica = %bound.replica,
            tcp = %bound.client,
            tcp_tls = ?bound.tcp_tls,
            ws = ?bound.ws,
            quic = ?bound.quic,
            "server-ng listeners started"
        );
    } else {
        info!(
            shard = shard.id,
            tcp = %bound.client,
            tcp_tls = ?bound.tcp_tls,
            ws = ?bound.ws,
            quic = ?bound.quic,
            "server-ng client listeners started"
        );
    }

    Ok(())
}

async fn start_manual_runtime(
    shard: &Rc<ServerNgShard>,
    config: &ServerNgConfig,
    topology: &TcpTopology,
    accepted_replica: AcceptedReplicaFn,
    accepted_clients: LocalClientAcceptFns,
) -> Result<(), ServerNgError> {
    let bound_replica = if config.cluster.enabled {
        let replica_addr = topology
            .replica_listen_addr
            .expect("cluster-enabled topology must include replica listener address");
        let (replica_listener, bound_addr) = replica_listener::bind(replica_addr)
            .await
            .map_err(ServerNgError::StartListeners)?;
        let token = shard.bus.token();
        let max_message_size = shard.bus.config().max_message_size;
        let handshake_grace = shard.bus.config().handshake_grace;
        let self_replica_id = topology.self_replica_id;
        let replica_count = topology.replica_count;
        let accepted_replica_for_listener = accepted_replica.clone();
        let replica_handle = compio::runtime::spawn(async move {
            replica_listener::run(
                replica_listener,
                token,
                CLUSTER_ID,
                self_replica_id,
                replica_count,
                accepted_replica_for_listener,
                max_message_size,
                handshake_grace,
            )
            .await;
        });
        shard.bus.track_background(replica_handle);
        connector::start(
            &shard.bus,
            CLUSTER_ID,
            topology.self_replica_id,
            topology.peers.clone(),
            accepted_replica,
            shard.bus.config().reconnect_period,
        )
        .await;
        Some(bound_addr)
    } else {
        None
    };

    let bound_clients = start_client_listeners(shard, config, topology, &accepted_clients).await?;
    write_current_config(
        config,
        Some(topology.self_replica_id),
        bound_clients.tcp,
        bound_replica,
        bound_clients.tcp_tls,
        bound_clients.quic,
        bound_clients.ws,
    )
    .await?;

    if config.cluster.enabled {
        info!(
            shard = shard.id,
            replica = ?bound_replica,
            tcp = ?bound_clients.tcp,
            tcp_tls = ?bound_clients.tcp_tls,
            ws = ?bound_clients.ws,
            quic = ?bound_clients.quic,
            "server-ng listeners started"
        );
    } else {
        info!(
            shard = shard.id,
            tcp = ?bound_clients.tcp,
            tcp_tls = ?bound_clients.tcp_tls,
            ws = ?bound_clients.ws,
            quic = ?bound_clients.quic,
            "server-ng client listeners started"
        );
    }

    Ok(())
}

fn make_replica_message_handler(shard: &Rc<ServerNgShard>) -> MessageHandler {
    let shard = Rc::clone(shard);
    Rc::new(move |_replica_id, message| {
        shard.dispatch(message);
    })
}

fn make_client_request_handler(shard: &Rc<ServerNgShard>) -> RequestHandler {
    let shard = Rc::clone(shard);
    Rc::new(move |client_id, message| {
        let request = match message.try_into_typed::<RequestHeader>() {
            Ok(request) => request,
            Err(error) => {
                warn!(client_id, error = %error, "dropping client request with invalid header");
                return;
            }
        };
        let request = request.transmute_header(|header, new_header: &mut RequestHeader| {
            *new_header = header;
            new_header.client = client_id;
        });
        shard.dispatch(request.into_generic());
    })
}

fn make_deferred_replica_message_handler(shard_handle: &ServerNgShardHandle) -> MessageHandler {
    let shard_handle = Rc::clone(shard_handle);
    Rc::new(move |_replica_id, message| {
        if let Some(shard) = upgrade_shard_handle(&shard_handle) {
            shard.dispatch(message);
        }
    })
}

fn make_deferred_client_request_handler(shard_handle: &ServerNgShardHandle) -> RequestHandler {
    let shard_handle = Rc::clone(shard_handle);
    Rc::new(move |client_id, message| {
        let Some(shard) = upgrade_shard_handle(&shard_handle) else {
            return;
        };
        let request = match message.try_into_typed::<RequestHeader>() {
            Ok(request) => request,
            Err(error) => {
                warn!(client_id, error = %error, "dropping client request with invalid header");
                return;
            }
        };
        let request = request.transmute_header(|header, new_header: &mut RequestHeader| {
            *new_header = header;
            new_header.client = client_id;
        });
        shard.dispatch(request.into_generic());
    })
}

fn upgrade_shard_handle(shard_handle: &ServerNgShardHandle) -> Option<Rc<ServerNgShard>> {
    shard_handle
        .borrow()
        .as_ref()
        .and_then(std::rc::Weak::upgrade)
}

fn make_local_replica_accept_fn(
    bus: &Rc<IggyMessageBus>,
    on_message: MessageHandler,
) -> AcceptedReplicaFn {
    let bus = Rc::clone(bus);
    Rc::new(move |stream, peer_id| {
        installer::install_replica_tcp(&bus, peer_id, stream, on_message.clone());
    })
}

fn make_local_client_accept_fns(
    bus: &Rc<IggyMessageBus>,
    on_request: RequestHandler,
) -> LocalClientAcceptFns {
    let tcp_bus = Rc::clone(bus);
    let ws_bus = Rc::clone(bus);
    let quic_bus = Rc::clone(bus);
    let tcp_tls_bus = Rc::clone(bus);
    let tcp_request = on_request.clone();
    let ws_request = on_request.clone();
    let quic_request = on_request.clone();
    let tcp_tls_request = on_request;
    let counter = Rc::new(Cell::new(1_u128));
    let shard_id = u128::from(bus.shard_id());

    let tcp_counter = Rc::clone(&counter);
    let tcp = Rc::new(move |stream| {
        let Some(meta) = client_meta_from_stream(
            &stream,
            tcp_counter.as_ref(),
            shard_id,
            ClientTransportKind::Tcp,
        ) else {
            return;
        };
        installer::install_client_tcp(&tcp_bus, meta, stream, tcp_request.clone());
    });

    let ws_counter = Rc::clone(&counter);
    let ws = Rc::new(move |stream| {
        let Some(meta) = client_meta_from_stream(
            &stream,
            ws_counter.as_ref(),
            shard_id,
            ClientTransportKind::Ws,
        ) else {
            return;
        };
        let fd = match fd_transfer::dup_fd(&stream) {
            Ok(fd) => fd,
            Err(error) => {
                warn!(
                    client_id = meta.client_id,
                    error = %error,
                    "dropping accepted websocket client after fd duplication failure"
                );
                return;
            }
        };
        ws_bus.install_client_ws_fd(fd, meta, ws_request.clone());
    });

    let quic_counter = Rc::clone(&counter);
    let quic = Rc::new(move |accepted: message_bus::AcceptedQuicConn| {
        let meta = mint_client_meta(
            quic_counter.as_ref(),
            shard_id,
            accepted.peer_addr(),
            ClientTransportKind::Quic,
        );
        installer::install_client_quic(&quic_bus, meta, accepted, quic_request.clone());
    });

    let tcp_tls_counter = Rc::clone(&counter);
    let tcp_tls = Rc::new(move |stream, tls_config| {
        let Some(meta) = client_meta_from_stream(
            &stream,
            tcp_tls_counter.as_ref(),
            shard_id,
            ClientTransportKind::TcpTls,
        ) else {
            return;
        };
        installer::install_client_tcp_tls(
            &tcp_tls_bus,
            meta,
            stream,
            tls_config,
            tcp_tls_request.clone(),
        );
    });

    LocalClientAcceptFns {
        tcp,
        ws,
        quic,
        tcp_tls,
    }
}

fn client_meta_from_stream(
    stream: &compio::net::TcpStream,
    counter: &Cell<u128>,
    shard_id: u128,
    transport: ClientTransportKind,
) -> Option<ClientConnMeta> {
    let peer_addr = match stream.peer_addr() {
        Ok(peer_addr) => peer_addr,
        Err(error) => {
            let client_id = preview_client_id(counter, shard_id);
            warn!(client_id, error = %error, "dropping accepted client with unknown peer address");
            return None;
        }
    };
    Some(mint_client_meta(counter, shard_id, peer_addr, transport))
}

const fn preview_client_id(counter: &Cell<u128>, shard_id: u128) -> u128 {
    (shard_id << 112) | counter.get()
}

fn mint_client_meta(
    counter: &Cell<u128>,
    shard_id: u128,
    peer_addr: SocketAddr,
    transport: ClientTransportKind,
) -> ClientConnMeta {
    let seq = counter.get();
    counter.set(seq.wrapping_add(1));
    ClientConnMeta::new((shard_id << 112) | seq, peer_addr, transport)
}

async fn start_client_listeners(
    shard: &Rc<ServerNgShard>,
    config: &ServerNgConfig,
    topology: &TcpTopology,
    accepted_clients: &LocalClientAcceptFns,
) -> Result<BoundClientListeners, ServerNgError> {
    let mut bound = BoundClientListeners::default();

    if config.tcp.enabled && !config.tcp.tls.enabled {
        let (listener, bound_addr) = client_listener::tcp::bind(topology.client_listen_addr)
            .await
            .map_err(ServerNgError::StartListeners)?;
        let token = shard.bus.token();
        let accepted_client = accepted_clients.tcp.clone();
        let client_handle = compio::runtime::spawn(async move {
            client_listener::tcp::run(listener, token, accepted_client).await;
        });
        shard.bus.track_background(client_handle);
        bound.tcp = Some(bound_addr);
    }

    if let Some(ws_addr) = topology.ws_listen_addr {
        let (listener, bound_addr) = client_listener::ws::bind(ws_addr)
            .await
            .map_err(ServerNgError::StartListeners)?;
        let token = shard.bus.token();
        let accepted_ws = accepted_clients.ws.clone();
        let ws_handle = compio::runtime::spawn(async move {
            client_listener::ws::run(listener, token, accepted_ws).await;
        });
        shard.bus.track_background(ws_handle);
        bound.ws = Some(bound_addr);
    }

    if let Some(quic_addr) = topology.quic_listen_addr {
        install_default_crypto_provider();
        let credentials = load_quic_server_credentials(config)?;
        let server_config = server_config_with_cert(
            credentials.cert_chain,
            credentials.key_der,
            &shard.bus.config().quic,
        )
        .map_err(|e| {
            ServerNgError::StartListeners(iggy_common::IggyError::IoError(format!(
                "QUIC server config build failed: {e}"
            )))
        })?;
        let (endpoint, bound_addr) = client_listener::quic::bind(quic_addr, server_config)
            .await
            .map_err(ServerNgError::StartListeners)?;
        let token = shard.bus.token();
        let handshake_grace = shard.bus.config().handshake_grace;
        let accepted_quic = accepted_clients.quic.clone();
        let quic_handle = compio::runtime::spawn(async move {
            client_listener::quic::run(endpoint, token, accepted_quic, handshake_grace).await;
        });
        shard.bus.track_background(quic_handle);
        bound.quic = Some(bound_addr);
    }

    if config.tcp.enabled && config.tcp.tls.enabled {
        let credentials = load_tcp_tls_server_credentials(config)?;
        let (listener, tls_config, bound_addr) =
            client_listener::tcp_tls::bind(topology.client_listen_addr, credentials)
                .await
                .map_err(ServerNgError::StartListeners)?;
        let token = shard.bus.token();
        let accepted_tls = accepted_clients.tcp_tls.clone();
        let tls_handle = compio::runtime::spawn(async move {
            client_listener::tcp_tls::run(listener, tls_config, token, accepted_tls).await;
        });
        shard.bus.track_background(tls_handle);
        bound.tcp_tls = Some(bound_addr);
    }

    Ok(bound)
}

fn load_tcp_tls_server_credentials(
    config: &ServerNgConfig,
) -> Result<TlsServerCredentials, ServerNgError> {
    let tls = &config.tcp.tls;
    if tls.self_signed && !Path::new(&tls.cert_file).exists() {
        return Ok(self_signed_for_loopback());
    }

    load_pem(Path::new(&tls.cert_file), Path::new(&tls.key_file)).map_err(|source| {
        ServerNgError::ListenerCredentials {
            transport: "tcp.tls",
            source,
        }
    })
}

fn load_quic_server_credentials(
    config: &ServerNgConfig,
) -> Result<replica_io::QuicServerCredentials, ServerNgError> {
    let certificate = &config.quic.certificate;
    if certificate.self_signed {
        let (cert_chain, key_der) = iggy_common::generate_self_signed_certificate("localhost")
            .map_err(|error| ServerNgError::ListenerCredentials {
                transport: "quic",
                source: std::io::Error::other(error.to_string()),
            })?;
        return Ok(replica_io::QuicServerCredentials {
            cert_chain,
            key_der,
        });
    }

    let credentials = load_pem(
        Path::new(&certificate.cert_file),
        Path::new(&certificate.key_file),
    )
    .map_err(|source| ServerNgError::ListenerCredentials {
        transport: "quic",
        source,
    })?;
    Ok(replica_io::QuicServerCredentials {
        cert_chain: credentials.cert_chain,
        key_der: credentials.key_der,
    })
}

fn parse_socket_addr(context: &'static str, address: &str) -> Result<SocketAddr, ServerNgError> {
    address
        .parse()
        .map_err(|source| ServerNgError::SocketAddressParse {
            context,
            address: address.to_string(),
            source,
        })
}

fn socket_addr_from_parts(
    context: &'static str,
    host: &str,
    port: u16,
) -> Result<SocketAddr, ServerNgError> {
    let ip = host
        .parse::<IpAddr>()
        .map_err(|source| ServerNgError::SocketAddressParse {
            context,
            address: format!("{host}:{port}"),
            source,
        })?;
    Ok(SocketAddr::new(ip, port))
}
