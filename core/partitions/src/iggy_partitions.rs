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

#![allow(dead_code)]

use crate::IggyPartition;
use crate::Partition;
use crate::PollingConsumer;
use crate::iggy_index_writer::IggyIndexWriter;
use crate::log::JournalInfo;
use crate::messages_writer::MessagesWriter;
use crate::segment::Segment;
use crate::types::PartitionsConfig;
use consensus::PlaneIdentity;
use consensus::{
    CommitLogEvent, Consensus, NamespacedPipeline, Pipeline, PipelineEntry, Plane, PlaneKind,
    Project, ReplicaLogContext, RequestLogEvent, Sequencer, SimEventKind, VsrConsensus,
    ack_preflight, ack_quorum_reached, build_reply_message, drain_committable_prefix,
    emit_namespace_progress_event, emit_sim_event, fence_old_prepare_by_commit,
    pipeline_prepare_common, replicate_preflight, replicate_to_next_in_chain, request_preflight,
    send_prepare_ok as send_prepare_ok_common,
};
use iggy_binary_protocol::{
    Command2, ConsensusHeader, GenericHeader, Message, Operation, PrepareHeader, PrepareOkHeader,
    RequestHeader,
};
use iggy_common::{
    IggyByteSize, IggyError, PartitionStats, SegmentStorage,
    send_messages2::{convert_request_message, decode_prepare_slice},
    sharding::{IggyNamespace, LocalIdx, ShardId},
};
use iobuf::Frozen;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use std::cell::UnsafeCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{debug, warn};

/// Per-shard collection of all partitions.
///
/// This struct manages ALL partitions assigned to a single shard, regardless
/// of which stream/topic they belong to.
///
/// Note: The `partition_id` within `IggyNamespace` may NOT equal the Vec index.
/// For example, shard 0 might have `partition_ids` [0, 2, 4] while shard 1
/// has `partition_ids` [1, 3, 5]. The `LocalIdx` provides the actual index
/// into the `partitions` Vec.
pub struct IggyPartitions<C, J = ()> {
    shard_id: ShardId,
    config: PartitionsConfig,
    /// Collection of partitions, the index of each partition isn't it's ID, but rather an local index (`LocalIdx`) which is used for lookups.
    ///
    /// Wrapped in `UnsafeCell` for interior mutability — matches the single-threaded
    /// per-shard execution model. Consensus trait methods take `&self` but need to
    /// mutate partition state (segments, offsets, journal).
    ///
    /// TODO: Move to more granular synchronization around partition substate and
    /// stop exposing `&mut IggyPartition` from `&self`. The long-term shape here
    /// should let append/commit paths coordinate on the exact mutable state they
    /// need instead of relying on `UnsafeCell` aliasing for the whole partition.
    partitions: UnsafeCell<Vec<IggyPartition>>,
    namespace_to_local: HashMap<IggyNamespace, LocalIdx>,
    consensus: Option<C>,
    /// Consensus-level WAL for prepare messages.
    ///
    /// Stores full `Message<PrepareHeader>` keyed by op. Operations are applied
    /// at commit time (not replicate time) by reading from this WAL in
    /// `commit_journal` (backup) and `on_ack` (primary).
    journal: Option<J>,
}

const fn freeze_client_reply(message: Message<GenericHeader>) -> Message<GenericHeader> {
    message
}

impl<C, J> IggyPartitions<C, J> {
    #[must_use]
    pub fn new(shard_id: ShardId, config: PartitionsConfig) -> Self {
        Self {
            shard_id,
            config,
            partitions: UnsafeCell::new(Vec::new()),
            namespace_to_local: HashMap::new(),
            consensus: None,
            journal: None,
        }
    }

    #[must_use]
    pub fn with_capacity(shard_id: ShardId, config: PartitionsConfig, capacity: usize) -> Self {
        Self {
            shard_id,
            config,
            partitions: UnsafeCell::new(Vec::with_capacity(capacity)),
            namespace_to_local: HashMap::with_capacity(capacity),
            consensus: None,
            journal: None,
        }
    }

    pub fn set_journal(&mut self, journal: J) {
        self.journal = Some(journal);
    }

    pub const fn journal(&self) -> Option<&J> {
        self.journal.as_ref()
    }

    pub const fn config(&self) -> &PartitionsConfig {
        &self.config
    }

    fn partitions(&self) -> &Vec<IggyPartition> {
        // Safety: single-threaded per-shard model, no concurrent access.
        unsafe { &*self.partitions.get() }
    }

    pub const fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn len(&self) -> usize {
        self.partitions().len()
    }

    pub fn is_empty(&self) -> bool {
        self.partitions().is_empty()
    }

    /// Get partition by local index.
    pub fn get(&self, local_idx: LocalIdx) -> Option<&IggyPartition> {
        self.partitions().get(*local_idx)
    }

    /// Get mutable partition by local index.
    #[allow(clippy::mut_from_ref)]
    fn get_mut(&self, local_idx: LocalIdx) -> Option<&mut IggyPartition> {
        // Safety: single-threaded per-shard model, no concurrent access.
        unsafe { (&mut *self.partitions.get()).get_mut(*local_idx) }
    }

    /// Lookup local index by namespace.
    pub fn local_idx(&self, namespace: &IggyNamespace) -> Option<LocalIdx> {
        self.namespace_to_local.get(namespace).copied()
    }

    /// Insert a new partition and return its local index.
    pub fn insert(&mut self, namespace: IggyNamespace, partition: IggyPartition) -> LocalIdx {
        let partitions = self.partitions.get_mut();
        let local_idx = LocalIdx::new(partitions.len());
        partitions.push(partition);
        self.namespace_to_local.insert(namespace, local_idx);
        local_idx
    }

    /// Check if a namespace exists.
    pub fn contains(&self, namespace: &IggyNamespace) -> bool {
        self.namespace_to_local.contains_key(namespace)
    }

    /// Get partition by namespace directly.
    pub fn get_by_ns(&self, namespace: &IggyNamespace) -> Option<&IggyPartition> {
        let idx = self.namespace_to_local.get(namespace)?;
        self.partitions().get(**idx)
    }

    /// Get mutable partition by namespace directly.
    #[allow(clippy::mut_from_ref)]
    fn get_mut_by_ns(&self, namespace: &IggyNamespace) -> Option<&mut IggyPartition> {
        let idx = self.namespace_to_local.get(namespace)?;
        // Safety: single-threaded per-shard model, no concurrent access.
        unsafe { (&mut *self.partitions.get()).get_mut(**idx) }
    }

    /// Remove a partition by namespace. Returns the removed partition if found.
    pub fn remove(&mut self, namespace: &IggyNamespace) -> Option<IggyPartition> {
        let local_idx = self.namespace_to_local.remove(namespace)?;
        let idx = *local_idx;
        let partitions = self.partitions.get_mut();

        if idx >= partitions.len() {
            return None;
        }

        // Swap-remove for O(1) deletion
        let partition = partitions.swap_remove(idx);

        // If we swapped an element, update its index in the map
        if idx < partitions.len() {
            // Find the namespace that was at the last position (now at idx)
            for lidx in self.namespace_to_local.values_mut() {
                if **lidx == partitions.len() {
                    *lidx = LocalIdx::new(idx);
                    break;
                }
            }
        }

        Some(partition)
    }

    /// Remove multiple partitions at once.
    pub fn remove_many(&mut self, namespaces: &[IggyNamespace]) -> Vec<IggyPartition> {
        namespaces.iter().filter_map(|ns| self.remove(ns)).collect()
    }

    /// Iterate over all namespaces owned by this shard.
    pub fn namespaces(&self) -> impl Iterator<Item = &IggyNamespace> {
        self.namespace_to_local.keys()
    }

    /// Iterate over all (namespace, partition) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&IggyNamespace, &IggyPartition)> {
        self.namespace_to_local
            .iter()
            .filter_map(|(ns, idx)| self.partitions().get(**idx).map(|p| (ns, p)))
    }

    /// Iterate over all (namespace, partition) pairs mutably.
    pub fn iter_mut(&self) -> impl Iterator<Item = (&IggyNamespace, &mut IggyPartition)> {
        // Safety: single-threaded per-shard model, no concurrent access.
        let partitions = unsafe { &mut *self.partitions.get() };
        let partitions_ptr = partitions.as_mut_ptr();
        let partitions_len = partitions.len();
        self.namespace_to_local.iter().filter_map(move |(ns, idx)| {
            let i = **idx;
            if i < partitions_len {
                // Safety: each LocalIdx is unique, so no two iterations alias the same element.
                Some((ns, unsafe { &mut *partitions_ptr.add(i) }))
            } else {
                None
            }
        })
    }

    /// Get partition by namespace, initializing if not present.
    pub fn get_or_init<F>(&mut self, namespace: IggyNamespace, init: F) -> &mut IggyPartition
    where
        F: FnOnce() -> IggyPartition,
    {
        // TODO: get_or_insert
        if !self.namespace_to_local.contains_key(&namespace) {
            let partition = init();
            self.insert(namespace, partition);
        }
        let idx = *self.namespace_to_local[&namespace];
        &mut self.partitions.get_mut()[idx]
    }

    pub const fn consensus(&self) -> Option<&C> {
        self.consensus.as_ref()
    }

    pub fn set_consensus(&mut self, consensus: C) {
        self.consensus = Some(consensus);
    }

    /// Initialize a new partition with in-memory storage (for testing/simulation).
    ///
    /// Idempotent: subsequent calls for the same namespace are no-ops returning
    /// the existing index. Consensus must be set separately via `set_consensus`.
    ///
    /// TODO: Make the log generic over its storage backend to support both
    /// in-memory (for testing) and file-backed (for production) storage without
    /// needing separate initialization methods.
    pub fn init_partition_in_memory(&mut self, namespace: IggyNamespace) -> LocalIdx {
        if let Some(idx) = self.local_idx(&namespace) {
            return idx;
        }

        // Create initial segment with default (in-memory) storage
        let start_offset = 0;
        let segment = Segment::new(start_offset, self.config.segment_size);
        let storage = SegmentStorage::default();

        // Create partition with initialized log
        let stats = Arc::new(PartitionStats::default());
        let mut partition = IggyPartition::new(stats);
        partition
            .log
            .add_persisted_segment(segment, storage, None, None);
        partition.offset.store(start_offset, Ordering::Release);
        partition
            .dirty_offset
            .store(start_offset, Ordering::Relaxed);
        partition.should_increment_offset = false;
        partition.stats.increment_segments_count(1);

        self.insert(namespace, partition)
    }

    /// Initialize a new partition with file-backed storage.
    ///
    /// This is the data plane initialization - creates the partition structure,
    /// initial segment, and storage. Skips the control plane metadata broadcasting.
    ///
    /// Corresponds to the "INITIATE PARTITION" phase in the server's flow:
    /// 1. Control plane: create `PartitionMeta` (SKIPPED in this method)
    /// 2. Control plane: broadcast to shards (SKIPPED in this method)
    /// 3. Data plane: INITIATE PARTITION (THIS METHOD)
    ///
    /// Idempotent: subsequent calls for the same namespace are no-ops.
    /// Consensus must be set separately via `set_consensus`.
    ///
    /// # Errors
    ///
    /// Returns an `IggyError` if the backing segment storage or writers cannot be created.
    pub async fn init_partition(
        &mut self,
        namespace: IggyNamespace,
    ) -> Result<LocalIdx, IggyError> {
        if let Some(idx) = self.local_idx(&namespace) {
            return Ok(idx);
        }

        // Create initial segment with storage
        let start_offset = 0;
        let segment = Segment::new(start_offset, self.config.segment_size);

        // TODO: Waiting for issue to move server config to shared module.
        // Once complete, paths will come from proper base_path/streams_path/etc config fields.
        let messages_path = self.config.get_messages_path(
            namespace.stream_id(),
            namespace.topic_id(),
            namespace.partition_id(),
            start_offset,
        );
        let index_path = self.config.get_index_path(
            namespace.stream_id(),
            namespace.topic_id(),
            namespace.partition_id(),
            start_offset,
        );

        let storage = SegmentStorage::new(
            &messages_path,
            &index_path,
            0, // messages_size (new segment)
            0, // indexes_size (new segment)
            self.config.enforce_fsync,
            self.config.enforce_fsync,
            false, // file_exists (new segment)
        )
        .await
        .map_err(|_| IggyError::CannotCreateSegmentLogFile(messages_path.clone()))?;
        let messages_size_bytes = storage
            .messages_writer
            .as_ref()
            .ok_or_else(|| IggyError::CannotCreateSegmentLogFile(messages_path.clone()))?
            .size_counter();
        let messages_writer = Rc::new(
            MessagesWriter::new(
                &messages_path,
                messages_size_bytes,
                self.config.enforce_fsync,
                false,
            )
            .await
            .map_err(|_| IggyError::CannotCreateSegmentLogFile(messages_path.clone()))?,
        );
        let index_writer = Rc::new(
            IggyIndexWriter::new(
                &index_path,
                Rc::new(std::sync::atomic::AtomicU64::new(0)),
                self.config.enforce_fsync,
                false,
            )
            .await
            .map_err(|_| IggyError::CannotCreateSegmentIndexFile(index_path.clone()))?,
        );

        // Create partition with initialized log
        let stats = Arc::new(PartitionStats::default());
        let mut partition = IggyPartition::new(stats);
        partition.log.add_persisted_segment(
            segment,
            storage,
            Some(messages_writer),
            Some(index_writer),
        );
        partition.offset.store(start_offset, Ordering::Release);
        partition
            .dirty_offset
            .store(start_offset, Ordering::Relaxed);
        partition.should_increment_offset = false;
        partition.stats.increment_segments_count(1);

        Ok(self.insert(namespace, partition))
    }
}

impl<B, J> Plane<VsrConsensus<B>> for IggyPartitions<VsrConsensus<B, NamespacedPipeline>, J>
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
    J: JournalHandle,
    J::Target: Journal<J::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
{
    async fn on_request(&self, message: <VsrConsensus<B> as Consensus>::Message<RequestHeader>) {
        let namespace = IggyNamespace::from_raw(message.header().namespace);
        let consensus = self
            .consensus()
            .expect("on_request: consensus not initialized");
        let client_id = message.header().client;
        let request = message.header().request;

        // TODO: Add a bounded request queue instead of dropping here.
        // When the prepare queue (8 max) is full, buffer
        // incoming requests in a request queue. On commit, pop the next request
        // from the request queue and begin preparing it. Only drop when both
        // queues are full.
        if consensus.pipeline().borrow().is_full() {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                replica_id = consensus.replica(),
                client = client_id,
                request = request,
                "on_request: pipeline full, dropping request"
            );
            return;
        }

        emit_sim_event(
            SimEventKind::ClientRequestReceived,
            &RequestLogEvent {
                replica: ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                client_id: message.header().client,
                request_id: message.header().request,
                operation: message.header().operation,
            },
        );

        let message = if message.header().operation == Operation::SendMessages {
            match convert_request_message(namespace, message) {
                Ok(message) => message,
                Err(error) => {
                    warn!(
                        target: "iggy.partitions.diag",
                        plane = "partitions",
                        replica_id = consensus.replica(),
                        namespace_raw = namespace.inner(),
                        operation = ?Operation::SendMessages,
                        error = %error,
                        "failed to convert send_messages request"
                    );
                    return;
                }
            }
        } else {
            message
        };

        let Some(_notify) = request_preflight(consensus, client_id, request).await else {
            return;
        };

        let prepare = message.project(consensus);
        pipeline_prepare_common(consensus, PlaneKind::Partitions, prepare, |prepare| {
            self.on_replicate(prepare)
        })
        .await;
    }

    async fn on_replicate(&self, message: <VsrConsensus<B> as Consensus>::Message<PrepareHeader>) {
        let header = *message.header();
        let consensus = self
            .consensus()
            .expect("on_replicate: consensus not initialized");

        let current_op = match replicate_preflight(consensus, &header) {
            Ok(current_op) => current_op,
            Err(reason) => {
                warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    replica_id = consensus.replica(),
                    view = consensus.view(),
                    op = header.op,
                    namespace_raw = header.namespace,
                    operation = ?header.operation,
                    reason = reason.as_str(),
                    "ignoring prepare during replicate preflight"
                );
                return;
            }
        };

        let is_old_prepare = fence_old_prepare_by_commit(consensus, &header);
        if is_old_prepare {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                replica_id = consensus.replica(),
                view = consensus.view(),
                op = header.op,
                commit = consensus.commit_max(),
                namespace_raw = header.namespace,
                operation = ?header.operation,
                "received old prepare, skipping replication"
            );
            return;
        }

        // TODO: Restore hard assert_eq!(header.op, current_op + 1) once message repair
        // is implemented. Without repair, the network can deliver prepares out of order
        // and the replica has no way to request the missing ones.
        if header.op != current_op + 1 {
            // Out-of-order prepare: the network delivered a future op before
            // we received the preceding ones. Drop it and rely on primary
            // retransmission to re-send both this op and the missing ones.
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                replica_id = consensus.replica(),
                op = header.op,
                expected = current_op + 1,
                "on_replicate: dropping out-of-order prepare (gap)"
            );
            return;
        }

        // Chain-replicate to the next replica before local journal append.
        let message = self.replicate(message).await;

        let journal = self
            .journal
            .as_ref()
            .expect("on_replicate: journal not initialized");
        if let Err(e) = journal.handle().append(message).await {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                replica_id = consensus.replica(),
                "on_replicate: WAL append failed for op={}: {e}", header.op
            );
            return;
        }

        consensus.sequencer().set_sequence(header.op);
        consensus.set_last_prepare_checksum(header.checksum);
        emit_namespace_progress_event(
            SimEventKind::NamespaceProgressUpdated,
            &ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
            header.op,
            consensus.pipeline().borrow().len(),
        );

        self.send_prepare_ok(&header).await;

        if consensus.is_follower() {
            self.commit_journal().await;
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn on_ack(&self, message: <VsrConsensus<B> as Consensus>::Message<PrepareOkHeader>) {
        let header = message.header();
        let consensus = self.consensus().expect("on_ack: consensus not initialized");

        if let Err(reason) = ack_preflight(consensus) {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                replica_id = consensus.replica(),
                view = consensus.view(),
                op = header.op,
                reason = reason.as_str(),
                "ignoring ack during preflight"
            );
            return;
        }

        {
            let pipeline = consensus.pipeline().borrow();
            if pipeline
                .entry_by_op_and_checksum(header.op, header.prepare_checksum)
                .is_none()
            {
                debug!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    replica_id = consensus.replica(),
                    op = header.op,
                    prepare_checksum = header.prepare_checksum,
                    "ack target prepare not in pipeline"
                );
                return;
            }
        }

        if !ack_quorum_reached(consensus, PlaneKind::Partitions, header) {
            return;
        }

        let drained = drain_committable_prefix(consensus);

        if drained.is_empty() {
            return;
        }

        if let (Some(first), Some(last)) = (drained.first(), drained.last()) {
            debug!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                replica_id = consensus.replica(),
                first_op = first.header.op,
                last_op = last.header.op,
                drained_count = drained.len(),
                "draining committed partition ops"
            );
        }

        let journal = self
            .journal
            .as_ref()
            .expect("on_ack: journal not initialized");
        for PipelineEntry {
            header: prepare_header,
            ..
        } in drained
        {
            let entry_namespace = IggyNamespace::from_raw(prepare_header.namespace);

            // Read full message from WAL and apply operation at commit time.
            let prepare = journal
                .handle()
                .entry(&prepare_header)
                .await
                .unwrap_or_else(|| {
                    panic!(
                        "on_ack: committed prepare op={} must be in WAL",
                        prepare_header.op
                    )
                });
            // Committed ops must be infallible — if the state machine cannot
            // apply a committed op, replicas will diverge.
            if let Err(e) = self
                .apply_replicated_operation(&entry_namespace, prepare)
                .await
            {
                panic!(
                    "on_ack: committed op={} failed to apply: {e}",
                    prepare_header.op
                );
            }

            // Flush after every op, each apply appends new data to the
            // partition journal that must be durable before advancing commit_min.
            if let Err(e) = self.commit_messages(&entry_namespace).await {
                panic!(
                    "on_ack: committed op={} failed to flush: {e}",
                    prepare_header.op
                );
            }

            consensus.advance_commit_min(prepare_header.op);

            let pipeline_depth = consensus.pipeline().borrow().len();
            let event = CommitLogEvent {
                replica: ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                op: prepare_header.op,
                client_id: prepare_header.client,
                request_id: prepare_header.request,
                operation: prepare_header.operation,
                pipeline_depth,
            };
            emit_sim_event(SimEventKind::OperationCommitted, &event);

            let reply = build_reply_message(consensus, &prepare_header, bytes::Bytes::new());
            consensus
                .client_table()
                .borrow_mut()
                .commit_reply(prepare_header.client, reply.clone());

            let reply_buffers = freeze_client_reply(reply.into_generic());
            emit_sim_event(SimEventKind::ClientReplyEmitted, &event);

            if let Err(e) = consensus
                .message_bus()
                .send_to_client(prepare_header.client, reply_buffers)
                .await
            {
                warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    client = prepare_header.client,
                    op = prepare_header.op,
                    "on_ack: failed to send reply to client: {e}"
                );
            }
        }
    }
}

impl<B, J> PlaneIdentity<VsrConsensus<B>> for IggyPartitions<VsrConsensus<B, NamespacedPipeline>, J>
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
{
    fn is_applicable<H>(&self, message: &<VsrConsensus<B> as Consensus>::Message<H>) -> bool
    where
        H: ConsensusHeader,
    {
        assert!(matches!(
            message.header().command(),
            Command2::Request | Command2::Prepare | Command2::PrepareOk
        ));
        message.header().operation().is_partition()
    }
}

impl<B, J> IggyPartitions<VsrConsensus<B, NamespacedPipeline>, J>
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
    J: JournalHandle,
    J::Target: Journal<J::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
{
    /// # Panics
    /// Panics if consensus is not initialized.
    pub fn register_namespace_in_pipeline(&self, ns: u64) {
        self.consensus()
            .expect("register_namespace_in_pipeline: consensus not initialized")
            .pipeline()
            .borrow_mut()
            .register_namespace(ns);
    }

    async fn apply_replicated_operation(
        &self,
        namespace: &IggyNamespace,
        message: Message<PrepareHeader>,
    ) -> Result<(), IggyError> {
        let consensus = self
            .consensus()
            .expect("apply_replicated_operation: consensus not initialized");
        let header = *message.header();

        // TODO: WE have to distinguish between an `message` recv by leader and follower.
        // In the follower path, we have to skip the `prepare_for_persistence` path, just append to journal.
        match header.operation {
            Operation::SendMessages => {
                self.append_send_messages_to_journal(namespace, message)
                    .await?;
                debug!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    replica = consensus.replica(),
                    op = header.op,
                    namespace_raw = namespace.inner(),
                    operation = ?header.operation,
                    "replicated send_messages appended to partition journal"
                );
                Ok(())
            }
            Operation::StoreConsumerOffset => {
                let total_size = header.size() as usize;
                let body = &message.as_slice()[std::mem::size_of::<PrepareHeader>()..total_size];
                let consumer_kind = *body.first().ok_or(IggyError::InvalidCommand)?;
                let consumer_id =
                    body.get(1..5)
                        .ok_or(IggyError::InvalidCommand)
                        .and_then(|bytes| {
                            <[u8; 4]>::try_from(bytes)
                                .map(u32::from_le_bytes)
                                .map(|value| value as usize)
                                .map_err(|_| IggyError::InvalidCommand)
                        })?;
                let offset =
                    body.get(5..13)
                        .ok_or(IggyError::InvalidCommand)
                        .and_then(|bytes| {
                            <[u8; 8]>::try_from(bytes)
                                .map(u64::from_le_bytes)
                                .map_err(|_| IggyError::InvalidCommand)
                        })?;
                let consumer = match consumer_kind {
                    1 => PollingConsumer::Consumer(consumer_id, 0),
                    2 => PollingConsumer::ConsumerGroup(consumer_id, 0),
                    _ => {
                        warn!(
                            target: "iggy.partitions.diag",
                            plane = "partitions",
                            replica = consensus.replica(),
                            op = header.op,
                            namespace_raw = namespace.inner(),
                            operation = ?header.operation,
                            consumer_kind,
                            "unknown consumer kind while applying replicated offset update"
                        );
                        return Err(IggyError::InvalidCommand);
                    }
                };

                let partition = self
                    .get_by_ns(namespace)
                    .expect("store_consumer_offset: partition not found for namespace");
                let _ = partition.store_consumer_offset(consumer, offset);

                debug!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    replica = consensus.replica(),
                    op = header.op,
                    namespace_raw = namespace.inner(),
                    operation = ?header.operation,
                    consumer_kind,
                    consumer_id,
                    offset,
                    "replicated consumer offset stored"
                );
                Ok(())
            }
            _ => {
                warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    replica = consensus.replica(),
                    namespace_raw = namespace.inner(),
                    op = header.op,
                    operation = ?header.operation,
                    "unexpected replicated partition operation"
                );
                Ok(())
            }
        }
    }

    /// Append a prepare message to a partition's journal with offset assignment.
    ///
    /// Updates `segment.current_position` (logical position for indexing) but
    /// not `segment.end_offset` or `segment.end_timestamp` (committed state).
    /// Those are updated during commit.
    ///
    /// Uses `dirty_offset` for offset assignment so that multiple prepares
    /// can be pipelined before any commit.
    async fn append_send_messages_to_journal(
        &self,
        namespace: &IggyNamespace,
        message: Message<PrepareHeader>,
    ) -> Result<(), IggyError> {
        let write_lock = self
            .get_by_ns(namespace)
            .expect("append_send_messages_to_journal: partition not found for namespace")
            .write_lock
            .clone();
        let _guard = write_lock.lock().await;
        let partition = self
            .get_mut_by_ns(namespace)
            .expect("append_send_messages_to_journal: partition not found for namespace");
        partition.append_messages(message).await.map(|_| ())
    }

    /// Replicate a prepare message to the next replica in the chain.
    ///
    /// Chain replication: primary -> first backup -> ... -> last backup.
    /// Stops when the next replica would be the primary.
    async fn replicate(&self, message: Message<PrepareHeader>) -> Message<PrepareHeader> {
        let consensus = self
            .consensus()
            .expect("replicate: consensus not initialized");
        replicate_to_next_in_chain(consensus, message).await
    }

    /// Walk ops from `commit_min+1` to `commit_max`, applying operations and
    /// updating the client table for each.
    ///
    /// The backup does NOT send replies to clients, only the primary does that.
    #[allow(clippy::cast_possible_truncation, clippy::missing_panics_doc)]
    pub async fn commit_journal(&self) {
        let consensus = self
            .consensus()
            .expect("commit_journal: consensus not initialized");
        let journal = self
            .journal
            .as_ref()
            .expect("commit_journal: journal not initialized");

        while consensus.commit_min() < consensus.commit_max() {
            let op = consensus.commit_min() + 1;

            let Some(header) = journal.handle().header(op as usize) else {
                // TODO: Implement message repair: request missing prepare from
                // primary or other replicas. Until then, the backup stalls here.
                break;
            };
            let header = *header;

            let Some(prepare) = journal.handle().entry(&header).await else {
                warn!("commit_journal: prepare body missing for op={op}, stopping");
                break;
            };

            let ns = IggyNamespace::from_raw(header.namespace);

            // Committed ops must be infallible (see on_ack comment).
            if let Err(e) = self.apply_replicated_operation(&ns, prepare).await {
                panic!("commit_journal: committed op={op} failed to apply: {e}");
            }
            if let Err(e) = self.commit_messages(&ns).await {
                panic!("commit_journal: committed op={op} failed to flush: {e}");
            }

            consensus.advance_commit_min(op);

            let reply = build_reply_message(consensus, &header, bytes::Bytes::new());
            consensus
                .client_table()
                .borrow_mut()
                .commit_reply(header.client, reply);
            debug!("commit_journal: committed op={op}");
        }
    }

    /// Persist prepared messages to segment storage and advance the durable frontier.
    ///
    /// Updates segment metadata, stats, flushes journal to disk if thresholds
    /// are exceeded, and advances the durable offset last.
    #[allow(clippy::too_many_lines)]
    async fn commit_messages(&self, namespace: &IggyNamespace) -> Result<(), IggyError> {
        let write_lock = self
            .get_by_ns(namespace)
            .expect("commit_messages: partition not found for namespace")
            .write_lock
            .clone();
        let _guard = write_lock.lock().await;

        let partition = self
            .get_mut_by_ns(namespace)
            .expect("commit_messages: partition not found for namespace");

        let journal = partition.log.journal();
        let journal_info = journal.info;

        if journal_info.messages_count == 0 {
            return Ok(());
        }

        // 1. Check flush thresholds.
        let is_full = partition.log.active_segment().is_full();

        let unsaved_messages_count_exceeded =
            journal_info.messages_count >= self.config.messages_required_to_save;
        let unsaved_messages_size_exceeded = journal_info.size.as_bytes_u64()
            >= self.config.size_of_messages_required_to_save.as_bytes_u64();
        let should_persist =
            is_full || unsaved_messages_count_exceeded || unsaved_messages_size_exceeded;

        if !should_persist {
            return Ok(());
        }

        // Freeze journal batches.
        let (frozen_batches, index_bytes, batch_count) = {
            let entries = partition.log.journal().inner.entries();
            let segment = partition.log.active_segment();
            let mut file_position = segment.size.as_bytes_u64();
            partition.log.ensure_indexes();
            let indexes = partition.log.active_indexes_mut().unwrap();
            let mut flush_index = None;
            let mut frozen = Vec::with_capacity(entries.len());
            let mut batch_count = 0u32;

            for entry in entries {
                let Ok(batch) = decode_prepare_slice(entry.as_slice()) else {
                    continue;
                };
                let message_count = batch.message_count();
                if message_count == 0 {
                    continue;
                }

                let index = crate::iggy_index::IggyIndex::new(
                    batch.header.base_offset,
                    batch.header.base_timestamp,
                    file_position,
                );
                if flush_index.is_none() {
                    indexes.insert(index.offset, index.timestamp, index.position);
                    flush_index = Some(index);
                }
                file_position += batch.header.total_size() as u64;
                batch_count += message_count;
                frozen.push(entry);
            }

            let index_bytes =
                flush_index.map(|index| crate::iggy_index::IggyIndexCache::serialize(&index));

            (frozen, index_bytes, batch_count)
        };

        let Some(index_bytes) = index_bytes else {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = namespace.inner(),
                "failed to build sparse index entry from pending journal batches"
            );
            return Err(IggyError::InvalidCommand);
        };

        // Persist to disk.
        self.persist_frozen_batches_to_disk(namespace, frozen_batches, index_bytes, batch_count)
            .await?;

        if is_full {
            self.rotate_segment(namespace).await?;
        }

        // Reset journal info after drain.
        let partition = self
            .get_mut_by_ns(namespace)
            .expect("commit_messages: partition not found");
        let _ = partition.log.journal_mut().inner.commit();
        partition.log.journal_mut().info = JournalInfo::default();

        // Update segment metadata from the just-persisted journal state.
        let partition = self
            .get_mut_by_ns(namespace)
            .expect("commit_messages: partition not found");
        let segment_index = partition.log.segments().len() - 1;
        let segment = &mut partition.log.segments_mut()[segment_index];

        if segment.end_offset == 0 && journal_info.first_timestamp != 0 {
            segment.start_timestamp = journal_info.first_timestamp;
        }
        segment.end_timestamp = journal_info.end_timestamp;
        segment.max_timestamp = segment.max_timestamp.max(journal_info.max_timestamp);
        segment.end_offset = journal_info.current_offset;

        partition
            .stats
            .increment_size_bytes(journal_info.size.as_bytes_u64());
        partition
            .stats
            .increment_messages_count(u64::from(journal_info.messages_count));

        let durable_offset = journal_info.current_offset;
        partition.offset.store(durable_offset, Ordering::Release);
        partition.stats.set_current_offset(durable_offset);
        Ok(())
    }

    async fn handle_committed_entries(
        &self,
        consensus: &VsrConsensus<B, NamespacedPipeline>,
        drained: Vec<PipelineEntry>,
    ) {
        if let (Some(first), Some(last)) = (drained.first(), drained.last()) {
            debug!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                replica_id = consensus.replica(),
                first_op = first.header.op,
                last_op = last.header.op,
                drained_count = drained.len(),
                "draining committed partition ops"
            );
        }

        let mut committed_ns: HashSet<IggyNamespace> = HashSet::new();
        let mut failed_ns: HashSet<IggyNamespace> = HashSet::new();
        let committed_visible_offsets = self
            .resolve_committed_visible_offsets(consensus, &drained, &mut failed_ns)
            .await;

        for PipelineEntry {
            header: prepare_header,
            ..
        } in drained
        {
            let entry_namespace = IggyNamespace::from_raw(prepare_header.namespace);
            if !self
                .commit_partition_entry(
                    consensus,
                    prepare_header,
                    entry_namespace,
                    &mut committed_ns,
                    &committed_visible_offsets,
                    &mut failed_ns,
                )
                .await
            {
                continue;
            }

            let pipeline_depth = consensus.pipeline().borrow().len();
            let event = CommitLogEvent {
                replica: ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                op: prepare_header.op,
                client_id: prepare_header.client,
                request_id: prepare_header.request,
                operation: prepare_header.operation,
                pipeline_depth,
            };
            emit_sim_event(SimEventKind::OperationCommitted, &event);
            emit_namespace_progress_event(
                SimEventKind::NamespaceProgressUpdated,
                &event.replica,
                prepare_header.op,
                pipeline_depth,
            );

            let reply = build_reply_message(consensus, &prepare_header, bytes::Bytes::new());
            // Cache reply for duplicate detection:
            consensus
                .client_table()
                .borrow_mut()
                .commit_reply(prepare_header.client, reply.clone());

            let reply_buffers = freeze_client_reply(reply.into_generic());
            emit_sim_event(SimEventKind::ClientReplyEmitted, &event);

            if let Err(error) = consensus
                .message_bus()
                .send_to_client(prepare_header.client, reply_buffers)
                .await
            {
                warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    client = prepare_header.client,
                    op = prepare_header.op,
                    namespace_raw = entry_namespace.inner(),
                    %error,
                    "failed to send reply to client"
                );
            }
        }
        if !failed_ns.is_empty() {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                replica_id = consensus.replica(),
                failed_namespaces = failed_ns.len(),
                "some namespaces failed local commit handling"
            );
        }
    }

    async fn resolve_committed_visible_offsets(
        &self,
        consensus: &VsrConsensus<B, NamespacedPipeline>,
        drained: &[PipelineEntry],
        failed_ns: &mut HashSet<IggyNamespace>,
    ) -> HashMap<IggyNamespace, u64> {
        let mut committed_visible_offsets = HashMap::new();

        for entry in drained {
            if entry.header.operation != Operation::SendMessages {
                continue;
            }

            let entry_namespace = IggyNamespace::from_raw(entry.header.namespace);
            match self
                .committed_end_offset_for_prepare(&entry_namespace, &entry.header)
                .await
            {
                Ok(Some(end_offset)) => {
                    committed_visible_offsets.insert(entry_namespace, end_offset);
                }
                Ok(None) => {}
                Err(error) => {
                    failed_ns.insert(entry_namespace);
                    warn!(
                        target: "iggy.partitions.diag",
                        plane = "partitions",
                        replica_id = consensus.replica(),
                        namespace_raw = entry_namespace.inner(),
                        op = entry.header.op,
                        operation = ?entry.header.operation,
                        %error,
                        "failed to resolve committed visible offset for partition entry"
                    );
                }
            }
        }

        committed_visible_offsets
    }

    async fn commit_partition_entry(
        &self,
        consensus: &VsrConsensus<B, NamespacedPipeline>,
        prepare_header: PrepareHeader,
        entry_namespace: IggyNamespace,
        committed_ns: &mut HashSet<IggyNamespace>,
        committed_visible_offsets: &HashMap<IggyNamespace, u64>,
        failed_ns: &mut HashSet<IggyNamespace>,
    ) -> bool {
        match prepare_header.operation {
            Operation::SendMessages => {
                if committed_ns.insert(entry_namespace)
                    && let Err(error) = self.commit_messages(&entry_namespace).await
                {
                    failed_ns.insert(entry_namespace);
                    warn!(
                        target: "iggy.partitions.diag",
                        plane = "partitions",
                        replica_id = consensus.replica(),
                        namespace_raw = entry_namespace.inner(),
                        op = prepare_header.op,
                        operation = ?prepare_header.operation,
                        %error,
                        "failed to commit partition messages"
                    );
                }

                if committed_ns.contains(&entry_namespace)
                    && !failed_ns.contains(&entry_namespace)
                    && let Some(visible_offset) =
                        committed_visible_offsets.get(&entry_namespace).copied()
                {
                    let partition = self
                        .get_by_ns(&entry_namespace)
                        .expect("commit_partition_entry: partition not found");
                    partition.offset.store(visible_offset, Ordering::Release);
                    partition.stats.set_current_offset(visible_offset);
                }

                !failed_ns.contains(&entry_namespace)
            }
            Operation::StoreConsumerOffset => {
                // TODO: Commit consumer offset update.
                debug!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    replica_id = consensus.replica(),
                    op = prepare_header.op,
                    namespace_raw = entry_namespace.inner(),
                    "consumer offset committed"
                );
                true
            }
            _ => {
                warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    replica_id = consensus.replica(),
                    op = prepare_header.op,
                    namespace_raw = entry_namespace.inner(),
                    operation = ?prepare_header.operation,
                    "unexpected committed partition operation"
                );
                true
            }
        }
    }

    async fn committed_end_offset_for_prepare(
        &self,
        namespace: &IggyNamespace,
        prepare_header: &PrepareHeader,
    ) -> Result<Option<u64>, IggyError> {
        let partition = self
            .get_by_ns(namespace)
            .expect("committed_end_offset_for_prepare: partition not found");
        let Some(entry) = partition.log.journal().inner.entry(prepare_header).await else {
            return Err(IggyError::InvalidCommand);
        };
        let batch =
            decode_prepare_slice(entry.as_slice()).map_err(|_| IggyError::InvalidCommand)?;
        let message_count = batch.message_count();
        if message_count == 0 {
            return Ok(None);
        }

        Ok(Some(
            batch.header.base_offset + u64::from(message_count) - 1,
        ))
    }

    /// Persist frozen batches to disk and update segment bookkeeping.
    async fn persist_frozen_batches_to_disk(
        &self,
        namespace: &IggyNamespace,
        frozen_batches: Vec<Frozen<4096>>,
        index_bytes: Vec<u8>,
        batch_count: u32,
    ) -> Result<(), IggyError> {
        if batch_count == 0 {
            return Ok(());
        }

        let partition = self
            .get_by_ns(namespace)
            .expect("persist: partition not found");

        if !partition.log.has_segments() {
            return Ok(());
        }

        let stripped_batches: Vec<_> = frozen_batches
            .into_iter()
            .map(|batch| batch.slice(std::mem::size_of::<PrepareHeader>()..))
            .collect();
        let messages_writer = partition
            .log
            .messages_writers()
            .last()
            .and_then(|writer| writer.as_ref())
            .cloned();
        let index_writer = partition
            .log
            .index_writers()
            .last()
            .and_then(|writer| writer.as_ref())
            .cloned();

        if messages_writer.is_none() || index_writer.is_none() {
            let saved_bytes = stripped_batches.iter().map(Frozen::len).sum::<usize>();
            debug!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = namespace.inner(),
                batch_count,
                saved_bytes,
                "simulated in-memory batch persistence"
            );

            let partition = self
                .get_mut_by_ns(namespace)
                .expect("persist: partition not found");
            let segment_index = partition.log.segments().len() - 1;
            let segment = &mut partition.log.segments_mut()[segment_index];
            segment.size = IggyByteSize::from(segment.size.as_bytes_u64() + saved_bytes as u64);
            partition.log.clear_in_flight();
            return Ok(());
        }

        let messages_writer = messages_writer.expect("checked above");
        let index_writer = index_writer.expect("checked above");

        let saved = messages_writer
            .save_frozen_batches(&stripped_batches)
            .await
            .map_err(|error| {
                warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    namespace_raw = namespace.inner(),
                    batch_count,
                    %error,
                    "failed to save frozen batches"
                );
                error
            })?;

        index_writer
            .save_indexes(index_bytes)
            .await
            .map_err(|error| {
                warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    namespace_raw = namespace.inner(),
                    batch_count,
                    %error,
                    "failed to save sparse indexes"
                );
                error
            })?;

        debug!(
            target: "iggy.partitions.diag",
            plane = "partitions",
            namespace_raw = namespace.inner(),
            batch_count,
            saved_bytes = saved.as_bytes_u64(),
            "persisted batches to disk"
        );

        let partition = self
            .get_mut_by_ns(namespace)
            .expect("persist: partition not found");

        // Recalculate index: segment deletion during async I/O shifts indices.
        let segment_index = partition.log.segments().len() - 1;

        let segment = &mut partition.log.segments_mut()[segment_index];
        segment.size = IggyByteSize::from(segment.size.as_bytes_u64() + saved.as_bytes_u64());

        partition.log.clear_in_flight();
        Ok(())
    }

    /// Rotate to a new segment when the current one is full.
    async fn rotate_segment(&self, namespace: &IggyNamespace) -> Result<(), IggyError> {
        let partition = self
            .get_mut_by_ns(namespace)
            .expect("rotate_segment: partition not found");

        let old_segment_index = partition.log.segments().len() - 1;
        let active_segment = partition.log.active_segment_mut();
        active_segment.sealed = true;
        let start_offset = active_segment.end_offset + 1;

        let segment = Segment::new(start_offset, self.config.segment_size);

        // TODO: Waiting for issue to move server config to shared module.
        // Once complete, paths will come from proper base_path/streams_path/etc config fields.
        let messages_path = self.config.get_messages_path(
            namespace.stream_id(),
            namespace.topic_id(),
            namespace.partition_id(),
            start_offset,
        );
        let index_path = self.config.get_index_path(
            namespace.stream_id(),
            namespace.topic_id(),
            namespace.partition_id(),
            start_offset,
        );

        let storage = SegmentStorage::new(
            &messages_path,
            &index_path,
            0, // messages_size (new segment)
            0, // indexes_size (new segment)
            self.config.enforce_fsync,
            self.config.enforce_fsync,
            false, // file_exists (new segment)
        )
        .await
        .map_err(|_| IggyError::CannotCreateSegmentLogFile(messages_path.clone()))?;
        let messages_size_bytes = storage
            .messages_writer
            .as_ref()
            .ok_or_else(|| IggyError::CannotCreateSegmentLogFile(messages_path.clone()))?
            .size_counter();
        let messages_writer = Rc::new(
            MessagesWriter::new(
                &messages_path,
                messages_size_bytes,
                self.config.enforce_fsync,
                false,
            )
            .await
            .map_err(|_| IggyError::CannotCreateSegmentLogFile(messages_path.clone()))?,
        );
        let index_writer = Rc::new(
            IggyIndexWriter::new(
                &index_path,
                Rc::new(std::sync::atomic::AtomicU64::new(0)),
                self.config.enforce_fsync,
                false,
            )
            .await
            .map_err(|_| IggyError::CannotCreateSegmentIndexFile(index_path.clone()))?,
        );

        // Close writers for the sealed segment.
        let old_storage = &mut partition.log.storages_mut()[old_segment_index];
        let _ = old_storage.shutdown();
        partition.log.messages_writers_mut()[old_segment_index] = None;
        partition.log.index_writers_mut()[old_segment_index] = None;

        partition.log.add_persisted_segment(
            segment,
            storage,
            Some(messages_writer),
            Some(index_writer),
        );
        partition.stats.increment_segments_count(1);

        debug!(
            target: "iggy.partitions.diag",
            plane = "partitions",
            namespace_raw = namespace.inner(),
            start_offset,
            "rotated to new segment"
        );
        Ok(())
    }

    async fn send_prepare_ok(&self, header: &PrepareHeader) {
        let consensus = self
            .consensus()
            .expect("send_prepare_ok: consensus not initialized");
        send_prepare_ok_common(consensus, header, Some(true)).await;
    }
}
