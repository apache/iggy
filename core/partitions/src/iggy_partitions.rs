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
use crate::types::PartitionsConfig;
use consensus::PlaneIdentity;
use consensus::{
    Consensus, NamespacedPipeline, Pipeline, PipelineEntry, Plane, Project, Sequencer,
    VsrConsensus, ack_preflight, build_reply_message, fence_old_prepare_by_commit,
    pipeline_prepare_common, replicate_preflight, replicate_to_next_in_chain,
    send_prepare_ok as send_prepare_ok_common,
};
use iggy_common::header::Command2;
use iggy_common::{
    INDEX_SIZE, IggyByteSize, IggyIndexesMut, IggyMessagesBatchMut, PartitionStats, PooledBuffer,
    Segment, SegmentStorage,
    header::{
        ConsensusHeader, GenericHeader, Operation, PrepareHeader, PrepareOkHeader, RequestHeader,
    },
    message::Message,
    sharding::{IggyNamespace, LocalIdx, ShardId},
};
use message_bus::MessageBus;
use std::cell::UnsafeCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{debug, warn};

/// Per-shard collection of all partitions.
///
/// This struct manages ALL partitions assigned to a single shard, regardless
/// of which stream/topic they belong to.
///
/// Note: The partition_id within IggyNamespace may NOT equal the Vec index.
/// For example, shard 0 might have partition_ids [0, 2, 4] while shard 1
/// has partition_ids [1, 3, 5]. The `LocalIdx` provides the actual index
/// into the `partitions` Vec.
pub struct IggyPartitions<C> {
    shard_id: ShardId,
    config: PartitionsConfig,
    /// Collection of partitions, the index of each partition isn't it's ID, but rather an local index (LocalIdx) which is used for lookups.
    ///
    /// Wrapped in `UnsafeCell` for interior mutability — matches the single-threaded
    /// per-shard execution model. Consensus trait methods take `&self` but need to
    /// mutate partition state (segments, offsets, journal).
    partitions: UnsafeCell<Vec<IggyPartition>>,
    namespace_to_local: HashMap<IggyNamespace, LocalIdx>,
    consensus: Option<C>,
}

impl<C> IggyPartitions<C> {
    pub fn new(shard_id: ShardId, config: PartitionsConfig) -> Self {
        Self {
            shard_id,
            config,
            partitions: UnsafeCell::new(Vec::new()),
            namespace_to_local: HashMap::new(),
            consensus: None,
        }
    }

    pub fn with_capacity(shard_id: ShardId, config: PartitionsConfig, capacity: usize) -> Self {
        Self {
            shard_id,
            config,
            partitions: UnsafeCell::new(Vec::with_capacity(capacity)),
            namespace_to_local: HashMap::with_capacity(capacity),
            consensus: None,
        }
    }

    pub fn config(&self) -> &PartitionsConfig {
        &self.config
    }

    fn partitions(&self) -> &Vec<IggyPartition> {
        // Safety: single-threaded per-shard model, no concurrent access.
        unsafe { &*self.partitions.get() }
    }

    // TODO: Figure out better way to do this, maybe inline the mutable access to ignore the lint.
    #[allow(clippy::mut_from_ref)]
    fn partitions_mut(&self) -> &mut Vec<IggyPartition> {
        // Safety: single-threaded per-shard model, no concurrent access.
        unsafe { &mut *self.partitions.get() }
    }

    pub fn shard_id(&self) -> ShardId {
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
    pub fn get_mut(&self, local_idx: LocalIdx) -> Option<&mut IggyPartition> {
        self.partitions_mut().get_mut(*local_idx)
    }

    /// Lookup local index by namespace.
    pub fn local_idx(&self, namespace: &IggyNamespace) -> Option<LocalIdx> {
        self.namespace_to_local.get(namespace).copied()
    }

    /// Insert a new partition and return its local index.
    pub fn insert(&mut self, namespace: IggyNamespace, partition: IggyPartition) -> LocalIdx {
        let partitions = self.partitions_mut();
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
    pub fn get_mut_by_ns(&self, namespace: &IggyNamespace) -> Option<&mut IggyPartition> {
        let idx = self.namespace_to_local.get(namespace)?;
        self.partitions_mut().get_mut(**idx)
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
            for (_ns, lidx) in self.namespace_to_local.iter_mut() {
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
        let partitions = self.partitions_mut();
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
        &mut self.partitions_mut()[idx]
    }

    pub fn consensus(&self) -> Option<&C> {
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
        let mut partition = IggyPartition::new(stats.clone());
        partition.log.add_persisted_segment(segment, storage);
        partition.offset.store(start_offset, Ordering::Relaxed);
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
    /// 1. Control plane: create PartitionMeta (SKIPPED in this method)
    /// 2. Control plane: broadcast to shards (SKIPPED in this method)
    /// 3. Data plane: INITIATE PARTITION (THIS METHOD)
    ///
    /// Idempotent: subsequent calls for the same namespace are no-ops.
    /// Consensus must be set separately via `set_consensus`.
    pub async fn init_partition(&mut self, namespace: IggyNamespace) -> LocalIdx {
        if let Some(idx) = self.local_idx(&namespace) {
            return idx;
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
        .expect("Failed to create segment storage");

        // Create partition with initialized log
        let stats = Arc::new(PartitionStats::default());
        let mut partition = IggyPartition::new(stats.clone());
        partition.log.add_persisted_segment(segment, storage);
        partition.offset.store(start_offset, Ordering::Relaxed);
        partition
            .dirty_offset
            .store(start_offset, Ordering::Relaxed);
        partition.should_increment_offset = false;
        partition.stats.increment_segments_count(1);

        self.insert(namespace, partition)
    }
}

impl<B> Plane<VsrConsensus<B, NamespacedPipeline>>
    for IggyPartitions<VsrConsensus<B, NamespacedPipeline>>
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
{
    async fn on_request(
        &self,
        message: <VsrConsensus<B, NamespacedPipeline> as Consensus>::Message<RequestHeader>,
    ) {
        let namespace = IggyNamespace::from_raw(message.header().namespace);
        let consensus = self
            .consensus()
            .expect("on_request: consensus not initialized");

        debug!(?namespace, "handling partition request");
        let prepare = message.project(consensus);
        pipeline_prepare_common(consensus, prepare, |prepare| self.on_replicate(prepare)).await;
    }

    async fn on_replicate(
        &self,
        message: <VsrConsensus<B, NamespacedPipeline> as Consensus>::Message<PrepareHeader>,
    ) {
        let header = message.header();
        let namespace = IggyNamespace::from_raw(header.namespace);
        let consensus = self
            .consensus()
            .expect("on_replicate: consensus not initialized");

        let current_op = match replicate_preflight(consensus, header) {
            Ok(current_op) => current_op,
            Err(reason) => {
                warn!(
                    replica = consensus.replica(),
                    "on_replicate: ignoring ({reason})"
                );
                return;
            }
        };

        let is_old_prepare = fence_old_prepare_by_commit(consensus, header);
        if !is_old_prepare {
            self.replicate(message.clone()).await;
        } else {
            warn!("received old prepare, not replicating");
        }

        // TODO: Make those assertions be toggleable through an feature flag, so they can be used only by simulator/tests.
        debug_assert_eq!(header.op, current_op + 1);
        consensus.sequencer().set_sequence(header.op);
        consensus.set_last_prepare_checksum(header.checksum);

        // TODO: Figure out the flow of the partition operations.
        // In metadata layer we assume that when an `on_request` or `on_replicate` is called, it's called from correct shard.
        // I think we need to do the same here, which means that the code from below is unfallable, the partition should always exist by now!
        self.apply_replicated_operation(&namespace, &message).await;

        self.send_prepare_ok(header).await;

        if consensus.is_follower() {
            self.commit_journal(&namespace);
        }
    }

    async fn on_ack(
        &self,
        message: <VsrConsensus<B, NamespacedPipeline> as Consensus>::Message<PrepareOkHeader>,
    ) {
        let header = message.header();
        let consensus = self.consensus().expect("on_ack: consensus not initialized");

        if let Err(reason) = ack_preflight(consensus) {
            warn!("on_ack: ignoring ({reason})");
            return;
        }

        {
            let pipeline = consensus.pipeline().borrow();
            if pipeline
                .message_by_op_and_checksum(header.op, header.prepare_checksum)
                .is_none()
            {
                debug!("on_ack: prepare not in pipeline op={}", header.op);
                return;
            }
        }

        consensus.handle_prepare_ok(header);

        // SAFETY(IGGY-66): Per-namespace drain independent of global commit.
        //
        // drain_committable_all() drains each namespace queue independently by
        // quorum flag, so ns_a ops can be drained and replied to clients while
        // ns_b ops block the global commit (e.g., ns_a ops 1,3 drain while
        // ns_b op 2 is pending). This is intentional for partition independence.
        //
        // View change risk: if a view change occurs before the global commit
        // covers a drained op, the new primary replays from max_commit+1 and
        // re-executes it. append_messages is NOT idempotent -- re-execution
        // produces duplicate partition data.
        //
        // Before this path handles real traffic, two guards are required:
        //   1. Op-based dedup in apply_replicated_operation: skip append if
        //      the partition journal already contains data for this op.
        //   2. Client reply dedup by (client_id, request_id): prevent
        //      duplicate replies after view change re-execution.
        let drained = {
            let mut pipeline = consensus.pipeline().borrow_mut();
            pipeline.drain_committable_all()
        };

        if drained.is_empty() {
            return;
        }

        // Advance global commit for VSR protocol correctness
        {
            let pipeline = consensus.pipeline().borrow();
            let new_commit = pipeline.global_commit_frontier(consensus.commit());
            drop(pipeline);
            consensus.advance_commit_number(new_commit);
        }

        if let (Some(first), Some(last)) = (drained.first(), drained.last()) {
            debug!(
                "on_ack: draining committed ops=[{}..={}] count={}",
                first.header.op,
                last.header.op,
                drained.len()
            );
        }

        let mut committed_ns: HashSet<IggyNamespace> = HashSet::new();

        for PipelineEntry {
            header: prepare_header,
            ..
        } in drained
        {
            let entry_namespace = IggyNamespace::from_raw(prepare_header.namespace);

            match prepare_header.operation {
                Operation::SendMessages => {
                    if committed_ns.insert(entry_namespace) {
                        self.commit_messages(&entry_namespace).await;
                    }
                    debug!("on_ack: messages committed for op={}", prepare_header.op,);
                }
                Operation::StoreConsumerOffset => {
                    // TODO: Commit consumer offset update.
                    debug!(
                        "on_ack: consumer offset committed for op={}",
                        prepare_header.op
                    );
                }
                _ => {
                    warn!(
                        "on_ack: unexpected operation {:?} for op={}",
                        prepare_header.operation, prepare_header.op
                    );
                }
            }

            let generic_reply = build_reply_message(consensus, &prepare_header).into_generic();
            debug!(
                "on_ack: sending reply to client={} for op={}",
                prepare_header.client, prepare_header.op
            );

            // TODO: Propagate send error instead of panicking; requires bus error design.
            consensus
                .message_bus()
                .send_to_client(prepare_header.client, generic_reply)
                .await
                .unwrap()
        }
    }
}

impl<B> PlaneIdentity<VsrConsensus<B>> for IggyPartitions<VsrConsensus<B, NamespacedPipeline>>
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
        let operation = message.header().operation();
        // TODO: Use better selection, smth like greater or equal based on op number.
        matches!(
            operation,
            Operation::DeleteSegments | Operation::SendMessages | Operation::StoreConsumerOffset
        )
    }
}

impl<B> IggyPartitions<VsrConsensus<B, NamespacedPipeline>>
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
{
    pub fn register_namespace_in_pipeline(&self, ns: u64) {
        self.consensus()
            .expect("register_namespace_in_pipeline: consensus not initialized")
            .pipeline()
            .borrow_mut()
            .register_namespace(ns);
    }

    // TODO: Move this elsewhere, also do not reallocate, we do reallocationg now becauise we use PooledBuffer for the batch body
    // but `Bytes` for `Message` payload.
    fn batch_from_body(body: &[u8]) -> IggyMessagesBatchMut {
        assert!(body.len() >= 4, "prepare body too small for batch header");
        let count = u32::from_le_bytes(body[0..4].try_into().unwrap());
        let indexes_len = count as usize * INDEX_SIZE;
        let indexes_end = 4 + indexes_len;
        assert!(
            body.len() >= indexes_end,
            "prepare body too small for {} indexes",
            count
        );

        let indexes = IggyIndexesMut::from_bytes(PooledBuffer::from(&body[4..indexes_end]), 0);
        let messages = PooledBuffer::from(&body[indexes_end..]);
        IggyMessagesBatchMut::from_indexes_and_messages(indexes, messages)
    }

    async fn apply_replicated_operation(
        &self,
        namespace: &IggyNamespace,
        message: &Message<PrepareHeader>,
    ) {
        let consensus = self
            .consensus()
            .expect("apply_replicated_operation: consensus not initialized");
        let header = message.header();

        match header.operation {
            Operation::SendMessages => {
                let body = message.body_bytes();
                self.append_send_messages_to_journal(namespace, body.as_ref())
                    .await;
                debug!(
                    replica = consensus.replica(),
                    op = header.op,
                    ?namespace,
                    "on_replicate: send_messages appended to partition journal"
                );
            }
            Operation::StoreConsumerOffset => {
                // TODO: Deserialize consumer offset from prepare body
                // and store in partition's consumer_offsets.
                debug!(
                    replica = consensus.replica(),
                    op = header.op,
                    "on_replicate: consumer offset stored"
                );
            }
            _ => {
                warn!(
                    replica = consensus.replica(),
                    op = header.op,
                    "on_replicate: unexpected operation {:?}",
                    header.operation
                );
            }
        }
    }

    async fn append_send_messages_to_journal(&self, namespace: &IggyNamespace, body: &[u8]) {
        let batch = Self::batch_from_body(body);
        self.append_messages_to_journal(namespace, batch).await;
    }

    /// Append a batch to a partition's journal with offset assignment.
    ///
    /// Updates `segment.current_position` (logical position for indexing) but
    /// not `segment.end_offset` or `segment.end_timestamp` (committed state).
    /// Those are updated during commit.
    ///
    /// Uses `dirty_offset` for offset assignment so that multiple prepares
    /// can be pipelined before any commit.
    async fn append_messages_to_journal(
        &self,
        namespace: &IggyNamespace,
        batch: IggyMessagesBatchMut,
    ) {
        let partition = self
            .get_mut_by_ns(namespace)
            .expect("append_messages_to_journal: partition not found for namespace");
        let _ = partition.append_messages(batch).await;
    }

    /// Replicate a prepare message to the next replica in the chain.
    ///
    /// Chain replication: primary -> first backup -> ... -> last backup.
    /// Stops when the next replica would be the primary.
    async fn replicate(&self, message: Message<PrepareHeader>) {
        let consensus = self
            .consensus()
            .expect("replicate: consensus not initialized");
        replicate_to_next_in_chain(consensus, message).await;
    }

    fn commit_journal(&self, _namespace: &IggyNamespace) {
        // TODO: Implement commit logic for followers.
        // Walk through journal from last committed to current commit number
        // Apply each entry to the partition state
    }

    /// Commit messages after quorum ack.
    ///
    /// Updates segment metadata, stats, flushes journal to disk if thresholds
    /// are exceeded, and advances the committed offset last.
    async fn commit_messages(&self, namespace: &IggyNamespace) {
        let partition = self
            .get_mut_by_ns(namespace)
            .expect("commit_messages: partition not found for namespace");

        let journal = partition.log.journal();
        let journal_info = journal.info;

        if journal_info.messages_count == 0 {
            return;
        }

        // 1. Update segment metadata from journal state.
        // Note: segment.current_position is already updated in append_batch (prepare phase).
        let segment_index = partition.log.segments().len() - 1;
        let segment = &mut partition.log.segments_mut()[segment_index];

        if segment.end_offset == 0 && journal_info.first_timestamp != 0 {
            segment.start_timestamp = journal_info.first_timestamp;
        }
        segment.end_timestamp = journal_info.end_timestamp;
        segment.end_offset = journal_info.current_offset;

        // 2. Update stats.
        partition
            .stats
            .increment_size_bytes(journal_info.size.as_bytes_u64());
        partition
            .stats
            .increment_messages_count(journal_info.messages_count as u64);

        // 3. Check flush thresholds.
        let is_full = segment.is_full();

        let unsaved_messages_count_exceeded =
            journal_info.messages_count >= self.config.messages_required_to_save;
        let unsaved_messages_size_exceeded = journal_info.size.as_bytes_u64()
            >= self.config.size_of_messages_required_to_save.as_bytes_u64();

        if is_full || unsaved_messages_count_exceeded || unsaved_messages_size_exceeded {
            // Freeze journal batches.
            let frozen_batches = {
                let batches = partition.log.journal_mut().inner.commit();
                partition.log.ensure_indexes();
                batches.append_indexes_to(partition.log.active_indexes_mut().unwrap());

                let frozen: Vec<_> = batches
                    .into_inner()
                    .into_iter()
                    .map(|mut b| b.freeze())
                    .collect();
                partition.log.set_in_flight(frozen.clone());
                frozen
            };

            // Persist to disk.
            self.persist_frozen_batches_to_disk(namespace, frozen_batches)
                .await;

            if is_full {
                self.rotate_segment(namespace).await;
            }

            // Reset journal info after drain.
            let partition = self
                .get_mut_by_ns(namespace)
                .expect("commit_messages: partition not found");
            partition.log.journal_mut().info = Default::default();
        }

        // 4. Advance committed offset (last, so consumers only see offset after data is durable).
        let partition = self
            .get_mut_by_ns(namespace)
            .expect("commit_messages: partition not found");
        let committed_offset = journal_info.current_offset;
        partition.offset.store(committed_offset, Ordering::Relaxed);
        partition.stats.set_current_offset(committed_offset);
    }

    /// Persist frozen batches to disk and update segment bookkeeping.
    async fn persist_frozen_batches_to_disk(
        &self,
        namespace: &IggyNamespace,
        frozen_batches: Vec<iggy_common::IggyMessagesBatch>,
    ) {
        let batch_count: u32 = frozen_batches.iter().map(|b| b.count()).sum();

        if batch_count == 0 {
            return;
        }

        let partition = self
            .get_by_ns(namespace)
            .expect("persist: partition not found");

        if !partition.log.has_segments() {
            return;
        }

        let messages_writer = partition
            .log
            .active_storage()
            .messages_writer
            .as_ref()
            .expect("Messages writer not initialized")
            .clone();
        let index_writer = partition
            .log
            .active_storage()
            .index_writer
            .as_ref()
            .expect("Index writer not initialized")
            .clone();

        let saved = messages_writer
            .as_ref()
            .save_frozen_batches(&frozen_batches)
            .await
            .expect("persist: failed to save frozen batches");

        let unsaved_indexes_slice = {
            let partition = self
                .get_by_ns(namespace)
                .expect("persist: partition not found");
            let segment_index = partition.log.segments().len() - 1;
            partition.log.indexes()[segment_index]
                .as_ref()
                .expect("indexes must exist for segment being persisted")
                .unsaved_slice()
        };

        index_writer
            .as_ref()
            .save_indexes(unsaved_indexes_slice)
            .await
            .expect("persist: failed to save indexes");

        debug!(?namespace, batch_count, ?saved, "persisted batches to disk");

        let partition = self
            .get_mut_by_ns(namespace)
            .expect("persist: partition not found");

        // Recalculate index: segment deletion during async I/O shifts indices.
        let segment_index = partition.log.segments().len() - 1;

        let indexes = partition.log.indexes_mut()[segment_index]
            .as_mut()
            .expect("indexes must exist for segment being persisted");
        indexes.mark_saved();

        let segment = &mut partition.log.segments_mut()[segment_index];
        segment.size = IggyByteSize::from(segment.size.as_bytes_u64() + saved.as_bytes_u64());

        partition.log.clear_in_flight();
    }

    /// Rotate to a new segment when the current one is full.
    async fn rotate_segment(&self, namespace: &IggyNamespace) {
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
        .expect("Failed to create segment storage");

        // Clear old segment's indexes.
        // TODO: Waiting for issue to move server config to shared module.
        // Once complete, conditionally clear based on cache_indexes config:
        // if !matches!(self.config.cache_indexes, CacheIndexesConfig::All) {
        //     partition.log.indexes_mut()[old_segment_index] = None;
        // }
        partition.log.indexes_mut()[old_segment_index] = None;

        // Close writers for the sealed segment.
        let old_storage = &mut partition.log.storages_mut()[old_segment_index];
        let _ = old_storage.shutdown();

        partition.log.add_persisted_segment(segment, storage);
        partition.stats.increment_segments_count(1);

        debug!(?namespace, start_offset, "rotated to new segment");
    }

    async fn send_prepare_ok(&self, header: &PrepareHeader) {
        let consensus = self
            .consensus()
            .expect("send_prepare_ok: consensus not initialized");
        // TODO: Verify the prepare is persisted in the partition journal.
        // The partition journal uses MessageLookup headers, so we cannot
        // check by PrepareHeader.op directly. For now, skip this check.
        send_prepare_ok_common(consensus, header, None).await;
    }
}
