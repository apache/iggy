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
use crate::Partitions;
use crate::types::PartitionsConfig;
use consensus::{Consensus, Project, Sequencer, Status, VsrConsensus};
use iggy_common::{
    INDEX_SIZE, IggyByteSize, IggyIndexesMut, IggyMessagesBatchMut, PartitionStats, PooledBuffer,
    Segment, SegmentStorage,
    header::{Command2, GenericHeader, Operation, PrepareHeader, PrepareOkHeader, ReplyHeader},
    message::Message,
    sharding::{IggyNamespace, LocalIdx, ShardId},
};
use journal::Journal as _;
use message_bus::MessageBus;
use std::cell::UnsafeCell;
use std::collections::HashMap;
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
    /// Some on shard0, None on other shards
    pub consensus: Option<C>,
}

impl<C> IggyPartitions<C> {
    pub fn new(shard_id: ShardId, config: PartitionsConfig, consensus: Option<C>) -> Self {
        Self {
            shard_id,
            config,
            partitions: UnsafeCell::new(Vec::new()),
            namespace_to_local: HashMap::new(),
            consensus,
        }
    }

    pub fn with_capacity(
        shard_id: ShardId,
        config: PartitionsConfig,
        consensus: Option<C>,
        capacity: usize,
    ) -> Self {
        Self {
            shard_id,
            config,
            partitions: UnsafeCell::new(Vec::with_capacity(capacity)),
            namespace_to_local: HashMap::with_capacity(capacity),
            consensus,
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

    /// Initialize a new partition with in-memory storage (for testing/simulation).
    ///
    /// This is a simplified version that doesn't create file-backed storage.
    /// Use `init_partition()` for production use with real files.
    ///
    /// TODO: Make the log generic over its storage backend to support both
    /// in-memory (for testing) and file-backed (for production) storage without
    /// needing separate initialization methods.
    pub fn init_partition_in_memory(&mut self, namespace: IggyNamespace) -> LocalIdx {
        // Check if already initialized
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

        // Insert and return local index
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
    /// Idempotent - returns existing LocalIdx if partition already exists.
    pub async fn init_partition(&mut self, namespace: IggyNamespace) -> LocalIdx {
        // Check if already initialized
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

        // Insert and return local index
        self.insert(namespace, partition)
    }
}

impl<B> Partitions<VsrConsensus<B>> for IggyPartitions<VsrConsensus<B>>
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
{
    async fn on_request(&self, message: <VsrConsensus<B> as Consensus>::RequestMessage) {
        let consensus = self.consensus.as_ref().unwrap();

        debug!("handling partition request");
        let prepare = message.project(consensus);
        self.pipeline_prepare(prepare).await;
    }

    async fn on_replicate(&self, message: <VsrConsensus<B> as Consensus>::ReplicateMessage) {
        let consensus = self.consensus.as_ref().unwrap();

        let header = message.header();

        assert_eq!(header.command, Command2::Prepare);

        if !self.fence_old_prepare(&message) {
            self.replicate(message.clone()).await;
        } else {
            warn!("received old prepare, not replicating");
        }

        // If syncing, ignore the replicate message.
        if consensus.is_syncing() {
            warn!(
                replica = consensus.replica(),
                "on_replicate: ignoring (sync)"
            );
            return;
        }

        let current_op = consensus.sequencer().current_sequence();

        // If status is not normal, ignore the replicate.
        if consensus.status() != Status::Normal {
            warn!(
                replica = consensus.replica(),
                "on_replicate: ignoring (not normal state)"
            );
            return;
        }

        // If message from future view, we ignore the replicate.
        if header.view > consensus.view() {
            warn!(
                replica = consensus.replica(),
                "on_replicate: ignoring (newer view)"
            );
            return;
        }

        // If we are a follower, we advance the commit number.
        if consensus.is_follower() {
            consensus.advance_commit_number(message.header().commit);
        }

        // TODO: Make those assertions be toggleable through an feature flag, so they can be used only by simulator/tests.
        debug_assert_eq!(header.op, current_op + 1);
        consensus.sequencer().set_sequence(header.op);

        // TODO: Figure out the flow of the partition operations.
        // In metadata layer we assume that when an `on_request` or `on_replicate` is called, it's called from correct shard.
        // I think we need to do the same here, which means that the code from below is unfallable, the partition should always exist by now!
        let namespace = IggyNamespace::from_raw(header.namespace);
        match header.operation {
            Operation::SendMessages => {
                let body = message.body_bytes();
                let batch = Self::batch_from_body(&body);
                self.append_batch(&namespace, batch).await;
                debug!(
                    replica = consensus.replica(),
                    op = header.op,
                    ?namespace,
                    "on_replicate: batch appended to partition journal"
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

        // After successful journal write, send prepare_ok to primary.
        self.send_prepare_ok(header).await;

        // If follower, commit any newly committable entries.
        if consensus.is_follower() {
            self.commit_journal();
        }
    }

    async fn on_ack(&self, message: <VsrConsensus<B> as Consensus>::AckMessage) {
        let consensus = self.consensus.as_ref().unwrap();
        let header = message.header();

        if !consensus.is_primary() {
            warn!("on_ack: ignoring (not primary)");
            return;
        }

        if consensus.status() != Status::Normal {
            warn!("on_ack: ignoring (not normal)");
            return;
        }

        // Verify checksum by checking pipeline entry exists
        {
            let pipeline = consensus.pipeline().borrow();
            let Some(entry) =
                pipeline.message_by_op_and_checksum(header.op, header.prepare_checksum)
            else {
                debug!("on_ack: prepare not in pipeline op={}", header.op);
                return;
            };

            if entry.header.checksum != header.prepare_checksum {
                warn!("on_ack: checksum mismatch");
                return;
            }
        }

        // Let consensus handle the ack increment and quorum check
        if consensus.handle_prepare_ok(header) {
            debug!("on_ack: quorum received for op={}", header.op);
            consensus.advance_commit_number(header.op);

            // Extract the prepare message from the pipeline by op
            // TODO: Commit from the head. ALWAYS
            let entry = consensus.pipeline().borrow_mut().extract_by_op(header.op);
            let Some(entry) = entry else {
                warn!("on_ack: prepare not found in pipeline for op={}", header.op);
                return;
            };

            let prepare_header = entry.header;

            // Data was already appended to the partition journal during
            // on_replicate. Now that quorum is reached, update the partition's
            // current offset and check whether the journal needs flushing.
            let namespace = IggyNamespace::from_raw(prepare_header.namespace);

            match prepare_header.operation {
                Operation::SendMessages => {
                    self.commit_messages(&namespace).await;
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

            // TODO: Figure out better infra for this, its messy.
            let reply = Message::<ReplyHeader>::new(std::mem::size_of::<ReplyHeader>())
                .transmute_header(|_, new| {
                    *new = ReplyHeader {
                        checksum: 0,
                        checksum_body: 0,
                        cluster: consensus.cluster(),
                        size: std::mem::size_of::<ReplyHeader>() as u32,
                        view: consensus.view(),
                        release: 0,
                        command: Command2::Reply,
                        replica: consensus.replica(),
                        reserved_frame: [0; 66],
                        request_checksum: prepare_header.request_checksum,
                        context: 0,
                        op: prepare_header.op,
                        commit: consensus.commit(),
                        timestamp: prepare_header.timestamp,
                        request: prepare_header.request,
                        operation: prepare_header.operation,
                        ..Default::default()
                    };
                });

            // Send reply to client
            let generic_reply = reply.into_generic();
            debug!(
                "on_ack: sending reply to client={} for op={}",
                prepare_header.client, prepare_header.op
            );

            // TODO: Error handling
            consensus
                .message_bus()
                .send_to_client(prepare_header.client, generic_reply)
                .await
                .unwrap()
        }
    }
}

impl<B> IggyPartitions<VsrConsensus<B>>
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
{
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

    /// Append a batch to a partition's journal with offset assignment.
    ///
    /// Updates `segment.current_position` (logical position for indexing) but
    /// not `segment.end_offset` or `segment.end_timestamp` (committed state).
    /// Those are updated during commit.
    ///
    /// Uses `dirty_offset` for offset assignment so that multiple prepares
    /// can be pipelined before any commit.
    async fn append_batch(&self, namespace: &IggyNamespace, mut batch: IggyMessagesBatchMut) {
        let partition = self
            .get_mut_by_ns(namespace)
            .expect("append_batch: partition not found for namespace");

        if batch.count() == 0 {
            return;
        }

        let dirty_offset = if partition.should_increment_offset {
            partition.dirty_offset.load(Ordering::Relaxed) + 1
        } else {
            0
        };

        let segment = partition.log.active_segment();
        let segment_start_offset = segment.start_offset;
        let current_position = segment.current_position;

        batch
            .prepare_for_persistence(segment_start_offset, dirty_offset, current_position, None)
            .await;

        let batch_messages_count = batch.count();
        let batch_messages_size = batch.size();

        // Advance dirty offset (committed offset is advanced in on_ack).
        let last_dirty_offset = if batch_messages_count == 0 {
            dirty_offset
        } else {
            dirty_offset + batch_messages_count as u64 - 1
        };

        if partition.should_increment_offset {
            partition
                .dirty_offset
                .store(last_dirty_offset, Ordering::Relaxed);
        } else {
            partition.should_increment_offset = true;
            partition
                .dirty_offset
                .store(last_dirty_offset, Ordering::Relaxed);
        }

        // Update segment.current_position for next prepare_for_persistence call.
        // This is the logical position (includes unflushed journal data).
        // segment.size is only updated after actual persist (in persist_frozen_batches_to_disk).
        let segment_index = partition.log.segments().len() - 1;
        partition.log.segments_mut()[segment_index].current_position += batch_messages_size;

        // Update journal tracking metadata.
        let journal = partition.log.journal_mut();
        journal.info.messages_count += batch_messages_count;
        journal.info.size += IggyByteSize::from(batch_messages_size as u64);
        journal.info.current_offset = last_dirty_offset;
        if let Some(ts) = batch.first_timestamp()
            && journal.info.first_timestamp == 0
        {
            journal.info.first_timestamp = ts;
        }
        if let Some(ts) = batch.last_timestamp() {
            journal.info.end_timestamp = ts;
        }

        journal.inner.append(batch).await;
    }

    async fn pipeline_prepare(&self, prepare: Message<PrepareHeader>) {
        let consensus = self.consensus.as_ref().unwrap();

        debug!("inserting prepare into partition pipeline");
        consensus.verify_pipeline();
        consensus.pipeline_message(prepare.clone());

        self.on_replicate(prepare.clone()).await;
        consensus.post_replicate_verify(&prepare);
    }

    fn fence_old_prepare(&self, prepare: &Message<PrepareHeader>) -> bool {
        let consensus = self.consensus.as_ref().unwrap();

        let header = prepare.header();
        // TODO: Check per-partition journal once namespace extraction is possible.
        // For now, only check if the op is already committed.
        header.op <= consensus.commit()
    }

    /// Replicate a prepare message to the next replica in the chain.
    ///
    /// Chain replication pattern:
    /// - Primary sends to first backup
    /// - Each backup forwards to the next
    /// - Stops when we would forward back to primary
    async fn replicate(&self, message: Message<PrepareHeader>) {
        let consensus = self.consensus.as_ref().unwrap();

        let header = message.header();

        assert_eq!(header.command, Command2::Prepare);
        assert!(header.op > consensus.commit());

        let next = (consensus.replica() + 1) % consensus.replica_count();

        let primary = consensus.primary_index(header.view);
        if next == primary {
            debug!(
                replica = consensus.replica(),
                op = header.op,
                "replicate: not replicating (ring complete)"
            );
            return;
        }

        assert_ne!(next, consensus.replica());

        debug!(
            replica = consensus.replica(),
            to = next,
            op = header.op,
            "replicate: forwarding"
        );

        let message = message.into_generic();
        consensus
            .message_bus()
            .send_to_replica(next, message)
            .await
            .unwrap();
    }

    /// Verify hash chain would not break if we add this header.
    fn panic_if_hash_chain_would_break_in_same_view(
        &self,
        previous: &PrepareHeader,
        current: &PrepareHeader,
    ) {
        // If both headers are in the same view, parent must chain correctly
        if previous.view == current.view {
            assert_eq!(
                current.parent, previous.checksum,
                "hash chain broken in same view: op={} parent={} expected={}",
                current.op, current.parent, previous.checksum
            );
        }
    }

    fn commit_journal(&self) {
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

    /// Send a prepare_ok message to the primary.
    /// Called after successfully writing a prepare to the journal.
    async fn send_prepare_ok(&self, header: &PrepareHeader) {
        let consensus = self.consensus.as_ref().unwrap();

        assert_eq!(header.command, Command2::Prepare);

        if consensus.status() != Status::Normal {
            debug!(
                replica = consensus.replica(),
                status = ?consensus.status(),
                "send_prepare_ok: not sending (not normal)"
            );
            return;
        }

        if consensus.is_syncing() {
            debug!(
                replica = consensus.replica(),
                "send_prepare_ok: not sending (syncing)"
            );
            return;
        }

        // TODO: Verify the prepare is persisted in the partition journal.
        // The partition journal uses MessageLookup headers, so we cannot
        // check by PrepareHeader.op directly. For now, skip this check.

        assert!(
            header.view <= consensus.view(),
            "send_prepare_ok: prepare view {} > our view {}",
            header.view,
            consensus.view()
        );

        if header.op > consensus.sequencer().current_sequence() {
            debug!(
                replica = consensus.replica(),
                op = header.op,
                our_op = consensus.sequencer().current_sequence(),
                "send_prepare_ok: not sending (op ahead)"
            );
            return;
        }

        debug!(
            replica = consensus.replica(),
            op = header.op,
            checksum = header.checksum,
            "send_prepare_ok: sending"
        );

        // Use current view, not the prepare's view.
        let prepare_ok_header = PrepareOkHeader {
            command: Command2::PrepareOk,
            cluster: consensus.cluster(),
            replica: consensus.replica(),
            view: consensus.view(),
            op: header.op,
            commit: consensus.commit(),
            timestamp: header.timestamp,
            parent: header.parent,
            prepare_checksum: header.checksum,
            request: header.request,
            operation: header.operation,
            namespace: header.namespace,
            size: std::mem::size_of::<PrepareOkHeader>() as u32,
            ..Default::default()
        };

        let message: Message<PrepareOkHeader> =
            Message::<PrepareOkHeader>::new(std::mem::size_of::<PrepareOkHeader>())
                .transmute_header(|_, new| *new = prepare_ok_header);
        let generic_message = message.into_generic();
        let primary = consensus.primary_index(consensus.view());

        if primary == consensus.replica() {
            debug!(
                replica = consensus.replica(),
                "send_prepare_ok: loopback to self"
            );
            // TODO: Queue for self-processing or call handle_prepare_ok directly
            // TODO: This is temporal, to test simulator, but we should send message to ourselves properly.
            consensus
                .message_bus()
                .send_to_replica(primary, generic_message)
                .await
                .unwrap();
        } else {
            debug!(
                replica = consensus.replica(),
                to = primary,
                op = header.op,
                "send_prepare_ok: sending to primary"
            );

            consensus
                .message_bus()
                .send_to_replica(primary, generic_message)
                .await
                .unwrap();
        }
    }
}
