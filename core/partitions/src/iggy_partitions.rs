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

use crate::poll_plan::PollPlan;
use crate::types::PartitionsConfig;
use crate::{IggyPartition, Partition, PollingArgs, PollingConsumer};
use ahash::AHashSet;
use consensus::{Consensus, Plane, PlaneIdentity, VsrConsensus};
use iggy_binary_protocol::{
    Command2, ConsensusHeader, Operation, PrepareHeader, PrepareOkHeader, RequestHeader,
};
use message_bus::MessageBus;
use server_common::send_messages2::{convert_request_message, encrypt_batch_request};
use server_common::sharding::{IggyNamespace, LocalIdx, ShardId};
#[cfg(debug_assertions)]
use std::cell::Cell;
use std::cell::{RefCell, UnsafeCell};
use std::collections::BTreeMap;
use tracing::warn;

/// RAII counter for live [`IggyPartitions::with_partition`] borrows. The
/// decrement runs on `Drop`, so a panic inside the closure still restores the
/// count instead of poisoning the `insert` / `remove` tripwire for the rest of
/// the process.
#[cfg(debug_assertions)]
struct BorrowGuard<'a>(&'a Cell<u32>);

#[cfg(debug_assertions)]
impl<'a> BorrowGuard<'a> {
    fn new(cell: &'a Cell<u32>) -> Self {
        cell.set(cell.get() + 1);
        Self(cell)
    }
}

#[cfg(debug_assertions)]
impl Drop for BorrowGuard<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get() - 1);
    }
}

/// Per-shard collection of all partitions.
///
/// This struct manages ALL partitions assigned to a single shard, regardless
/// of which stream/topic they belong to.
///
/// Note: The `partition_id` within `IggyNamespace` may NOT equal the Vec index.
/// For example, shard 0 might have `partition_ids` [0, 2, 4] while shard 1
/// has `partition_ids` [1, 3, 5]. The `LocalIdx` provides the actual index
/// into the `partitions` Vec.
pub struct IggyPartitions<B>
where
    B: MessageBus,
{
    shard_id: ShardId,
    config: PartitionsConfig,
    /// Index is `LocalIdx`, not `partition_id`.
    ///
    /// # Safety invariant
    ///
    /// Container-level mutation (`Vec::push` / `swap_remove`) must run
    /// only on the shard's pump task. Reconciler routes mutations
    /// through `ReconcileOp` + `ReconcileApply`. Cross-task
    /// access would be UB under cooperative `.await` interleaving.
    partitions: UnsafeCell<Vec<IggyPartition<B>>>,
    /// Same single-pump invariant as `partitions`.
    ///
    /// `BTreeMap`, not `HashMap`: iteration order via [`Self::namespaces`] must
    /// be a deterministic function of the key set. The shard pump fans out over
    /// it (`tick_partitions`, `process_loopback`), and the simulator hashes that
    /// fan-out order into its schedule; `HashMap`'s per-process `RandomState`
    /// would make the order, and the hash, non-reproducible across runs.
    ///
    /// Perf: this makes `get_by_ns` O(log n) rather than O(1). Not expected to
    /// bite, but revisit for high-partition-count shards if it shows up in
    /// profiling.
    namespace_to_local: UnsafeCell<BTreeMap<IggyNamespace, LocalIdx>>,
    /// Tombstone gate: reconciler sets it synchronously before awaiting
    /// disk delete; pump clears on `ConfirmRemove`. Pump's `Plane::on_*`
    /// short-circuits frames hitting a tombstoned namespace.
    ///
    /// `RefCell` (not `UnsafeCell`) because both the reconciler task and
    /// the pump task mutate it, so the single-pump invariant protecting
    /// `partitions` / `namespace_to_local` does NOT hold here. Compio's
    /// per-shard runtime is single-threaded, so runtime borrow checks
    /// suffice; callers must not hold a borrow across `.await`.
    tombstoned: RefCell<AHashSet<IggyNamespace>>,
    /// Debug-only tripwire: counts live [`Self::with_partition`] borrows so
    /// `insert` / `remove` can assert the partitions vec is never mutated
    /// while a sanctioned non-pump read borrow is outstanding. Cannot fire for
    /// correct code: the closure is `FnOnce` (so it cannot span an `.await`)
    /// and [`BorrowGuard`] decrements on unwind, so a panic inside it does not
    /// leave the count stuck.
    #[cfg(debug_assertions)]
    borrow_active: Cell<u32>,
}

impl<B> IggyPartitions<B>
where
    B: MessageBus,
{
    #[must_use]
    pub fn new(shard_id: ShardId, config: PartitionsConfig) -> Self {
        Self {
            shard_id,
            config,
            partitions: UnsafeCell::new(Vec::new()),
            namespace_to_local: UnsafeCell::new(BTreeMap::new()),
            tombstoned: RefCell::new(AHashSet::new()),
            #[cfg(debug_assertions)]
            borrow_active: Cell::new(0),
        }
    }

    #[must_use]
    pub fn with_capacity(shard_id: ShardId, config: PartitionsConfig, capacity: usize) -> Self {
        Self {
            shard_id,
            config,
            partitions: UnsafeCell::new(Vec::with_capacity(capacity)),
            // BTreeMap has no capacity hint; the Vec above absorbs the sizing.
            namespace_to_local: UnsafeCell::new(BTreeMap::new()),
            tombstoned: RefCell::new(AHashSet::new()),
            #[cfg(debug_assertions)]
            borrow_active: Cell::new(0),
        }
    }

    pub const fn config(&self) -> &PartitionsConfig {
        &self.config
    }

    fn partitions(&self) -> &Vec<IggyPartition<B>> {
        // SAFETY: see the `partitions` field doc. The returned `&` is sound only
        // while not held across an `.await` on a non-pump task (a sibling
        // reconcile could realloc); single-threadedness alone is not enough.
        unsafe { &*self.partitions.get() }
    }

    fn namespace_map(&self) -> &BTreeMap<IggyNamespace, LocalIdx> {
        // SAFETY: shared read, same borrow rule as `partitions` above.
        unsafe { &*self.namespace_to_local.get() }
    }

    #[allow(clippy::mut_from_ref)]
    fn namespace_map_mut(&self) -> &mut BTreeMap<IggyNamespace, LocalIdx> {
        // SAFETY: `&mut` is sound because map mutation runs only on the pump
        // task, the sole mutator; single-threadedness alone is not enough.
        unsafe { &mut *self.namespace_to_local.get() }
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
    pub fn get(&self, local_idx: LocalIdx) -> Option<&IggyPartition<B>> {
        self.partitions().get(*local_idx)
    }

    /// Get mutable partition by local index.
    #[allow(clippy::mut_from_ref)]
    fn get_mut(&self, local_idx: LocalIdx) -> Option<&mut IggyPartition<B>> {
        // SAFETY: `&mut` is sound on the pump task only (the sole mutator); see
        // `namespace_map_mut`. Single-threadedness alone is not enough.
        unsafe { (&mut *self.partitions.get()).get_mut(*local_idx) }
    }

    /// Lookup local index by namespace.
    pub fn local_idx(&self, namespace: &IggyNamespace) -> Option<LocalIdx> {
        self.namespace_map().get(namespace).copied()
    }

    /// Insert a new partition and return its local index.
    ///
    /// # Safety discipline (compiler cannot enforce)
    ///
    /// Must only be called from the shard's pump task (i.e. inside
    /// `IggyShard::apply_reconcile_ops`). `Vec::push` may reallocate +
    /// invalidate any live `&mut IggyPartition` returned by
    /// [`Self::get_mut_by_ns`] / [`Self::get_mut`] held by a sibling
    /// task across an `.await`. New external call sites MUST route
    /// through `ReconcileOp::InsertOwned` instead.
    ///
    /// The debug tripwire tracks the `&` borrows handed out by
    /// [`Self::with_partition`]; the `&mut` path above is uncounted (it is
    /// pump-only, so it cannot alias this same-task mutation).
    #[doc(hidden)]
    pub fn insert(&self, namespace: IggyNamespace, partition: IggyPartition<B>) -> LocalIdx {
        #[cfg(debug_assertions)]
        debug_assert_eq!(
            self.borrow_active.get(),
            0,
            "IggyPartitions::insert while a with_partition borrow is live"
        );
        // Safety: pump-only invariant, caller responsibility.
        let partitions = unsafe { &mut *self.partitions.get() };
        let local_idx = LocalIdx::new(partitions.len());
        partitions.push(partition);
        self.namespace_map_mut().insert(namespace, local_idx);
        local_idx
    }

    /// Check if a namespace exists.
    pub fn contains(&self, namespace: &IggyNamespace) -> bool {
        self.namespace_map().contains_key(namespace)
    }

    /// Get partition by namespace directly.
    ///
    /// Returns `None` for tombstoned namespaces so callers outside
    /// [`Plane`] (view-change handlers, loopback drain, `tick_partitions`)
    /// can't drive journal writes against a partition the reconciler has
    /// already fenced for delete.
    ///
    /// A pump-task caller MAY hold the returned reference across an `.await`:
    /// the vec mutators (`apply_reconcile_ops`) run on the same task and so
    /// cannot interleave and reallocate while the pump is suspended.
    /// `tick_partitions` is such a caller (it holds the partition across the
    /// VSR action dispatch await) and still relies on the tombstone gate above.
    /// A non-pump caller must instead use [`Self::with_partition`], which scopes
    /// the borrow to a synchronous closure, and must never hold the reference
    /// across an `.await` (a sibling task's reconcile could reallocate the vec
    /// mid-await).
    pub fn get_by_ns(&self, namespace: &IggyNamespace) -> Option<&IggyPartition<B>> {
        if self.is_tombstoned(namespace) {
            return None;
        }
        let idx = self.namespace_map().get(namespace)?;
        self.partitions().get(**idx)
    }

    /// Run `f` against the partition for `namespace`, returning its result.
    ///
    /// Sanctioned non-pump read path: unlike [`Self::get_by_ns`], the borrow
    /// cannot escape `f`, so it cannot be held across an `.await` while the
    /// pump task mutates the partitions vec. Returns `None` for a missing or
    /// tombstoned namespace.
    pub fn with_partition<R>(
        &self,
        namespace: &IggyNamespace,
        f: impl FnOnce(&IggyPartition<B>) -> R,
    ) -> Option<R> {
        let partition = self.get_by_ns(namespace)?;
        #[cfg(debug_assertions)]
        let _guard = BorrowGuard::new(&self.borrow_active);
        Some(f(partition))
    }

    /// TEST / SIMULATOR ONLY, DELIBERATELY UNSOUND. Acquire a
    /// [`Self::with_partition`]-style borrow and hold it across `suspend.await`
    /// -- the exact borrow-across-`.await` anti-pattern PR #3557 closed and
    /// which `with_partition`'s `FnOnce` bound structurally forbids.
    ///
    /// Exists only so the simulator's deterministic dispatch shell can prove
    /// its detector for that async-concurrency class: parked here (borrow
    /// live), a concurrent [`Self::insert`] / [`Self::remove`] on a sibling
    /// task trips the debug borrow tripwire, exactly as a real reconcile
    /// reallocating the vec under a stale read would. The `simulator` feature
    /// / `cfg(test)` gate excludes it from every production build.
    #[cfg(any(test, feature = "simulator"))]
    pub async fn hold_borrow_across_await(&self, suspend: impl std::future::Future<Output = ()>) {
        // The guard IS the borrow the tripwire counts; holding it across the
        // await models a partition reference outliving a suspension point.
        #[cfg(debug_assertions)]
        let _guard = BorrowGuard::new(&self.borrow_active);
        suspend.await;
    }

    /// Get mutable partition by namespace directly. Tombstone-gated like
    /// [`Self::get_by_ns`].
    #[allow(clippy::mut_from_ref)]
    pub fn get_mut_by_ns(&self, namespace: &IggyNamespace) -> Option<&mut IggyPartition<B>> {
        if self.is_tombstoned(namespace) {
            return None;
        }
        let idx = self.namespace_map().get(namespace)?;
        // SAFETY: `&mut` is sound on the pump task only (the sole mutator); see
        // `namespace_map_mut`. Single-threadedness alone is not enough.
        unsafe { (&mut *self.partitions.get()).get_mut(**idx) }
    }

    /// Remove a partition by namespace.
    ///
    /// # Safety discipline (compiler cannot enforce)
    ///
    /// Must only be called from the shard's pump task. `Vec::swap_remove`
    /// invalidates any live `&mut IggyPartition` returned by
    /// [`Self::get_mut_by_ns`] / [`Self::get_mut`] held by a sibling
    /// task across an `.await`. New external call sites MUST route
    /// through `ReconcileOp::ConfirmRemove` instead.
    ///
    /// # Panics
    ///
    /// Panics if the stored `LocalIdx` is past `partitions.len()`, an
    /// invariant violation. Silent `None` would leave the map half-mutated
    /// and prime the next `insert` for a colliding index.
    ///
    /// The debug tripwire tracks the `&` borrows handed out by
    /// [`Self::with_partition`]; the `&mut` path above is uncounted (it is
    /// pump-only, so it cannot alias this same-task mutation).
    #[doc(hidden)]
    pub fn remove(&self, namespace: &IggyNamespace) -> Option<IggyPartition<B>> {
        #[cfg(debug_assertions)]
        debug_assert_eq!(
            self.borrow_active.get(),
            0,
            "IggyPartitions::remove while a with_partition borrow is live"
        );
        let local_idx = self.namespace_map_mut().remove(namespace)?;
        let idx = *local_idx;
        let partitions = unsafe { &mut *self.partitions.get() };

        assert!(
            idx < partitions.len(),
            "IggyPartitions invariant: LocalIdx({idx}) >= len {len}",
            idx = idx,
            len = partitions.len(),
        );

        let partition = partitions.swap_remove(idx);

        if idx < partitions.len() {
            // `swap_remove` moved the tail entry into `idx`. Update the map
            // by the moved partition's namespace key in O(1); the previous
            // linear value-scan turned bulk DeleteStream into O(K²) on the
            // pump task, stalling client traffic for ~10k-partition topics.
            let moved_ns = IggyNamespace::from_raw(partitions[idx].consensus().namespace());
            let entry = self.namespace_map_mut().get_mut(&moved_ns).expect(
                "IggyPartitions invariant: swapped-in partition missing namespace_to_local entry",
            );
            *entry = LocalIdx::new(idx);
        }

        Some(partition)
    }

    /// Remove multiple partitions at once.
    ///
    /// Same pump-only safety discipline as [`Self::remove`].
    #[doc(hidden)]
    pub fn remove_many(&self, namespaces: &[IggyNamespace]) -> Vec<IggyPartition<B>> {
        namespaces.iter().filter_map(|ns| self.remove(ns)).collect()
    }

    /// Iterate over all namespaces owned by this shard.
    pub fn namespaces(&self) -> impl Iterator<Item = &IggyNamespace> {
        self.namespace_map().keys()
    }

    pub fn is_tombstoned(&self, namespace: &IggyNamespace) -> bool {
        self.tombstoned.borrow().contains(namespace)
    }

    /// Mark a namespace as tombstoned. Callable from any task on the
    /// shard's runtime (reconciler sets the fence synchronously before
    /// awaiting disk delete).
    pub fn tombstone(&self, namespace: IggyNamespace) {
        self.tombstoned.borrow_mut().insert(namespace);
    }

    /// Clear a namespace tombstone. Pump-side hook called from
    /// `ReconcileOp::ConfirmRemove` after the partition is dropped.
    pub fn untombstone(&self, namespace: &IggyNamespace) {
        self.tombstoned.borrow_mut().remove(namespace);
    }

    /// Build an owned [`PollPlan`] for a partition poll synchronously, under a
    /// single [`Self::with_partition`] borrow (the in-memory journal tier + the
    /// resident-tail straddle snapshot are read here; mem reads never yield).
    /// Returns `None` for a missing or tombstoned namespace.
    ///
    /// Pairs with [`PollPlan::execute`], which runs the disk read +
    /// offset persist/apply off the borrow on the owned plan. Splitting the
    /// borrow-bound plan build from the borrow-free execution is what keeps
    /// poll-read sound: the only partition reference is taken here, on the pump,
    /// sequential with the pump's own `&mut` mutations, never on a sibling task.
    pub fn build_poll_snapshot(
        &self,
        namespace: &IggyNamespace,
        consumer: PollingConsumer,
        args: &PollingArgs,
    ) -> Option<PollPlan> {
        self.with_partition(namespace, |partition| {
            partition.build_poll_plan(consumer, args)
        })
    }

    /// Read a consumer's stored offset + the partition commit offset. Fully
    /// synchronous (atomics + lock-free maps), so it runs under a single
    /// [`Self::with_partition`] borrow. `None` for a missing/tombstoned
    /// namespace.
    pub fn consumer_offset_read(
        &self,
        namespace: &IggyNamespace,
        consumer: PollingConsumer,
    ) -> Option<(Option<u64>, u64)> {
        self.with_partition(namespace, |partition| {
            (
                partition.get_consumer_offset(consumer),
                partition.offsets().commit_offset,
            )
        })
    }

    /// Cooperative-rebalance: a group's `(last_polled, committed)` offsets on the
    /// partition for `namespace`. Synchronous (lock-free maps), under a single
    /// [`Self::with_partition`] borrow. `None` for a missing/tombstoned namespace.
    pub fn group_offset_state(
        &self,
        namespace: &IggyNamespace,
        group_id: u64,
    ) -> Option<(Option<u64>, Option<u64>)> {
        self.with_partition(namespace, |partition| {
            partition.group_offset_state(group_id)
        })
    }

    /// Drop a group's ephemeral `last_polled` mark on the partition for
    /// `namespace`. `None` for a missing/tombstoned namespace.
    pub fn clear_group_last_polled(&self, namespace: &IggyNamespace, group_id: u64) -> Option<()> {
        self.with_partition(namespace, |partition| {
            partition.clear_group_last_polled(group_id);
        })
    }

    /// Resolve a `DeleteSegments` count to the `end_offset` of the `count`-th
    /// oldest sealed segment on the partition for `namespace`. The outer `None`
    /// is a missing/tombstoned namespace; the inner `None` is a partition with
    /// no deletable sealed segment.
    pub fn nth_oldest_sealed_end_offset(
        &self,
        namespace: &IggyNamespace,
        count: u32,
    ) -> Option<Option<u64>> {
        self.with_partition(namespace, |partition| {
            partition.nth_oldest_sealed_end_offset(count)
        })
    }

    /// [`Self::nth_oldest_sealed_end_offset`] plus whether this replica is
    /// still behind the replicated log, read in one partition access so the
    /// pair is consistent. "Nothing sealed to delete" is settled on a
    /// converged replica (a committed-but-unflushed resident tail is normal
    /// under a large `messages_required_to_save` and must ack as a no-op),
    /// but transient on a lagging one (a backup that has not learned the
    /// commit frontier may be missing whole sealed segments).
    pub fn segment_delete_resolution(
        &self,
        namespace: &IggyNamespace,
        count: u32,
    ) -> Option<(Option<u64>, bool)> {
        self.with_partition(namespace, |partition| {
            let consensus = partition.consensus();
            let lagging = consensus.is_follower()
                || !consensus.is_normal()
                || consensus.is_syncing()
                || consensus.commit_min() < consensus.commit_max();
            (partition.nth_oldest_sealed_end_offset(count), lagging)
        })
    }
}

impl<B> Plane<VsrConsensus<B>> for IggyPartitions<B>
where
    B: MessageBus,
{
    async fn on_request(&self, message: <VsrConsensus<B> as Consensus>::Message<RequestHeader>) {
        let namespace = IggyNamespace::from_raw(message.header().namespace);
        if self.is_tombstoned(&namespace) {
            warn!(
                target: "iggy.partitions.diag",
                namespace_raw = namespace.inner(),
                "dropping request: namespace tombstoned"
            );
            return;
        }
        // At-rest encryption happens HERE, once, before the op enters
        // consensus: canonicalize the wire form first so both the legacy and
        // v2 request encodings encrypt identically, then encrypt payload +
        // user headers per message. The ciphertext is what gets journaled,
        // replicated, checksummed, and persisted -- every replica stores
        // identical bytes -- and the poll reply is the single decrypt point.
        let message = if message.header().operation == Operation::SendMessages
            && let Some(encryptor) = &self.config().encryptor
        {
            let canonical = convert_request_message(namespace, message)
                .and_then(|message| encrypt_batch_request(message, encryptor));
            match canonical {
                Ok(message) => message,
                Err(error) => {
                    warn!(
                        target: "iggy.partitions.diag",
                        namespace_raw = namespace.inner(),
                        %error,
                        "dropping send_messages: failed to encrypt batch at ingestion"
                    );
                    return;
                }
            }
        } else {
            message
        };
        let Some(partition) = self.get_mut_by_ns(&namespace) else {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = namespace.inner(),
                operation = ?message.header().operation,
                "partition not initialized for namespace"
            );
            return;
        };
        partition.on_request(message).await;
    }

    async fn on_replicate(&self, message: <VsrConsensus<B> as Consensus>::Message<PrepareHeader>) {
        let namespace = IggyNamespace::from_raw(message.header().namespace);
        if self.is_tombstoned(&namespace) {
            warn!(
                target: "iggy.partitions.diag",
                namespace_raw = namespace.inner(),
                "dropping prepare: namespace tombstoned"
            );
            return;
        }
        let Some(partition) = self.get_mut_by_ns(&namespace) else {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = namespace.inner(),
                op = message.header().op,
                operation = ?message.header().operation,
                "partition not initialized for namespace"
            );
            return;
        };
        partition.on_replicate(message).await;
    }

    #[allow(clippy::too_many_lines)]
    async fn on_ack(&self, message: <VsrConsensus<B> as Consensus>::Message<PrepareOkHeader>) {
        let namespace = IggyNamespace::from_raw(message.header().namespace);
        if self.is_tombstoned(&namespace) {
            warn!(
                target: "iggy.partitions.diag",
                namespace_raw = namespace.inner(),
                "dropping prepare-ok: namespace tombstoned"
            );
            return;
        }
        let config = self.config.clone();
        let Some(partition) = self.get_mut_by_ns(&namespace) else {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = namespace.inner(),
                op = message.header().op,
                "partition not initialized for namespace"
            );
            return;
        };
        partition.on_ack(message, &config).await;
    }
}

impl<B> PlaneIdentity<VsrConsensus<B>> for IggyPartitions<B>
where
    B: MessageBus,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::MessageLookup;
    use bytes::Bytes;
    use consensus::LocalPipeline;
    use iggy_binary_protocol::Operation;
    use iggy_common::{IggyByteSize, PartitionStats};
    use journal::Journal as _;
    use message_bus::IggyMessageBus;
    use server_common::send_messages2::{
        IggyMessage2, IggyMessage2Header, IggyMessages2, PREPARE_SPLIT_POINT, SendMessages2Owned,
        stamp_prepare_for_persistence,
    };
    use server_common::{Message, iobuf::Frozen};
    use std::sync::Arc;

    const TEST_CLUSTER: u128 = 1;

    fn build_partition() -> IggyPartition<IggyMessageBus> {
        let namespace = IggyNamespace::new(1, 1, 0);
        let consensus = VsrConsensus::new(
            TEST_CLUSTER,
            0,
            1,
            namespace.inner(),
            IggyMessageBus::new(0),
            LocalPipeline::new(),
        );
        consensus.init();
        IggyPartition::with_in_memory_storage(
            Arc::new(PartitionStats::default()),
            consensus,
            IggyByteSize::from(1024 * 1024),
            false,
        )
    }

    /// One-message `SendMessages` journal entry stamped at `op` / `base_offset`.
    /// Reuses the production blob builder + checksum stamping so the entry
    /// decodes through `decode_prepare_slice` and indexes into `offset_to_op`,
    /// the map `candidate_start_op` and the contiguity guard read.
    fn build_send_messages_entry(
        namespace: IggyNamespace,
        op: u64,
        base_offset: u64,
    ) -> Frozen<4096> {
        let mut batch = IggyMessages2::with_capacity(1);
        batch.push(IggyMessage2 {
            header: IggyMessage2Header {
                payload_length: 8,
                ..Default::default()
            },
            payload: Bytes::from_static(b"abcdefgh"),
            user_headers: None,
        });
        let owned = SendMessages2Owned::from_messages(namespace, &batch)
            .expect("build send_messages batch");

        let total_size = PREPARE_SPLIT_POINT + owned.blob.len();
        let prepare = Message::<PrepareHeader>::new(total_size).transmute_header(
            |_, header: &mut PrepareHeader| {
                header.command = Command2::Prepare;
                header.operation = Operation::SendMessages;
                header.op = op;
                header.size = u32::try_from(total_size).expect("size fits u32");
            },
        );

        let mut prepare = prepare;
        {
            let bytes = prepare.as_mut_slice();
            owned
                .header
                .encode_into(&mut bytes[std::mem::size_of::<PrepareHeader>()..PREPARE_SPLIT_POINT]);
            bytes[PREPARE_SPLIT_POINT..PREPARE_SPLIT_POINT + owned.blob.len()]
                .copy_from_slice(&owned.blob);
        }

        let (prepare, _command, _count) = stamp_prepare_for_persistence(prepare, base_offset, 1)
            .expect("stamp prepare for persistence");
        prepare.into_frozen()
    }

    /// Resident-tail straddle continuation over the OWNED snapshot, equivalent
    /// to the removed `journal_get_contiguous`. Reproduces the disk->journal
    /// straddle gap: a commit between the plan snapshot and the splice persists
    /// offsets `D+1..R-1` and evicts them, leaving `oldest_resident > D+1`. The
    /// continuation must be refused there (so the poll returns disk-only and the
    /// next poll re-routes the evicted run to the flushed disk tier) instead of
    /// silently splicing the next resident op `R` over the gap.
    ///
    /// `count: 1` makes `last_matching_offset` the single matched offset, so the
    /// skip is provable without decoding fragment bytes: a lookup that asked for
    /// offset 1 returning offset 3 means 1 and 2 were skipped. This drives the
    /// exact `select_resident` walk + `oldest_resident <= from_offset` gate that
    /// `ResidentTailSnapshot::straddle_continuation` + `PollPlan::execute` apply.
    #[compio::test]
    async fn straddle_continuation_refuses_journal_when_oldest_resident_advanced_past_gap() {
        let namespace = IggyNamespace::new(1, 1, 0);
        let partition = build_partition();

        // Journal offsets 0..=4 (op N carries message offset N).
        for offset in 0..=4u64 {
            partition
                .log
                .journal()
                .inner
                .append(build_send_messages_entry(namespace, offset + 1, offset))
                .await
                .expect("append journal entry");
        }

        // Commit-time flush of the prefix: offsets 0,1,2 leave the journal,
        // so the oldest resident offset advances to 3 (the gap edge).
        let prefix = partition.log.journal().inner.committed_prefix(3);
        assert_eq!(prefix.len(), 3, "ops for offsets 0,1,2 are the prefix");
        partition
            .log
            .journal()
            .inner
            .evict_prefix(prefix.len())
            .await;

        // Snapshot the resident tail the way `build_poll_plan` does.
        let oldest_resident = partition.log.journal().inner.oldest_resident_offset();
        let entries = partition.log.journal().inner.resident_entries();
        assert_eq!(
            oldest_resident,
            Some(3),
            "offsets 0,1,2 evicted; 3 is the oldest resident",
        );

        // D = 0 (last disk match), so a straddle would continue at offset 1.
        // A raw snapshot walk from offset 1 falls through to the next resident
        // op (offset 3): exactly the silent skip the gate prevents.
        let (_, leaky_offset) = crate::journal::select_resident(
            &entries,
            MessageLookup::Offset {
                offset: 1,
                count: 1,
                ceiling: u64::MAX,
            },
        )
        .expect("raw snapshot walk still returns the post-gap batch");
        assert_eq!(
            leaky_offset,
            Some(3),
            "raw select_resident skips the evicted 1,2 and serves offset 3 (the hazard)",
        );

        // The contiguity gate refuses that continuation: oldest_resident (3) > 1.
        let from_offset = 1u64;
        let contiguous = oldest_resident.is_some_and(|oldest| oldest <= from_offset);
        assert!(
            !contiguous,
            "contiguity gate must not serve offset 1 over the eviction gap",
        );

        // A contiguous continuation (start == oldest_resident) passes the gate
        // and serves the resident tail.
        let from_offset = 3u64;
        let contiguous = oldest_resident.is_some_and(|oldest| oldest <= from_offset);
        assert!(contiguous, "offset 3 is resident; gate must allow it");
        let (_, contiguous_offset) = crate::journal::select_resident(
            &entries,
            MessageLookup::Offset {
                offset: from_offset,
                count: 1,
                ceiling: u64::MAX,
            },
        )
        .expect("offset 3 is resident, continuation must serve it");
        assert_eq!(
            contiguous_offset,
            Some(3),
            "contiguous continuation returns the resident tail starting at 3",
        );
    }

    /// The resident journal holds replicated-but-uncommitted prepares ahead of
    /// the commit frontier. A poll must clamp at `ceiling` (the commit offset)
    /// so it never returns a dirty read of view-change-rollbackable data, even
    /// when the requested count would otherwise reach into the uncommitted tail.
    #[compio::test]
    async fn poll_clamps_at_commit_ceiling() {
        let namespace = IggyNamespace::new(1, 1, 0);
        let partition = build_partition();

        // Offsets 0..=5 all resident; only 0..=2 are committed.
        for offset in 0..=5u64 {
            partition
                .log
                .journal()
                .inner
                .append(build_send_messages_entry(namespace, offset + 1, offset))
                .await
                .expect("append journal entry");
        }

        let entries = partition.log.journal().inner.resident_entries();
        let (_, last) = crate::journal::select_resident(
            &entries,
            MessageLookup::Offset {
                offset: 0,
                count: 10,
                ceiling: 2,
            },
        )
        .expect("offsets 0..=2 are within the ceiling and must be served");
        assert_eq!(
            last,
            Some(2),
            "poll must stop at the commit ceiling, never serving 3..=5",
        );

        // A poll starting past the ceiling returns nothing.
        assert!(
            crate::journal::select_resident(
                &entries,
                MessageLookup::Offset {
                    offset: 3,
                    count: 10,
                    ceiling: 2,
                },
            )
            .is_none(),
            "no committed message at or after offset 3, so the poll is empty",
        );
    }
}
