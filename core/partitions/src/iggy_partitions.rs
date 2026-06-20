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
use crate::types::PartitionsConfig;
use ahash::AHashSet;
use consensus::{Consensus, Plane, PlaneIdentity, VsrConsensus};
use iggy_binary_protocol::{
    Command2, ConsensusHeader, PrepareHeader, PrepareOkHeader, RequestHeader,
};
use message_bus::MessageBus;
use server_common::sharding::{IggyNamespace, LocalIdx, ShardId};
use std::cell::{RefCell, UnsafeCell};
use std::collections::HashMap;
use tracing::warn;

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
    namespace_to_local: UnsafeCell<HashMap<IggyNamespace, LocalIdx>>,
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
            namespace_to_local: UnsafeCell::new(HashMap::new()),
            tombstoned: RefCell::new(AHashSet::new()),
        }
    }

    #[must_use]
    pub fn with_capacity(shard_id: ShardId, config: PartitionsConfig, capacity: usize) -> Self {
        Self {
            shard_id,
            config,
            partitions: UnsafeCell::new(Vec::with_capacity(capacity)),
            namespace_to_local: UnsafeCell::new(HashMap::with_capacity(capacity)),
            tombstoned: RefCell::new(AHashSet::new()),
        }
    }

    pub const fn config(&self) -> &PartitionsConfig {
        &self.config
    }

    fn partitions(&self) -> &Vec<IggyPartition<B>> {
        // Safety: single-threaded per-shard model, no concurrent access.
        unsafe { &*self.partitions.get() }
    }

    fn namespace_map(&self) -> &HashMap<IggyNamespace, LocalIdx> {
        // Safety: single-threaded per-shard model, no concurrent access.
        unsafe { &*self.namespace_to_local.get() }
    }

    #[allow(clippy::mut_from_ref)]
    fn namespace_map_mut(&self) -> &mut HashMap<IggyNamespace, LocalIdx> {
        // Safety: single-threaded per-shard model, no concurrent access.
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
        // Safety: single-threaded per-shard model, no concurrent access.
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
    #[doc(hidden)]
    pub fn insert(&self, namespace: IggyNamespace, partition: IggyPartition<B>) -> LocalIdx {
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
    /// [`Plane`] (view-change handlers, `tick_partitions`, loopback drain)
    /// can't drive journal writes against a partition the reconciler has
    /// already fenced for delete.
    pub fn get_by_ns(&self, namespace: &IggyNamespace) -> Option<&IggyPartition<B>> {
        if self.is_tombstoned(namespace) {
            return None;
        }
        let idx = self.namespace_map().get(namespace)?;
        self.partitions().get(**idx)
    }

    /// Get mutable partition by namespace directly. Tombstone-gated like
    /// [`Self::get_by_ns`].
    #[allow(clippy::mut_from_ref)]
    pub fn get_mut_by_ns(&self, namespace: &IggyNamespace) -> Option<&mut IggyPartition<B>> {
        if self.is_tombstoned(namespace) {
            return None;
        }
        let idx = self.namespace_map().get(namespace)?;
        // Safety: single-threaded per-shard model, no concurrent access.
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
    #[doc(hidden)]
    pub fn remove(&self, namespace: &IggyNamespace) -> Option<IggyPartition<B>> {
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
