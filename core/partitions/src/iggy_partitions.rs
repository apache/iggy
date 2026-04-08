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
use consensus::{Consensus, Plane, PlaneIdentity, VsrConsensus};
use iggy_binary_protocol::{
    Command2, ConsensusHeader, GenericHeader, Message, PrepareHeader, PrepareOkHeader,
    RequestHeader,
};
use iggy_common::sharding::{IggyNamespace, LocalIdx, ShardId};
use message_bus::MessageBus;
use std::cell::UnsafeCell;
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
    /// Collection of partitions, the index of each partition isn't it's ID, but rather a local index (`LocalIdx`) which is used for lookups.
    ///
    /// Wrapped in `UnsafeCell` for interior mutability — matches the single-threaded
    /// per-shard execution model. Consensus trait methods take `&self` but need to
    /// mutate partition state (segments, offsets, journal).
    partitions: UnsafeCell<Vec<IggyPartition<B>>>,
    namespace_to_local: HashMap<IggyNamespace, LocalIdx>,
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
            namespace_to_local: HashMap::new(),
        }
    }

    #[must_use]
    pub fn with_capacity(shard_id: ShardId, config: PartitionsConfig, capacity: usize) -> Self {
        Self {
            shard_id,
            config,
            partitions: UnsafeCell::new(Vec::with_capacity(capacity)),
            namespace_to_local: HashMap::with_capacity(capacity),
        }
    }

    pub const fn config(&self) -> &PartitionsConfig {
        &self.config
    }

    fn partitions(&self) -> &Vec<IggyPartition<B>> {
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
        self.namespace_to_local.get(namespace).copied()
    }

    /// Insert a new partition and return its local index.
    pub fn insert(&mut self, namespace: IggyNamespace, partition: IggyPartition<B>) -> LocalIdx {
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
    pub fn get_by_ns(&self, namespace: &IggyNamespace) -> Option<&IggyPartition<B>> {
        let idx = self.namespace_to_local.get(namespace)?;
        self.partitions().get(**idx)
    }

    /// Get mutable partition by namespace directly.
    #[allow(clippy::mut_from_ref)]
    pub fn get_mut_by_ns(&self, namespace: &IggyNamespace) -> Option<&mut IggyPartition<B>> {
        let idx = self.namespace_to_local.get(namespace)?;
        // Safety: single-threaded per-shard model, no concurrent access.
        unsafe { (&mut *self.partitions.get()).get_mut(**idx) }
    }

    /// Remove a partition by namespace. Returns the removed partition if found.
    pub fn remove(&mut self, namespace: &IggyNamespace) -> Option<IggyPartition<B>> {
        let local_idx = self.namespace_to_local.remove(namespace)?;
        let idx = *local_idx;
        let partitions = self.partitions.get_mut();

        if idx >= partitions.len() {
            return None;
        }

        let partition = partitions.swap_remove(idx);

        if idx < partitions.len() {
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
    pub fn remove_many(&mut self, namespaces: &[IggyNamespace]) -> Vec<IggyPartition<B>> {
        namespaces.iter().filter_map(|ns| self.remove(ns)).collect()
    }

    /// Iterate over all namespaces owned by this shard.
    pub fn namespaces(&self) -> impl Iterator<Item = &IggyNamespace> {
        self.namespace_to_local.keys()
    }

    /// Get partition by namespace, initializing if not present.
    pub fn get_or_init<F>(&mut self, namespace: IggyNamespace, init: F) -> &mut IggyPartition<B>
    where
        F: FnOnce() -> IggyPartition<B>,
    {
        if !self.namespace_to_local.contains_key(&namespace) {
            self.insert(namespace, init());
        }
        let idx = *self.namespace_to_local[&namespace];
        &mut self.partitions.get_mut()[idx]
    }
}

impl<B> Plane<VsrConsensus<B>> for IggyPartitions<B>
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
{
    async fn on_request(&self, message: <VsrConsensus<B> as Consensus>::Message<RequestHeader>) {
        let namespace = IggyNamespace::from_raw(message.header().namespace);
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
