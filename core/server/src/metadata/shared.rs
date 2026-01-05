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

use crate::metadata::{
    ConsumerGroupMember, ConsumerGroupMeta, MetadataSnapshot, PartitionMeta,
    PersonalAccessTokenMeta, StreamMeta, TopicMeta, UserMeta,
};
use arc_swap::{ArcSwap, Guard};
use iggy_common::sharding::IggyNamespace;
use iggy_common::{
    CompressionAlgorithm, Identifier, IggyError, IggyExpiry, IggyTimestamp, MaxTopicSize,
    Permissions, UserStatus,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

/// Shared metadata accessible by all shards.
///
/// Uses ArcSwap for lock-free reads and atomic updates.
/// Only shard 0 should call write methods (create/update/delete).
/// All shards can call read methods (get/exists/load).
pub struct SharedMetadata {
    inner: ArcSwap<MetadataSnapshot>,

    // Atomic ID generators (only shard 0 increments these)
    next_stream_id: AtomicUsize,
    next_user_id: AtomicU32,
}

impl Default for SharedMetadata {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedMetadata {
    pub fn new() -> Self {
        Self {
            inner: ArcSwap::from_pointee(MetadataSnapshot::new()),
            next_stream_id: AtomicUsize::new(0),
            next_user_id: AtomicU32::new(0),
        }
    }

    /// Initialize with existing state (used during server startup).
    pub fn init_from_snapshot(
        &self,
        snapshot: MetadataSnapshot,
        next_stream_id: usize,
        next_user_id: u32,
    ) {
        self.inner.store(Arc::new(snapshot));
        self.next_stream_id.store(next_stream_id, Ordering::SeqCst);
        self.next_user_id.store(next_user_id, Ordering::SeqCst);
    }

    // ==================== Read Operations (any shard) ====================

    /// Load the current snapshot. Lock-free read.
    pub fn load(&self) -> Guard<Arc<MetadataSnapshot>> {
        self.inner.load()
    }

    /// Get a clone of the current snapshot.
    pub fn snapshot(&self) -> Arc<MetadataSnapshot> {
        Arc::clone(&self.inner.load())
    }

    // Stream reads

    pub fn stream_exists(&self, id: &Identifier) -> bool {
        self.inner.load().stream_exists(id)
    }

    pub fn stream_exists_by_name(&self, name: &str) -> bool {
        self.inner.load().stream_exists_by_name(name)
    }

    pub fn get_stream(&self, id: &Identifier) -> Option<StreamMeta> {
        self.inner.load().get_stream(id).cloned()
    }

    pub fn get_stream_id(&self, id: &Identifier) -> Option<usize> {
        self.inner.load().get_stream_id(id)
    }

    pub fn get_streams(&self) -> Vec<StreamMeta> {
        self.inner.load().streams.values().cloned().collect()
    }

    // Partition reads (flat lookup)

    /// Check if a partition exists by namespace.
    pub fn partition_exists(&self, ns: &IggyNamespace) -> bool {
        self.inner.load().partition_exists(ns)
    }

    /// Get partition metadata by namespace.
    pub fn get_partition(&self, ns: &IggyNamespace) -> Option<PartitionMeta> {
        self.inner.load().get_partition(ns).cloned()
    }

    /// Get partition offset by stream/topic/partition IDs.
    pub fn get_partition_offset(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) -> Option<u64> {
        let snapshot = self.inner.load();
        let stream = snapshot.streams.get(&stream_id)?;
        let topic = stream.topics.get(&topic_id)?;
        let partition = topic.partitions.get(&partition_id)?;
        Some(partition.offset.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// Get partition IDs to delete (the last N partitions by ID).
    /// Returns partition IDs in descending order so highest IDs are deleted first.
    pub fn get_partition_ids_to_delete(
        &self,
        stream_id: usize,
        topic_id: usize,
        count: u32,
    ) -> Vec<usize> {
        let snapshot = self.inner.load();
        let Some(stream) = snapshot.streams.get(&stream_id) else {
            return Vec::new();
        };
        let Some(topic) = stream.topics.get(&topic_id) else {
            return Vec::new();
        };

        // Get all partition IDs and sort them descending
        let mut partition_ids: Vec<usize> = topic.partitions.keys().copied().collect();
        partition_ids.sort_by(|a, b| b.cmp(a)); // Descending order

        // Take the last N (highest IDs)
        partition_ids.truncate(count as usize);
        partition_ids
    }

    /// Get topic ID by name within a stream.
    pub fn get_topic_id(&self, stream_id: usize, topic_id: &Identifier) -> Option<usize> {
        self.inner.load().get_topic_id(stream_id, topic_id)
    }

    /// Zero out all partition offsets for a topic.
    /// Called during purge_topic to reset partition state across all shards.
    /// Since offset Arcs are shared via ArcSwap, zeroing them here affects all shards.
    pub fn zero_out_topic_partition_offsets(&self, stream_id: usize, topic_id: usize) {
        use std::sync::atomic::Ordering;
        let snapshot = self.inner.load();
        for (ns, partition_meta) in snapshot.partitions.iter() {
            if ns.stream_id() == stream_id && ns.topic_id() == topic_id {
                let old_val = partition_meta.offset.load(Ordering::Relaxed);
                partition_meta.offset.store(0, Ordering::Relaxed);
                partition_meta
                    .should_increment_offset
                    .store(false, Ordering::Relaxed);
                tracing::debug!(
                    "SharedMetadata: Zeroed partition offset ({},{},{}) from {} to 0",
                    stream_id,
                    topic_id,
                    ns.partition_id(),
                    old_val
                );
            }
        }
    }

    // User reads

    pub fn user_exists(&self, id: &Identifier) -> bool {
        self.inner.load().user_exists(id)
    }

    pub fn user_exists_by_name(&self, username: &str) -> bool {
        self.inner.load().user_exists_by_name(username)
    }

    pub fn get_user(&self, id: &Identifier) -> Option<UserMeta> {
        self.inner.load().get_user(id).cloned()
    }

    pub fn get_user_id(&self, id: &Identifier) -> Option<u32> {
        self.inner.load().get_user_id(id)
    }

    pub fn get_users(&self) -> Vec<UserMeta> {
        self.inner.load().users.values().cloned().collect()
    }

    pub fn get_user_by_username(&self, username: &str) -> Option<UserMeta> {
        self.inner.load().get_user_by_username(username).cloned()
    }

    // ==================== Write Operations (shard 0 only) ====================

    // Stream operations

    /// Add a stream with a specific ID (used by dual-write from shard 0).
    /// The ID should come from the actual slab allocation.
    pub fn add_stream(
        &self,
        id: usize,
        name: String,
        created_at: IggyTimestamp,
    ) -> Result<StreamMeta, IggyError> {
        let current = self.inner.load();

        if current.stream_exists_by_name(&name) {
            return Err(IggyError::StreamNameAlreadyExists(name));
        }

        let stream = StreamMeta::new(id, name.clone(), created_at);

        let mut new_snapshot = (**current).clone();
        new_snapshot.stream_index.insert(name, id);
        new_snapshot.streams.insert(id, stream.clone());

        self.inner.store(Arc::new(new_snapshot));

        Ok(stream)
    }

    /// Create a new stream with auto-generated ID.
    /// Primarily for tests - production code should use `add_stream` with the actual slab ID.
    pub fn create_stream(&self, name: String) -> Result<StreamMeta, IggyError> {
        let id = self.next_stream_id.fetch_add(1, Ordering::SeqCst);
        self.add_stream(id, name, IggyTimestamp::now())
    }

    /// Add a stream from persisted state (ID already assigned).
    /// Updates the next_stream_id counter to be greater than the loaded ID.
    pub fn add_stream_from_state(
        &self,
        id: usize,
        name: String,
        created_at: IggyTimestamp,
    ) -> Result<StreamMeta, IggyError> {
        // Update counter to be at least id + 1
        self.next_stream_id.fetch_max(id + 1, Ordering::SeqCst);
        self.add_stream(id, name, created_at)
    }

    /// Delete a stream. Returns the deleted stream metadata.
    pub fn delete_stream(&self, id: &Identifier) -> Result<StreamMeta, IggyError> {
        let current = self.inner.load();

        let stream_id = current
            .get_stream_id(id)
            .ok_or_else(|| IggyError::StreamIdNotFound(id.clone()))?;

        let mut new_snapshot = (**current).clone();

        // Remove all partitions for this stream from the flat map
        let partitions_to_remove: Vec<_> = new_snapshot
            .partitions
            .keys()
            .filter(|ns| ns.stream_id() == stream_id)
            .cloned()
            .collect();
        for ns in partitions_to_remove {
            new_snapshot.partitions.remove(&ns);
        }

        let stream = new_snapshot
            .streams
            .remove(&stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(id.clone()))?;
        new_snapshot.stream_index.remove(&stream.name);

        self.inner.store(Arc::new(new_snapshot));

        Ok(stream)
    }

    /// Update a stream's name.
    pub fn update_stream(&self, id: &Identifier, new_name: String) -> Result<(), IggyError> {
        let current = self.inner.load();

        let stream_id = current
            .get_stream_id(id)
            .ok_or_else(|| IggyError::StreamIdNotFound(id.clone()))?;

        let stream = current
            .streams
            .get(&stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(id.clone()))?;

        if stream.name == new_name {
            return Ok(());
        }

        if current.stream_exists_by_name(&new_name) {
            return Err(IggyError::StreamNameAlreadyExists(new_name));
        }

        let mut new_snapshot = (**current).clone();
        let old_name = new_snapshot.streams.get(&stream_id).unwrap().name.clone();
        new_snapshot.stream_index.remove(&old_name);
        new_snapshot
            .stream_index
            .insert(new_name.clone(), stream_id);
        new_snapshot.streams.get_mut(&stream_id).unwrap().name = new_name;

        self.inner.store(Arc::new(new_snapshot));

        Ok(())
    }

    // Topic operations

    /// Add a topic with a specific ID (used by dual-write from shard 0).
    /// The ID should come from the actual slab allocation.
    #[allow(clippy::too_many_arguments)]
    pub fn add_topic(
        &self,
        stream_id: &Identifier,
        topic_id: usize,
        name: String,
        created_at: IggyTimestamp,
        replication_factor: u8,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
    ) -> Result<TopicMeta, IggyError> {
        let current = self.inner.load();

        let stream_idx = current
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        let stream = current.streams.get(&stream_idx).unwrap();
        if stream.topic_exists(&name) {
            return Err(IggyError::TopicNameAlreadyExists(name, stream_id.clone()));
        }

        let topic = TopicMeta::new(
            topic_id,
            name.clone(),
            created_at,
            replication_factor,
            message_expiry,
            compression_algorithm,
            max_topic_size,
        );

        let mut new_snapshot = (**current).clone();
        new_snapshot
            .streams
            .get_mut(&stream_idx)
            .unwrap()
            .add_topic(topic.clone());

        self.inner.store(Arc::new(new_snapshot));

        Ok(topic)
    }

    /// Create a new topic with auto-generated ID.
    /// Primarily for tests - production code should use `add_topic` with the actual slab ID.
    #[allow(clippy::too_many_arguments)]
    pub fn create_topic(
        &self,
        stream_id: &Identifier,
        name: String,
        replication_factor: u8,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
    ) -> Result<TopicMeta, IggyError> {
        let current = self.inner.load();
        let stream_idx = current
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let stream = current.streams.get(&stream_idx).unwrap();
        let topic_id = stream.topics.len();

        self.add_topic(
            stream_id,
            topic_id,
            name,
            IggyTimestamp::now(),
            replication_factor,
            message_expiry,
            compression_algorithm,
            max_topic_size,
        )
    }

    /// Add a topic from persisted state (ID already assigned).
    #[allow(clippy::too_many_arguments)]
    pub fn add_topic_from_state(
        &self,
        stream_id: usize,
        topic_id: usize,
        name: String,
        compression_algorithm: CompressionAlgorithm,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
        created_at: IggyTimestamp,
    ) -> Result<TopicMeta, IggyError> {
        let stream_ident =
            Identifier::numeric(stream_id as u32).map_err(|_| IggyError::InvalidIdentifier)?;
        self.add_topic(
            &stream_ident,
            topic_id,
            name,
            created_at,
            replication_factor.unwrap_or(1),
            message_expiry,
            compression_algorithm,
            max_topic_size,
        )
    }

    /// Delete a topic from a stream.
    pub fn delete_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<TopicMeta, IggyError> {
        let current = self.inner.load();

        let stream_idx = current
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        let stream = current.streams.get(&stream_idx).unwrap();
        let topic_idx = match topic_id.kind {
            iggy_common::IdKind::Numeric => topic_id.get_u32_value().unwrap() as usize,
            iggy_common::IdKind::String => {
                let name = topic_id.get_string_value().unwrap();
                stream.get_topic_id_by_name(&name).ok_or_else(|| {
                    IggyError::TopicNameNotFound(name.to_string(), stream.name.clone())
                })?
            }
        };

        let mut new_snapshot = (**current).clone();

        // Remove all partitions for this topic from the flat map
        let partitions_to_remove: Vec<_> = new_snapshot
            .partitions
            .keys()
            .filter(|ns| ns.stream_id() == stream_idx && ns.topic_id() == topic_idx)
            .cloned()
            .collect();
        for ns in partitions_to_remove {
            new_snapshot.partitions.remove(&ns);
        }

        let topic = new_snapshot
            .streams
            .get_mut(&stream_idx)
            .unwrap()
            .remove_topic(topic_idx)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        self.inner.store(Arc::new(new_snapshot));

        Ok(topic)
    }

    /// Update topic configuration.
    #[allow(clippy::too_many_arguments)]
    pub fn update_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: Option<String>,
        message_expiry: Option<IggyExpiry>,
        compression_algorithm: Option<CompressionAlgorithm>,
        max_topic_size: Option<MaxTopicSize>,
        replication_factor: Option<u8>,
    ) -> Result<(), IggyError> {
        let current = self.inner.load();

        let stream_idx = current
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        let stream = current.streams.get(&stream_idx).unwrap();
        let stream_name = stream.name.clone();
        let topic_idx = match topic_id.kind {
            iggy_common::IdKind::Numeric => topic_id.get_u32_value().unwrap() as usize,
            iggy_common::IdKind::String => {
                let topic_name = topic_id.get_string_value().unwrap();
                stream.get_topic_id_by_name(&topic_name).ok_or_else(|| {
                    IggyError::TopicNameNotFound(topic_name.to_string(), stream_name.clone())
                })?
            }
        };

        let mut new_snapshot = (**current).clone();
        let stream_mut = new_snapshot.streams.get_mut(&stream_idx).unwrap();

        // Check name availability before getting mutable topic reference
        if let Some(ref new_name) = name {
            let old_name = stream_mut
                .topics
                .get(&topic_idx)
                .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?
                .name
                .clone();
            if *new_name != old_name && stream_mut.topic_exists(new_name) {
                return Err(IggyError::TopicNameAlreadyExists(
                    new_name.clone(),
                    stream_id.clone(),
                ));
            }
        }

        let topic = stream_mut
            .topics
            .get_mut(&topic_idx)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        if let Some(new_name) = name
            && topic.name != new_name
        {
            stream_mut.topic_index.remove(&topic.name);
            stream_mut.topic_index.insert(new_name.clone(), topic_idx);
            topic.name = new_name;
        }
        if let Some(expiry) = message_expiry {
            topic.message_expiry = expiry;
        }
        if let Some(compression) = compression_algorithm {
            topic.compression_algorithm = compression;
        }
        if let Some(size) = max_topic_size {
            topic.max_topic_size = size;
        }
        if let Some(factor) = replication_factor {
            topic.replication_factor = factor;
        }

        self.inner.store(Arc::new(new_snapshot));

        Ok(())
    }

    // Partition operations

    /// Add partitions to a topic.
    pub fn create_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        count: u32,
    ) -> Result<Vec<PartitionMeta>, IggyError> {
        let current = self.inner.load();

        let stream_idx = current
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        let stream = current.streams.get(&stream_idx).unwrap();
        let topic_idx = match topic_id.kind {
            iggy_common::IdKind::Numeric => topic_id.get_u32_value().unwrap() as usize,
            iggy_common::IdKind::String => {
                let name = topic_id.get_string_value().unwrap();
                stream.get_topic_id_by_name(&name).ok_or_else(|| {
                    IggyError::TopicNameNotFound(name.to_string(), stream.name.clone())
                })?
            }
        };

        let topic = stream.topics.get(&topic_idx).unwrap();
        let start_id = topic.partitions.len();

        let mut new_partitions = Vec::with_capacity(count as usize);
        for i in 0..count {
            let partition_id = start_id + i as usize;
            new_partitions.push(PartitionMeta::new(partition_id, IggyTimestamp::now()));
        }

        let mut new_snapshot = (**current).clone();
        let topic_mut = new_snapshot
            .streams
            .get_mut(&stream_idx)
            .unwrap()
            .topics
            .get_mut(&topic_idx)
            .unwrap();

        for partition in &new_partitions {
            topic_mut.add_partition(partition.clone());
            // Also add to flat partitions map
            let ns = IggyNamespace::new(stream_idx, topic_idx, partition.id);
            new_snapshot.partitions.insert(ns, partition.clone());
        }

        self.inner.store(Arc::new(new_snapshot));

        Ok(new_partitions)
    }

    /// Add a partition from persisted state (ID already assigned).
    pub fn add_partition_from_state(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        created_at: IggyTimestamp,
    ) -> Result<PartitionMeta, IggyError> {
        let current = self.inner.load();

        let stream = current
            .streams
            .get(&stream_id)
            .ok_or(IggyError::StreamIdNotFound(
                Identifier::numeric(stream_id as u32).unwrap(),
            ))?;

        if !stream.topics.contains_key(&topic_id) {
            return Err(IggyError::TopicIdNotFound(
                Identifier::numeric(topic_id as u32).unwrap(),
                Identifier::numeric(stream_id as u32).unwrap(),
            ));
        }

        let partition = PartitionMeta::new(partition_id, created_at);

        let mut new_snapshot = (**current).clone();
        let topic_mut = new_snapshot
            .streams
            .get_mut(&stream_id)
            .unwrap()
            .topics
            .get_mut(&topic_id)
            .unwrap();

        topic_mut.add_partition(partition.clone());

        // Also add to flat partitions map
        let ns = IggyNamespace::new(stream_id, topic_id, partition_id);
        new_snapshot.partitions.insert(ns, partition.clone());

        self.inner.store(Arc::new(new_snapshot));

        Ok(partition)
    }

    /// Delete partitions from a topic.
    pub fn delete_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_ids: &[usize],
    ) -> Result<Vec<PartitionMeta>, IggyError> {
        let current = self.inner.load();

        let stream_idx = current
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        let stream = current.streams.get(&stream_idx).unwrap();
        let topic_idx = match topic_id.kind {
            iggy_common::IdKind::Numeric => topic_id.get_u32_value().unwrap() as usize,
            iggy_common::IdKind::String => {
                let name = topic_id.get_string_value().unwrap();
                stream.get_topic_id_by_name(&name).ok_or_else(|| {
                    IggyError::TopicNameNotFound(name.to_string(), stream.name.clone())
                })?
            }
        };

        let mut new_snapshot = (**current).clone();
        let topic = new_snapshot
            .streams
            .get_mut(&stream_idx)
            .unwrap()
            .topics
            .get_mut(&topic_idx)
            .unwrap();

        let mut deleted = Vec::with_capacity(partition_ids.len());
        for &id in partition_ids {
            if let Some(partition) = topic.remove_partition(id) {
                deleted.push(partition);
                // Also remove from flat partitions map
                let ns = IggyNamespace::new(stream_idx, topic_idx, id);
                new_snapshot.partitions.remove(&ns);
            }
        }

        self.inner.store(Arc::new(new_snapshot));

        Ok(deleted)
    }

    // User operations

    /// Create a new user.
    pub fn create_user(
        &self,
        username: String,
        password_hash: String,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<UserMeta, IggyError> {
        let current = self.inner.load();

        if current.user_exists_by_name(&username) {
            return Err(IggyError::UserAlreadyExists);
        }

        let id = self.next_user_id.fetch_add(1, Ordering::SeqCst);
        let user = UserMeta::new(
            id,
            username.clone(),
            password_hash,
            IggyTimestamp::now(),
            status,
            permissions,
        );

        let mut new_snapshot = (**current).clone();
        new_snapshot.user_index.insert(username, id);
        new_snapshot.users.insert(id, user.clone());

        self.inner.store(Arc::new(new_snapshot));

        Ok(user)
    }

    /// Add a user from persisted state (ID already assigned).
    /// Updates the next_user_id counter to be greater than the loaded ID.
    pub fn add_user_from_state(
        &self,
        id: u32,
        username: String,
        password_hash: String,
        status: UserStatus,
        permissions: Option<Permissions>,
        created_at: IggyTimestamp,
    ) -> Result<UserMeta, IggyError> {
        // Update counter to be at least id + 1
        self.next_user_id.fetch_max(id + 1, Ordering::SeqCst);

        let current = self.inner.load();

        // Skip duplicate check - persisted state is authoritative
        let user = UserMeta::new(
            id,
            username.clone(),
            password_hash,
            created_at,
            status,
            permissions,
        );

        let mut new_snapshot = (**current).clone();
        new_snapshot.user_index.insert(username, id);
        new_snapshot.users.insert(id, user.clone());

        self.inner.store(Arc::new(new_snapshot));

        Ok(user)
    }

    /// Delete a user.
    pub fn delete_user(&self, id: &Identifier) -> Result<UserMeta, IggyError> {
        let current = self.inner.load();

        let user_id = current
            .get_user_id(id)
            .ok_or_else(|| IggyError::ResourceNotFound(format!("user with ID {id}")))?;

        let mut new_snapshot = (**current).clone();
        let user = new_snapshot
            .users
            .remove(&user_id)
            .ok_or_else(|| IggyError::ResourceNotFound(format!("user with ID {user_id}")))?;
        new_snapshot.user_index.remove(&user.username);

        self.inner.store(Arc::new(new_snapshot));

        Ok(user)
    }

    /// Update user metadata.
    pub fn update_user(
        &self,
        id: &Identifier,
        username: Option<String>,
        status: Option<UserStatus>,
    ) -> Result<(), IggyError> {
        let current = self.inner.load();

        let user_id = current
            .get_user_id(id)
            .ok_or_else(|| IggyError::ResourceNotFound(format!("user with ID {id}")))?;

        let mut new_snapshot = (**current).clone();
        let user = new_snapshot
            .users
            .get_mut(&user_id)
            .ok_or_else(|| IggyError::ResourceNotFound(format!("user with ID {user_id}")))?;

        if let Some(new_username) = username
            && user.username != new_username
        {
            if new_snapshot.user_index.contains_key(&new_username) {
                return Err(IggyError::UserAlreadyExists);
            }
            new_snapshot.user_index.remove(&user.username);
            new_snapshot
                .user_index
                .insert(new_username.clone(), user_id);
            user.username = new_username;
        }
        if let Some(new_status) = status {
            user.status = new_status;
        }

        self.inner.store(Arc::new(new_snapshot));

        Ok(())
    }

    /// Update user permissions.
    pub fn update_permissions(
        &self,
        id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        let current = self.inner.load();

        let user_id = current
            .get_user_id(id)
            .ok_or_else(|| IggyError::ResourceNotFound(format!("user with ID {id}")))?;

        let mut new_snapshot = (**current).clone();
        let user = new_snapshot
            .users
            .get_mut(&user_id)
            .ok_or_else(|| IggyError::ResourceNotFound(format!("user with ID {user_id}")))?;
        user.permissions = permissions;

        self.inner.store(Arc::new(new_snapshot));

        Ok(())
    }

    /// Change user password.
    pub fn change_password(
        &self,
        id: &Identifier,
        new_password_hash: String,
    ) -> Result<(), IggyError> {
        let current = self.inner.load();

        let user_id = current
            .get_user_id(id)
            .ok_or_else(|| IggyError::ResourceNotFound(format!("user with ID {id}")))?;

        let mut new_snapshot = (**current).clone();
        let user = new_snapshot
            .users
            .get_mut(&user_id)
            .ok_or_else(|| IggyError::ResourceNotFound(format!("user with ID {user_id}")))?;
        user.password_hash = new_password_hash;

        self.inner.store(Arc::new(new_snapshot));

        Ok(())
    }

    // Personal access token operations

    /// Create a personal access token for a user.
    pub fn create_personal_access_token(
        &self,
        user_id: u32,
        name: String,
        token_hash: String,
        expiry_at: Option<IggyTimestamp>,
    ) -> Result<(), IggyError> {
        let current = self.inner.load();

        if !current.users.contains_key(&user_id) {
            return Err(IggyError::ResourceNotFound(format!(
                "user with ID {user_id}"
            )));
        }

        let mut new_snapshot = (**current).clone();
        let user = new_snapshot.users.get_mut(&user_id).unwrap();

        if user.personal_access_tokens.contains_key(&name) {
            return Err(IggyError::PersonalAccessTokenAlreadyExists(name, user_id));
        }

        let pat = PersonalAccessTokenMeta::new(name.clone(), token_hash, expiry_at);
        user.personal_access_tokens.insert(name, pat);

        self.inner.store(Arc::new(new_snapshot));

        Ok(())
    }

    /// Delete a personal access token for a user.
    pub fn delete_personal_access_token(&self, user_id: u32, name: &str) -> Result<(), IggyError> {
        let current = self.inner.load();

        if !current.users.contains_key(&user_id) {
            return Err(IggyError::ResourceNotFound(format!(
                "user with ID {user_id}"
            )));
        }

        let mut new_snapshot = (**current).clone();
        let user = new_snapshot.users.get_mut(&user_id).unwrap();

        if user.personal_access_tokens.remove(name).is_none() {
            return Err(IggyError::ResourceNotFound(name.to_owned()));
        }

        self.inner.store(Arc::new(new_snapshot));

        Ok(())
    }

    /// Clean up expired personal access tokens across all users.
    /// Returns the total number of tokens removed.
    pub fn cleanup_expired_personal_access_tokens(&self, now: IggyTimestamp) -> usize {
        let current = self.inner.load();

        // Collect expired tokens for each user
        let mut expired_by_user: Vec<(u32, Vec<String>)> = Vec::new();
        for (user_id, user) in current.users.iter() {
            let expired: Vec<String> = user
                .personal_access_tokens
                .iter()
                .filter(|(_, pat)| pat.is_expired(now))
                .map(|(name, _)| name.clone())
                .collect();
            if !expired.is_empty() {
                expired_by_user.push((*user_id, expired));
            }
        }

        if expired_by_user.is_empty() {
            return 0;
        }

        let mut total_removed = 0;
        let mut new_snapshot = (**current).clone();
        for (user_id, expired_names) in expired_by_user {
            if let Some(user) = new_snapshot.users.get_mut(&user_id) {
                for name in expired_names {
                    user.personal_access_tokens.remove(&name);
                    total_removed += 1;
                }
            }
        }

        self.inner.store(Arc::new(new_snapshot));
        total_removed
    }

    // Consumer group operations

    /// Create a consumer group.
    pub fn create_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: String,
        partition_ids: Vec<usize>,
    ) -> Result<ConsumerGroupMeta, IggyError> {
        let current = self.inner.load();

        let stream_idx = current
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        let stream = current.streams.get(&stream_idx).unwrap();
        let topic_idx = match topic_id.kind {
            iggy_common::IdKind::Numeric => topic_id.get_u32_value().unwrap() as usize,
            iggy_common::IdKind::String => {
                let topic_name = topic_id.get_string_value().unwrap();
                stream.get_topic_id_by_name(&topic_name).ok_or_else(|| {
                    IggyError::TopicNameNotFound(topic_name.to_string(), stream.name.clone())
                })?
            }
        };

        let topic = stream.topics.get(&topic_idx).unwrap();
        if topic.get_consumer_group_id_by_name(&name).is_some() {
            return Err(IggyError::ConsumerGroupNameAlreadyExists(
                name,
                topic_id.clone(),
            ));
        }

        let group_id = topic.consumer_groups.len();
        let group = ConsumerGroupMeta::new(group_id, name, partition_ids);

        let mut new_snapshot = (**current).clone();
        new_snapshot
            .streams
            .get_mut(&stream_idx)
            .unwrap()
            .topics
            .get_mut(&topic_idx)
            .unwrap()
            .add_consumer_group(group.clone());

        self.inner.store(Arc::new(new_snapshot));

        Ok(group)
    }

    /// Add a consumer group from persisted state (ID already assigned).
    pub fn add_consumer_group_from_state(
        &self,
        stream_id: usize,
        topic_id: usize,
        group_id: usize,
        name: String,
        partition_ids: &[usize],
    ) -> Result<ConsumerGroupMeta, IggyError> {
        let current = self.inner.load();

        let stream = current
            .streams
            .get(&stream_id)
            .ok_or(IggyError::StreamIdNotFound(
                Identifier::numeric(stream_id as u32).unwrap(),
            ))?;

        if !stream.topics.contains_key(&topic_id) {
            return Err(IggyError::TopicIdNotFound(
                Identifier::numeric(topic_id as u32).unwrap(),
                Identifier::numeric(stream_id as u32).unwrap(),
            ));
        }

        let group = ConsumerGroupMeta::new(group_id, name, partition_ids.to_vec());

        let mut new_snapshot = (**current).clone();
        new_snapshot
            .streams
            .get_mut(&stream_id)
            .unwrap()
            .topics
            .get_mut(&topic_id)
            .unwrap()
            .add_consumer_group(group.clone());

        self.inner.store(Arc::new(new_snapshot));

        Ok(group)
    }

    /// Delete a consumer group.
    pub fn delete_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<ConsumerGroupMeta, IggyError> {
        let current = self.inner.load();

        let stream_idx = current
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        let stream = current.streams.get(&stream_idx).unwrap();
        let topic_idx = match topic_id.kind {
            iggy_common::IdKind::Numeric => topic_id.get_u32_value().unwrap() as usize,
            iggy_common::IdKind::String => {
                let topic_name = topic_id.get_string_value().unwrap();
                stream.get_topic_id_by_name(&topic_name).ok_or_else(|| {
                    IggyError::TopicNameNotFound(topic_name.to_string(), stream.name.clone())
                })?
            }
        };

        let topic = stream.topics.get(&topic_idx).unwrap();
        let group_idx = match group_id.kind {
            iggy_common::IdKind::Numeric => group_id.get_u32_value().unwrap() as usize,
            iggy_common::IdKind::String => {
                let group_name = group_id.get_string_value().unwrap();
                topic
                    .get_consumer_group_id_by_name(&group_name)
                    .ok_or_else(|| {
                        IggyError::ConsumerGroupNameNotFound(
                            group_name.to_string(),
                            topic_id.clone(),
                        )
                    })?
            }
        };

        let mut new_snapshot = (**current).clone();
        let group = new_snapshot
            .streams
            .get_mut(&stream_idx)
            .unwrap()
            .topics
            .get_mut(&topic_idx)
            .unwrap()
            .remove_consumer_group(group_idx)
            .ok_or_else(|| {
                IggyError::ConsumerGroupIdNotFound(group_id.clone(), topic_id.clone())
            })?;

        self.inner.store(Arc::new(new_snapshot));

        Ok(group)
    }

    /// Get a consumer group.
    pub fn get_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Option<ConsumerGroupMeta> {
        let snapshot = self.inner.load();
        let stream_idx = snapshot.get_stream_id(stream_id)?;
        let stream = snapshot.streams.get(&stream_idx)?;

        let topic_idx = match topic_id.kind {
            iggy_common::IdKind::Numeric => topic_id.get_u32_value().ok()? as usize,
            iggy_common::IdKind::String => {
                let name = topic_id.get_string_value().ok()?;
                stream.get_topic_id_by_name(&name)?
            }
        };

        let topic = stream.topics.get(&topic_idx)?;
        let group_idx = match group_id.kind {
            iggy_common::IdKind::Numeric => group_id.get_u32_value().ok()? as usize,
            iggy_common::IdKind::String => {
                let name = group_id.get_string_value().ok()?;
                topic.get_consumer_group_id_by_name(&name)?
            }
        };

        topic.consumer_groups.get(&group_idx).cloned()
    }

    /// Join a consumer group. Returns the assigned partitions for this member.
    pub fn join_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
        client_id: u32,
    ) -> Result<Vec<usize>, IggyError> {
        let current = self.inner.load();

        let stream_idx = current
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        let stream = current.streams.get(&stream_idx).unwrap();
        let topic_idx = match topic_id.kind {
            iggy_common::IdKind::Numeric => topic_id.get_u32_value().unwrap() as usize,
            iggy_common::IdKind::String => {
                let topic_name = topic_id.get_string_value().unwrap();
                stream.get_topic_id_by_name(&topic_name).ok_or_else(|| {
                    IggyError::TopicNameNotFound(topic_name.to_string(), stream.name.clone())
                })?
            }
        };

        let topic = stream.topics.get(&topic_idx).unwrap();
        let group_idx = match group_id.kind {
            iggy_common::IdKind::Numeric => group_id.get_u32_value().unwrap() as usize,
            iggy_common::IdKind::String => {
                let group_name = group_id.get_string_value().unwrap();
                topic
                    .get_consumer_group_id_by_name(&group_name)
                    .ok_or_else(|| {
                        IggyError::ConsumerGroupNameNotFound(
                            group_name.to_string(),
                            topic_id.clone(),
                        )
                    })?
            }
        };

        let group = topic.consumer_groups.get(&group_idx).ok_or_else(|| {
            IggyError::ConsumerGroupIdNotFound(group_id.clone(), topic_id.clone())
        })?;

        // Check if already a member
        if group.has_member(client_id) {
            // Return existing partition assignment
            return Ok(group
                .get_member(client_id)
                .map(|m| m.partitions.clone())
                .unwrap_or_default());
        }

        let mut new_snapshot = (**current).clone();
        let group_mut = new_snapshot
            .streams
            .get_mut(&stream_idx)
            .unwrap()
            .topics
            .get_mut(&topic_idx)
            .unwrap()
            .consumer_groups
            .get_mut(&group_idx)
            .unwrap();

        // Add new member
        group_mut
            .members
            .insert(client_id, ConsumerGroupMember::new(client_id));

        // Rebalance partitions
        group_mut.members = group_mut.rebalance_partitions();

        // Get assigned partitions for the new member
        let assigned = group_mut
            .members
            .get(&client_id)
            .map(|m| m.partitions.clone())
            .unwrap_or_default();

        self.inner.store(Arc::new(new_snapshot));

        Ok(assigned)
    }

    /// Leave a consumer group.
    pub fn leave_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
        client_id: u32,
    ) -> Result<(), IggyError> {
        let current = self.inner.load();

        let stream_idx = current
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        let stream = current.streams.get(&stream_idx).unwrap();
        let topic_idx = match topic_id.kind {
            iggy_common::IdKind::Numeric => topic_id.get_u32_value().unwrap() as usize,
            iggy_common::IdKind::String => {
                let topic_name = topic_id.get_string_value().unwrap();
                stream.get_topic_id_by_name(&topic_name).ok_or_else(|| {
                    IggyError::TopicNameNotFound(topic_name.to_string(), stream.name.clone())
                })?
            }
        };

        let topic = stream.topics.get(&topic_idx).unwrap();
        let group_idx = match group_id.kind {
            iggy_common::IdKind::Numeric => group_id.get_u32_value().unwrap() as usize,
            iggy_common::IdKind::String => {
                let group_name = group_id.get_string_value().unwrap();
                topic
                    .get_consumer_group_id_by_name(&group_name)
                    .ok_or_else(|| {
                        IggyError::ConsumerGroupNameNotFound(
                            group_name.to_string(),
                            topic_id.clone(),
                        )
                    })?
            }
        };

        let group = topic.consumer_groups.get(&group_idx).ok_or_else(|| {
            IggyError::ConsumerGroupIdNotFound(group_id.clone(), topic_id.clone())
        })?;

        // Check if member exists
        if !group.has_member(client_id) {
            return Err(IggyError::ConsumerGroupMemberNotFound(
                client_id,
                group_id.clone(),
                topic_id.clone(),
            ));
        }

        let mut new_snapshot = (**current).clone();
        let group_mut = new_snapshot
            .streams
            .get_mut(&stream_idx)
            .unwrap()
            .topics
            .get_mut(&topic_idx)
            .unwrap()
            .consumer_groups
            .get_mut(&group_idx)
            .unwrap();

        // Remove member
        group_mut.members.remove(&client_id);

        // Rebalance remaining partitions
        group_mut.members = group_mut.rebalance_partitions();

        self.inner.store(Arc::new(new_snapshot));

        Ok(())
    }

    /// Rebalance all consumer groups in a topic with new partition IDs.
    /// Called after partitions are added or deleted.
    pub fn rebalance_consumer_groups(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_ids: &[usize],
    ) -> Result<(), IggyError> {
        let current = self.inner.load();

        let stream_idx = current
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        let stream = current.streams.get(&stream_idx).unwrap();
        let topic_idx = match topic_id.kind {
            iggy_common::IdKind::Numeric => topic_id.get_u32_value().unwrap() as usize,
            iggy_common::IdKind::String => {
                let topic_name = topic_id.get_string_value().unwrap();
                stream.get_topic_id_by_name(&topic_name).ok_or_else(|| {
                    IggyError::TopicNameNotFound(topic_name.to_string(), stream.name.clone())
                })?
            }
        };

        let mut new_snapshot = (**current).clone();
        let topic_mut = new_snapshot
            .streams
            .get_mut(&stream_idx)
            .unwrap()
            .topics
            .get_mut(&topic_idx)
            .unwrap();

        // Update partitions for all consumer groups and rebalance
        for (_, group) in topic_mut.consumer_groups.iter_mut() {
            group.partitions = partition_ids.to_vec();
            group.members = group.rebalance_partitions();
        }

        self.inner.store(Arc::new(new_snapshot));

        Ok(())
    }

    /// Increment the round-robin partition counter for a topic.
    /// Used for balanced partitioning in message sends.
    pub fn increment_topic_partition_counter(&self, stream_id: usize, topic_id: usize) {
        let current = self.inner.load();
        let mut new_snapshot = (**current).clone();
        if let Some(stream) = new_snapshot.streams.get_mut(&stream_id) {
            if let Some(topic) = stream.topics.get_mut(&topic_id) {
                topic.increment_partition_counter();
            }
        }
        self.inner.store(Arc::new(new_snapshot));
    }

    // Bound address operations

    /// Set the bound TCP address.
    pub fn set_tcp_address(&self, address: SocketAddr) {
        let current = self.inner.load();
        let mut new_snapshot = (**current).clone();
        new_snapshot.bound_addresses.tcp = Some(address);
        self.inner.store(Arc::new(new_snapshot));
    }

    /// Set the bound HTTP address.
    pub fn set_http_address(&self, address: SocketAddr) {
        let current = self.inner.load();
        let mut new_snapshot = (**current).clone();
        new_snapshot.bound_addresses.http = Some(address);
        self.inner.store(Arc::new(new_snapshot));
    }

    /// Set the bound QUIC address.
    pub fn set_quic_address(&self, address: SocketAddr) {
        let current = self.inner.load();
        let mut new_snapshot = (**current).clone();
        new_snapshot.bound_addresses.quic = Some(address);
        self.inner.store(Arc::new(new_snapshot));
    }

    /// Set the bound WebSocket address.
    pub fn set_websocket_address(&self, address: SocketAddr) {
        let current = self.inner.load();
        let mut new_snapshot = (**current).clone();
        new_snapshot.bound_addresses.websocket = Some(address);
        self.inner.store(Arc::new(new_snapshot));
    }

    /// Get the current next stream ID (for testing/debugging).
    pub fn next_stream_id(&self) -> usize {
        self.next_stream_id.load(Ordering::SeqCst)
    }

    /// Get the current next user ID (for testing/debugging).
    pub fn next_user_id(&self) -> u32 {
        self.next_user_id.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_stream() {
        let metadata = SharedMetadata::new();

        let stream = metadata.create_stream("test-stream".to_string()).unwrap();
        assert_eq!(stream.id, 0);
        assert_eq!(stream.name, "test-stream");

        assert!(metadata.stream_exists_by_name("test-stream"));
        assert!(!metadata.stream_exists_by_name("nonexistent"));
    }

    #[test]
    fn test_create_duplicate_stream_fails() {
        let metadata = SharedMetadata::new();

        metadata.create_stream("test-stream".to_string()).unwrap();
        let result = metadata.create_stream("test-stream".to_string());

        assert!(matches!(result, Err(IggyError::StreamNameAlreadyExists(_))));
    }

    #[test]
    fn test_delete_stream() {
        let metadata = SharedMetadata::new();

        let stream = metadata.create_stream("test-stream".to_string()).unwrap();
        let stream_id = Identifier::numeric(stream.id as u32).unwrap();

        let deleted = metadata.delete_stream(&stream_id).unwrap();
        assert_eq!(deleted.id, stream.id);
        assert!(!metadata.stream_exists_by_name("test-stream"));
    }

    #[test]
    fn test_create_user() {
        let metadata = SharedMetadata::new();

        let user = metadata
            .create_user(
                "testuser".to_string(),
                "hashed_password".to_string(),
                UserStatus::Active,
                None,
            )
            .unwrap();

        assert_eq!(user.id, 0);
        assert_eq!(user.username, "testuser");
        assert!(metadata.user_exists_by_name("testuser"));
    }

    #[test]
    fn test_concurrent_stream_ids() {
        let metadata = SharedMetadata::new();

        let stream1 = metadata.create_stream("stream-1".to_string()).unwrap();
        let stream2 = metadata.create_stream("stream-2".to_string()).unwrap();
        let stream3 = metadata.create_stream("stream-3".to_string()).unwrap();

        assert_eq!(stream1.id, 0);
        assert_eq!(stream2.id, 1);
        assert_eq!(stream3.id, 2);
    }
}
