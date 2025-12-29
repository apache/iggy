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
    ConsumerGroupMeta, MetadataSnapshot, PartitionMeta, StreamMeta, TopicMeta, UserMeta,
};
use arc_swap::{ArcSwap, Guard};
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

    /// Create a new stream. Returns the created stream metadata.
    pub fn create_stream(&self, name: String) -> Result<StreamMeta, IggyError> {
        let current = self.inner.load();

        if current.stream_exists_by_name(&name) {
            return Err(IggyError::StreamNameAlreadyExists(name));
        }

        let id = self.next_stream_id.fetch_add(1, Ordering::SeqCst);
        let stream = StreamMeta::new(id, name.clone(), IggyTimestamp::now());

        let mut new_snapshot = (**current).clone();
        new_snapshot.stream_index.insert(name, id);
        new_snapshot.streams.insert(id, stream.clone());

        self.inner.store(Arc::new(new_snapshot));

        Ok(stream)
    }

    /// Delete a stream. Returns the deleted stream metadata.
    pub fn delete_stream(&self, id: &Identifier) -> Result<StreamMeta, IggyError> {
        let current = self.inner.load();

        let stream_id = current
            .get_stream_id(id)
            .ok_or_else(|| IggyError::StreamIdNotFound(id.clone()))?;

        let mut new_snapshot = (**current).clone();
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

    /// Create a new topic in a stream.
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
        if stream.topic_exists(&name) {
            return Err(IggyError::TopicNameAlreadyExists(name, stream_id.clone()));
        }

        // Topic IDs are sequential within a stream
        let topic_id = stream.topics.len();
        let topic = TopicMeta::new(
            topic_id,
            name.clone(),
            IggyTimestamp::now(),
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
        }

        self.inner.store(Arc::new(new_snapshot));

        Ok(new_partitions)
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
