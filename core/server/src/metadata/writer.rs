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

use crate::metadata::inner::InnerMetadata;
use crate::metadata::ops::MetadataOp;
use crate::metadata::reader::Metadata;
use crate::metadata::{
    ConsumerGroupId, ConsumerGroupMeta, PartitionId, PartitionMeta, StreamId, StreamMeta, TopicId,
    TopicMeta, UserId, UserMeta,
};
use crate::streaming::partitions::consumer_group_offsets::ConsumerGroupOffsets;
use crate::streaming::partitions::consumer_offsets::ConsumerOffsets;
use crate::streaming::stats::{PartitionStats, StreamStats, TopicStats};
use iggy_common::{
    CompressionAlgorithm, Identifier, IggyError, IggyExpiry, IggyTimestamp, MaxTopicSize,
    Permissions, PersonalAccessToken, UserStatus,
};
use left_right::WriteHandle;
use slab::Slab;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct MetadataWriter {
    inner: WriteHandle<InnerMetadata, MetadataOp>,
    revision: u64,
}

impl MetadataWriter {
    pub fn new(handle: WriteHandle<InnerMetadata, MetadataOp>) -> Self {
        Self {
            inner: handle,
            revision: 0,
        }
    }

    fn next_revision(&mut self) -> u64 {
        self.revision += 1;
        self.revision
    }

    pub fn append(&mut self, op: MetadataOp) {
        self.inner.append(op);
    }

    pub fn publish(&mut self) {
        self.inner.publish();
    }

    pub fn initialize(&mut self, initial: InnerMetadata) {
        self.append(MetadataOp::Initialize(Box::new(initial)));
        self.publish();
    }

    pub fn add_stream(&mut self, meta: StreamMeta) -> StreamId {
        let assigned_id = Arc::new(AtomicUsize::new(usize::MAX));
        self.append(MetadataOp::AddStream {
            meta,
            assigned_id: assigned_id.clone(),
        });
        self.publish();
        let id = assigned_id.load(Ordering::Acquire);
        debug_assert_ne!(id, usize::MAX, "add_stream should always succeed");
        id
    }

    pub fn update_stream(&mut self, id: StreamId, new_name: Arc<str>) {
        self.append(MetadataOp::UpdateStream { id, new_name });
        self.publish();
    }

    pub fn delete_stream(&mut self, id: StreamId) {
        self.append(MetadataOp::DeleteStream { id });
        self.publish();
    }

    pub fn add_topic(&mut self, stream_id: StreamId, meta: TopicMeta) -> Option<TopicId> {
        let assigned_id = Arc::new(AtomicUsize::new(usize::MAX));
        self.append(MetadataOp::AddTopic {
            stream_id,
            meta,
            assigned_id: assigned_id.clone(),
        });
        self.publish();
        let id = assigned_id.load(Ordering::Acquire);
        if id == usize::MAX { None } else { Some(id) }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_topic(
        &mut self,
        stream_id: StreamId,
        topic_id: TopicId,
        new_name: Arc<str>,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: u8,
    ) {
        self.append(MetadataOp::UpdateTopic {
            stream_id,
            topic_id,
            new_name,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor,
        });
        self.publish();
    }

    pub fn delete_topic(&mut self, stream_id: StreamId, topic_id: TopicId) {
        self.append(MetadataOp::DeleteTopic {
            stream_id,
            topic_id,
        });
        self.publish();
    }

    /// Add partitions to a topic. Returns the assigned partition IDs (sequential from current count).
    pub fn add_partitions(
        &mut self,
        reader: &Metadata,
        stream_id: StreamId,
        topic_id: TopicId,
        partitions: Vec<PartitionMeta>,
    ) -> Vec<PartitionId> {
        if partitions.is_empty() {
            return Vec::new();
        }

        let count_before = reader
            .get_partitions_count(stream_id, topic_id)
            .expect("stream and topic must exist when adding partitions");
        let count = partitions.len();

        let revision_id = self.next_revision();
        self.append(MetadataOp::AddPartitions {
            stream_id,
            topic_id,
            partitions,
            revision_id,
        });
        self.publish();

        (count_before..count_before + count).collect()
    }

    /// Delete partitions from the end of a topic.
    pub fn delete_partitions(&mut self, stream_id: StreamId, topic_id: TopicId, count: u32) {
        if count == 0 {
            return;
        }
        self.append(MetadataOp::DeletePartitions {
            stream_id,
            topic_id,
            count,
        });
        self.publish();
    }

    pub fn add_user(&mut self, meta: UserMeta) -> UserId {
        let assigned_id = Arc::new(AtomicUsize::new(usize::MAX));
        self.append(MetadataOp::AddUser {
            meta,
            assigned_id: assigned_id.clone(),
        });
        self.publish();
        let id = assigned_id.load(Ordering::Acquire);
        debug_assert_ne!(id, usize::MAX, "add_user should always succeed");
        id as UserId
    }

    pub fn update_user_meta(&mut self, id: UserId, meta: UserMeta) {
        self.append(MetadataOp::UpdateUserMeta { id, meta });
        self.publish();
    }

    pub fn delete_user(&mut self, id: UserId) {
        self.append(MetadataOp::DeleteUser { id });
        self.publish();
    }

    pub fn add_personal_access_token(&mut self, user_id: UserId, pat: PersonalAccessToken) {
        self.append(MetadataOp::AddPersonalAccessToken { user_id, pat });
        self.publish();
    }

    pub fn delete_personal_access_token(&mut self, user_id: UserId, token_hash: Arc<str>) {
        self.append(MetadataOp::DeletePersonalAccessToken {
            user_id,
            token_hash,
        });
        self.publish();
    }

    pub fn add_consumer_group(
        &mut self,
        stream_id: StreamId,
        topic_id: TopicId,
        meta: ConsumerGroupMeta,
    ) -> Option<ConsumerGroupId> {
        let assigned_id = Arc::new(AtomicUsize::new(usize::MAX));
        self.append(MetadataOp::AddConsumerGroup {
            stream_id,
            topic_id,
            meta,
            assigned_id: assigned_id.clone(),
        });
        self.publish();
        let id = assigned_id.load(Ordering::Acquire);
        if id == usize::MAX { None } else { Some(id) }
    }

    pub fn delete_consumer_group(
        &mut self,
        stream_id: StreamId,
        topic_id: TopicId,
        group_id: ConsumerGroupId,
    ) {
        self.append(MetadataOp::DeleteConsumerGroup {
            stream_id,
            topic_id,
            group_id,
        });
        self.publish();
    }

    pub fn join_consumer_group(
        &mut self,
        stream_id: StreamId,
        topic_id: TopicId,
        group_id: ConsumerGroupId,
        client_id: u32,
        valid_client_ids: Option<Vec<u32>>,
    ) -> Option<usize> {
        let member_id = Arc::new(AtomicUsize::new(usize::MAX));
        self.append(MetadataOp::JoinConsumerGroup {
            stream_id,
            topic_id,
            group_id,
            client_id,
            member_id: member_id.clone(),
            valid_client_ids,
        });
        self.publish();
        let id = member_id.load(Ordering::Acquire);
        if id == usize::MAX { None } else { Some(id) }
    }

    pub fn leave_consumer_group(
        &mut self,
        stream_id: StreamId,
        topic_id: TopicId,
        group_id: ConsumerGroupId,
        client_id: u32,
    ) -> Option<usize> {
        let removed_member_id = Arc::new(AtomicUsize::new(usize::MAX));
        self.append(MetadataOp::LeaveConsumerGroup {
            stream_id,
            topic_id,
            group_id,
            client_id,
            removed_member_id: removed_member_id.clone(),
        });
        self.publish();
        let id = removed_member_id.load(Ordering::Acquire);
        if id == usize::MAX { None } else { Some(id) }
    }

    pub fn rebalance_consumer_groups_for_topic(
        &mut self,
        stream_id: StreamId,
        topic_id: TopicId,
        partitions_count: u32,
    ) {
        self.append(MetadataOp::RebalanceConsumerGroupsForTopic {
            stream_id,
            topic_id,
            partitions_count,
        });
        self.publish();
    }

    // High-level registration methods with validation

    pub fn create_stream(
        &mut self,
        reader: &Metadata,
        name: Arc<str>,
        created_at: IggyTimestamp,
    ) -> Result<(StreamId, Arc<StreamStats>), IggyError> {
        if reader.stream_name_exists(&name) {
            return Err(IggyError::StreamNameAlreadyExists(name.to_string()));
        }

        let stats = Arc::new(StreamStats::default());
        let meta = StreamMeta::with_stats(0, name, created_at, stats.clone());
        let id = self.add_stream(meta);
        Ok((id, stats))
    }

    pub fn try_update_stream(
        &mut self,
        reader: &Metadata,
        id: StreamId,
        new_name: Arc<str>,
    ) -> Result<(), IggyError> {
        let guard = reader.load();
        let Some(stream) = guard.streams.get(id) else {
            return Err(IggyError::StreamIdNotFound(
                Identifier::numeric(id as u32).unwrap(),
            ));
        };

        if stream.name == new_name {
            return Ok(());
        }

        if let Some(&existing_id) = guard.stream_index.get(&new_name)
            && existing_id != id
        {
            return Err(IggyError::StreamNameAlreadyExists(new_name.to_string()));
        }
        drop(guard);

        self.update_stream(id, new_name);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_topic(
        &mut self,
        reader: &Metadata,
        stream_id: StreamId,
        name: Arc<str>,
        created_at: IggyTimestamp,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: u8,
    ) -> Result<(TopicId, Arc<TopicStats>), IggyError> {
        let parent_stats = reader.get_stream_stats(stream_id).ok_or_else(|| {
            IggyError::StreamIdNotFound(Identifier::numeric(stream_id as u32).unwrap())
        })?;

        let guard = reader.load();
        let Some(stream) = guard.streams.get(stream_id) else {
            return Err(IggyError::StreamIdNotFound(
                Identifier::numeric(stream_id as u32).unwrap(),
            ));
        };

        if stream.topic_index.contains_key(&name) {
            return Err(IggyError::TopicNameAlreadyExists(
                name.to_string(),
                Identifier::numeric(stream_id as u32).unwrap(),
            ));
        }
        drop(guard);

        let stats = Arc::new(TopicStats::new(parent_stats));
        let meta = TopicMeta {
            id: 0,
            name,
            created_at,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor,
            stats: stats.clone(),
            partitions: Vec::new(),
            consumer_groups: Slab::new(),
            consumer_group_index: ahash::AHashMap::default(),
            round_robin_counter: Arc::new(AtomicUsize::new(0)),
        };

        // change to create_topic
        let id = self.add_topic(stream_id, meta).ok_or_else(|| {
            IggyError::StreamIdNotFound(Identifier::numeric(stream_id as u32).unwrap())
        })?;
        Ok((id, stats))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_update_topic(
        &mut self,
        reader: &Metadata,
        stream_id: StreamId,
        topic_id: TopicId,
        new_name: Arc<str>,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: u8,
    ) -> Result<(), IggyError> {
        let guard = reader.load();
        let Some(stream) = guard.streams.get(stream_id) else {
            return Err(IggyError::StreamIdNotFound(
                Identifier::numeric(stream_id as u32).unwrap(),
            ));
        };

        let Some(topic) = stream.topics.get(topic_id) else {
            return Err(IggyError::TopicIdNotFound(
                Identifier::numeric(topic_id as u32).unwrap(),
                Identifier::numeric(stream_id as u32).unwrap(),
            ));
        };

        if topic.name != new_name
            && let Some(&existing_id) = stream.topic_index.get(&new_name)
            && existing_id != topic_id
        {
            return Err(IggyError::TopicNameAlreadyExists(
                new_name.to_string(),
                Identifier::numeric(stream_id as u32).unwrap(),
            ));
        }
        drop(guard);

        self.update_topic(
            stream_id,
            topic_id,
            new_name,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor,
        );
        Ok(())
    }

    pub fn register_partitions(
        &mut self,
        reader: &Metadata,
        stream_id: StreamId,
        topic_id: TopicId,
        count: usize,
        created_at: IggyTimestamp,
    ) -> Vec<(PartitionId, Arc<PartitionStats>)> {
        if count == 0 {
            return Vec::new();
        }

        let parent_stats = reader
            .get_topic_stats(stream_id, topic_id)
            .expect("Parent topic stats must exist before registering partitions");

        let mut metas = Vec::with_capacity(count);
        let mut stats_list = Vec::with_capacity(count);

        for _ in 0..count {
            let stats = Arc::new(PartitionStats::new(parent_stats.clone()));
            metas.push(PartitionMeta {
                id: 0,
                created_at,
                revision_id: 0,
                stats: stats.clone(),
                consumer_offsets: Arc::new(ConsumerOffsets::with_capacity(0)),
                consumer_group_offsets: Arc::new(ConsumerGroupOffsets::with_capacity(0)),
            });
            stats_list.push(stats);
        }

        let ids = self.add_partitions(reader, stream_id, topic_id, metas);
        ids.into_iter().zip(stats_list).collect()
    }

    pub fn create_user(
        &mut self,
        reader: &Metadata,
        username: Arc<str>,
        password_hash: Arc<str>,
        status: UserStatus,
        permissions: Option<Arc<Permissions>>,
        max_users: usize,
    ) -> Result<UserId, IggyError> {
        if reader.username_exists(&username) {
            return Err(IggyError::UserAlreadyExists);
        }

        if reader.users_count() >= max_users {
            return Err(IggyError::UsersLimitReached);
        }

        let meta = UserMeta {
            id: 0,
            username,
            password_hash,
            status,
            permissions,
            created_at: IggyTimestamp::now(),
        };
        let id = self.add_user(meta);
        Ok(id)
    }

    pub fn update_user(
        &mut self,
        reader: &Metadata,
        id: UserId,
        username: Option<Arc<str>>,
        status: Option<UserStatus>,
    ) -> Result<UserMeta, IggyError> {
        let Some(mut meta) = reader.get_user(id) else {
            return Err(IggyError::ResourceNotFound(format!("user:{id}")));
        };

        if let Some(new_username) = username {
            if meta.username != new_username && reader.username_exists(&new_username) {
                return Err(IggyError::UserAlreadyExists);
            }
            meta.username = new_username;
        }

        if let Some(new_status) = status {
            meta.status = new_status;
        }

        let updated = meta.clone();
        self.update_user_meta(id, meta);
        Ok(updated)
    }

    pub fn create_consumer_group(
        &mut self,
        reader: &Metadata,
        stream_id: StreamId,
        topic_id: TopicId,
        name: Arc<str>,
        partitions_count: u32,
    ) -> Result<ConsumerGroupId, IggyError> {
        let guard = reader.load();
        let Some(stream) = guard.streams.get(stream_id) else {
            return Err(IggyError::StreamIdNotFound(
                Identifier::numeric(stream_id as u32).unwrap(),
            ));
        };

        let Some(topic) = stream.topics.get(topic_id) else {
            return Err(IggyError::TopicIdNotFound(
                Identifier::numeric(topic_id as u32).unwrap(),
                Identifier::numeric(stream_id as u32).unwrap(),
            ));
        };

        if topic.consumer_group_index.contains_key(&name) {
            return Err(IggyError::ConsumerGroupNameAlreadyExists(
                name.to_string(),
                Identifier::numeric(topic_id as u32).unwrap(),
            ));
        }
        drop(guard);

        let meta = ConsumerGroupMeta {
            id: 0,
            name,
            partitions: (0..partitions_count as usize).collect(),
            members: Slab::new(),
        };

        let id = self
            .add_consumer_group(stream_id, topic_id, meta)
            .ok_or_else(|| {
                IggyError::TopicIdNotFound(
                    Identifier::numeric(topic_id as u32).unwrap(),
                    Identifier::numeric(stream_id as u32).unwrap(),
                )
            })?;
        Ok(id)
    }
}
