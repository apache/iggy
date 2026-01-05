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
    ConsumerGroupId, ConsumerGroupMemberMeta, ConsumerGroupMeta, InnerMetadata, PartitionId,
    PartitionMeta, StreamId, StreamMeta, TopicId, TopicMeta, UserId, UserMeta,
};
use crate::streaming::partitions::partition::{ConsumerGroupOffsets, ConsumerOffsets};
use crate::streaming::stats::{PartitionStats, StreamStats, TopicStats};
use arc_swap::{ArcSwap, Guard};
use iggy_common::collections::SegmentedSlab;
use iggy_common::{
    CompressionAlgorithm, IdKind, Identifier, IggyError, IggyExpiry, IggyTimestamp, MaxTopicSize,
    PersonalAccessToken, UserStatus,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Thread-safe wrapper for GlobalMetadata using ArcSwap for lock-free reads.
/// Uses hierarchical structure: streams contain topics, topics contain partitions and consumer groups.
/// IDs are assigned by SegmentedSlab::insert() at each level.
pub struct Metadata {
    inner: ArcSwap<InnerMetadata>,
}

impl Default for Metadata {
    fn default() -> Self {
        Self::new(InnerMetadata::new())
    }
}

impl Metadata {
    pub fn new(initial: InnerMetadata) -> Self {
        Self {
            inner: ArcSwap::from_pointee(initial),
        }
    }

    #[inline]
    pub fn load(&self) -> Guard<Arc<InnerMetadata>> {
        self.inner.load()
    }

    #[inline]
    pub fn load_full(&self) -> Arc<InnerMetadata> {
        self.inner.load_full()
    }

    /// Add a stream with a specific ID (for bootstrap/recovery).
    pub fn add_stream_with_id(&self, id: StreamId, meta: StreamMeta) {
        self.inner.rcu(move |current| {
            let meta = meta.clone();
            let mut new = (**current).clone();
            let entries: Vec<_> = new
                .streams
                .iter()
                .map(|(k, v)| (k, v.clone()))
                .chain(std::iter::once((id, meta.clone())))
                .collect();
            new.streams = SegmentedSlab::from_entries(entries);
            new.stream_index = new.stream_index.update(meta.name.clone(), id);
            new.revision += 1;
            Arc::new(new)
        });
    }

    /// Add a new stream with slab-assigned ID. Returns the assigned ID.
    pub fn add_stream(&self, meta: StreamMeta) -> StreamId {
        let assigned_id = Arc::new(AtomicUsize::new(0));
        let assigned_id_clone = assigned_id.clone();

        self.inner.rcu(move |current| {
            let meta = meta.clone();
            let mut new = (**current).clone();
            let (streams, id) = new.streams.insert(meta.clone());
            assigned_id_clone.store(id, Ordering::Release);
            new.streams = streams;
            new.stream_index = new.stream_index.update(meta.name.clone(), id);
            new.revision += 1;
            Arc::new(new)
        });

        assigned_id.load(Ordering::Acquire)
    }

    /// Atomically validates name uniqueness and updates stream name.
    /// Returns Ok(()) if update succeeded, or appropriate error.
    pub fn try_update_stream(&self, id: StreamId, new_name: Arc<str>) -> Result<(), IggyError> {
        let stream_not_found = Arc::new(AtomicBool::new(false));
        let name_conflict = Arc::new(AtomicBool::new(false));
        let unchanged = Arc::new(AtomicBool::new(false));

        let stream_not_found_clone = stream_not_found.clone();
        let name_conflict_clone = name_conflict.clone();
        let unchanged_clone = unchanged.clone();
        let new_name_clone = new_name.clone();

        self.inner.rcu(move |current| {
            let Some(old_meta) = current.streams.get(id) else {
                stream_not_found_clone.store(true, Ordering::Release);
                return Arc::clone(current);
            };

            if old_meta.name == new_name_clone {
                unchanged_clone.store(true, Ordering::Release);
                return Arc::clone(current);
            }

            if let Some(&existing_id) = current.stream_index.get(&new_name_clone)
                && existing_id != id
            {
                name_conflict_clone.store(true, Ordering::Release);
                return Arc::clone(current);
            }

            stream_not_found_clone.store(false, Ordering::Release);
            name_conflict_clone.store(false, Ordering::Release);
            unchanged_clone.store(false, Ordering::Release);

            let mut new = (**current).clone();
            new.stream_index = new.stream_index.without(&old_meta.name);

            let mut updated = old_meta.clone();
            updated.name = new_name_clone.clone();
            let (streams, _) = new.streams.update(id, updated);
            new.streams = streams;
            new.stream_index = new.stream_index.update(new_name_clone.clone(), id);
            new.revision += 1;

            Arc::new(new)
        });

        if stream_not_found.load(Ordering::Acquire) {
            Err(IggyError::StreamIdNotFound(
                Identifier::numeric(id as u32).unwrap(),
            ))
        } else if name_conflict.load(Ordering::Acquire) {
            Err(IggyError::StreamNameAlreadyExists(new_name.to_string()))
        } else {
            Ok(())
        }
    }

    /// Delete a stream and all its nested topics/partitions/consumer groups.
    pub fn delete_stream(&self, id: StreamId) {
        self.inner.rcu(|current| {
            let mut new = (**current).clone();
            if let Some(stream) = new.streams.get(id) {
                new.stream_index = new.stream_index.without(&stream.name);
            }
            let (streams, _) = new.streams.remove(id);
            new.streams = streams;
            new.revision += 1;
            Arc::new(new)
        });
    }

    /// Add a topic with a specific ID (for bootstrap/recovery).
    pub fn add_topic_with_id(&self, stream_id: StreamId, topic_id: TopicId, meta: TopicMeta) {
        self.inner.rcu(move |current| {
            let meta = meta.clone();
            let mut new = (**current).clone();

            if let Some(stream) = new.streams.get(stream_id) {
                let mut updated_stream = stream.clone();
                let entries: Vec<_> = updated_stream
                    .topics
                    .iter()
                    .map(|(k, v)| (k, v.clone()))
                    .chain(std::iter::once((topic_id, meta.clone())))
                    .collect();
                updated_stream.topics = SegmentedSlab::from_entries(entries);
                updated_stream.topic_index = updated_stream
                    .topic_index
                    .update(meta.name.clone(), topic_id);

                let (streams, _) = new.streams.update(stream_id, updated_stream);
                new.streams = streams;
            }
            new.revision += 1;
            Arc::new(new)
        });
    }

    /// Add a new topic with slab-assigned ID. Returns the assigned ID.
    pub fn add_topic(&self, stream_id: StreamId, meta: TopicMeta) -> Option<TopicId> {
        let assigned_id = Arc::new(AtomicUsize::new(usize::MAX));
        let assigned_id_clone = assigned_id.clone();

        self.inner.rcu(move |current| {
            let meta = meta.clone();
            let mut new = (**current).clone();

            if let Some(stream) = new.streams.get(stream_id) {
                let mut updated_stream = stream.clone();
                let (topics, id) = updated_stream.topics.insert(meta.clone());
                assigned_id_clone.store(id, Ordering::Release);
                updated_stream.topics = topics;
                updated_stream.topic_index =
                    updated_stream.topic_index.update(meta.name.clone(), id);

                let (streams, _) = new.streams.update(stream_id, updated_stream);
                new.streams = streams;
            }
            new.revision += 1;
            Arc::new(new)
        });

        let id = assigned_id.load(Ordering::Acquire);
        if id == usize::MAX { None } else { Some(id) }
    }

    /// Atomically validates name uniqueness and updates topic.
    /// Returns Ok(()) if update succeeded, or appropriate error.
    #[allow(clippy::too_many_arguments)]
    pub fn try_update_topic(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        new_name: Arc<str>,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: u8,
    ) -> Result<(), IggyError> {
        let stream_not_found = Arc::new(AtomicBool::new(false));
        let topic_not_found = Arc::new(AtomicBool::new(false));
        let name_conflict = Arc::new(AtomicBool::new(false));

        let stream_not_found_clone = stream_not_found.clone();
        let topic_not_found_clone = topic_not_found.clone();
        let name_conflict_clone = name_conflict.clone();
        let new_name_clone = new_name.clone();

        self.inner.rcu(move |current| {
            let Some(stream) = current.streams.get(stream_id) else {
                stream_not_found_clone.store(true, Ordering::Release);
                return Arc::clone(current);
            };

            let Some(old_meta) = stream.topics.get(topic_id) else {
                topic_not_found_clone.store(true, Ordering::Release);
                return Arc::clone(current);
            };

            if old_meta.name != new_name_clone
                && let Some(&existing_id) = stream.topic_index.get(&new_name_clone)
                && existing_id != topic_id
            {
                name_conflict_clone.store(true, Ordering::Release);
                return Arc::clone(current);
            }

            stream_not_found_clone.store(false, Ordering::Release);
            topic_not_found_clone.store(false, Ordering::Release);
            name_conflict_clone.store(false, Ordering::Release);

            let mut new = (**current).clone();
            let mut updated_stream = stream.clone();

            if old_meta.name != new_name_clone {
                updated_stream.topic_index = updated_stream.topic_index.without(&old_meta.name);
                updated_stream.topic_index = updated_stream
                    .topic_index
                    .update(new_name_clone.clone(), topic_id);
            }

            let updated_meta = TopicMeta {
                id: topic_id,
                name: new_name_clone.clone(),
                created_at: old_meta.created_at,
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor,
                partitions_count: old_meta.partitions_count,
                stats: old_meta.stats.clone(),
                partitions: old_meta.partitions.clone(),
                consumer_groups: old_meta.consumer_groups.clone(),
                consumer_group_index: old_meta.consumer_group_index.clone(),
                partition_counter: old_meta.partition_counter.clone(),
            };

            let (topics, _) = updated_stream.topics.update(topic_id, updated_meta);
            updated_stream.topics = topics;

            let (streams, _) = new.streams.update(stream_id, updated_stream);
            new.streams = streams;
            new.revision += 1;

            Arc::new(new)
        });

        if stream_not_found.load(Ordering::Acquire) {
            Err(IggyError::StreamIdNotFound(
                Identifier::numeric(stream_id as u32).unwrap(),
            ))
        } else if topic_not_found.load(Ordering::Acquire) {
            Err(IggyError::TopicIdNotFound(
                Identifier::numeric(topic_id as u32).unwrap(),
                Identifier::numeric(stream_id as u32).unwrap(),
            ))
        } else if name_conflict.load(Ordering::Acquire) {
            Err(IggyError::TopicNameAlreadyExists(
                new_name.to_string(),
                Identifier::numeric(stream_id as u32).unwrap(),
            ))
        } else {
            Ok(())
        }
    }

    /// Delete a topic and all its nested partitions/consumer groups.
    pub fn delete_topic(&self, stream_id: StreamId, topic_id: TopicId) {
        self.inner.rcu(|current| {
            let mut new = (**current).clone();

            if let Some(stream) = new.streams.get(stream_id) {
                let mut updated_stream = stream.clone();

                if let Some(topic) = updated_stream.topics.get(topic_id) {
                    updated_stream.topic_index = updated_stream.topic_index.without(&topic.name);
                }
                let (topics, _) = updated_stream.topics.remove(topic_id);
                updated_stream.topics = topics;

                let (streams, _) = new.streams.update(stream_id, updated_stream);
                new.streams = streams;
            }
            new.revision += 1;
            Arc::new(new)
        });
    }

    /// Add partitions with specific IDs (for bootstrap/recovery).
    pub fn add_partitions_with_ids(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        partitions: Vec<(PartitionId, PartitionMeta)>,
    ) {
        if partitions.is_empty() {
            return;
        }
        self.inner.rcu(move |current| {
            let partitions = partitions.clone();
            let mut new = (**current).clone();
            let new_version = new.revision + 1;

            if let Some(stream) = new.streams.get(stream_id) {
                let mut updated_stream = stream.clone();

                if let Some(topic) = updated_stream.topics.get(topic_id) {
                    let mut updated_topic = topic.clone();

                    let entries: Vec<_> = updated_topic
                        .partitions
                        .iter()
                        .map(|(k, v)| (k, v.clone()))
                        .chain(partitions.into_iter().map(|(id, mut meta)| {
                            meta.revision_id = new_version;
                            (id, meta)
                        }))
                        .collect();
                    updated_topic.partitions = SegmentedSlab::from_entries(entries);
                    updated_topic.partitions_count = updated_topic.partitions.len() as u32;

                    let (topics, _) = updated_stream.topics.update(topic_id, updated_topic);
                    updated_stream.topics = topics;
                }

                let (streams, _) = new.streams.update(stream_id, updated_stream);
                new.streams = streams;
            }
            new.revision = new_version;
            Arc::new(new)
        });
    }

    /// Add new partitions with slab-assigned IDs. Returns the assigned IDs.
    pub fn add_partitions(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        partitions: Vec<PartitionMeta>,
    ) -> Vec<PartitionId> {
        if partitions.is_empty() {
            return Vec::new();
        }

        let assigned_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
        let assigned_ids_clone = assigned_ids.clone();

        self.inner.rcu(move |current| {
            let partitions = partitions.clone();
            let mut new = (**current).clone();
            let new_version = new.revision + 1;

            if let Some(stream) = new.streams.get(stream_id) {
                let mut updated_stream = stream.clone();

                if let Some(topic) = updated_stream.topics.get(topic_id) {
                    let mut updated_topic = topic.clone();
                    let mut ids = Vec::new();

                    for mut meta in partitions {
                        meta.revision_id = new_version;
                        let (parts, id) = updated_topic.partitions.insert(meta.clone());

                        meta.id = id;
                        let (parts, _) = parts.update(id, meta);
                        updated_topic.partitions = parts;
                        ids.push(id);
                    }
                    updated_topic.partitions_count = updated_topic.partitions.len() as u32;

                    *assigned_ids_clone.lock().unwrap() = ids;

                    let (topics, _) = updated_stream.topics.update(topic_id, updated_topic);
                    updated_stream.topics = topics;
                }

                let (streams, _) = new.streams.update(stream_id, updated_stream);
                new.streams = streams;
            }
            new.revision = new_version;
            Arc::new(new)
        });

        Arc::try_unwrap(assigned_ids).unwrap().into_inner().unwrap()
    }

    pub fn delete_partitions(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        partition_ids: &[PartitionId],
    ) {
        if partition_ids.is_empty() {
            return;
        }
        let partition_ids = partition_ids.to_vec();

        self.inner.rcu(move |current| {
            let mut new = (**current).clone();

            if let Some(stream) = new.streams.get(stream_id) {
                let mut updated_stream = stream.clone();

                if let Some(topic) = updated_stream.topics.get(topic_id) {
                    let mut updated_topic = topic.clone();

                    for &partition_id in &partition_ids {
                        let (parts, _) = updated_topic.partitions.remove(partition_id);
                        updated_topic.partitions = parts;
                    }
                    updated_topic.partitions_count = updated_topic.partitions.len() as u32;

                    let (topics, _) = updated_stream.topics.update(topic_id, updated_topic);
                    updated_stream.topics = topics;
                }

                let (streams, _) = new.streams.update(stream_id, updated_stream);
                new.streams = streams;
            }
            new.revision += 1;
            Arc::new(new)
        });
    }

    /// Add a user with a specific ID (for bootstrap/recovery).
    pub fn add_user_with_id(&self, id: UserId, meta: UserMeta) {
        self.inner.rcu(move |current| {
            let meta = meta.clone();
            let mut new = (**current).clone();
            let entries: Vec<_> = new
                .users
                .iter()
                .map(|(k, v)| (k, v.clone()))
                .chain(std::iter::once((id as usize, meta.clone())))
                .collect();
            new.users = SegmentedSlab::from_entries(entries);
            new.user_index = new.user_index.update(meta.username.clone(), id);
            new.revision += 1;
            Arc::new(new)
        });
    }

    /// Add a new user with slab-assigned ID. Returns the assigned ID.
    pub fn add_user(&self, meta: UserMeta) -> UserId {
        let assigned_id = Arc::new(AtomicUsize::new(0));
        let assigned_id_clone = assigned_id.clone();

        self.inner.rcu(move |current| {
            let meta = meta.clone();
            let mut new = (**current).clone();
            let (users, id) = new.users.insert(meta.clone());
            assigned_id_clone.store(id, Ordering::Release);
            new.users = users;
            new.user_index = new.user_index.update(meta.username.clone(), id as UserId);
            new.revision += 1;
            Arc::new(new)
        });

        assigned_id.load(Ordering::Acquire) as UserId
    }

    /// Atomically validates username uniqueness and updates user.
    /// Returns the updated UserMeta if successful.
    pub fn try_update_user(
        &self,
        id: UserId,
        new_username: Option<Arc<str>>,
        new_status: Option<UserStatus>,
    ) -> Result<UserMeta, IggyError> {
        let user_not_found = Arc::new(AtomicBool::new(false));
        let name_conflict = Arc::new(AtomicBool::new(false));
        let updated_meta: Arc<std::sync::Mutex<Option<UserMeta>>> =
            Arc::new(std::sync::Mutex::new(None));

        let user_not_found_clone = user_not_found.clone();
        let name_conflict_clone = name_conflict.clone();
        let updated_meta_clone = updated_meta.clone();
        let new_username_clone = new_username.clone();

        self.inner.rcu(move |current| {
            let Some(old_meta) = current.users.get(id as usize) else {
                user_not_found_clone.store(true, Ordering::Release);
                return Arc::clone(current);
            };

            let final_username = new_username_clone
                .clone()
                .unwrap_or_else(|| old_meta.username.clone());

            if final_username != old_meta.username
                && let Some(&existing_id) = current.user_index.get(&final_username)
                && existing_id != id
            {
                name_conflict_clone.store(true, Ordering::Release);
                return Arc::clone(current);
            }

            user_not_found_clone.store(false, Ordering::Release);
            name_conflict_clone.store(false, Ordering::Release);

            let mut new = (**current).clone();

            if final_username != old_meta.username {
                new.user_index = new.user_index.without(&old_meta.username);
                new.user_index = new.user_index.update(final_username.clone(), id);
            }

            let meta = UserMeta {
                id: old_meta.id,
                username: final_username,
                password_hash: old_meta.password_hash.clone(),
                status: new_status.unwrap_or(old_meta.status),
                permissions: old_meta.permissions.clone(),
                created_at: old_meta.created_at,
            };

            *updated_meta_clone.lock().unwrap() = Some(meta.clone());

            let (users, _) = new.users.update(id as usize, meta);
            new.users = users;
            new.revision += 1;
            Arc::new(new)
        });

        if user_not_found.load(Ordering::Acquire) {
            Err(IggyError::ResourceNotFound(format!("User {}", id)))
        } else if name_conflict.load(Ordering::Acquire) {
            Err(IggyError::UserAlreadyExists)
        } else {
            Ok(updated_meta.lock().unwrap().take().unwrap())
        }
    }

    /// Updates user metadata directly. Use only when username is not changing
    /// or when caller has already verified username uniqueness.
    pub fn update_user_meta(&self, id: UserId, meta: UserMeta) {
        self.inner.rcu(move |current| {
            let meta = meta.clone();
            let mut new = (**current).clone();

            if let Some(old_meta) = new.users.get(id as usize)
                && old_meta.username != meta.username
            {
                new.user_index = new.user_index.without(&old_meta.username);
                new.user_index = new.user_index.update(meta.username.clone(), id);
            }

            let (users, _) = new.users.update(id as usize, meta);
            new.users = users;
            new.revision += 1;
            Arc::new(new)
        });
    }

    pub fn delete_user(&self, id: UserId) {
        self.inner.rcu(|current| {
            let mut new = (**current).clone();
            if let Some(user) = new.users.get(id as usize) {
                new.user_index = new.user_index.without(&user.username);
            }
            let (users, _) = new.users.remove(id as usize);
            new.users = users;
            new.personal_access_tokens = new.personal_access_tokens.without(&id);
            new.revision += 1;
            Arc::new(new)
        });
    }

    pub fn add_personal_access_token(&self, user_id: UserId, pat: PersonalAccessToken) {
        self.inner.rcu(move |current| {
            let pat = pat.clone();
            let mut new = (**current).clone();
            let user_pats = new
                .personal_access_tokens
                .get(&user_id)
                .cloned()
                .unwrap_or_default();
            new.personal_access_tokens = new
                .personal_access_tokens
                .update(user_id, user_pats.update(pat.token.clone(), pat));
            new.revision += 1;
            Arc::new(new)
        });
    }

    pub fn delete_personal_access_token(&self, user_id: UserId, token_hash: &Arc<str>) {
        let token_hash = token_hash.clone();
        self.inner.rcu(move |current| {
            let mut new = (**current).clone();
            if let Some(user_pats) = new.personal_access_tokens.get(&user_id) {
                new.personal_access_tokens = new
                    .personal_access_tokens
                    .update(user_id, user_pats.without(&token_hash));
            }
            new.revision += 1;
            Arc::new(new)
        });
    }

    pub fn get_user_personal_access_tokens(&self, user_id: UserId) -> Vec<PersonalAccessToken> {
        self.load()
            .personal_access_tokens
            .get(&user_id)
            .map(|pats| pats.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn get_personal_access_token_by_hash(
        &self,
        token_hash: &str,
    ) -> Option<PersonalAccessToken> {
        let token_hash_arc: Arc<str> = Arc::from(token_hash);
        let metadata = self.load();
        for (_, user_pats) in metadata.personal_access_tokens.iter() {
            if let Some(pat) = user_pats.get(&token_hash_arc) {
                return Some(pat.clone());
            }
        }
        None
    }

    pub fn user_pat_count(&self, user_id: UserId) -> usize {
        self.load()
            .personal_access_tokens
            .get(&user_id)
            .map(|pats| pats.len())
            .unwrap_or(0)
    }

    pub fn user_has_pat_with_name(&self, user_id: UserId, name: &str) -> bool {
        self.load()
            .personal_access_tokens
            .get(&user_id)
            .map(|pats| pats.values().any(|pat| &*pat.name == name))
            .unwrap_or(false)
    }

    pub fn find_pat_token_hash_by_name(&self, user_id: UserId, name: &str) -> Option<Arc<str>> {
        self.load()
            .personal_access_tokens
            .get(&user_id)
            .and_then(|pats| {
                pats.iter()
                    .find(|(_, pat)| &*pat.name == name)
                    .map(|(hash, _)| hash.clone())
            })
    }

    /// Add a consumer group with a specific ID (for bootstrap/recovery).
    pub fn add_consumer_group_with_id(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        group_id: ConsumerGroupId,
        meta: ConsumerGroupMeta,
    ) {
        self.inner.rcu(move |current| {
            let meta = meta.clone();
            let mut new = (**current).clone();

            if let Some(stream) = new.streams.get(stream_id) {
                let mut updated_stream = stream.clone();

                if let Some(topic) = updated_stream.topics.get(topic_id) {
                    let mut updated_topic = topic.clone();

                    let entries: Vec<_> = updated_topic
                        .consumer_groups
                        .iter()
                        .map(|(k, v)| (k, v.clone()))
                        .chain(std::iter::once((group_id, meta.clone())))
                        .collect();
                    updated_topic.consumer_groups = SegmentedSlab::from_entries(entries);
                    updated_topic.consumer_group_index = updated_topic
                        .consumer_group_index
                        .update(meta.name.clone(), group_id);

                    let (topics, _) = updated_stream.topics.update(topic_id, updated_topic);
                    updated_stream.topics = topics;
                }

                let (streams, _) = new.streams.update(stream_id, updated_stream);
                new.streams = streams;
            }
            new.revision += 1;
            Arc::new(new)
        });
    }

    /// Add a new consumer group with slab-assigned ID. Returns the assigned ID.
    pub fn add_consumer_group(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        meta: ConsumerGroupMeta,
    ) -> Option<ConsumerGroupId> {
        let assigned_id = Arc::new(AtomicUsize::new(usize::MAX));
        let assigned_id_clone = assigned_id.clone();

        self.inner.rcu(move |current| {
            let meta = meta.clone();
            let mut new = (**current).clone();

            if let Some(stream) = new.streams.get(stream_id) {
                let mut updated_stream = stream.clone();

                if let Some(topic) = updated_stream.topics.get(topic_id) {
                    let mut updated_topic = topic.clone();

                    let (groups, id) = updated_topic.consumer_groups.insert(meta.clone());
                    assigned_id_clone.store(id, Ordering::Release);
                    updated_topic.consumer_groups = groups;
                    updated_topic.consumer_group_index = updated_topic
                        .consumer_group_index
                        .update(meta.name.clone(), id);

                    let (topics, _) = updated_stream.topics.update(topic_id, updated_topic);
                    updated_stream.topics = topics;
                }

                let (streams, _) = new.streams.update(stream_id, updated_stream);
                new.streams = streams;
            }
            new.revision += 1;
            Arc::new(new)
        });

        let id = assigned_id.load(Ordering::Acquire);
        if id == usize::MAX { None } else { Some(id) }
    }

    pub fn delete_consumer_group(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        group_id: ConsumerGroupId,
    ) {
        self.inner.rcu(|current| {
            let mut new = (**current).clone();

            if let Some(stream) = new.streams.get(stream_id) {
                let mut updated_stream = stream.clone();

                if let Some(topic) = updated_stream.topics.get(topic_id) {
                    let mut updated_topic = topic.clone();

                    if let Some(group) = updated_topic.consumer_groups.get(group_id) {
                        updated_topic.consumer_group_index =
                            updated_topic.consumer_group_index.without(&group.name);
                    }
                    let (groups, _) = updated_topic.consumer_groups.remove(group_id);
                    updated_topic.consumer_groups = groups;

                    let (topics, _) = updated_stream.topics.update(topic_id, updated_topic);
                    updated_stream.topics = topics;
                }

                let (streams, _) = new.streams.update(stream_id, updated_stream);
                new.streams = streams;
            }
            new.revision += 1;
            Arc::new(new)
        });
    }

    pub fn join_consumer_group(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        group_id: ConsumerGroupId,
        client_id: u32,
    ) -> Option<usize> {
        let member_id = Arc::new(AtomicUsize::new(usize::MAX));
        let member_id_clone = member_id.clone();

        self.inner.rcu(|current| {
            let mut new = (**current).clone();

            if let Some(stream) = new.streams.get(stream_id) {
                let mut updated_stream = stream.clone();

                if let Some(topic) = updated_stream.topics.get(topic_id) {
                    let mut updated_topic = topic.clone();

                    if let Some(group) = updated_topic.consumer_groups.get(group_id) {
                        let mut updated_group = group.clone();

                        let next_id = updated_group
                            .members
                            .iter()
                            .map(|(_, m)| m.id)
                            .max()
                            .map(|m| m + 1)
                            .unwrap_or(0);

                        let new_member = ConsumerGroupMemberMeta::new(next_id, client_id);
                        let (members, _) = updated_group.members.insert(new_member);
                        updated_group.members = members;
                        updated_group.rebalance_members();

                        member_id_clone.store(next_id, Ordering::Release);

                        let (groups, _) = updated_topic
                            .consumer_groups
                            .update(group_id, updated_group);
                        updated_topic.consumer_groups = groups;
                    }

                    let (topics, _) = updated_stream.topics.update(topic_id, updated_topic);
                    updated_stream.topics = topics;
                }

                let (streams, _) = new.streams.update(stream_id, updated_stream);
                new.streams = streams;
            }
            new.revision += 1;
            Arc::new(new)
        });

        let id = member_id.load(Ordering::Acquire);
        if id == usize::MAX { None } else { Some(id) }
    }

    pub fn leave_consumer_group(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        group_id: ConsumerGroupId,
        client_id: u32,
    ) -> Option<usize> {
        let member_id = Arc::new(AtomicUsize::new(usize::MAX));
        let member_id_clone = member_id.clone();

        self.inner.rcu(|current| {
            let mut new = (**current).clone();

            if let Some(stream) = new.streams.get(stream_id) {
                let mut updated_stream = stream.clone();

                if let Some(topic) = updated_stream.topics.get(topic_id) {
                    let mut updated_topic = topic.clone();

                    if let Some(group) = updated_topic.consumer_groups.get(group_id) {
                        let mut updated_group = group.clone();

                        let member_to_remove: Option<usize> = updated_group
                            .members
                            .iter()
                            .find(|(_, m)| m.client_id == client_id)
                            .map(|(id, _)| id);

                        if let Some(mid) = member_to_remove {
                            member_id_clone.store(mid, Ordering::Release);
                            let (members, _) = updated_group.members.remove(mid);
                            updated_group.members = members;
                            updated_group.rebalance_members();

                            let (groups, _) = updated_topic
                                .consumer_groups
                                .update(group_id, updated_group);
                            updated_topic.consumer_groups = groups;
                        }
                    }

                    let (topics, _) = updated_stream.topics.update(topic_id, updated_topic);
                    updated_stream.topics = topics;
                }

                let (streams, _) = new.streams.update(stream_id, updated_stream);
                new.streams = streams;
            }
            new.revision += 1;
            Arc::new(new)
        });

        let id = member_id.load(Ordering::Acquire);
        if id == usize::MAX { None } else { Some(id) }
    }

    pub fn is_consumer_group_member(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        group_id: ConsumerGroupId,
        client_id: u32,
    ) -> bool {
        let metadata = self.load();
        metadata
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .and_then(|t| t.consumer_groups.get(group_id))
            .map(|g| g.members.iter().any(|(_, m)| m.client_id == client_id))
            .unwrap_or(false)
    }

    pub fn rebalance_consumer_groups_for_topic(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        partition_ids: &[PartitionId],
    ) {
        let partition_ids = partition_ids.to_vec();

        self.inner.rcu(move |current| {
            let mut new = (**current).clone();

            if let Some(stream) = new.streams.get(stream_id) {
                let mut updated_stream = stream.clone();

                if let Some(topic) = updated_stream.topics.get(topic_id) {
                    let mut updated_topic = topic.clone();

                    let group_ids: Vec<_> = updated_topic
                        .consumer_groups
                        .iter()
                        .map(|(id, _)| id)
                        .collect();

                    for gid in group_ids {
                        if let Some(group) = updated_topic.consumer_groups.get(gid) {
                            let mut updated_group = group.clone();
                            updated_group.partitions = partition_ids.clone();
                            updated_group.rebalance_members();
                            let (groups, _) =
                                updated_topic.consumer_groups.update(gid, updated_group);
                            updated_topic.consumer_groups = groups;
                        }
                    }

                    let (topics, _) = updated_stream.topics.update(topic_id, updated_topic);
                    updated_stream.topics = topics;
                }

                let (streams, _) = new.streams.update(stream_id, updated_stream);
                new.streams = streams;
            }
            new.revision += 1;
            Arc::new(new)
        });
    }

    pub fn get_stream_id(&self, identifier: &Identifier) -> Option<StreamId> {
        let metadata = self.load();
        match identifier.kind {
            IdKind::Numeric => {
                let stream_id = identifier.get_u32_value().ok()? as StreamId;
                if metadata.streams.get(stream_id).is_some() {
                    Some(stream_id)
                } else {
                    None
                }
            }
            IdKind::String => {
                let name = identifier.get_cow_str_value().ok()?;
                metadata.stream_index.get(name.as_ref()).copied()
            }
        }
    }

    pub fn stream_name_exists(&self, name: &str) -> bool {
        self.load().stream_index.contains_key(name)
    }

    pub fn get_topic_id(&self, stream_id: StreamId, identifier: &Identifier) -> Option<TopicId> {
        let metadata = self.load();
        let stream = metadata.streams.get(stream_id)?;

        match identifier.kind {
            IdKind::Numeric => {
                let topic_id = identifier.get_u32_value().ok()? as TopicId;
                if stream.topics.get(topic_id).is_some() {
                    Some(topic_id)
                } else {
                    None
                }
            }
            IdKind::String => {
                let name = identifier.get_cow_str_value().ok()?;
                stream.topic_index.get(&Arc::from(name.as_ref())).copied()
            }
        }
    }

    pub fn get_user_id(&self, identifier: &Identifier) -> Option<UserId> {
        let metadata = self.load();
        match identifier.kind {
            IdKind::Numeric => Some(identifier.get_u32_value().ok()? as UserId),
            IdKind::String => {
                let name = identifier.get_cow_str_value().ok()?;
                metadata.user_index.get(name.as_ref()).copied()
            }
        }
    }

    pub fn get_consumer_group_id(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        identifier: &Identifier,
    ) -> Option<ConsumerGroupId> {
        let metadata = self.load();
        let stream = metadata.streams.get(stream_id)?;
        let topic = stream.topics.get(topic_id)?;

        match identifier.kind {
            IdKind::Numeric => {
                let group_id = identifier.get_u32_value().ok()? as ConsumerGroupId;
                if topic.consumer_groups.get(group_id).is_some() {
                    Some(group_id)
                } else {
                    None
                }
            }
            IdKind::String => {
                let name = identifier.get_cow_str_value().ok()?;
                topic
                    .consumer_group_index
                    .get(&Arc::from(name.as_ref()))
                    .copied()
            }
        }
    }

    pub fn stream_exists(&self, id: StreamId) -> bool {
        self.load().streams.get(id).is_some()
    }

    pub fn topic_exists(&self, stream_id: StreamId, topic_id: TopicId) -> bool {
        self.load()
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .is_some()
    }

    pub fn partition_exists(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> bool {
        self.load()
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .and_then(|t| t.partitions.get(partition_id))
            .is_some()
    }

    pub fn user_exists(&self, id: UserId) -> bool {
        self.load().users.get(id as usize).is_some()
    }

    pub fn consumer_group_exists(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        group_id: ConsumerGroupId,
    ) -> bool {
        self.load()
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .and_then(|t| t.consumer_groups.get(group_id))
            .is_some()
    }

    pub fn consumer_group_exists_by_name(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        name: &str,
    ) -> bool {
        self.load()
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .map(|t| t.consumer_group_index.contains_key(name))
            .unwrap_or(false)
    }

    pub fn streams_count(&self) -> usize {
        self.load().streams.len()
    }

    pub fn topics_count(&self, stream_id: StreamId) -> usize {
        self.load()
            .streams
            .get(stream_id)
            .map(|s| s.topics.len())
            .unwrap_or(0)
    }

    pub fn partitions_count(&self, stream_id: StreamId, topic_id: TopicId) -> usize {
        self.load()
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .map(|t| t.partitions.len())
            .unwrap_or(0)
    }

    pub fn get_next_partition_id(&self, stream_id: StreamId, topic_id: TopicId) -> Option<usize> {
        let metadata = self.load();
        let topic = metadata.streams.get(stream_id)?.topics.get(topic_id)?;
        let partitions_count = topic.partitions.len();

        if partitions_count == 0 {
            return None;
        }

        let counter = &topic.partition_counter;
        let mut partition_id = counter.fetch_add(1, Ordering::AcqRel);
        if partition_id >= partitions_count {
            partition_id %= partitions_count;
            counter.store(partition_id + 1, Ordering::Relaxed);
        }
        Some(partition_id)
    }

    pub fn get_next_member_partition_id(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        group_id: ConsumerGroupId,
        member_id: usize,
        calculate: bool,
    ) -> Option<PartitionId> {
        let metadata = self.load();
        let member = metadata
            .streams
            .get(stream_id)?
            .topics
            .get(topic_id)?
            .consumer_groups
            .get(group_id)?
            .members
            .get(member_id)?;

        let assigned_partitions = &member.partitions;
        if assigned_partitions.is_empty() {
            return None;
        }

        let partitions_count = assigned_partitions.len();
        let counter = &member.partition_index;

        if calculate {
            let current = counter
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some((current + 1) % partitions_count)
                })
                .unwrap();
            Some(assigned_partitions[current % partitions_count])
        } else {
            let current = counter.load(Ordering::Relaxed);
            Some(assigned_partitions[current % partitions_count])
        }
    }

    pub fn users_count(&self) -> usize {
        self.load().users.len()
    }

    pub fn username_exists(&self, username: &str) -> bool {
        self.load().user_index.contains_key(username)
    }

    pub fn consumer_groups_count(&self, stream_id: StreamId, topic_id: TopicId) -> usize {
        self.load()
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .map(|t| t.consumer_groups.len())
            .unwrap_or(0)
    }

    pub fn get_stream_stats(&self, id: StreamId) -> Option<Arc<StreamStats>> {
        self.load().streams.get(id).map(|s| s.stats.clone())
    }

    pub fn get_topic_stats(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Option<Arc<TopicStats>> {
        self.load()
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .map(|t| t.stats.clone())
    }

    pub fn get_partition_stats(
        &self,
        ns: &crate::shard::namespace::IggyNamespace,
    ) -> Option<Arc<PartitionStats>> {
        self.load()
            .streams
            .get(ns.stream_id())
            .and_then(|s| s.topics.get(ns.topic_id()))
            .and_then(|t| t.partitions.get(ns.partition_id()))
            .map(|p| p.stats.clone())
    }

    pub fn get_partition_stats_by_ids(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Option<Arc<PartitionStats>> {
        self.load()
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .and_then(|t| t.partitions.get(partition_id))
            .map(|p| p.stats.clone())
    }

    /// Set consumer offsets for a partition
    pub fn set_partition_offsets(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        partition_id: PartitionId,
        consumer_offsets: Arc<ConsumerOffsets>,
        consumer_group_offsets: Arc<ConsumerGroupOffsets>,
    ) {
        self.inner.rcu(move |current| {
            let consumer_offsets = consumer_offsets.clone();
            let consumer_group_offsets = consumer_group_offsets.clone();
            let mut new = (**current).clone();

            if let Some(stream) = new.streams.get(stream_id) {
                let mut updated_stream = stream.clone();

                if let Some(topic) = updated_stream.topics.get(topic_id) {
                    let mut updated_topic = topic.clone();

                    if let Some(partition) = updated_topic.partitions.get(partition_id) {
                        let mut updated_partition = partition.clone();
                        updated_partition.consumer_offsets = Some(consumer_offsets);
                        updated_partition.consumer_group_offsets = Some(consumer_group_offsets);

                        let (partitions, _) = updated_topic
                            .partitions
                            .update(partition_id, updated_partition);
                        updated_topic.partitions = partitions;
                    }

                    let (topics, _) = updated_stream.topics.update(topic_id, updated_topic);
                    updated_stream.topics = topics;
                }

                let (streams, _) = new.streams.update(stream_id, updated_stream);
                new.streams = streams;
            }
            new.revision += 1;
            Arc::new(new)
        });
    }

    pub fn get_partition_consumer_offsets(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Option<Arc<crate::streaming::partitions::partition::ConsumerOffsets>> {
        self.load()
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .and_then(|t| t.partitions.get(partition_id))
            .and_then(|p| p.consumer_offsets.clone())
    }

    pub fn get_partition_consumer_group_offsets(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Option<Arc<crate::streaming::partitions::partition::ConsumerGroupOffsets>> {
        self.load()
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .and_then(|t| t.partitions.get(partition_id))
            .and_then(|p| p.consumer_group_offsets.clone())
    }

    /// Register a stream with a specific ID (for bootstrap/recovery).
    /// Unlike try_register_stream, this uses a pre-determined stream_id.
    pub fn register_stream(
        &self,
        stream_id: StreamId,
        name: Arc<str>,
        created_at: IggyTimestamp,
    ) -> Arc<StreamStats> {
        let stats = Arc::new(StreamStats::default());
        let meta = StreamMeta::with_stats(stream_id, name, created_at, stats.clone());
        self.add_stream_with_id(stream_id, meta);
        stats
    }

    /// Atomically validates name uniqueness and registers stream.
    pub fn try_register_stream(
        &self,
        name: Arc<str>,
        created_at: IggyTimestamp,
    ) -> Result<(StreamId, Arc<StreamStats>), IggyError> {
        let stats = Arc::new(StreamStats::default());

        let name_existed = Arc::new(AtomicBool::new(false));
        let assigned_id = Arc::new(AtomicUsize::new(0));
        let name_existed_clone = name_existed.clone();
        let assigned_id_clone = assigned_id.clone();
        let name_clone = name.clone();
        let stats_clone = stats.clone();

        self.inner.rcu(move |current| {
            if current.stream_index.contains_key(&name_clone) {
                name_existed_clone.store(true, Ordering::Release);
                return Arc::clone(current);
            }

            name_existed_clone.store(false, Ordering::Release);

            let mut meta =
                StreamMeta::with_stats(0, name_clone.clone(), created_at, stats_clone.clone());
            let mut new = (**current).clone();
            let (streams, id) = new.streams.insert(meta.clone());

            meta.id = id;
            let (streams, _) = streams.update(id, meta);
            assigned_id_clone.store(id, Ordering::Release);
            new.streams = streams;
            new.stream_index = new.stream_index.update(name_clone.clone(), id);
            new.revision += 1;
            Arc::new(new)
        });

        if name_existed.load(Ordering::Acquire) {
            Err(IggyError::StreamNameAlreadyExists(name.to_string()))
        } else {
            Ok((assigned_id.load(Ordering::Acquire), stats))
        }
    }

    /// Atomically validates name uniqueness and registers topic.
    #[allow(clippy::too_many_arguments)]
    pub fn try_register_topic(
        &self,
        stream_id: StreamId,
        name: Arc<str>,
        created_at: IggyTimestamp,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: u8,
        partitions_count: u32,
    ) -> Result<(TopicId, Arc<TopicStats>), IggyError> {
        let parent_stats = self.get_stream_stats(stream_id).ok_or_else(|| {
            IggyError::StreamIdNotFound(Identifier::numeric(stream_id as u32).unwrap())
        })?;

        let stats = Arc::new(TopicStats::new(parent_stats));

        let name_existed = Arc::new(AtomicBool::new(false));
        let stream_not_found = Arc::new(AtomicBool::new(false));
        let assigned_id = Arc::new(AtomicUsize::new(0));
        let name_existed_clone = name_existed.clone();
        let stream_not_found_clone = stream_not_found.clone();
        let assigned_id_clone = assigned_id.clone();
        let name_clone = name.clone();
        let stats_clone = stats.clone();

        self.inner.rcu(move |current| {
            let Some(stream) = current.streams.get(stream_id) else {
                stream_not_found_clone.store(true, Ordering::Release);
                return Arc::clone(current);
            };

            if stream.topic_index.contains_key(&name_clone) {
                name_existed_clone.store(true, Ordering::Release);
                return Arc::clone(current);
            }

            name_existed_clone.store(false, Ordering::Release);
            stream_not_found_clone.store(false, Ordering::Release);

            let mut meta = TopicMeta {
                id: 0,
                name: name_clone.clone(),
                created_at,
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor,
                partitions_count,
                stats: stats_clone.clone(),
                partitions: SegmentedSlab::new(),
                consumer_groups: SegmentedSlab::new(),
                consumer_group_index: imbl::HashMap::new(),
                partition_counter: Arc::new(AtomicUsize::new(0)),
            };

            let mut new = (**current).clone();
            let mut updated_stream = stream.clone();
            let (topics, id) = updated_stream.topics.insert(meta.clone());

            meta.id = id;
            let (topics, _) = topics.update(id, meta);
            assigned_id_clone.store(id, Ordering::Release);
            updated_stream.topics = topics;
            updated_stream.topic_index = updated_stream.topic_index.update(name_clone.clone(), id);

            let (streams, _) = new.streams.update(stream_id, updated_stream);
            new.streams = streams;
            new.revision += 1;
            Arc::new(new)
        });

        if stream_not_found.load(Ordering::Acquire) {
            Err(IggyError::StreamIdNotFound(
                Identifier::numeric(stream_id as u32).unwrap(),
            ))
        } else if name_existed.load(Ordering::Acquire) {
            Err(IggyError::TopicNameAlreadyExists(
                name.to_string(),
                Identifier::numeric(stream_id as u32).unwrap(),
            ))
        } else {
            Ok((assigned_id.load(Ordering::Acquire), stats))
        }
    }

    /// Register a topic with a specific ID (for bootstrap/recovery).
    /// Unlike try_register_topic, this uses a pre-determined topic_id.
    #[allow(clippy::too_many_arguments)]
    pub fn register_topic(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        name: Arc<str>,
        created_at: IggyTimestamp,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: u8,
        partitions_count: u32,
    ) -> Arc<TopicStats> {
        let parent_stats = self
            .get_stream_stats(stream_id)
            .expect("Stream must exist when registering topic");

        let stats = Arc::new(TopicStats::new(parent_stats));

        let stats_clone = stats.clone();
        let name_clone = name.clone();

        self.inner.rcu(move |current| {
            let Some(stream) = current.streams.get(stream_id) else {
                return Arc::clone(current);
            };

            let meta = TopicMeta {
                id: topic_id,
                name: name_clone.clone(),
                created_at,
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor,
                partitions_count,
                stats: stats_clone.clone(),
                partitions: SegmentedSlab::new(),
                consumer_groups: SegmentedSlab::new(),
                consumer_group_index: imbl::HashMap::new(),
                partition_counter: Arc::new(AtomicUsize::new(0)),
            };

            let mut new = (**current).clone();
            let mut updated_stream = stream.clone();
            let entries: Vec<_> = updated_stream
                .topics
                .iter()
                .map(|(k, v)| (k, v.clone()))
                .chain(std::iter::once((topic_id, meta)))
                .collect();
            updated_stream.topics = SegmentedSlab::from_entries(entries);
            updated_stream.topic_index = updated_stream
                .topic_index
                .update(name_clone.clone(), topic_id);

            let (streams, _) = new.streams.update(stream_id, updated_stream);
            new.streams = streams;
            new.revision += 1;
            Arc::new(new)
        });

        stats
    }

    /// Atomically validates username uniqueness and registers user.
    pub fn try_register_user(
        &self,
        username: Arc<str>,
        password_hash: Arc<str>,
        status: iggy_common::UserStatus,
        permissions: Option<Arc<iggy_common::Permissions>>,
        max_users: usize,
    ) -> Result<UserId, IggyError> {
        let name_existed = Arc::new(AtomicBool::new(false));
        let limit_reached = Arc::new(AtomicBool::new(false));
        let assigned_id = Arc::new(AtomicUsize::new(0));
        let name_existed_clone = name_existed.clone();
        let limit_reached_clone = limit_reached.clone();
        let assigned_id_clone = assigned_id.clone();
        let username_clone = username.clone();

        self.inner.rcu(move |current| {
            if current.user_index.contains_key(&username_clone) {
                name_existed_clone.store(true, Ordering::Release);
                return Arc::clone(current);
            }

            if current.users.len() >= max_users {
                limit_reached_clone.store(true, Ordering::Release);
                return Arc::clone(current);
            }

            name_existed_clone.store(false, Ordering::Release);
            limit_reached_clone.store(false, Ordering::Release);

            let mut meta = UserMeta {
                id: 0,
                username: username_clone.clone(),
                password_hash: password_hash.clone(),
                status,
                permissions: permissions.clone(),
                created_at: IggyTimestamp::now(),
            };

            let mut new = (**current).clone();
            let (users, id) = new.users.insert(meta.clone());

            meta.id = id as UserId;
            let (users, _) = users.update(id, meta);
            assigned_id_clone.store(id, Ordering::Release);
            new.users = users;
            new.user_index = new.user_index.update(username_clone.clone(), id as UserId);
            new.revision += 1;
            Arc::new(new)
        });

        if name_existed.load(Ordering::Acquire) {
            Err(IggyError::UserAlreadyExists)
        } else if limit_reached.load(Ordering::Acquire) {
            Err(IggyError::UsersLimitReached)
        } else {
            Ok(assigned_id.load(Ordering::Acquire) as UserId)
        }
    }

    /// Atomically validates consumer group name uniqueness and registers the group.
    pub fn try_register_consumer_group(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        name: Arc<str>,
        partitions: Vec<PartitionId>,
    ) -> Result<ConsumerGroupId, IggyError> {
        let name_existed = Arc::new(AtomicBool::new(false));
        let not_found = Arc::new(AtomicBool::new(false));
        let assigned_id = Arc::new(AtomicUsize::new(0));
        let name_existed_clone = name_existed.clone();
        let not_found_clone = not_found.clone();
        let assigned_id_clone = assigned_id.clone();
        let name_clone = name.clone();

        self.inner.rcu(move |current| {
            let Some(stream) = current.streams.get(stream_id) else {
                not_found_clone.store(true, Ordering::Release);
                return Arc::clone(current);
            };

            let Some(topic) = stream.topics.get(topic_id) else {
                not_found_clone.store(true, Ordering::Release);
                return Arc::clone(current);
            };

            if topic.consumer_group_index.contains_key(&name_clone) {
                name_existed_clone.store(true, Ordering::Release);
                return Arc::clone(current);
            }

            name_existed_clone.store(false, Ordering::Release);
            not_found_clone.store(false, Ordering::Release);

            let mut meta = ConsumerGroupMeta {
                id: 0,
                name: name_clone.clone(),
                partitions: partitions.clone(),
                members: SegmentedSlab::new(),
            };

            let mut new = (**current).clone();
            let mut updated_stream = stream.clone();
            let mut updated_topic = topic.clone();

            let (groups, id) = updated_topic.consumer_groups.insert(meta.clone());

            meta.id = id;
            let (groups, _) = groups.update(id, meta);
            assigned_id_clone.store(id, Ordering::Release);
            updated_topic.consumer_groups = groups;
            updated_topic.consumer_group_index = updated_topic
                .consumer_group_index
                .update(name_clone.clone(), id);

            let (topics, _) = updated_stream.topics.update(topic_id, updated_topic);
            updated_stream.topics = topics;

            let (streams, _) = new.streams.update(stream_id, updated_stream);
            new.streams = streams;
            new.revision += 1;
            Arc::new(new)
        });

        if not_found.load(Ordering::Acquire) {
            Err(IggyError::TopicIdNotFound(
                Identifier::numeric(topic_id as u32).unwrap(),
                Identifier::numeric(stream_id as u32).unwrap(),
            ))
        } else if name_existed.load(Ordering::Acquire) {
            Err(IggyError::ConsumerGroupNameAlreadyExists(
                name.to_string(),
                Identifier::numeric(topic_id as u32).unwrap(),
            ))
        } else {
            Ok(assigned_id.load(Ordering::Acquire))
        }
    }

    pub fn register_partitions(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        count: usize,
        created_at: IggyTimestamp,
    ) -> Vec<Arc<PartitionStats>> {
        if count == 0 {
            return Vec::new();
        }

        let parent_stats = self
            .get_topic_stats(stream_id, topic_id)
            .expect("Parent topic stats must exist before registering partitions");

        let mut stats_list = Vec::with_capacity(count);
        let mut metas = Vec::with_capacity(count);

        for _ in 0..count {
            let stats = Arc::new(PartitionStats::new(parent_stats.clone()));
            metas.push(PartitionMeta {
                id: 0,
                created_at,
                revision_id: 0,
                stats: stats.clone(),
                consumer_offsets: None,
                consumer_group_offsets: None,
            });
            stats_list.push(stats);
        }

        self.add_partitions(stream_id, topic_id, metas);
        stats_list
    }

    /// Register a single partition with a specific ID (for bootstrap/recovery).
    pub fn register_partition(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        partition_id: PartitionId,
        created_at: IggyTimestamp,
    ) -> Arc<PartitionStats> {
        let parent_stats = self
            .get_topic_stats(stream_id, topic_id)
            .expect("Parent topic stats must exist before registering partition");

        let stats = Arc::new(PartitionStats::new(parent_stats));
        let meta = PartitionMeta {
            id: partition_id,
            created_at,
            revision_id: 0,
            stats: stats.clone(),
            consumer_offsets: None,
            consumer_group_offsets: None,
        };

        self.add_partitions_with_ids(stream_id, topic_id, vec![(partition_id, meta)]);
        stats
    }

    pub fn get_user(&self, id: UserId) -> Option<UserMeta> {
        self.load().users.get(id as usize).cloned()
    }

    pub fn get_all_users(&self) -> Vec<UserMeta> {
        self.load().users.iter().map(|(_, u)| u.clone()).collect()
    }

    pub fn get_stream(&self, id: StreamId) -> Option<StreamMeta> {
        self.load().streams.get(id).cloned()
    }

    pub fn get_topic(&self, stream_id: StreamId, topic_id: TopicId) -> Option<TopicMeta> {
        self.load()
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id).cloned())
    }

    pub fn get_partition(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Option<PartitionMeta> {
        self.load()
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .and_then(|t| t.partitions.get(partition_id).cloned())
    }

    pub fn get_consumer_group(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        group_id: ConsumerGroupId,
    ) -> Option<ConsumerGroupMeta> {
        self.load()
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .and_then(|t| t.consumer_groups.get(group_id).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_crud() {
        let metadata = Metadata::default();

        let stream_meta = StreamMeta::with_stats(
            0,
            Arc::from("test-stream"),
            IggyTimestamp::now(),
            Arc::new(StreamStats::default()),
        );
        let id = metadata.add_stream(stream_meta);

        assert!(metadata.stream_exists(id));
        assert_eq!(metadata.streams_count(), 1);

        metadata
            .try_update_stream(id, Arc::from("renamed-stream"))
            .unwrap();
        let loaded = metadata.load();
        assert_eq!(
            loaded.streams.get(id).unwrap().name.as_ref(),
            "renamed-stream"
        );
        assert!(loaded.stream_index.contains_key("renamed-stream"));
        assert!(!loaded.stream_index.contains_key("test-stream"));

        metadata.delete_stream(id);
        assert!(!metadata.stream_exists(id));
        assert_eq!(metadata.streams_count(), 0);
    }

    #[test]
    fn test_cascade_delete() {
        let metadata = Metadata::default();

        let stream_stats = Arc::new(StreamStats::default());
        let stream_meta = StreamMeta::with_stats(
            0,
            Arc::from("stream-1"),
            IggyTimestamp::now(),
            stream_stats.clone(),
        );
        let stream_id = metadata.add_stream(stream_meta);

        let topic_stats = Arc::new(TopicStats::new(stream_stats));
        let topic_meta = TopicMeta {
            id: 0,
            name: Arc::from("topic-1"),
            created_at: IggyTimestamp::now(),
            message_expiry: IggyExpiry::NeverExpire,
            compression_algorithm: CompressionAlgorithm::None,
            max_topic_size: MaxTopicSize::Unlimited,
            replication_factor: 1,
            partitions_count: 0,
            stats: topic_stats.clone(),
            partitions: SegmentedSlab::new(),
            consumer_groups: SegmentedSlab::new(),
            consumer_group_index: imbl::HashMap::new(),
            partition_counter: Arc::new(AtomicUsize::new(0)),
        };
        let topic_id = metadata.add_topic(stream_id, topic_meta).unwrap();

        let partitions = vec![
            PartitionMeta {
                id: 0,
                created_at: IggyTimestamp::now(),
                revision_id: 0,
                stats: Arc::new(PartitionStats::new(topic_stats.clone())),
                consumer_offsets: None,
                consumer_group_offsets: None,
            },
            PartitionMeta {
                id: 0,
                created_at: IggyTimestamp::now(),
                revision_id: 0,
                stats: Arc::new(PartitionStats::new(topic_stats)),
                consumer_offsets: None,
                consumer_group_offsets: None,
            },
        ];
        let partition_ids = metadata.add_partitions(stream_id, topic_id, partitions);

        assert!(metadata.stream_exists(stream_id));
        assert!(metadata.topic_exists(stream_id, topic_id));
        assert!(metadata.partition_exists(stream_id, topic_id, partition_ids[0]));
        assert!(metadata.partition_exists(stream_id, topic_id, partition_ids[1]));

        metadata.delete_stream(stream_id);

        assert!(!metadata.stream_exists(stream_id));
        assert!(!metadata.topic_exists(stream_id, topic_id));
        assert!(!metadata.partition_exists(stream_id, topic_id, partition_ids[0]));
        assert!(!metadata.partition_exists(stream_id, topic_id, partition_ids[1]));
    }
}
