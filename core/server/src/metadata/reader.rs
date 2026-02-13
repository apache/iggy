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
    ConsumerGroupId, ConsumerGroupMeta, InnerMetadata, MetadataReadHandle, PartitionId,
    PartitionMeta, StreamId, StreamMeta, TopicId, TopicMeta, UserId, UserMeta,
};
use crate::shard::transmission::message::{ResolvedPartition, ResolvedTopic};
use crate::streaming::partitions::consumer_group_offsets::ConsumerGroupOffsets;
use crate::streaming::partitions::consumer_offsets::ConsumerOffsets;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::stats::{PartitionStats, StreamStats, TopicStats};
use iggy_common::sharding::IggyNamespace;
use iggy_common::{
    IdKind, Identifier, IggyError, IggyExpiry, IggyTimestamp, MaxTopicSize, PersonalAccessToken,
};
use left_right::ReadGuard;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// Thread-safe wrapper for GlobalMetadata using left-right for lock-free reads.
/// Uses hierarchical structure: streams contain topics, topics contain partitions and consumer groups.
/// All mutations go through MetadataWriter (shard 0 only).
///
/// Each shard should own its own `Metadata` instance (cloned from a common source).
/// The underlying data is shared via left-right's internal mechanism.
#[derive(Clone)]
pub struct Metadata {
    inner: MetadataReadHandle,
}

impl Metadata {
    pub fn new(reader: MetadataReadHandle) -> Self {
        Self { inner: reader }
    }

    #[inline]
    pub(super) fn load(&self) -> ReadGuard<'_, InnerMetadata> {
        self.inner
            .enter()
            .expect("metadata not initialized - writer must publish before reads")
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

    pub fn next_stream_id(&self) -> usize {
        self.load().streams.vacant_key()
    }

    pub fn topics_count(&self, stream_id: StreamId) -> usize {
        self.load()
            .streams
            .get(stream_id)
            .map(|s| s.topics.len())
            .unwrap_or(0)
    }

    pub fn next_topic_id(&self, stream_id: StreamId) -> Option<usize> {
        self.load()
            .streams
            .get(stream_id)
            .map(|s| s.topics.vacant_key())
    }

    pub fn partitions_count(&self, stream_id: StreamId, topic_id: TopicId) -> usize {
        self.load()
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .map(|t| t.partitions.len())
            .unwrap_or(0)
    }

    pub fn get_partitions_count(&self, stream_id: StreamId, topic_id: TopicId) -> Option<usize> {
        self.load()
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .map(|t| t.partitions.len())
    }

    pub fn get_next_partition_id(&self, stream_id: StreamId, topic_id: TopicId) -> Option<usize> {
        let metadata = self.load();
        let topic = metadata.streams.get(stream_id)?.topics.get(topic_id)?;
        let partitions_count = topic.partitions.len();

        if partitions_count == 0 {
            return None;
        }

        let counter = &topic.round_robin_counter;
        let current = counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                Some((c + 1) % partitions_count)
            })
            .unwrap();
        Some(current % partitions_count)
    }

    /// Resolve consumer group partition under a single metadata read guard.
    pub fn resolve_consumer_group_partition(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        group_identifier: &Identifier,
        client_id: u32,
        explicit_partition_id: Option<u32>,
        calculate_partition_id: bool,
    ) -> Result<Option<(PollingConsumer, usize)>, IggyError> {
        let metadata = self.load();

        // Step 1: Resolve group ID
        let group_id = {
            let stream = metadata.streams.get(stream_id).ok_or_else(|| {
                IggyError::ConsumerGroupIdNotFound(
                    group_identifier.clone(),
                    Identifier::numeric(topic_id as u32).unwrap(),
                )
            })?;
            let topic = stream.topics.get(topic_id).ok_or_else(|| {
                IggyError::ConsumerGroupIdNotFound(
                    group_identifier.clone(),
                    Identifier::numeric(topic_id as u32).unwrap(),
                )
            })?;
            match group_identifier.kind {
                IdKind::Numeric => {
                    let gid = group_identifier.get_u32_value().map_err(|_| {
                        IggyError::ConsumerGroupIdNotFound(
                            group_identifier.clone(),
                            Identifier::numeric(topic_id as u32).unwrap(),
                        )
                    })? as ConsumerGroupId;
                    if topic.consumer_groups.get(gid).is_none() {
                        return Err(IggyError::ConsumerGroupIdNotFound(
                            group_identifier.clone(),
                            Identifier::numeric(topic_id as u32).unwrap(),
                        ));
                    }
                    gid
                }
                IdKind::String => {
                    let name = group_identifier.get_cow_str_value().map_err(|_| {
                        IggyError::ConsumerGroupIdNotFound(
                            group_identifier.clone(),
                            Identifier::numeric(topic_id as u32).unwrap(),
                        )
                    })?;
                    *topic
                        .consumer_group_index
                        .get(name.as_ref())
                        .ok_or_else(|| {
                            IggyError::ConsumerGroupIdNotFound(
                                group_identifier.clone(),
                                Identifier::numeric(topic_id as u32).unwrap(),
                            )
                        })?
                }
            }
        };

        // Step 2: Find member by client_id (same read guard, same metadata snapshot)
        let group = metadata
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .and_then(|t| t.consumer_groups.get(group_id))
            .ok_or_else(|| {
                IggyError::ConsumerGroupIdNotFound(
                    group_identifier.clone(),
                    Identifier::numeric(topic_id as u32).unwrap(),
                )
            })?;

        let (member_slab_id, member) = group
            .members
            .iter()
            .find(|(_, m)| m.client_id == client_id)
            .ok_or_else(|| {
                IggyError::ConsumerGroupMemberNotFound(
                    client_id,
                    group_identifier.clone(),
                    Identifier::numeric(topic_id as u32).unwrap(),
                )
            })?;

        // Step 3a: If explicit partition_id provided, validate member owns it
        if let Some(pid) = explicit_partition_id {
            let pid_usize = pid as usize;
            if !member.partitions.contains(&pid_usize) {
                // Member doesn't own this partition — check if it's pending revocation
                let is_pending = member
                    .pending_revocations
                    .iter()
                    .any(|revocation| revocation.partition_id == pid_usize);
                if !is_pending {
                    return Ok(None);
                }
            }
            return Ok(Some((
                PollingConsumer::consumer_group(group_id, member_slab_id),
                pid_usize,
            )));
        }

        // Step 3b: Round-robin partition selection (same snapshot, no race)
        if member.pending_revocations.is_empty() {
            // Fast path
            let partitions = &member.partitions;
            let count = partitions.len();
            if count == 0 {
                return Ok(None);
            }
            let counter = &member.partition_index;
            let partition_id = if calculate_partition_id {
                let current = counter
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                        Some((c + 1) % count)
                    })
                    .unwrap();
                partitions[current % count]
            } else {
                let current = counter.load(Ordering::Relaxed);
                partitions[current % count]
            };
            return Ok(Some((
                PollingConsumer::consumer_group(group_id, member_slab_id),
                partition_id,
            )));
        }

        // Slow path: skip revoked partitions
        let effective_count = member.partitions.len() - member.pending_revocations.len();
        if effective_count == 0 {
            return Ok(None);
        }

        let counter = &member.partition_index;
        let idx = if calculate_partition_id {
            counter
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                    Some((c + 1) % effective_count)
                })
                .unwrap()
                % effective_count
        } else {
            counter.load(Ordering::Relaxed) % effective_count
        };

        let mut seen = 0;
        for &pid in &member.partitions {
            let is_revoked = member
                .pending_revocations
                .iter()
                .any(|revocation| revocation.partition_id == pid);
            if is_revoked {
                continue;
            }
            if seen == idx {
                return Ok(Some((
                    PollingConsumer::consumer_group(group_id, member_slab_id),
                    pid,
                )));
            }
            seen += 1;
        }

        Ok(None)
    }

    /// Record the last offset returned to a CG member during poll.
    pub fn record_polled_offset(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        group_id: ConsumerGroupId,
        partition_id: PartitionId,
        offset: u64,
    ) {
        use crate::streaming::polling_consumer::ConsumerGroupId as CgIdNewtype;

        let metadata = self.load();
        if let Some(partition) = metadata
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .and_then(|t| t.partitions.get(partition_id))
        {
            let key = CgIdNewtype(group_id);
            let guard = partition.last_polled_offsets.pin();
            match guard.get(&key) {
                Some(existing) => {
                    existing.store(offset, Ordering::Release);
                }
                None => {
                    guard.insert(key, Arc::new(std::sync::atomic::AtomicU64::new(offset)));
                }
            }
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

    pub fn get_partition_stats(&self, ns: &IggyNamespace) -> Option<Arc<PartitionStats>> {
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

    pub fn get_partition_consumer_offsets(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Option<Arc<ConsumerOffsets>> {
        self.load()
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .and_then(|t| t.partitions.get(partition_id))
            .map(|p| p.consumer_offsets.clone())
    }

    pub fn get_partition_consumer_group_offsets(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Option<Arc<ConsumerGroupOffsets>> {
        self.load()
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .and_then(|t| t.partitions.get(partition_id))
            .map(|p| p.consumer_group_offsets.clone())
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

    /// Execute a closure with read access to the metadata snapshot.
    /// This is the safe way to perform complex read operations that need
    /// atomic access to multiple metadata fields.
    ///
    /// The closure receives an immutable reference to the metadata and must
    /// return owned data (not references). This ensures the ReadGuard is
    /// dropped before any async operations can occur.
    #[inline]
    pub fn with_metadata<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&InnerMetadata) -> R,
    {
        let guard = self.load();
        f(&guard)
    }

    /// Get all partition IDs for a topic, sorted.
    pub fn get_partition_ids(&self, stream_id: StreamId, topic_id: TopicId) -> Vec<PartitionId> {
        self.with_metadata(|m| {
            m.streams
                .get(stream_id)
                .and_then(|s| s.topics.get(topic_id))
                .map(|t| {
                    let mut ids: Vec<_> = t.partitions.iter().enumerate().map(|(k, _)| k).collect();
                    ids.sort_unstable();
                    ids
                })
                .unwrap_or_default()
        })
    }

    /// Get all topic IDs for a stream, sorted.
    pub fn get_topic_ids(&self, stream_id: StreamId) -> Vec<TopicId> {
        self.with_metadata(|m| {
            m.streams
                .get(stream_id)
                .map(|s| {
                    let mut ids: Vec<_> = s.topics.iter().map(|(k, _)| k).collect();
                    ids.sort_unstable();
                    ids
                })
                .unwrap_or_default()
        })
    }

    /// Get all stream IDs, sorted.
    pub fn get_stream_ids(&self) -> Vec<StreamId> {
        self.with_metadata(|m| {
            let mut ids: Vec<_> = m.streams.iter().map(|(k, _)| k).collect();
            ids.sort_unstable();
            ids
        })
    }

    /// Get all namespaces (stream/topic/partition combinations).
    pub fn get_all_namespaces(&self) -> Vec<IggyNamespace> {
        self.with_metadata(|m| {
            let mut namespaces = Vec::new();
            for (stream_id, stream) in m.streams.iter() {
                for (topic_id, topic) in stream.topics.iter() {
                    for (partition_id, _) in topic.partitions.iter().enumerate() {
                        namespaces.push(IggyNamespace::new(stream_id, topic_id, partition_id));
                    }
                }
            }
            namespaces
        })
    }

    /// Get topic configuration (message_expiry, max_topic_size).
    pub fn get_topic_config(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Option<(IggyExpiry, MaxTopicSize)> {
        self.with_metadata(|m| {
            m.streams
                .get(stream_id)
                .and_then(|s| s.topics.get(topic_id))
                .map(|t| (t.message_expiry, t.max_topic_size))
        })
    }

    /// Get partition initialization info needed for LocalPartition setup.
    pub fn get_partition_init_info(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Option<PartitionInitInfo> {
        self.with_metadata(|m| {
            m.streams
                .get(stream_id)
                .and_then(|s| s.topics.get(topic_id))
                .and_then(|t| t.partitions.get(partition_id))
                .map(|p| PartitionInitInfo {
                    created_at: p.created_at,
                    revision_id: p.revision_id,
                    stats: p.stats.clone(),
                    consumer_offsets: p.consumer_offsets.clone(),
                    consumer_group_offsets: p.consumer_group_offsets.clone(),
                })
        })
    }

    /// Get consumer group member ID for a client.
    pub fn get_consumer_group_member_id(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
        group_id: ConsumerGroupId,
        client_id: u32,
    ) -> Option<usize> {
        self.with_metadata(|m| {
            m.streams
                .get(stream_id)
                .and_then(|s| s.topics.get(topic_id))
                .and_then(|t| t.consumer_groups.get(group_id))
                .and_then(|g| {
                    g.members
                        .iter()
                        .find(|(_, member)| member.client_id == client_id)
                        .map(|(id, _)| id)
                })
        })
    }

    /// Get all consumer groups for a topic.
    pub fn get_all_consumer_groups(
        &self,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Vec<ConsumerGroupMeta> {
        self.with_metadata(|m| {
            m.streams
                .get(stream_id)
                .and_then(|s| s.topics.get(topic_id))
                .map(|t| t.consumer_groups.iter().map(|(_, cg)| cg.clone()).collect())
                .unwrap_or_default()
        })
    }

    /// Inheritance: manage_streams → read_streams → read_topics → poll_messages
    pub fn perm_poll_messages(
        &self,
        user_id: u32,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Result<(), IggyError> {
        let metadata = self.load();

        if metadata.users_can_poll_all_streams.contains(&user_id) {
            return Ok(());
        }

        if let Some(global) = metadata.users_global_permissions.get(&user_id)
            && (global.read_topics
                || global.manage_topics
                || global.read_streams
                || global.manage_streams)
        {
            return Ok(());
        }

        if metadata
            .users_can_poll_stream
            .contains(&(user_id, stream_id))
        {
            return Ok(());
        }

        let Some(stream_permissions) = metadata.users_stream_permissions.get(&(user_id, stream_id))
        else {
            return Err(IggyError::Unauthorized);
        };

        if stream_permissions.manage_stream || stream_permissions.read_stream {
            return Ok(());
        }

        if stream_permissions.manage_topics || stream_permissions.read_topics {
            return Ok(());
        }

        if stream_permissions.poll_messages {
            return Ok(());
        }

        if let Some(topics) = &stream_permissions.topics
            && let Some(topic_permissions) = topics.get(&topic_id)
            && (topic_permissions.manage_topic
                || topic_permissions.read_topic
                || topic_permissions.poll_messages)
        {
            return Ok(());
        }

        Err(IggyError::Unauthorized)
    }

    /// Inheritance: manage_streams → manage_topics → send_messages
    pub fn perm_append_messages(
        &self,
        user_id: u32,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Result<(), IggyError> {
        let metadata = self.load();

        if metadata.users_can_send_all_streams.contains(&user_id) {
            return Ok(());
        }

        if let Some(global) = metadata.users_global_permissions.get(&user_id)
            && (global.manage_streams || global.manage_topics)
        {
            return Ok(());
        }

        if metadata
            .users_can_send_stream
            .contains(&(user_id, stream_id))
        {
            return Ok(());
        }

        let Some(stream_permissions) = metadata.users_stream_permissions.get(&(user_id, stream_id))
        else {
            return Err(IggyError::Unauthorized);
        };

        if stream_permissions.manage_stream || stream_permissions.manage_topics {
            return Ok(());
        }

        if stream_permissions.send_messages {
            return Ok(());
        }

        if let Some(topics) = &stream_permissions.topics
            && let Some(topic_permissions) = topics.get(&topic_id)
            && (topic_permissions.manage_topic || topic_permissions.send_messages)
        {
            return Ok(());
        }

        Err(IggyError::Unauthorized)
    }

    pub fn perm_get_stream(&self, user_id: u32, stream_id: StreamId) -> Result<(), IggyError> {
        let metadata = self.load();

        if let Some(global_permissions) = metadata.users_global_permissions.get(&user_id)
            && (global_permissions.manage_streams || global_permissions.read_streams)
        {
            return Ok(());
        }

        if let Some(stream_permissions) =
            metadata.users_stream_permissions.get(&(user_id, stream_id))
            && (stream_permissions.manage_stream || stream_permissions.read_stream)
        {
            return Ok(());
        }

        Err(IggyError::Unauthorized)
    }

    pub fn perm_get_streams(&self, user_id: u32) -> Result<(), IggyError> {
        let metadata = self.load();

        if let Some(global_permissions) = metadata.users_global_permissions.get(&user_id)
            && (global_permissions.manage_streams || global_permissions.read_streams)
        {
            return Ok(());
        }

        Err(IggyError::Unauthorized)
    }

    pub fn perm_create_stream(&self, user_id: u32) -> Result<(), IggyError> {
        let metadata = self.load();

        if let Some(global_permissions) = metadata.users_global_permissions.get(&user_id)
            && global_permissions.manage_streams
        {
            return Ok(());
        }

        Err(IggyError::Unauthorized)
    }

    pub fn perm_update_stream(&self, user_id: u32, stream_id: StreamId) -> Result<(), IggyError> {
        self.perm_manage_stream(user_id, stream_id)
    }

    pub fn perm_delete_stream(&self, user_id: u32, stream_id: StreamId) -> Result<(), IggyError> {
        self.perm_manage_stream(user_id, stream_id)
    }

    pub fn perm_purge_stream(&self, user_id: u32, stream_id: StreamId) -> Result<(), IggyError> {
        self.perm_manage_stream(user_id, stream_id)
    }

    fn perm_manage_stream(&self, user_id: u32, stream_id: StreamId) -> Result<(), IggyError> {
        let metadata = self.load();

        if let Some(global_permissions) = metadata.users_global_permissions.get(&user_id)
            && global_permissions.manage_streams
        {
            return Ok(());
        }

        if let Some(stream_permissions) =
            metadata.users_stream_permissions.get(&(user_id, stream_id))
            && stream_permissions.manage_stream
        {
            return Ok(());
        }

        Err(IggyError::Unauthorized)
    }

    /// Inheritance: manage_streams → read_streams → read_topics
    pub fn perm_get_topic(
        &self,
        user_id: u32,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Result<(), IggyError> {
        let metadata = self.load();

        if let Some(global) = metadata.users_global_permissions.get(&user_id)
            && (global.read_streams
                || global.manage_streams
                || global.manage_topics
                || global.read_topics)
        {
            return Ok(());
        }

        if let Some(stream_permissions) =
            metadata.users_stream_permissions.get(&(user_id, stream_id))
        {
            if stream_permissions.manage_stream
                || stream_permissions.read_stream
                || stream_permissions.manage_topics
                || stream_permissions.read_topics
            {
                return Ok(());
            }

            if let Some(topics) = &stream_permissions.topics
                && let Some(topic_permissions) = topics.get(&topic_id)
                && (topic_permissions.manage_topic || topic_permissions.read_topic)
            {
                return Ok(());
            }
        }

        Err(IggyError::Unauthorized)
    }

    pub fn perm_get_topics(&self, user_id: u32, stream_id: StreamId) -> Result<(), IggyError> {
        let metadata = self.load();

        if let Some(global) = metadata.users_global_permissions.get(&user_id)
            && (global.read_streams
                || global.manage_streams
                || global.manage_topics
                || global.read_topics)
        {
            return Ok(());
        }

        if let Some(stream_permissions) =
            metadata.users_stream_permissions.get(&(user_id, stream_id))
            && (stream_permissions.manage_stream
                || stream_permissions.read_stream
                || stream_permissions.manage_topics
                || stream_permissions.read_topics)
        {
            return Ok(());
        }

        Err(IggyError::Unauthorized)
    }

    /// Inheritance: manage_streams → manage_topics
    pub fn perm_create_topic(&self, user_id: u32, stream_id: StreamId) -> Result<(), IggyError> {
        let metadata = self.load();

        if let Some(global) = metadata.users_global_permissions.get(&user_id)
            && (global.manage_streams || global.manage_topics)
        {
            return Ok(());
        }

        if let Some(stream_permissions) =
            metadata.users_stream_permissions.get(&(user_id, stream_id))
            && (stream_permissions.manage_stream || stream_permissions.manage_topics)
        {
            return Ok(());
        }

        Err(IggyError::Unauthorized)
    }

    pub fn perm_update_topic(
        &self,
        user_id: u32,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Result<(), IggyError> {
        self.perm_manage_topic(user_id, stream_id, topic_id)
    }

    pub fn perm_delete_topic(
        &self,
        user_id: u32,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Result<(), IggyError> {
        self.perm_manage_topic(user_id, stream_id, topic_id)
    }

    pub fn perm_purge_topic(
        &self,
        user_id: u32,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Result<(), IggyError> {
        self.perm_manage_topic(user_id, stream_id, topic_id)
    }

    /// Inheritance: manage_streams → manage_topics
    fn perm_manage_topic(
        &self,
        user_id: u32,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Result<(), IggyError> {
        let metadata = self.load();

        if let Some(global) = metadata.users_global_permissions.get(&user_id)
            && (global.manage_streams || global.manage_topics)
        {
            return Ok(());
        }

        if let Some(stream_permissions) =
            metadata.users_stream_permissions.get(&(user_id, stream_id))
        {
            if stream_permissions.manage_stream || stream_permissions.manage_topics {
                return Ok(());
            }

            if let Some(topics) = &stream_permissions.topics
                && let Some(topic_permissions) = topics.get(&topic_id)
                && topic_permissions.manage_topic
            {
                return Ok(());
            }
        }

        Err(IggyError::Unauthorized)
    }

    pub fn perm_create_partitions(
        &self,
        user_id: u32,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Result<(), IggyError> {
        self.perm_update_topic(user_id, stream_id, topic_id)
    }

    pub fn perm_delete_partitions(
        &self,
        user_id: u32,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Result<(), IggyError> {
        self.perm_update_topic(user_id, stream_id, topic_id)
    }

    pub fn perm_delete_segments(
        &self,
        user_id: u32,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Result<(), IggyError> {
        self.perm_update_topic(user_id, stream_id, topic_id)
    }

    pub fn perm_create_consumer_group(
        &self,
        user_id: u32,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Result<(), IggyError> {
        self.perm_get_topic(user_id, stream_id, topic_id)
    }

    pub fn perm_delete_consumer_group(
        &self,
        user_id: u32,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Result<(), IggyError> {
        self.perm_get_topic(user_id, stream_id, topic_id)
    }

    pub fn perm_get_consumer_group(
        &self,
        user_id: u32,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Result<(), IggyError> {
        self.perm_get_topic(user_id, stream_id, topic_id)
    }

    pub fn perm_get_consumer_groups(
        &self,
        user_id: u32,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Result<(), IggyError> {
        self.perm_get_topic(user_id, stream_id, topic_id)
    }

    pub fn perm_join_consumer_group(
        &self,
        user_id: u32,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Result<(), IggyError> {
        self.perm_get_topic(user_id, stream_id, topic_id)
    }

    pub fn perm_leave_consumer_group(
        &self,
        user_id: u32,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Result<(), IggyError> {
        self.perm_get_topic(user_id, stream_id, topic_id)
    }

    pub fn perm_get_consumer_offset(
        &self,
        user_id: u32,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Result<(), IggyError> {
        self.perm_poll_messages(user_id, stream_id, topic_id)
    }

    pub fn perm_store_consumer_offset(
        &self,
        user_id: u32,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Result<(), IggyError> {
        self.perm_poll_messages(user_id, stream_id, topic_id)
    }

    pub fn perm_delete_consumer_offset(
        &self,
        user_id: u32,
        stream_id: StreamId,
        topic_id: TopicId,
    ) -> Result<(), IggyError> {
        self.perm_poll_messages(user_id, stream_id, topic_id)
    }

    pub fn perm_get_user(&self, user_id: u32) -> Result<(), IggyError> {
        self.perm_read_users(user_id)
    }

    pub fn perm_get_users(&self, user_id: u32) -> Result<(), IggyError> {
        self.perm_read_users(user_id)
    }

    pub fn perm_create_user(&self, user_id: u32) -> Result<(), IggyError> {
        self.perm_manage_users(user_id)
    }

    pub fn perm_delete_user(&self, user_id: u32) -> Result<(), IggyError> {
        self.perm_manage_users(user_id)
    }

    pub fn perm_update_user(&self, user_id: u32) -> Result<(), IggyError> {
        self.perm_manage_users(user_id)
    }

    pub fn perm_update_permissions(&self, user_id: u32) -> Result<(), IggyError> {
        self.perm_manage_users(user_id)
    }

    pub fn perm_change_password(&self, user_id: u32) -> Result<(), IggyError> {
        self.perm_manage_users(user_id)
    }

    fn perm_manage_users(&self, user_id: u32) -> Result<(), IggyError> {
        let metadata = self.load();

        if let Some(global_permissions) = metadata.users_global_permissions.get(&user_id)
            && global_permissions.manage_users
        {
            return Ok(());
        }

        Err(IggyError::Unauthorized)
    }

    fn perm_read_users(&self, user_id: u32) -> Result<(), IggyError> {
        let metadata = self.load();

        if let Some(global_permissions) = metadata.users_global_permissions.get(&user_id)
            && (global_permissions.manage_users || global_permissions.read_users)
        {
            return Ok(());
        }

        Err(IggyError::Unauthorized)
    }

    pub fn perm_get_stats(&self, user_id: u32) -> Result<(), IggyError> {
        self.perm_get_server_info(user_id)
    }

    pub fn perm_get_clients(&self, user_id: u32) -> Result<(), IggyError> {
        self.perm_get_server_info(user_id)
    }

    pub fn perm_get_client(&self, user_id: u32) -> Result<(), IggyError> {
        self.perm_get_server_info(user_id)
    }

    fn perm_get_server_info(&self, user_id: u32) -> Result<(), IggyError> {
        let metadata = self.load();

        if let Some(global_permissions) = metadata.users_global_permissions.get(&user_id)
            && (global_permissions.manage_servers || global_permissions.read_servers)
        {
            return Ok(());
        }

        Err(IggyError::Unauthorized)
    }

    /// Atomically resolve, authorize, and return stream metadata.
    pub fn query_stream(
        &self,
        user_id: u32,
        stream_id: &Identifier,
    ) -> Result<Option<StreamMeta>, IggyError> {
        self.with_metadata(|m| {
            let sid = match resolve_stream_id_inner(m, stream_id) {
                Some(s) => s,
                None => return Ok(None),
            };
            perm_get_stream_inner(m, user_id, sid)?;
            Ok(m.streams.get(sid).cloned())
        })
    }

    /// Atomically authorize and return all streams.
    pub fn query_streams(&self, user_id: u32) -> Result<Vec<StreamMeta>, IggyError> {
        self.with_metadata(|m| {
            perm_get_streams_inner(m, user_id)?;
            Ok(m.streams.iter().map(|(_, s)| s.clone()).collect())
        })
    }

    /// Atomically resolve, authorize, and return topic metadata.
    pub fn query_topic(
        &self,
        user_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Option<TopicMeta>, IggyError> {
        self.with_metadata(|m| {
            let sid = match resolve_stream_id_inner(m, stream_id) {
                Some(s) => s,
                None => return Ok(None),
            };
            let tid = match resolve_topic_id_inner(m, sid, topic_id) {
                Some(id) => id,
                None => return Ok(None),
            };
            perm_get_topic_inner(m, user_id, sid, tid)?;
            Ok(m.streams.get(sid).and_then(|s| s.topics.get(tid).cloned()))
        })
    }

    /// Atomically resolve, authorize, and return all topics for a stream.
    pub fn query_topics(
        &self,
        user_id: u32,
        stream_id: &Identifier,
    ) -> Result<Option<Vec<TopicMeta>>, IggyError> {
        self.with_metadata(|m| {
            let sid = match resolve_stream_id_inner(m, stream_id) {
                Some(s) => s,
                None => return Ok(None),
            };
            perm_get_topics_inner(m, user_id, sid)?;
            Ok(m.streams
                .get(sid)
                .map(|s| s.topics.iter().map(|(_, t)| t.clone()).collect()))
        })
    }

    /// Atomically resolve, authorize, and return consumer group metadata.
    pub fn query_consumer_group(
        &self,
        user_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<Option<ConsumerGroupMeta>, IggyError> {
        self.with_metadata(|m| {
            let sid = match resolve_stream_id_inner(m, stream_id) {
                Some(s) => s,
                None => return Ok(None),
            };
            let tid = match resolve_topic_id_inner(m, sid, topic_id) {
                Some(id) => id,
                None => return Ok(None),
            };
            let gid = match resolve_consumer_group_id_inner(m, sid, tid, group_id) {
                Some(id) => id,
                None => return Ok(None),
            };
            perm_get_consumer_group_inner(m, user_id, sid, tid)?;
            Ok(m.streams
                .get(sid)
                .and_then(|s| s.topics.get(tid))
                .and_then(|t| t.consumer_groups.get(gid).cloned()))
        })
    }

    /// Atomically resolve, authorize, and return all consumer groups for a topic.
    pub fn query_consumer_groups(
        &self,
        user_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Option<Vec<ConsumerGroupMeta>>, IggyError> {
        self.with_metadata(|m| {
            let sid = match resolve_stream_id_inner(m, stream_id) {
                Some(s) => s,
                None => return Ok(None),
            };
            let tid = match resolve_topic_id_inner(m, sid, topic_id) {
                Some(id) => id,
                None => return Ok(None),
            };
            perm_get_consumer_group_inner(m, user_id, sid, tid)?;
            Ok(m.streams
                .get(sid)
                .and_then(|s| s.topics.get(tid))
                .map(|t| t.consumer_groups.iter().map(|(_, cg)| cg.clone()).collect()))
        })
    }

    /// Atomically resolve, authorize, and return user metadata.
    /// Permission check skipped when requesting own data.
    pub fn query_user(
        &self,
        requesting_user_id: u32,
        target_user_id: &Identifier,
    ) -> Result<Option<UserMeta>, IggyError> {
        self.with_metadata(|m| {
            let uid = match resolve_user_id_inner(m, target_user_id) {
                Some(id) => id,
                None => return Ok(None),
            };
            if uid != requesting_user_id {
                perm_get_user_inner(m, requesting_user_id)?;
            }
            Ok(m.users.get(uid as usize).cloned())
        })
    }

    /// Atomically authorize and return all users.
    pub fn query_users(&self, user_id: u32) -> Result<Vec<UserMeta>, IggyError> {
        self.with_metadata(|m| {
            perm_get_users_inner(m, user_id)?;
            Ok(m.users.iter().map(|(_, u)| u.clone()).collect())
        })
    }

    /// Atomically resolve topic and check permission for consumer offset query.
    /// Returns resolved topic for use with get_consumer_offset.
    pub fn resolve_for_consumer_offset(
        &self,
        user_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Option<ResolvedTopic>, IggyError> {
        self.with_metadata(|m| {
            let sid = match resolve_stream_id_inner(m, stream_id) {
                Some(s) => s,
                None => return Ok(None),
            };
            let tid = match resolve_topic_id_inner(m, sid, topic_id) {
                Some(id) => id,
                None => return Ok(None),
            };
            perm_get_consumer_offset_inner(m, user_id, sid, tid)?;
            Ok(Some(ResolvedTopic {
                stream_id: sid,
                topic_id: tid,
            }))
        })
    }

    /// Atomically resolve topic and check append permission.
    pub fn resolve_for_append(
        &self,
        user_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<ResolvedTopic, IggyError> {
        self.with_metadata(|m| {
            let sid = resolve_stream_id_inner(m, stream_id)
                .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
            let tid = resolve_topic_id_inner(m, sid, topic_id)
                .ok_or_else(|| IggyError::TopicIdNotFound(stream_id.clone(), topic_id.clone()))?;
            perm_append_messages_inner(m, user_id, sid, tid)?;
            Ok(ResolvedTopic {
                stream_id: sid,
                topic_id: tid,
            })
        })
    }

    /// Atomically resolve topic and check poll permission.
    pub fn resolve_for_poll(
        &self,
        user_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<ResolvedTopic, IggyError> {
        self.with_metadata(|m| {
            let sid = resolve_stream_id_inner(m, stream_id)
                .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
            let tid = resolve_topic_id_inner(m, sid, topic_id)
                .ok_or_else(|| IggyError::TopicIdNotFound(stream_id.clone(), topic_id.clone()))?;
            perm_poll_messages_inner(m, user_id, sid, tid)?;
            Ok(ResolvedTopic {
                stream_id: sid,
                topic_id: tid,
            })
        })
    }

    /// Atomically resolve topic and check store consumer offset permission.
    pub fn resolve_for_store_consumer_offset(
        &self,
        user_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<ResolvedTopic, IggyError> {
        self.with_metadata(|m| {
            let sid = resolve_stream_id_inner(m, stream_id)
                .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
            let tid = resolve_topic_id_inner(m, sid, topic_id)
                .ok_or_else(|| IggyError::TopicIdNotFound(stream_id.clone(), topic_id.clone()))?;
            perm_get_consumer_offset_inner(m, user_id, sid, tid)?;
            Ok(ResolvedTopic {
                stream_id: sid,
                topic_id: tid,
            })
        })
    }

    /// Atomically resolve topic and check delete consumer offset permission.
    pub fn resolve_for_delete_consumer_offset(
        &self,
        user_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<ResolvedTopic, IggyError> {
        self.with_metadata(|m| {
            let sid = resolve_stream_id_inner(m, stream_id)
                .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
            let tid = resolve_topic_id_inner(m, sid, topic_id)
                .ok_or_else(|| IggyError::TopicIdNotFound(stream_id.clone(), topic_id.clone()))?;
            perm_get_consumer_offset_inner(m, user_id, sid, tid)?;
            Ok(ResolvedTopic {
                stream_id: sid,
                topic_id: tid,
            })
        })
    }

    /// Atomically resolve partition and check delete segments permission.
    pub fn resolve_for_delete_segments(
        &self,
        user_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: PartitionId,
    ) -> Result<ResolvedPartition, IggyError> {
        self.with_metadata(|m| {
            let sid = resolve_stream_id_inner(m, stream_id)
                .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
            let tid = resolve_topic_id_inner(m, sid, topic_id)
                .ok_or_else(|| IggyError::TopicIdNotFound(stream_id.clone(), topic_id.clone()))?;
            let exists = m
                .streams
                .get(sid)
                .and_then(|s| s.topics.get(tid))
                .and_then(|t| t.partitions.get(partition_id))
                .is_some();
            if !exists {
                return Err(IggyError::PartitionNotFound(
                    partition_id,
                    topic_id.clone(),
                    stream_id.clone(),
                ));
            }
            perm_manage_topic_inner(m, user_id, sid, tid)?;
            Ok(ResolvedPartition {
                stream_id: sid,
                topic_id: tid,
                partition_id,
            })
        })
    }
}

/// Information needed to initialize a LocalPartition.
#[derive(Clone, Debug)]
pub struct PartitionInitInfo {
    pub created_at: IggyTimestamp,
    pub revision_id: u64,
    pub stats: Arc<PartitionStats>,
    pub consumer_offsets: Arc<ConsumerOffsets>,
    pub consumer_group_offsets: Arc<ConsumerGroupOffsets>,
}

pub(crate) fn resolve_stream_id_inner(
    m: &InnerMetadata,
    stream_id: &Identifier,
) -> Option<StreamId> {
    match stream_id.kind {
        IdKind::Numeric => {
            let sid = stream_id.get_u32_value().ok()? as StreamId;
            if m.streams.get(sid).is_some() {
                Some(sid)
            } else {
                None
            }
        }
        IdKind::String => {
            let name = stream_id.get_cow_str_value().ok()?;
            m.stream_index.get(name.as_ref()).copied()
        }
    }
}

pub(crate) fn resolve_topic_id_inner(
    m: &InnerMetadata,
    stream_id: StreamId,
    topic_id: &Identifier,
) -> Option<TopicId> {
    let stream = m.streams.get(stream_id)?;
    match topic_id.kind {
        IdKind::Numeric => {
            let tid = topic_id.get_u32_value().ok()? as TopicId;
            if stream.topics.get(tid).is_some() {
                Some(tid)
            } else {
                None
            }
        }
        IdKind::String => {
            let name = topic_id.get_cow_str_value().ok()?;
            stream.topic_index.get(&Arc::from(name.as_ref())).copied()
        }
    }
}

pub(crate) fn resolve_consumer_group_id_inner(
    m: &InnerMetadata,
    stream_id: StreamId,
    topic_id: TopicId,
    group_id: &Identifier,
) -> Option<ConsumerGroupId> {
    let stream = m.streams.get(stream_id)?;
    let topic = stream.topics.get(topic_id)?;
    match group_id.kind {
        IdKind::Numeric => {
            let gid = group_id.get_u32_value().ok()? as ConsumerGroupId;
            if topic.consumer_groups.get(gid).is_some() {
                Some(gid)
            } else {
                None
            }
        }
        IdKind::String => {
            let name = group_id.get_cow_str_value().ok()?;
            topic
                .consumer_group_index
                .get(&Arc::from(name.as_ref()))
                .copied()
        }
    }
}

fn resolve_user_id_inner(m: &InnerMetadata, user_id: &Identifier) -> Option<UserId> {
    match user_id.kind {
        IdKind::Numeric => Some(user_id.get_u32_value().ok()?),
        IdKind::String => {
            let name = user_id.get_cow_str_value().ok()?;
            m.user_index.get(name.as_ref()).copied()
        }
    }
}

fn perm_get_stream_inner(
    m: &InnerMetadata,
    user_id: u32,
    stream_id: StreamId,
) -> Result<(), IggyError> {
    if let Some(global) = m.users_global_permissions.get(&user_id)
        && (global.manage_streams || global.read_streams)
    {
        return Ok(());
    }
    if let Some(stream_perm) = m.users_stream_permissions.get(&(user_id, stream_id))
        && (stream_perm.manage_stream || stream_perm.read_stream)
    {
        return Ok(());
    }
    Err(IggyError::Unauthorized)
}

fn perm_get_streams_inner(m: &InnerMetadata, user_id: u32) -> Result<(), IggyError> {
    if let Some(global) = m.users_global_permissions.get(&user_id)
        && (global.manage_streams || global.read_streams)
    {
        return Ok(());
    }
    Err(IggyError::Unauthorized)
}

/// Inheritance: manage_streams -> manage_topics -> manage_topic
fn perm_manage_topic_inner(
    m: &InnerMetadata,
    user_id: u32,
    stream_id: StreamId,
    topic_id: TopicId,
) -> Result<(), IggyError> {
    if let Some(global) = m.users_global_permissions.get(&user_id)
        && (global.manage_streams || global.manage_topics)
    {
        return Ok(());
    }

    if let Some(stream_permissions) = m.users_stream_permissions.get(&(user_id, stream_id)) {
        if stream_permissions.manage_stream || stream_permissions.manage_topics {
            return Ok(());
        }

        if let Some(topics) = &stream_permissions.topics
            && let Some(topic_permissions) = topics.get(&topic_id)
            && topic_permissions.manage_topic
        {
            return Ok(());
        }
    }

    Err(IggyError::Unauthorized)
}

fn perm_get_topic_inner(
    m: &InnerMetadata,
    user_id: u32,
    stream_id: StreamId,
    topic_id: TopicId,
) -> Result<(), IggyError> {
    if let Some(global) = m.users_global_permissions.get(&user_id)
        && (global.read_streams
            || global.manage_streams
            || global.manage_topics
            || global.read_topics)
    {
        return Ok(());
    }

    if let Some(stream_permissions) = m.users_stream_permissions.get(&(user_id, stream_id)) {
        if stream_permissions.manage_stream
            || stream_permissions.read_stream
            || stream_permissions.manage_topics
            || stream_permissions.read_topics
        {
            return Ok(());
        }

        if let Some(topics) = &stream_permissions.topics
            && let Some(topic_permissions) = topics.get(&topic_id)
            && (topic_permissions.manage_topic || topic_permissions.read_topic)
        {
            return Ok(());
        }
    }

    Err(IggyError::Unauthorized)
}

fn perm_get_topics_inner(
    m: &InnerMetadata,
    user_id: u32,
    stream_id: StreamId,
) -> Result<(), IggyError> {
    if let Some(global) = m.users_global_permissions.get(&user_id)
        && (global.read_streams
            || global.manage_streams
            || global.manage_topics
            || global.read_topics)
    {
        return Ok(());
    }

    if let Some(stream_permissions) = m.users_stream_permissions.get(&(user_id, stream_id))
        && (stream_permissions.manage_stream
            || stream_permissions.read_stream
            || stream_permissions.manage_topics
            || stream_permissions.read_topics)
    {
        return Ok(());
    }

    Err(IggyError::Unauthorized)
}

fn perm_get_consumer_group_inner(
    m: &InnerMetadata,
    user_id: u32,
    stream_id: StreamId,
    topic_id: TopicId,
) -> Result<(), IggyError> {
    perm_get_topic_inner(m, user_id, stream_id, topic_id)
}

fn perm_get_user_inner(m: &InnerMetadata, user_id: u32) -> Result<(), IggyError> {
    if let Some(global) = m.users_global_permissions.get(&user_id)
        && (global.manage_users || global.read_users)
    {
        return Ok(());
    }
    Err(IggyError::Unauthorized)
}

fn perm_get_users_inner(m: &InnerMetadata, user_id: u32) -> Result<(), IggyError> {
    perm_get_user_inner(m, user_id)
}

fn perm_get_consumer_offset_inner(
    m: &InnerMetadata,
    user_id: u32,
    stream_id: StreamId,
    topic_id: TopicId,
) -> Result<(), IggyError> {
    if m.users_can_poll_all_streams.contains(&user_id) {
        return Ok(());
    }

    if let Some(global) = m.users_global_permissions.get(&user_id)
        && (global.read_topics
            || global.manage_topics
            || global.read_streams
            || global.manage_streams)
    {
        return Ok(());
    }

    if m.users_can_poll_stream.contains(&(user_id, stream_id)) {
        return Ok(());
    }

    let Some(stream_permissions) = m.users_stream_permissions.get(&(user_id, stream_id)) else {
        return Err(IggyError::Unauthorized);
    };

    if stream_permissions.manage_stream || stream_permissions.read_stream {
        return Ok(());
    }

    if stream_permissions.manage_topics || stream_permissions.read_topics {
        return Ok(());
    }

    if stream_permissions.poll_messages {
        return Ok(());
    }

    if let Some(topics) = &stream_permissions.topics
        && let Some(topic_permissions) = topics.get(&topic_id)
        && (topic_permissions.manage_topic
            || topic_permissions.read_topic
            || topic_permissions.poll_messages)
    {
        return Ok(());
    }

    Err(IggyError::Unauthorized)
}

fn perm_poll_messages_inner(
    m: &InnerMetadata,
    user_id: u32,
    stream_id: StreamId,
    topic_id: TopicId,
) -> Result<(), IggyError> {
    perm_get_consumer_offset_inner(m, user_id, stream_id, topic_id)
}

fn perm_append_messages_inner(
    m: &InnerMetadata,
    user_id: u32,
    stream_id: StreamId,
    topic_id: TopicId,
) -> Result<(), IggyError> {
    if m.users_can_send_all_streams.contains(&user_id) {
        return Ok(());
    }

    if let Some(global) = m.users_global_permissions.get(&user_id)
        && (global.manage_streams || global.manage_topics)
    {
        return Ok(());
    }

    if m.users_can_send_stream.contains(&(user_id, stream_id)) {
        return Ok(());
    }

    let Some(stream_permissions) = m.users_stream_permissions.get(&(user_id, stream_id)) else {
        return Err(IggyError::Unauthorized);
    };

    if stream_permissions.manage_stream
        || stream_permissions.manage_topics
        || stream_permissions.send_messages
    {
        return Ok(());
    }

    if let Some(topics) = &stream_permissions.topics
        && let Some(topic_permissions) = topics.get(&topic_id)
        && (topic_permissions.manage_topic || topic_permissions.send_messages)
    {
        return Ok(());
    }

    Err(IggyError::Unauthorized)
}
