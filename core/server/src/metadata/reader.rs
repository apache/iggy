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
use crate::streaming::partitions::consumer_group_offsets::ConsumerGroupOffsets;
use crate::streaming::partitions::consumer_offsets::ConsumerOffsets;
use crate::streaming::stats::{PartitionStats, StreamStats, TopicStats};
use iggy_common::sharding::IggyNamespace;
use iggy_common::{
    IdKind, Identifier, IggyExpiry, IggyTimestamp, MaxTopicSize, PersonalAccessToken,
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
            .and_then(|p| p.consumer_offsets.clone())
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
            .and_then(|p| p.consumer_group_offsets.clone())
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
}

/// Information needed to initialize a LocalPartition.
#[derive(Clone, Debug)]
pub struct PartitionInitInfo {
    pub created_at: IggyTimestamp,
    pub revision_id: u64,
    pub stats: Arc<PartitionStats>,
    pub consumer_offsets: Option<Arc<ConsumerOffsets>>,
    pub consumer_group_offsets: Option<Arc<ConsumerGroupOffsets>>,
}
