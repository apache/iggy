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

use crate::metadata::ConsumerGroupMemberMeta;
use crate::metadata::inner::InnerMetadata;
use crate::metadata::ops::MetadataOp;
use left_right::Absorb;
use std::sync::atomic::Ordering;

impl Absorb<MetadataOp> for InnerMetadata {
    fn absorb_first(&mut self, op: &mut MetadataOp, _other: &Self) {
        apply_op(self, op, true);
    }

    fn absorb_second(&mut self, op: MetadataOp, _other: &Self) {
        apply_op(self, &op, false);
    }

    fn sync_with(&mut self, first: &Self) {
        *self = first.clone();
    }
}

fn apply_op(metadata: &mut InnerMetadata, op: &MetadataOp, populate_ids: bool) {
    match op {
        MetadataOp::Initialize(initial) => {
            *metadata = (**initial).clone();
        }

        MetadataOp::AddStream { meta, assigned_id } => {
            let entry = metadata.streams.vacant_entry();
            let id = entry.key();
            if populate_ids {
                assigned_id.store(id, Ordering::Release);
            }
            let mut meta = meta.clone();
            meta.id = id;
            let name = meta.name.clone();
            entry.insert(meta);
            metadata.stream_index.insert(name, id);
        }

        MetadataOp::UpdateStream { id, new_name } => {
            if let Some(stream) = metadata.streams.get_mut(*id) {
                let old_name = stream.name.clone();
                stream.name = new_name.clone();
                metadata.stream_index.remove(&old_name);
                metadata.stream_index.insert(new_name.clone(), *id);
            }
        }

        MetadataOp::DeleteStream { id } => {
            if metadata.streams.contains(*id) {
                let stream = metadata.streams.remove(*id);
                metadata.stream_index.remove(&stream.name);
            }
        }

        MetadataOp::AddTopic {
            stream_id,
            meta,
            assigned_id,
        } => {
            if let Some(stream) = metadata.streams.get_mut(*stream_id) {
                let entry = stream.topics.vacant_entry();
                let id = entry.key();
                if populate_ids {
                    assigned_id.store(id, Ordering::Release);
                }
                let mut meta = meta.clone();
                meta.id = id;
                let name = meta.name.clone();
                entry.insert(meta);
                stream.topic_index.insert(name, id);
            }
        }

        MetadataOp::UpdateTopic {
            stream_id,
            topic_id,
            new_name,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor,
        } => {
            if let Some(stream) = metadata.streams.get_mut(*stream_id)
                && let Some(topic) = stream.topics.get_mut(*topic_id)
            {
                let old_name = topic.name.clone();

                topic.name = new_name.clone();
                topic.message_expiry = *message_expiry;
                topic.compression_algorithm = *compression_algorithm;
                topic.max_topic_size = *max_topic_size;
                topic.replication_factor = *replication_factor;

                if old_name != *new_name {
                    stream.topic_index.remove(&old_name);
                    stream.topic_index.insert(new_name.clone(), *topic_id);
                }
            }
        }

        MetadataOp::DeleteTopic {
            stream_id,
            topic_id,
        } => {
            if let Some(stream) = metadata.streams.get_mut(*stream_id)
                && stream.topics.contains(*topic_id)
            {
                let topic = stream.topics.remove(*topic_id);
                stream.topic_index.remove(&topic.name);
            }
        }

        MetadataOp::AddPartitions {
            stream_id,
            topic_id,
            partitions,
            revision_id,
        } => {
            if partitions.is_empty() {
                return;
            }
            if let Some(stream) = metadata.streams.get_mut(*stream_id)
                && let Some(topic) = stream.topics.get_mut(*topic_id)
            {
                for meta in partitions {
                    let mut meta = meta.clone();
                    meta.id = topic.partitions.len();
                    meta.revision_id = *revision_id;
                    topic.partitions.push(meta);
                }
            }
        }

        MetadataOp::DeletePartitions {
            stream_id,
            topic_id,
            count,
        } => {
            if *count == 0 {
                return;
            }
            if let Some(stream) = metadata.streams.get_mut(*stream_id)
                && let Some(topic) = stream.topics.get_mut(*topic_id)
            {
                let new_len = topic.partitions.len().saturating_sub(*count as usize);
                topic.partitions.truncate(new_len);
            }
        }

        MetadataOp::SetPartitionOffsets {
            stream_id,
            topic_id,
            partition_id,
            consumer_offsets,
            consumer_group_offsets,
        } => {
            if let Some(stream) = metadata.streams.get_mut(*stream_id)
                && let Some(topic) = stream.topics.get_mut(*topic_id)
                && let Some(partition) = topic.partitions.get_mut(*partition_id)
            {
                partition.consumer_offsets = Some(consumer_offsets.clone());
                partition.consumer_group_offsets = Some(consumer_group_offsets.clone());
            }
        }

        MetadataOp::AddUser { meta, assigned_id } => {
            let entry = metadata.users.vacant_entry();
            let id = entry.key();
            if populate_ids {
                assigned_id.store(id, Ordering::Release);
            }
            let mut meta = meta.clone();
            meta.id = id as u32;
            let username = meta.username.clone();
            entry.insert(meta);
            metadata.user_index.insert(username, id as u32);
        }

        MetadataOp::UpdateUserMeta { id, meta } => {
            let user_id = *id as usize;
            if let Some(old_user) = metadata.users.get(user_id)
                && old_user.username != meta.username
            {
                metadata.user_index.remove(&old_user.username);
                metadata.user_index.insert(meta.username.clone(), *id);
            }
            if metadata.users.contains(user_id) {
                metadata.users[user_id] = meta.clone();
            }
        }

        MetadataOp::DeleteUser { id } => {
            let user_id = *id as usize;
            if metadata.users.contains(user_id) {
                let user = metadata.users.remove(user_id);
                metadata.user_index.remove(&user.username);
            }
            metadata.personal_access_tokens.remove(id);
        }

        MetadataOp::AddPersonalAccessToken { user_id, pat } => {
            metadata
                .personal_access_tokens
                .entry(*user_id)
                .or_default()
                .insert(pat.token.clone(), pat.clone());
        }

        MetadataOp::DeletePersonalAccessToken {
            user_id,
            token_hash,
        } => {
            if let Some(user_pats) = metadata.personal_access_tokens.get_mut(user_id) {
                user_pats.remove(token_hash);
            }
        }

        MetadataOp::AddConsumerGroup {
            stream_id,
            topic_id,
            meta,
            assigned_id,
        } => {
            if let Some(stream) = metadata.streams.get_mut(*stream_id)
                && let Some(topic) = stream.topics.get_mut(*topic_id)
            {
                let entry = topic.consumer_groups.vacant_entry();
                let id = entry.key();
                if populate_ids {
                    assigned_id.store(id, Ordering::Release);
                }
                let mut meta = meta.clone();
                meta.id = id;
                let name = meta.name.clone();
                entry.insert(meta);
                topic.consumer_group_index.insert(name, id);
            }
        }

        MetadataOp::DeleteConsumerGroup {
            stream_id,
            topic_id,
            group_id,
        } => {
            if let Some(stream) = metadata.streams.get_mut(*stream_id)
                && let Some(topic) = stream.topics.get_mut(*topic_id)
                && topic.consumer_groups.contains(*group_id)
            {
                let group = topic.consumer_groups.remove(*group_id);
                topic.consumer_group_index.remove(&group.name);
            }
        }

        MetadataOp::JoinConsumerGroup {
            stream_id,
            topic_id,
            group_id,
            client_id,
            member_id,
        } => {
            if let Some(stream) = metadata.streams.get_mut(*stream_id)
                && let Some(topic) = stream.topics.get_mut(*topic_id)
                && let Some(group) = topic.consumer_groups.get_mut(*group_id)
            {
                let next_id = group
                    .members
                    .iter()
                    .map(|(_, m)| m.id)
                    .max()
                    .map(|m| m + 1)
                    .unwrap_or(0);

                if populate_ids {
                    member_id.store(next_id, Ordering::Release);
                }

                let new_member = ConsumerGroupMemberMeta::new(next_id, *client_id);
                group.members.insert(new_member);
                group.rebalance_members();
            }
        }

        MetadataOp::LeaveConsumerGroup {
            stream_id,
            topic_id,
            group_id,
            client_id,
            removed_member_id,
        } => {
            if let Some(stream) = metadata.streams.get_mut(*stream_id)
                && let Some(topic) = stream.topics.get_mut(*topic_id)
                && let Some(group) = topic.consumer_groups.get_mut(*group_id)
            {
                let member_to_remove: Option<usize> = group
                    .members
                    .iter()
                    .find(|(_, m)| m.client_id == *client_id)
                    .map(|(id, _)| id);

                if let Some(mid) = member_to_remove {
                    if populate_ids {
                        removed_member_id.store(mid, Ordering::Release);
                    }
                    group.members.remove(mid);
                    group.rebalance_members();
                }
            }
        }

        MetadataOp::RebalanceConsumerGroupsForTopic {
            stream_id,
            topic_id,
            partitions_count,
        } => {
            if let Some(stream) = metadata.streams.get_mut(*stream_id)
                && let Some(topic) = stream.topics.get_mut(*topic_id)
            {
                let partition_ids: Vec<usize> = (0..*partitions_count as usize).collect();
                let group_ids: Vec<_> = topic.consumer_groups.iter().map(|(id, _)| id).collect();

                for gid in group_ids {
                    if let Some(group) = topic.consumer_groups.get_mut(gid) {
                        group.partitions = partition_ids.clone();
                        group.rebalance_members();
                    }
                }
            }
        }
    }
}
