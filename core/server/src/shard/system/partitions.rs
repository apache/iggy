/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use super::COMPONENT;
use crate::configs::system::SystemConfig;
use crate::shard::IggyShard;
use crate::slab::traits_ext::EntityComponentSystem;
use crate::slab::traits_ext::EntityComponentSystemMutCell;
use crate::slab::traits_ext::EntityMarker;
use crate::slab::traits_ext::IntoComponents;
use crate::slab::traits_ext::IntoComponentsById;
use crate::streaming::deduplication::message_deduplicator::MessageDeduplicator;
use crate::streaming::partitions;
use crate::streaming::partitions::helpers::create_message_deduplicator;
use crate::streaming::partitions::partition2;
use crate::streaming::partitions::storage2::create_partition_file_hierarchy;
use crate::streaming::partitions::storage2::delete_partitions_from_disk;
use crate::streaming::session::Session;
use crate::streaming::stats::stats::PartitionStats;
use crate::streaming::stats::stats::TopicStats;
use crate::streaming::streams;
use crate::streaming::topics;
use error_set::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;
use iggy_common::IggyTimestamp;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

impl IggyShard {
    fn validate_partition_permissions(
        &self,
        session: &Session,
        stream_id: u32,
        topic_id: u32,
        operation: &str,
    ) -> Result<(), IggyError> {
        let permissioner = self.permissioner.borrow();
        let result = match operation {
            "create" => permissioner.create_partitions(session.get_user_id(), stream_id, topic_id),
            "delete" => permissioner.delete_partitions(session.get_user_id(), stream_id, topic_id),
            _ => return Err(IggyError::InvalidCommand),
        };

        result.with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - permission denied to {operation} partitions for user {} on stream ID: {}, topic ID: {}",
                session.get_user_id(),
                stream_id,
                topic_id
            )
        })
    }

    pub async fn create_partitions2(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<Vec<partition2::Partition>, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_topic_exists(stream_id, topic_id)?;
        let numeric_stream_id = self
            .streams2
            .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
        let numeric_topic_id =
            self.streams2
                .with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());

        // Claude garbage, rework this.
        self.validate_partition_permissions(
            session,
            numeric_stream_id as u32,
            numeric_topic_id as u32,
            "create",
        )?;
        let parent_stats =
            self.streams2
                .with_topic_by_id(stream_id, topic_id, topics::helpers::get_stats());
        let partitions = self.create_and_insert_partitions_mem(
            stream_id,
            topic_id,
            parent_stats,
            partitions_count,
        );
        let stats = partitions.first().map(|p| p.stats());
        if let Some(stats) = stats {
            // One segment per partition created.
            stats.increment_segments_count(partitions_count);
        }
        self.metrics.increment_partitions(partitions_count);
        self.metrics.increment_segments(partitions_count);

        // TODO: Figure out how to do this operation in a batch.
        for partition_id in partitions.iter().map(|p| p.id()) {
            create_partition_file_hierarchy(
                self.id,
                numeric_stream_id as usize,
                numeric_topic_id as usize,
                partition_id,
                &self.config.system,
            )
            .await?;
        }
        Ok(partitions)
    }

    fn create_and_insert_partitions_mem(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        parent_stats: Arc<TopicStats>,
        partitions_count: u32,
    ) -> Vec<partition2::Partition> {
        let range = 0..partitions_count as usize;
        let created_at = IggyTimestamp::now();
        range
            .map(|_| {
                // Areczkuuuu.
                let stats = Arc::new(PartitionStats::new(parent_stats.clone()));
                let should_increment_offset = false;
                let deduplicator = create_message_deduplicator(&self.config.system);
                let offset = Arc::new(AtomicU64::new(0));
                let consumer_offset = Arc::new(papaya::HashMap::with_capacity(2137));
                let consumer_group_offset = Arc::new(papaya::HashMap::with_capacity(2137));

                let mut partition = partition2::Partition::new(
                    created_at,
                    should_increment_offset,
                    stats,
                    deduplicator,
                    offset,
                    consumer_offset,
                    consumer_group_offset,
                );
                let id = self.insert_partition_mem(stream_id, topic_id, partition.clone());
                partition.update_id(id);
                partition
            })
            .collect()
    }

    fn insert_partition_mem(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition: partition2::Partition,
    ) -> usize {
        self.streams2.with_partitions_mut(
            stream_id,
            topic_id,
            partitions::helpers::insert_partition(partition),
        )
    }

    pub fn create_partitions2_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions: Vec<partition2::Partition>,
    ) -> Result<(), IggyError> {
        for partition in partitions {
            let actual_id = partition.id();
            let id = self.insert_partition_mem(stream_id, topic_id, partition);
            assert_eq!(
                id, actual_id,
                "create_partitions_bypass_auth: partition mismatch ID, wrong creation order ?!"
            );
        }
        Ok(())
    }

    pub async fn delete_partitions2(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<Vec<partition2::Partition>, IggyError> {
        self.ensure_authenticated(session)?;
        let numeric_stream_id = self
            .streams2
            .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
        let numeric_topic_id =
            self.streams2
                .with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());
        // Claude garbage, rework this.
        self.validate_partition_permissions(
            session,
            numeric_stream_id as u32,
            numeric_topic_id as u32,
            "delete",
        )?;

        let partitions = self.delete_partitions_base2(stream_id, topic_id, partitions_count);
        self.delete_partitions_dirs(numeric_stream_id, numeric_topic_id, &partitions)
            .await?;
        // Reassign the partitions count as it could get clamped by the `delete_partitions_base2` method.
        let partitions_count = partitions.len() as u32;
        let parent = partitions
            .first()
            .map(|p| p.stats().parent().clone())
            .expect("delete_partitions: no partitions to deletion");
        let (messages_count, segments_count, size_bytes) = partitions.iter().fold(
            (0, 0, 0),
            |(mut msg_acc, mut seg_acc, mut size_acc), partition| {
                let stats = partition.stats();
                let seg = stats.segments_count_inconsistent();
                let msg = stats.messages_count_inconsistent();
                let size = stats.size_bytes_inconsistent();
                msg_acc += msg;
                seg_acc += seg;
                size_acc += size;
                (msg_acc, seg_acc, size_acc)
            },
        );
        self.metrics.decrement_partitions(partitions_count);
        self.metrics.decrement_segments(segments_count);
        parent.decrement_messages_count(messages_count);
        parent.decrement_size_bytes(size_bytes);
        parent.decrement_segments_count(segments_count);

        Ok(partitions)
    }

    async fn delete_partitions_dirs(
        &self,
        stream_id: usize,
        topic_id: usize,
        partitions: &Vec<partition2::Partition>,
    ) -> Result<(), IggyError> {
        let ids = partitions.iter().map(|p| p.id());
        let segments = self.streams2.with_components(|stream| {
            let (root, ..) = stream.into_components_by_id(stream_id);
            root.topics()
                .with_components_by_id_mut(topic_id, |(mut root, ..)| {
                    let partitions = root.partitions_mut();
                    ids.map(|id| partitions.remove_segments_for_partition(id))
                        .flatten()
                        .collect::<Vec<_>>()
                })
        });
        delete_partitions_from_disk(self.id, stream_id, topic_id, segments, &self.config.system)
            .await?;
        Ok(())
    }

    fn delete_partitions_base2(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Vec<partition2::Partition> {
        self.streams2.with_partitions_mut(
            stream_id,
            topic_id,
            partitions::helpers::delete_partitions(partitions_count),
        )
    }

    pub fn delete_partitions2_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
        partition_ids: Vec<usize>,
    ) -> Result<(), IggyError> {
        assert_eq!(partitions_count as usize, partition_ids.len());

        let partitions = self.delete_partitions_base2(stream_id, topic_id, partitions_count);
        for (deleted_partition_id, actual_deleted_partition_id) in partitions
            .iter()
            .map(|p| p.id())
            .zip(partition_ids.into_iter())
        {
            assert_eq!(
                deleted_partition_id, actual_deleted_partition_id,
                "delete_partitions2_bypass_auth: partition mismatch ID"
            );
        }
        Ok(())
    }
}
