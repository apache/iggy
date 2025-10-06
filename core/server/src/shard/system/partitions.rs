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
use crate::shard::IggyShard;
use crate::shard::ShardInfo;
use crate::shard::calculate_shard_assignment;
use crate::shard::namespace::IggyNamespace;
use crate::shard_info;
use crate::slab::traits_ext::EntityMarker;
use crate::slab::traits_ext::IntoComponents;
use crate::streaming::partitions;
use crate::streaming::partitions::journal::MemoryMessageJournal;
use crate::streaming::partitions::log::SegmentedLog;
use crate::streaming::partitions::partition2;
use crate::streaming::partitions::storage2::create_partition_file_hierarchy;
use crate::streaming::partitions::storage2::delete_partitions_from_disk;
use crate::streaming::segments::Segment2;
use crate::streaming::segments::storage::create_segment_storage;
use crate::streaming::session::Session;
use crate::streaming::streams;
use crate::streaming::topics;

use error_set::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;

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
        let partitions = partition2::create_and_insert_partitions_mem(
            &self.streams2,
            stream_id,
            topic_id,
            parent_stats,
            partitions_count,
            &self.config.system,
        );

        self.metrics.increment_partitions(partitions_count);
        self.metrics.increment_segments(partitions_count);

        let shards_count = self.get_available_shards_count();
        for (partition_id, stats) in partitions.iter().map(|p| (p.id(), p.stats())) {
            // TODO: Create shard table recordsj.
            let ns = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);
            let shard_id = calculate_shard_assignment(&ns, shards_count);
            let shard_info = ShardInfo::new(shard_id);
            let is_current_shard = self.id == shard_info.id;
            self.insert_shard_table_record(ns, shard_info);

            create_partition_file_hierarchy(
                self.id,
                numeric_stream_id as usize,
                numeric_topic_id as usize,
                partition_id,
                &self.config.system,
            )
            .await?;
            stats.increment_segments_count(1);
            if is_current_shard {
                self.init_log(stream_id, topic_id, partition_id).await?;
            }
        }
        Ok(partitions)
    }

    pub async fn init_log(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
    ) -> Result<(), IggyError> {
        let numeric_stream_id = self
            .streams2
            .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
        let numeric_topic_id =
            self.streams2
                .with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());

        let start_offset = 0;
        shard_info!(
            self.id,
            "Initializing log for partition ID: {} for topic ID: {} for stream ID: {} with start offset: {}",
            partition_id,
            numeric_topic_id,
            numeric_stream_id,
            start_offset
        );

        let segment = Segment2::new(
            start_offset,
            self.config.system.segment.size,
            self.config.system.segment.message_expiry,
        );

        let numeric_stream_id = self
            .streams2
            .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
        let numeric_topic_id =
            self.streams2
                .with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());

        let messages_size = 0;
        let indexes_size = 0;
        let storage = create_segment_storage(
            &self.config.system,
            numeric_stream_id,
            numeric_topic_id,
            partition_id,
            messages_size,
            indexes_size,
            start_offset,
        )
        .await?;

        self.streams2
            .with_partition_by_id_mut(stream_id, topic_id, partition_id, |(.., log)| {
                log.add_persisted_segment(segment, storage);
            });
        shard_info!(
            self.id,
            "Initialized log for partition ID: {} for topic ID: {} for stream ID: {} with start offset: {}",
            partition_id,
            numeric_topic_id,
            numeric_stream_id,
            start_offset
        );

        Ok(())
    }

    pub async fn create_partitions2_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions: Vec<partition2::Partition>,
    ) -> Result<(), IggyError> {
        let numeric_stream_id = self
            .streams2
            .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
        let numeric_topic_id =
            self.streams2
                .with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());
        let shards_count = self.get_available_shards_count();
        for partition in partitions {
            let actual_id = partition.id();
            let id = self.streams2.with_partitions_mut(
                stream_id,
                topic_id,
                partitions::helpers::insert_partition(partition),
            );
            assert_eq!(
                id, actual_id,
                "create_partitions_bypass_auth: partition mismatch ID, wrong creation order ?!"
            );
            let ns = IggyNamespace::new(numeric_stream_id, numeric_topic_id, id);
            let shard_id = crate::shard::calculate_shard_assignment(&ns, shards_count);
            let shard_info = ShardInfo::new(shard_id);
            self.insert_shard_table_record(ns, shard_info);
            if self.id == shard_info.id {
                self.init_log(stream_id, topic_id, id).await?;
            }
        }

        Ok(())
    }

    pub async fn delete_partitions2(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<Vec<usize>, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_partitions_exist(stream_id, topic_id, partitions_count)?;

        let numeric_stream_id = self
            .streams2
            .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
        let numeric_topic_id =
            self.streams2
                .with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());

        self.validate_partition_permissions(
            session,
            numeric_stream_id as u32,
            numeric_topic_id as u32,
            "delete",
        )?;

        let partitions = self.delete_partitions_base2(stream_id, topic_id, partitions_count);
        let parent = partitions
            .first()
            .map(|p| p.stats().parent().clone())
            .expect("delete_partitions: no partitions to deletion");
        // Reassign the partitions count as it could get clamped by the `delete_partitions_base2` method.

        let mut deleted_ids = Vec::with_capacity(partitions.len());
        let mut total_messages_count = 0;
        let mut total_segments_count = 0;
        let mut total_size_bytes = 0;

        for partition in partitions {
            let (root, stats, _, _, _, _, mut log) = partition.into_components();
            let partition_id = root.id();
            let ns = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);
            self.remove_shard_table_record(&ns);

            self.delete_partition_dir(numeric_stream_id, numeric_topic_id, partition_id, &mut log)
                .await?;
            let segments_count = stats.segments_count_inconsistent();
            let messages_count = stats.messages_count_inconsistent();
            let size_bytes = stats.size_bytes_inconsistent();
            total_messages_count += messages_count;
            total_segments_count += segments_count;
            total_size_bytes += size_bytes;

            deleted_ids.push(partition_id);
        }

        self.metrics.decrement_partitions(partitions_count);
        self.metrics.decrement_segments(total_segments_count);
        parent.decrement_messages_count(total_messages_count);
        parent.decrement_size_bytes(total_size_bytes);
        parent.decrement_segments_count(total_segments_count);

        Ok(deleted_ids)
    }

    async fn delete_partition_dir(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        log: &mut SegmentedLog<MemoryMessageJournal>,
    ) -> Result<(), IggyError> {
        delete_partitions_from_disk(
            self.id,
            stream_id,
            topic_id,
            partition_id,
            log,
            &self.config.system,
        )
        .await
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
        self.ensure_partitions_exist(stream_id, topic_id, partitions_count)?;

        if partitions_count as usize != partition_ids.len() {
            return Err(IggyError::InvalidPartitionsCount);
        }

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
