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

//! Lazy initialization of partition structures.
//!
//! When a non-shard-0 receives a data plane request (SendMessages, PollMessages),
//! it may not have the partition in its local partition_store. This module
//! provides lazy creation from SharedMetadata.

use crate::metadata::PartitionMeta;
use crate::partition_store::PartitionData;
use crate::shard::IggyShard;
use crate::streaming::partitions::helpers::create_message_deduplicator;
use crate::streaming::partitions::log::SegmentedLog;
use crate::streaming::partitions::partition::{ConsumerGroupOffsets, ConsumerOffsets};
use crate::streaming::partitions::storage::create_partition_file_hierarchy;
use iggy_common::sharding::IggyNamespace;
use iggy_common::{Identifier, IggyError};
use std::sync::Arc;
use tracing::{debug, info};

impl IggyShard {
    /// Ensures the partition exists locally for data operations.
    /// Creates it from SharedMetadata if it doesn't exist.
    ///
    /// This is the core of lazy initialization - non-shard-0 shards create
    /// data structures on-demand when they receive data plane requests.
    pub async fn ensure_local_partition(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
    ) -> Result<(), IggyError> {
        // Resolve identifiers to numeric IDs using SharedMetadata
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        // Check if partition already exists in partition_store
        let ns = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);
        if self.partition_store.borrow().contains(&ns) {
            return Ok(());
        }

        // Get partition metadata
        let partition_meta =
            self.shared_metadata
                .get_partition(&ns)
                .ok_or(IggyError::PartitionNotFound(
                    partition_id,
                    topic_id.clone(),
                    stream_id.clone(),
                ))?;

        // Create partition from metadata
        self.create_partition_from_meta(numeric_stream_id, numeric_topic_id, &partition_meta)
            .await?;

        Ok(())
    }

    /// Creates a Partition locally from metadata with SegmentedLog.
    /// Uses SharedStatsStore to get the same Arc<PartitionStats> as shard 0.
    /// Uses partition_store for data plane operations.
    async fn create_partition_from_meta(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_meta: &PartitionMeta,
    ) -> Result<(), IggyError> {
        let partition_id = partition_meta.id;

        info!(
            "Lazy creating partition: stream={}, topic={}, partition={} on shard {}",
            stream_id, topic_id, partition_id, self.id
        );

        // Create partition directory if needed
        create_partition_file_hierarchy(stream_id, topic_id, partition_id, &self.config.system)
            .await?;

        // Get the shared stats from SharedStatsStore (registered by shard 0)
        let stats = self
            .shared_stats
            .get_partition_stats(stream_id, topic_id, partition_id)
            .ok_or_else(|| {
                IggyError::PartitionNotFound(
                    partition_id,
                    Identifier::numeric(topic_id as u32).unwrap(),
                    Identifier::numeric(stream_id as u32).unwrap(),
                )
            })?;

        // Get the shared offset from PartitionMeta (source of truth in SharedMetadata)
        let offset = Arc::clone(&partition_meta.offset);
        let should_increment = partition_meta
            .should_increment_offset
            .load(std::sync::atomic::Ordering::Relaxed);
        debug!(
            "lazy_init: Using PartitionMeta offset ({},{},{}) Arc ptr: {:p}, val: {}, should_increment: {}",
            stream_id,
            topic_id,
            partition_id,
            Arc::as_ptr(&offset),
            offset.load(std::sync::atomic::Ordering::Relaxed),
            should_increment
        );

        // Create message deduplicator if configured
        let message_deduplicator = create_message_deduplicator(&self.config.system).map(Arc::new);

        // Get the shared consumer offsets from SharedConsumerOffsetsStore (registered by shard 0)
        let consumer_offsets = self
            .shared_consumer_offsets
            .get_consumer_offsets(stream_id, topic_id, partition_id)
            .unwrap_or_else(|| Arc::new(ConsumerOffsets::with_capacity(10)));

        let consumer_group_offsets = self
            .shared_consumer_offsets
            .get_consumer_group_offsets(stream_id, topic_id, partition_id)
            .unwrap_or_else(|| Arc::new(ConsumerGroupOffsets::with_capacity(10)));

        // Create namespace for partition_store
        let ns = IggyNamespace::new(stream_id, topic_id, partition_id);

        // Create PartitionData and insert into partition_store
        let partition_data = PartitionData::new(
            SegmentedLog::default(),
            offset,
            consumer_offsets,
            consumer_group_offsets,
            message_deduplicator,
            stats,
            partition_meta.created_at,
            should_increment,
        );
        self.partition_store
            .borrow_mut()
            .insert(ns.clone(), partition_data);

        // Initialize the log with a segment
        let stream_ident = Identifier::numeric(stream_id as u32)?;
        let topic_ident = Identifier::numeric(topic_id as u32)?;
        self.init_log(&stream_ident, &topic_ident, partition_id)
            .await?;

        info!(
            "Lazy created partition: stream={}, topic={}, partition={} on shard {}",
            stream_id, topic_id, partition_id, self.id
        );

        Ok(())
    }
}
