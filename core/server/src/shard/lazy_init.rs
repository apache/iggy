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

//! Lazy initialization of Stream/Topic/Partition structures.
//!
//! When a non-shard-0 receives a data plane request (SendMessages, PollMessages),
//! it may not have the Stream/Topic/Partition in its local slab. This module
//! provides lazy creation from SharedMetadata.

use crate::metadata::{PartitionMeta, StreamMeta, TopicMeta};
use crate::shard::IggyShard;
use crate::slab::traits_ext::{EntityMarker, Insert, InsertCell};
use crate::streaming::partitions::helpers::create_message_deduplicator;
use crate::streaming::partitions::log::SegmentedLog;
use crate::streaming::partitions::partition::{ConsumerGroupOffsets, ConsumerOffsets, Partition};
use crate::streaming::partitions::storage::create_partition_file_hierarchy;
use crate::streaming::stats::{StreamStats, TopicStats};
use crate::streaming::streams::stream::Stream;
use crate::streaming::topics::topic::Topic;
use iggy_common::{Identifier, IggyError};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tracing::{debug, info};

impl IggyShard {
    /// Ensures the stream/topic/partition exists locally for data operations.
    /// Creates them from SharedMetadata if they don't exist.
    ///
    /// This is the core of lazy initialization - non-shard-0 shards create
    /// data structures on-demand when they receive data plane requests.
    ///
    /// Accepts `Identifier` references (can be numeric or string) and resolves
    /// them to numeric IDs using SharedMetadata.
    pub async fn ensure_local_partition(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
    ) -> Result<(), IggyError> {
        // Load metadata to resolve identifiers
        let metadata = self.shared_metadata.load();

        // Resolve stream identifier to numeric ID
        let numeric_stream_id = metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        // Get stream metadata and resolve topic identifier
        let stream_meta = metadata
            .streams
            .get(&numeric_stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        let numeric_topic_id = Self::resolve_topic_id(topic_id, stream_meta)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        // Create numeric identifiers for slab lookups
        let stream_ident = Identifier::numeric(numeric_stream_id as u32)?;
        let topic_ident = Identifier::numeric(numeric_topic_id as u32)?;

        // Fast path: check if partition already exists
        if self.streams.exists(&stream_ident) {
            let topic_exists = self
                .streams
                .with_topics(&stream_ident, |topics| topics.exists(&topic_ident));

            if topic_exists {
                let partition_exists =
                    self.streams
                        .with_partitions(&stream_ident, &topic_ident, |partitions| {
                            partitions.exists(partition_id)
                        });

                if partition_exists {
                    return Ok(());
                }
            }
        }

        // Slow path: need to create from metadata
        debug!(
            "Lazy init: stream={}, topic={}, partition={} on shard {}",
            numeric_stream_id, numeric_topic_id, partition_id, self.id
        );

        let topic_meta = stream_meta
            .topics
            .get(&numeric_topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        let partition_meta =
            topic_meta
                .partitions
                .get(&partition_id)
                .ok_or(IggyError::PartitionNotFound(
                    partition_id,
                    topic_ident.clone(),
                    stream_ident.clone(),
                ))?;

        // Create stream if needed (and get its stats)
        let stream_stats = if !self.streams.exists(&stream_ident) {
            self.create_stream_from_meta(stream_meta)?
        } else {
            // Get existing stream's stats
            self.streams
                .with_stream_by_id(&stream_ident, |(_root, stats)| Arc::clone(&stats))
        };

        // Create topic if needed (and get its stats)
        let topic_exists = self
            .streams
            .with_topics(&stream_ident, |topics| topics.exists(&topic_ident));

        let topic_stats = if !topic_exists {
            self.create_topic_from_meta(numeric_stream_id, topic_meta, stream_stats)?
        } else {
            // Get existing topic's stats
            self.streams
                .with_topic_by_id(&stream_ident, &topic_ident, |(_root, _, stats)| {
                    Arc::clone(&stats)
                })
        };

        // Create partition if needed
        let partition_exists =
            self.streams
                .with_partitions(&stream_ident, &topic_ident, |partitions| {
                    partitions.exists(partition_id)
                });

        if !partition_exists {
            self.create_partition_from_meta(
                numeric_stream_id,
                numeric_topic_id,
                partition_meta,
                topic_stats,
            )
            .await?;
        }

        Ok(())
    }

    /// Resolves a topic identifier to its numeric ID.
    fn resolve_topic_id(topic_id: &Identifier, stream_meta: &StreamMeta) -> Option<usize> {
        match topic_id.kind {
            iggy_common::IdKind::Numeric => {
                let id = topic_id.get_u32_value().unwrap() as usize;
                if stream_meta.topics.contains_key(&id) {
                    Some(id)
                } else {
                    None
                }
            }
            iggy_common::IdKind::String => {
                let name = topic_id.get_string_value().unwrap();
                stream_meta.get_topic_id_by_name(&name)
            }
        }
    }

    /// Creates a Stream locally from metadata. Returns the stream stats.
    /// Uses SharedStatsStore to get the same Arc<StreamStats> as shard 0.
    fn create_stream_from_meta(&self, meta: &StreamMeta) -> Result<Arc<StreamStats>, IggyError> {
        info!(
            "Lazy creating stream: id={}, name={} on shard {}",
            meta.id, meta.name, self.id
        );

        // Get the shared stats from SharedStatsStore (registered by shard 0)
        let stats = self.shared_stats.get_stream_stats(meta.id).ok_or_else(|| {
            IggyError::StreamIdNotFound(Identifier::numeric(meta.id as u32).unwrap())
        })?;

        let mut stream = Stream::new(meta.name.clone(), Arc::clone(&stats), meta.created_at);
        stream.update_id(meta.id);

        // Insert into local streams slab
        // Note: slab position may differ from logical ID - that's fine,
        // lookups use the name index which maps name -> slab position
        let _ = self.streams.insert(stream);

        Ok(stats)
    }

    /// Creates a Topic locally from metadata. Returns the topic stats.
    /// Uses SharedStatsStore to get the same Arc<TopicStats> as shard 0.
    fn create_topic_from_meta(
        &self,
        stream_id: usize,
        meta: &TopicMeta,
        _stream_stats: Arc<StreamStats>,
    ) -> Result<Arc<TopicStats>, IggyError> {
        info!(
            "Lazy creating topic: stream={}, id={}, name={} on shard {}",
            stream_id, meta.id, meta.name, self.id
        );

        // Get the shared stats from SharedStatsStore (registered by shard 0)
        let stats = self
            .shared_stats
            .get_topic_stats(stream_id, meta.id)
            .ok_or_else(|| {
                IggyError::TopicIdNotFound(
                    Identifier::numeric(meta.id as u32).unwrap(),
                    Identifier::numeric(stream_id as u32).unwrap(),
                )
            })?;

        let mut topic = Topic::new(
            meta.name.clone(),
            Arc::clone(&stats),
            meta.created_at,
            meta.replication_factor,
            meta.message_expiry,
            meta.compression_algorithm,
            meta.max_topic_size,
        );
        topic.update_id(meta.id);

        // Insert into stream's topics
        // Note: slab position may differ from logical ID
        let stream_ident = Identifier::numeric(stream_id as u32)?;
        self.streams.with_topics(&stream_ident, |topics| {
            let _ = topics.insert(topic);
        });

        Ok(stats)
    }

    /// Creates a Partition locally from metadata with SegmentedLog.
    /// Uses SharedStatsStore to get the same Arc<PartitionStats> as shard 0.
    async fn create_partition_from_meta(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_meta: &PartitionMeta,
        _topic_stats: Arc<TopicStats>,
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

        // Create message deduplicator if configured
        let message_deduplicator = create_message_deduplicator(&self.config.system);

        // Create empty SegmentedLog
        let log = SegmentedLog::default();

        // Create partition
        let partition = Partition::new(
            partition_meta.created_at,
            true, // should_increment_offset
            stats,
            message_deduplicator,
            Arc::new(AtomicU64::new(0)),
            Arc::new(ConsumerOffsets::with_capacity(10)),
            Arc::new(ConsumerGroupOffsets::with_capacity(10)),
            log,
        );

        // Insert partition into topic
        let stream_ident = Identifier::numeric(stream_id as u32)?;
        let topic_ident = Identifier::numeric(topic_id as u32)?;

        // Note: slab position may differ from logical ID
        self.streams
            .with_partitions_mut(&stream_ident, &topic_ident, |partitions| {
                let _ = partitions.insert(partition);
            });

        // Initialize the log with a segment
        self.init_log(&stream_ident, &topic_ident, partition_id)
            .await?;

        info!(
            "Lazy created partition: stream={}, topic={}, partition={} on shard {}",
            stream_id, topic_id, partition_id, self.id
        );

        Ok(())
    }
}
