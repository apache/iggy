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

use common::streaming_helper::StreamingTestHelper;
use iggy_common::sharding::IggyNamespace;
use iggy_common::{CompressionAlgorithm, Identifier, IggyError, IggyExpiry, MaxTopicSize};
use server::{
    configs::system::SystemConfig,
    metadata::SharedMetadata,
    partition_store::{PartitionData, PartitionDataStore},
    shard::{task_registry::TaskRegistry, transmission::connector::ShardConnector},
    streaming::{
        partitions::{
            helpers::create_message_deduplicator,
            journal::MemoryMessageJournal,
            log::SegmentedLog,
            partition::{ConsumerGroupOffsets, ConsumerOffsets},
            storage::create_partition_file_hierarchy,
        },
        segments::{Segment, storage::create_segment_storage},
        stats::PartitionStats,
        streams::storage::create_stream_file_hierarchy,
        topics::storage::create_topic_file_hierarchy,
    },
};
use std::rc::Rc;
use std::sync::Arc;

mod common;
mod get_by_offset;
mod get_by_timestamp;
mod snapshot;

struct BootstrapResult {
    #[allow(dead_code)]
    shared_metadata: SharedMetadata,
    streaming_helper: StreamingTestHelper,
    stream_id: Identifier,
    topic_id: Identifier,
    partition_id: usize,
    task_registry: Rc<TaskRegistry>,
    namespace: IggyNamespace,
}

async fn bootstrap_test_environment(
    shard_id: u16,
    config: &SystemConfig,
) -> Result<BootstrapResult, IggyError> {
    let stream_name = "stream-1".to_owned();
    let topic_name = "topic-1".to_owned();
    let topic_expiry = IggyExpiry::NeverExpire;
    let topic_size = MaxTopicSize::Unlimited;
    let partitions_count = 1;

    // Create SharedMetadata and PartitionDataStore
    let shared_metadata = SharedMetadata::new();
    let mut partition_store = PartitionDataStore::new();

    // Create stream in SharedMetadata
    let stream_meta = shared_metadata.create_stream(stream_name)?;
    let stream_id_numeric = stream_meta.id;
    create_stream_file_hierarchy(stream_id_numeric, config).await?;

    // Create topic in SharedMetadata
    let stream_id = Identifier::numeric(stream_id_numeric as u32).unwrap();
    let message_expiry = config.resolve_message_expiry(topic_expiry);
    let max_topic_size = config.resolve_max_topic_size(topic_size)?;

    let topic_meta = shared_metadata.create_topic(
        &stream_id,
        topic_name,
        1, // replication_factor
        message_expiry,
        CompressionAlgorithm::default(),
        max_topic_size,
    )?;
    let topic_id_numeric = topic_meta.id;
    create_topic_file_hierarchy(stream_id_numeric, topic_id_numeric, config).await?;

    // Create partitions in SharedMetadata
    let topic_id = Identifier::numeric(topic_id_numeric as u32).unwrap();
    let partition_metas =
        shared_metadata.create_partitions(&stream_id, &topic_id, partitions_count)?;

    // For each partition, create file hierarchy and add to partition_store
    for partition_meta in &partition_metas {
        let partition_id = partition_meta.id;
        create_partition_file_hierarchy(stream_id_numeric, topic_id_numeric, partition_id, config)
            .await?;

        // Create a segment and storage
        let start_offset = 0;
        let segment = Segment::new(
            start_offset,
            config.segment.size,
            config.segment.message_expiry,
        );
        let messages_size = 0;
        let indexes_size = 0;
        let storage = create_segment_storage(
            config,
            stream_id_numeric,
            topic_id_numeric,
            partition_id,
            messages_size,
            indexes_size,
            start_offset,
        )
        .await?;

        // Create SegmentedLog and add the persisted segment
        let mut log: SegmentedLog<MemoryMessageJournal> = Default::default();
        log.add_persisted_segment(segment, storage);

        // Create partition stats
        let stats = Arc::new(PartitionStats::default());

        // Create PartitionData
        let partition_data = PartitionData::new(
            log,
            Arc::clone(&partition_meta.offset),
            Arc::new(ConsumerOffsets::with_capacity(128)),
            Arc::new(ConsumerGroupOffsets::with_capacity(128)),
            create_message_deduplicator(config).map(Arc::new),
            stats,
            partition_meta.created_at,
            partition_meta
                .should_increment_offset
                .load(std::sync::atomic::Ordering::Relaxed),
        );

        // Insert into partition_store
        let namespace = IggyNamespace::new(stream_id_numeric, topic_id_numeric, partition_id);
        partition_store.insert(namespace, partition_data);
    }

    // Create a test task registry with dummy stop sender from ShardConnector
    let connector: ShardConnector<()> = ShardConnector::new(shard_id);
    let task_registry = Rc::new(TaskRegistry::new(shard_id, vec![connector.stop_sender]));

    let namespace = IggyNamespace::new(stream_id_numeric, topic_id_numeric, 0);

    // Create the streaming helper
    let streaming_helper = StreamingTestHelper::new(partition_store);

    Ok(BootstrapResult {
        shared_metadata,
        streaming_helper,
        stream_id,
        topic_id,
        partition_id: 0,
        task_registry,
        namespace,
    })
}
