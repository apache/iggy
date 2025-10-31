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

use iggy_common::{CompressionAlgorithm, Identifier, IggyError, IggyExpiry, MaxTopicSize};
use server::{
    configs::system::SystemConfig,
    shard::task_registry::TaskRegistry,
    slab::{streams::Streams, traits_ext::EntityMarker},
    streaming::{
        self,
        partitions::{partition, storage::create_partition_file_hierarchy},
        segments::{Segment, storage::create_segment_storage},
        streams::{storage::create_stream_file_hierarchy, stream},
        topics::{storage::create_topic_file_hierarchy, topic},
    },
};
use std::rc::Rc;

mod common;
mod get_by_offset;
mod get_by_timestamp;
mod snapshot;

struct BootstrapResult {
    streams: Streams,
    stream_id: Identifier,
    topic_id: Identifier,
    partition_id: usize,
    task_registry: Rc<TaskRegistry>,
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

    let streams = Streams::default();
    // Create stream together with its dirs
    let stream = stream::create_and_insert_stream_mem(&streams, stream_name);
    create_stream_file_hierarchy(stream.id(), config).await?;
    // Create topic together with its dirs
    let stream_id = Identifier::numeric(stream.id() as u32).unwrap();
    let parent_stats = streams.with_stream_by_id(&stream_id, |(_, stats)| stats.clone());
    let message_expiry = config.resolve_message_expiry(topic_expiry);
    let max_topic_size = config.resolve_max_topic_size(topic_size)?;

    let topic = topic::create_and_insert_topics_mem(
        &streams,
        &stream_id,
        topic_name,
        1,
        message_expiry,
        CompressionAlgorithm::default(),
        max_topic_size,
        parent_stats,
    );
    create_topic_file_hierarchy(stream.id(), topic.id(), config).await?;
    // Create partition together with its dirs
    let topic_id = Identifier::numeric(topic.id() as u32).unwrap();
    let parent_stats = streams.with_topic_by_id(
        &stream_id,
        &topic_id,
        streaming::topics::helpers::get_stats(),
    );
    let partitions = partition::create_and_insert_partitions_mem(
        &streams,
        &stream_id,
        &topic_id,
        parent_stats,
        partitions_count,
        config,
    );
    for partition in partitions {
        create_partition_file_hierarchy(stream.id(), topic.id(), partition.id(), config).await?;

        // Open the log
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
            stream.id(),
            topic.id(),
            partition.id(),
            messages_size,
            indexes_size,
            start_offset,
        )
        .await?;

        streams.with_partition_by_id_mut(&stream_id, &topic_id, partition.id(), |(.., log)| {
            log.add_persisted_segment(segment, storage);
        });
    }

    // Create a test task registry
    let task_registry = Rc::new(TaskRegistry::new(shard_id));

    Ok(BootstrapResult {
        streams,
        stream_id,
        topic_id,
        partition_id: 0,
        task_registry,
    })
}
