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

use bytes::Bytes;
use iggy::prelude::IggyMessage;
use iggy_common::{CompressionAlgorithm, Identifier, IggyError, IggyExpiry, MaxTopicSize};
use server::{
    configs::system::SystemConfig,
    slab::{streams::Streams, traits_ext::EntityMarker},
    streaming::{
        self,
        partitions::{partition2, storage2::create_partition_file_hierarchy},
        segments::{Segment2, storage::create_segment_storage},
        streams::{storage2::create_stream_file_hierarchy, stream2},
        topics::{storage2::create_topic_file_hierarchy, topic2},
    },
};

mod common;
mod get_by_offset;
mod get_by_timestamp;
mod snapshot;

struct BootstrapResult {
    streams: Streams,
    stream_id: Identifier,
    topic_id: Identifier,
    partition_id: usize,
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
    let stream = stream2::create_and_insert_stream_mem(&streams, stream_name);
    create_stream_file_hierarchy(shard_id, stream.id(), &config).await?;
    // Create topic together with its dirs
    let stream_id = Identifier::numeric(stream.id() as u32).unwrap();
    let parent_stats = streams.with_stream_by_id(&stream_id, |(_, stats)| stats.clone());
    let message_expiry = config.resolve_message_expiry(topic_expiry);
    let max_topic_size = config.resolve_max_topic_size(topic_size)?;

    let topic = topic2::create_and_insert_topics_mem(
        &streams,
        &stream_id,
        topic_name,
        1,
        message_expiry,
        CompressionAlgorithm::default(),
        max_topic_size,
        parent_stats,
    );
    create_topic_file_hierarchy(shard_id, stream.id(), topic.id(), &config).await?;
    // Create partition together with its dirs
    let topic_id = Identifier::numeric(topic.id() as u32).unwrap();
    let parent_stats = streams.with_topic_by_id(
        &stream_id,
        &topic_id,
        streaming::topics::helpers::get_stats(),
    );
    let partitions = partition2::create_and_insert_partitions_mem(
        &streams,
        &stream_id,
        &topic_id,
        parent_stats,
        partitions_count,
        config,
    );
    for partition in partitions {
        create_partition_file_hierarchy(shard_id, stream.id(), topic.id(), partition.id(), &config)
            .await?;

        // Open the log
        let start_offset = 0;
        let segment = Segment2::new(
            start_offset,
            config.segment.size,
            config.segment.message_expiry,
        );
        let messages_size = 0;
        let indexes_size = 0;
        let storage = create_segment_storage(
            &config,
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

    Ok(BootstrapResult {
        streams,
        stream_id,
        topic_id,
        partition_id: 0,
    })
}

fn create_messages() -> Vec<IggyMessage> {
    vec![
        create_message(1, "message 1"),
        create_message(2, "message 2"),
        create_message(3, "message 3"),
        create_message(4, "message 3.2"),
        create_message(5, "message 1.2"),
        create_message(6, "message 3.3"),
    ]
}

fn create_message(id: u128, payload: &str) -> IggyMessage {
    let payload = Bytes::from(payload.to_string());
    IggyMessage::builder()
        .id(id)
        .payload(payload)
        .build()
        .expect("Failed to create message with valid payload and headers")
}
