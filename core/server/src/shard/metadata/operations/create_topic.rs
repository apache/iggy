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

use crate::shard::metadata::MetadataOperation;
use crate::shard::transmission::{
    event::ShardEvent, frame::ShardResponse, message::ShardRequestPayload,
};
use crate::shard::{IggyShard, shard_info};
use crate::slab::traits_ext::{EntityMarker, InsertCell};
use crate::streaming::topics::{storage2::create_topic_file_hierarchy, topic2};
use iggy_common::{
    CompressionAlgorithm, Identifier, IggyError, IggyExpiry, IggyTimestamp, MaxTopicSize,
};
use std::sync::{Arc, atomic::Ordering};

/// Event broadcast when a topic is created
#[derive(Debug, Clone)]
pub struct TopicCreatedEvent {
    pub stream_id: Identifier,
    pub topic: topic2::Topic,
}

impl From<TopicCreatedEvent> for ShardEvent {
    fn from(event: TopicCreatedEvent) -> Self {
        ShardEvent::CreatedTopic2 {
            stream_id: event.stream_id,
            topic: event.topic,
        }
    }
}

/// Operation to create a new topic
#[derive(Debug, Clone)]
pub struct CreateTopicOp {
    pub stream_id: Identifier,
    pub name: String,
    pub message_expiry: IggyExpiry,
    pub compression_algorithm: CompressionAlgorithm,
    pub max_topic_size: MaxTopicSize,
    pub replication_factor: Option<u8>,
}

impl CreateTopicOp {
    pub fn new(
        stream_id: Identifier,
        name: String,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Self {
        CreateTopicOp {
            stream_id,
            name,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor,
        }
    }
}

impl MetadataOperation for CreateTopicOp {
    type Response = topic2::Topic;
    type Event = TopicCreatedEvent;

    async fn execute_on_coordinator(
        &self,
        shard: &IggyShard,
    ) -> Result<(Self::Response, Self::Event), IggyError> {
        let config = &shard.config.system;
        let numeric_stream_id = shard.streams2.get_index(&self.stream_id);

        // Get parent stream stats
        let parent_stats = shard
            .streams2
            .with_stream_by_id(&self.stream_id, |(_, stats)| stats.clone());

        // Resolve configuration values
        let message_expiry = config.resolve_message_expiry(self.message_expiry);
        let max_topic_size = config.resolve_max_topic_size(self.max_topic_size)?;

        // Generate the next topic ID (only shard 0 has the ID generator)
        let topic_id = shard.next_topic_id.fetch_add(1, Ordering::SeqCst);

        shard_info!(
            shard.id,
            "Creating topic '{}' with ID {} in stream {}",
            self.name,
            topic_id,
            numeric_stream_id
        );

        // Create topic with the assigned ID
        let mut topic = topic2::Topic::new(
            self.name.clone(),
            Arc::new(crate::streaming::stats::TopicStats::new(parent_stats)),
            IggyTimestamp::now(),
            self.replication_factor.unwrap_or(1),
            message_expiry,
            self.compression_algorithm,
            max_topic_size,
        );

        // Set the ID
        topic.update_id(topic_id);

        // Insert into our local state
        let _slab_id = shard
            .streams2
            .with_topics(&self.stream_id, |topics| topics.insert(topic.clone()));
        shard.metrics.increment_topics(1);

        // Create file hierarchy for the topic
        create_topic_file_hierarchy(shard.id, numeric_stream_id, topic_id, &shard.config.system)
            .await?;

        shard_info!(
            shard.id,
            "Topic '{}' created successfully with ID {} in stream {}",
            self.name,
            topic_id,
            numeric_stream_id
        );

        // Prepare event for broadcast
        let event = TopicCreatedEvent {
            stream_id: self.stream_id.clone(),
            topic: topic.clone(),
        };

        Ok((topic, event))
    }

    fn apply_event(&self, shard: &IggyShard, event: &Self::Event) -> Result<(), IggyError> {
        // This is called on non-coordinator shards when they receive the broadcast
        let _topic_id = shard.streams2.with_topics(&event.stream_id, |topics| {
            topics.insert(event.topic.clone())
        });
        shard.metrics.increment_topics(1);

        shard_info!(
            shard.id,
            "Applied CreateTopic event: topic '{}' with ID {} in stream {:?}",
            event.topic.root().name(),
            event.topic.id(),
            event.stream_id
        );

        Ok(())
    }

    fn to_request_payload(&self) -> ShardRequestPayload {
        ShardRequestPayload::CreateTopic {
            session_id: 0, // Will be filled by the handler if needed
            stream_id: self.stream_id.clone(),
            name: self.name.clone(),
            partitions_count: 0, // Partitions are handled separately after topic creation
            message_expiry: self.message_expiry,
            compression_algorithm: self.compression_algorithm,
            max_topic_size: self.max_topic_size,
            replication_factor: self.replication_factor,
        }
    }

    fn from_shard_response(&self, response: ShardResponse) -> Result<Self::Response, IggyError> {
        match response {
            ShardResponse::CreatedTopic { topic_id } => {
                // Create a placeholder topic with just the ID
                // The actual topic will be populated when the broadcast arrives
                let mut topic = topic2::Topic::new(
                    String::new(), // Name will be filled from broadcast
                    Arc::new(crate::streaming::stats::TopicStats::new(Arc::new(
                        crate::streaming::stats::StreamStats::default(),
                    ))),
                    IggyTimestamp::now(),
                    1,
                    self.message_expiry,
                    self.compression_algorithm,
                    self.max_topic_size,
                );
                topic.update_id(topic_id);
                Ok(topic)
            }
            ShardResponse::ErrorResponse(err) => Err(err),
            _ => Err(IggyError::InvalidCommand),
        }
    }

    fn check_local_state(&self, shard: &IggyShard) -> Option<Self::Response> {
        // Check if the topic has been created in our local state
        // This is used for synchronization after forwarding to shard 0
        use crate::streaming::topics::helpers;
        use std::str::FromStr;

        let topic_id = Identifier::from_str(&self.name).unwrap();

        if shard
            .streams2
            .with_topics(&self.stream_id, helpers::exists(&topic_id))
        {
            Some(shard.streams2.with_topic_by_id(
                &self.stream_id,
                &topic_id,
                |(root, aux, stats)| {
                    topic2::Topic::new_with_components(root.clone(), aux.clone(), stats.clone())
                },
            ))
        } else {
            None
        }
    }
}
