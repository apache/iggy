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
use crate::streaming::streams::{storage2::create_stream_file_hierarchy, stream2};
use iggy_common::{Identifier, IggyError, IggyTimestamp};
use std::sync::{Arc, atomic::Ordering};

/// Event broadcast when a stream is created
#[derive(Debug, Clone)]
pub struct StreamCreatedEvent {
    pub id: usize,
    pub stream: stream2::Stream,
}

impl From<StreamCreatedEvent> for ShardEvent {
    fn from(event: StreamCreatedEvent) -> Self {
        ShardEvent::CreatedStream2 {
            id: event.id,
            stream: event.stream,
        }
    }
}

/// Operation to create a new stream
#[derive(Debug, Clone)]
pub struct CreateStreamOp {
    pub name: String,
}

impl CreateStreamOp {
    pub fn new(name: String) -> Self {
        CreateStreamOp { name }
    }
}

impl MetadataOperation for CreateStreamOp {
    type Response = stream2::Stream;
    type Event = StreamCreatedEvent;

    async fn execute_on_coordinator(
        &self,
        shard: &IggyShard,
    ) -> Result<(Self::Response, Self::Event), IggyError> {
        // Generate the next stream ID (only shard 0 has the ID generator)
        let stream_id = shard.next_stream_id.fetch_add(1, Ordering::SeqCst);

        shard_info!(
            shard.id,
            "Creating stream '{}' with ID {}",
            self.name,
            stream_id
        );

        // Create stream with the assigned ID
        let mut stream = stream2::Stream::new(
            self.name.clone(),
            Arc::new(crate::streaming::stats::StreamStats::default()),
            IggyTimestamp::now(),
        );

        // Set the ID
        stream.update_id(stream_id);

        // Insert into our local state
        let _slab_id = shard.streams2.insert(stream.clone());
        shard.metrics.increment_streams(1);

        // Create file hierarchy for the stream
        create_stream_file_hierarchy(shard.id, stream_id, &shard.config.system).await?;

        shard_info!(
            shard.id,
            "Stream '{}' created successfully with ID {}",
            self.name,
            stream_id
        );

        // Prepare event for broadcast
        let event = StreamCreatedEvent {
            id: stream_id,
            stream: stream.clone(),
        };

        Ok((stream, event))
    }

    fn apply_event(&self, shard: &IggyShard, event: &Self::Event) -> Result<(), IggyError> {
        // This is called on non-coordinator shards when they receive the broadcast
        // Use insert_with_id to preserve the stream ID from shard 0
        let _slab_id = shard
            .streams2
            .insert_with_id(event.id, event.stream.clone());
        shard.metrics.increment_streams(1);

        shard_info!(
            shard.id,
            "Applied CreateStream event: stream '{}' with ID {}",
            event.stream.root().name(),
            event.id
        );

        Ok(())
    }

    fn to_request_payload(&self) -> ShardRequestPayload {
        ShardRequestPayload::CreateStream {
            session_id: 0, // Will be filled by the handler if needed
            name: self.name.clone(),
        }
    }

    fn from_shard_response(&self, response: ShardResponse) -> Result<Self::Response, IggyError> {
        match response {
            ShardResponse::CreatedStream { stream_id } => {
                // Create a placeholder stream with just the ID
                // The actual stream will be populated when the broadcast arrives
                let mut stream = stream2::Stream::new(
                    String::new(), // Name will be filled from broadcast
                    Arc::new(crate::streaming::stats::StreamStats::default()),
                    IggyTimestamp::now(),
                );
                stream.update_id(stream_id);
                Ok(stream)
            }
            ShardResponse::ErrorResponse(err) => Err(err),
            _ => Err(IggyError::InvalidCommand),
        }
    }

    fn check_local_state(&self, shard: &IggyShard) -> Option<Self::Response> {
        // Check if the stream has been created in our local state
        // This is used for synchronization after forwarding to shard 0
        let stream_id = Identifier::from_str_value(&self.name).unwrap();

        if shard.streams2.exists(&stream_id) {
            Some(
                shard
                    .streams2
                    .with_stream_by_id(&stream_id, |(root, stats)| {
                        stream2::Stream::new_with_components(root.clone(), stats.clone())
                    }),
            )
        } else {
            None
        }
    }
}
