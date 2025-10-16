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

use super::MetadataOperation;
use crate::shard::transmission::{
    frame::ShardResponse,
    message::{ShardMessage, ShardRequest, ShardRequestPayload},
};
use crate::shard::{IggyShard, shard_info};
use crate::slab::traits_ext::EntityMarker;
use iggy_common::{Identifier, IggyError};
use std::time::{Duration, Instant};
use tracing::debug;

/// Default timeout for waiting for broadcast synchronization
const BROADCAST_SYNC_TIMEOUT: Duration = Duration::from_millis(100);
const BROADCAST_SYNC_POLL_INTERVAL: Duration = Duration::from_millis(1);

impl IggyShard {
    /// Generic handler for metadata operations that require shard 0 coordination
    ///
    /// This method handles the common pattern of:
    /// - If shard 0: Execute operation directly and broadcast to other shards
    /// - If non-shard 0: Forward to shard 0 and wait for broadcast synchronization
    pub async fn coordinate_metadata_operation<Op: MetadataOperation>(
        &self,
        operation: Op,
    ) -> Result<Op::Response, IggyError> {
        if self.id == 0 {
            // We're the coordinator, execute directly
            debug!("Shard 0 executing metadata operation: {:?}", operation);

            let (response, event) = operation.execute_on_coordinator(self).await?;

            // Broadcast to other shards (fire-and-forget for metadata)
            // We don't wait for success as metadata operations are eventually consistent
            let broadcast_event = event.clone().into();
            let _ = self.broadcast_event_to_all_shards(broadcast_event).await;

            shard_info!(
                self.id,
                "Metadata operation completed and broadcast: {:?}",
                operation
            );

            Ok(response)
        } else {
            // Forward to shard 0
            debug!(
                "Shard {} forwarding metadata operation to shard 0: {:?}",
                self.id, operation
            );
            self.forward_to_coordinator(operation).await
        }
    }

    /// Forward a metadata operation to shard 0 (the coordinator)
    async fn forward_to_coordinator<Op: MetadataOperation>(
        &self,
        operation: Op,
    ) -> Result<Op::Response, IggyError> {
        // Create request for shard 0
        // Using dummy values for stream_id, topic_id, partition_id as these are not used
        // for metadata operations
        let request = ShardRequest::new(
            Identifier::numeric(0).unwrap(), // Dummy stream_id
            Identifier::numeric(0).unwrap(), // Dummy topic_id
            0,                               // Dummy partition_id
            operation.to_request_payload(),
        );

        // Send to shard 0
        debug!("Sending metadata request to shard 0: {:?}", request.payload);
        let response = self.shards[0]
            .send_request(ShardMessage::Request(request))
            .await?;

        debug!("Received response from shard 0: {:?}", response);

        // Extract typed response
        let initial_response = operation.from_shard_response(response)?;

        // Wait for the broadcast to arrive and update our local state
        // This replaces the fragile sleep(10ms) pattern with proper synchronization
        match self
            .await_broadcast_sync(BROADCAST_SYNC_TIMEOUT, || operation.check_local_state(self))
            .await
        {
            Ok(synced_response) => {
                debug!("Local state synchronized after broadcast");
                Ok(synced_response)
            }
            Err(_) => {
                // If synchronization times out, return the initial response
                // The local state will eventually be updated when the broadcast arrives
                debug!("Broadcast sync timeout, returning initial response");
                Ok(initial_response)
            }
        }
    }

    /// Wait for a broadcast event to be applied locally
    /// This replaces the fragile sleep(10ms) pattern with proper polling
    pub async fn await_broadcast_sync<F, R>(
        &self,
        timeout: Duration,
        check_fn: F,
    ) -> Result<R, IggyError>
    where
        F: Fn() -> Option<R>,
    {
        let start = Instant::now();

        while start.elapsed() < timeout {
            if let Some(result) = check_fn() {
                return Ok(result);
            }
            compio::time::sleep(BROADCAST_SYNC_POLL_INTERVAL).await;
        }

        Err(IggyError::TaskTimeout)
    }

    /// Handle a metadata request received by shard 0
    /// This is called from handle_request when a metadata operation is received
    pub async fn handle_metadata_request(
        &self,
        payload: ShardRequestPayload,
    ) -> Result<ShardResponse, IggyError> {
        use super::operations::{CreateStreamOp, CreateTopicOp};

        // Only shard 0 should receive these directly from other shards
        if self.id != 0 {
            return Err(IggyError::InvalidCommand);
        }

        match payload {
            ShardRequestPayload::CreateStream {
                session_id: _,
                name,
            } => {
                let op = CreateStreamOp::new(name);
                let (stream, event) = op.execute_on_coordinator(self).await?;

                // Broadcast to other shards
                let _ = self.broadcast_event_to_all_shards(event.into()).await;

                Ok(ShardResponse::CreatedStream {
                    stream_id: stream.id(),
                })
            }
            ShardRequestPayload::CreateTopic {
                session_id: _,
                stream_id,
                name,
                partitions_count: _, // Handled separately after topic creation
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor,
            } => {
                let op = CreateTopicOp::new(
                    stream_id,
                    name,
                    message_expiry,
                    compression_algorithm,
                    max_topic_size,
                    replication_factor,
                );
                let (topic, event) = op.execute_on_coordinator(self).await?;

                // Broadcast to other shards
                let _ = self.broadcast_event_to_all_shards(event.into()).await;

                Ok(ShardResponse::CreatedTopic {
                    topic_id: topic.id(),
                })
            }
            ShardRequestPayload::DeleteStream { .. } | ShardRequestPayload::DeleteTopic { .. } => {
                // TODO: Implement delete operations following the same pattern
                todo!("Implement delete metadata operations")
            }
            _ => {
                // This shouldn't happen as we filter metadata operations before calling this
                Err(IggyError::InvalidCommand)
            }
        }
    }
}
