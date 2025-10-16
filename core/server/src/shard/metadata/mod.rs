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

pub mod coordinator;
pub mod operations;

use crate::shard::IggyShard;
use crate::shard::transmission::{
    event::ShardEvent, frame::ShardResponse, message::ShardRequestPayload,
};
use crate::streaming::session::Session;
use iggy_common::IggyError;
use std::fmt::Debug;
use std::rc::Rc;

/// Lightweight session information for metadata operations
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub client_id: u32,
    pub user_id: u32,
}

impl From<&Session> for SessionInfo {
    fn from(session: &Session) -> Self {
        SessionInfo {
            client_id: session.client_id,
            user_id: session.get_user_id(),
        }
    }
}

impl From<&Rc<Session>> for SessionInfo {
    fn from(session: &Rc<Session>) -> Self {
        SessionInfo {
            client_id: session.client_id,
            user_id: session.get_user_id(),
        }
    }
}

/// Trait for operations that must be coordinated by shard 0
///
/// This trait abstracts the common pattern of:
/// 1. Non-shard-0: Forward request to shard 0
/// 2. Shard-0: Execute operation, generate ID, create entity, broadcast event
/// 3. All shards: Apply the event to local state
pub trait MetadataOperation: Debug + Clone + Send + 'static {
    /// The response type for this operation
    type Response: Debug + Clone + Send;

    /// The event to broadcast after successful execution
    type Event: Into<ShardEvent> + Clone + Debug;

    /// Execute the operation on shard 0 (the coordinator)
    /// This is where ID generation, entity creation, and persistence happens
    async fn execute_on_coordinator(
        &self,
        shard: &IggyShard,
    ) -> Result<(Self::Response, Self::Event), IggyError>;

    /// Apply the event locally after receiving it via broadcast
    /// This is called on non-coordinator shards to update their local state
    fn apply_event(&self, shard: &IggyShard, event: &Self::Event) -> Result<(), IggyError>;

    /// Convert this operation to a ShardRequestPayload for forwarding to shard 0
    fn to_request_payload(&self) -> ShardRequestPayload;

    /// Extract the response from a ShardResponse received from shard 0
    fn from_shard_response(&self, response: ShardResponse) -> Result<Self::Response, IggyError>;

    /// Check if the local state has been updated after a broadcast
    /// Used for synchronization after forwarding to shard 0
    fn check_local_state(&self, shard: &IggyShard) -> Option<Self::Response>;
}

/// Extension trait to check if a request payload is a metadata operation
pub trait MetadataPayload {
    fn is_metadata_operation(&self) -> bool;
}

impl MetadataPayload for ShardRequestPayload {
    fn is_metadata_operation(&self) -> bool {
        matches!(
            self,
            ShardRequestPayload::CreateStream { .. }
                | ShardRequestPayload::CreateTopic { .. }
                | ShardRequestPayload::DeleteStream { .. }
                | ShardRequestPayload::DeleteTopic { .. }
        )
    }
}
