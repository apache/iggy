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

use crate::prelude::IggyClient;
use async_dropper::AsyncDrop;
use async_trait::async_trait;
use iggy_binary_protocol::{
    WireDecode, WireEncode, codes::SYNC_CONSUMER_GROUP_CODE,
    requests::consumer_groups::SyncConsumerGroupRequest,
    responses::consumer_groups::SyncConsumerGroupResponse,
};
use iggy_common::{
    ConsumerGroup, ConsumerGroupDetails, Identifier, IggyError, locking::IggyRwLockFn,
    wire_conversions::identifier_to_wire,
};
use iggy_common::{ConsumerGroupClient, UserClient};

impl IggyClient {
    /// Reads this connection's partition assignment from the group coordinator.
    /// Returns `None` if the connection is not a member, and an empty vector if it owns
    /// no partitions. The snapshot can change during a rebalance. HTTP does not support
    /// consumer-group membership and returns [`IggyError::FeatureUnavailable`].
    pub async fn get_consumer_group_assignment(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<Option<Vec<u32>>, IggyError> {
        let request = SyncConsumerGroupRequest {
            stream_id: identifier_to_wire(stream_id)?,
            topic_id: identifier_to_wire(topic_id)?,
            group_id: identifier_to_wire(group_id)?,
        };
        let response = self
            .send_binary_request(SYNC_CONSUMER_GROUP_CODE, request.to_bytes())
            .await?;
        if response.is_empty() {
            return Ok(None);
        }
        let (assignment, _) =
            SyncConsumerGroupResponse::decode(&response).map_err(|_| IggyError::InvalidCommand)?;
        Ok(Some(assignment.partitions))
    }
}

#[async_trait]
impl ConsumerGroupClient for IggyClient {
    async fn get_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<Option<ConsumerGroupDetails>, IggyError> {
        self.client
            .read()
            .await
            .get_consumer_group(stream_id, topic_id, group_id)
            .await
    }

    async fn get_consumer_groups(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Vec<ConsumerGroup>, IggyError> {
        self.client
            .read()
            .await
            .get_consumer_groups(stream_id, topic_id)
            .await
    }

    async fn create_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
    ) -> Result<ConsumerGroupDetails, IggyError> {
        self.client
            .read()
            .await
            .create_consumer_group(stream_id, topic_id, name)
            .await
    }

    async fn delete_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .delete_consumer_group(stream_id, topic_id, group_id)
            .await
    }

    async fn join_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .join_consumer_group(stream_id, topic_id, group_id)
            .await
    }

    async fn leave_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .leave_consumer_group(stream_id, topic_id, group_id)
            .await
    }
}

#[async_trait]
impl AsyncDrop for IggyClient {
    async fn async_drop(&mut self) {
        let _ = self.client.read().await.logout_user().await;
    }
}
