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

use crate::traits::binary_auth::fail_if_not_authenticated;
use crate::wire_conversions::{consumer_groups_from_wire, identifier_to_wire};
use crate::{
    BinaryClient, ConsumerGroup, ConsumerGroupClient, ConsumerGroupDetails, Identifier, IggyError,
};
use iggy_binary_protocol::WireName;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::codes::{
    CREATE_CONSUMER_GROUP_CODE, DELETE_CONSUMER_GROUP_CODE, GET_CONSUMER_GROUP_CODE,
    GET_CONSUMER_GROUPS_CODE, JOIN_CONSUMER_GROUP_CODE, LEAVE_CONSUMER_GROUP_CODE,
};
use iggy_binary_protocol::requests::consumer_groups::{
    CreateConsumerGroupRequest, DeleteConsumerGroupRequest, GetConsumerGroupRequest,
    GetConsumerGroupsRequest, JoinConsumerGroupRequest, LeaveConsumerGroupRequest,
};
use iggy_binary_protocol::responses::consumer_groups::get_consumer_group::ConsumerGroupDetailsResponse;
use iggy_binary_protocol::responses::consumer_groups::get_consumer_groups::GetConsumerGroupsResponse;

#[async_trait::async_trait]
impl<B: BinaryClient> ConsumerGroupClient for B {
    async fn get_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<Option<ConsumerGroupDetails>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_stream_id = identifier_to_wire(stream_id)?;
        let wire_topic_id = identifier_to_wire(topic_id)?;
        let wire_group_id = identifier_to_wire(group_id)?;
        let response = self
            .send_raw_with_response(
                GET_CONSUMER_GROUP_CODE,
                GetConsumerGroupRequest {
                    stream_id: wire_stream_id,
                    topic_id: wire_topic_id,
                    group_id: wire_group_id,
                }
                .to_bytes(),
            )
            .await?;
        if response.is_empty() {
            return Ok(None);
        }
        let wire_resp = super::decode_response::<ConsumerGroupDetailsResponse>(&response)?;
        Ok(Some(ConsumerGroupDetails::from(wire_resp)))
    }

    async fn get_consumer_groups(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Vec<ConsumerGroup>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_stream_id = identifier_to_wire(stream_id)?;
        let wire_topic_id = identifier_to_wire(topic_id)?;
        let response = self
            .send_raw_with_response(
                GET_CONSUMER_GROUPS_CODE,
                GetConsumerGroupsRequest {
                    stream_id: wire_stream_id,
                    topic_id: wire_topic_id,
                }
                .to_bytes(),
            )
            .await?;
        if response.is_empty() {
            return Ok(Vec::new());
        }
        let wire_resp = super::decode_response::<GetConsumerGroupsResponse>(&response)?;
        Ok(consumer_groups_from_wire(wire_resp))
    }

    async fn create_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
    ) -> Result<ConsumerGroupDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_stream_id = identifier_to_wire(stream_id)?;
        let wire_topic_id = identifier_to_wire(topic_id)?;
        let wire_name = WireName::new(name).map_err(|_| IggyError::InvalidFormat)?;
        let response = self
            .send_raw_with_response(
                CREATE_CONSUMER_GROUP_CODE,
                CreateConsumerGroupRequest {
                    stream_id: wire_stream_id,
                    topic_id: wire_topic_id,
                    name: wire_name,
                }
                .to_bytes(),
            )
            .await?;
        let wire_resp = super::decode_response::<ConsumerGroupDetailsResponse>(&response)?;
        Ok(ConsumerGroupDetails::from(wire_resp))
    }

    async fn delete_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_stream_id = identifier_to_wire(stream_id)?;
        let wire_topic_id = identifier_to_wire(topic_id)?;
        let wire_group_id = identifier_to_wire(group_id)?;
        self.send_raw_with_response(
            DELETE_CONSUMER_GROUP_CODE,
            DeleteConsumerGroupRequest {
                stream_id: wire_stream_id,
                topic_id: wire_topic_id,
                group_id: wire_group_id,
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn join_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_stream_id = identifier_to_wire(stream_id)?;
        let wire_topic_id = identifier_to_wire(topic_id)?;
        let wire_group_id = identifier_to_wire(group_id)?;
        self.send_raw_with_response(
            JOIN_CONSUMER_GROUP_CODE,
            JoinConsumerGroupRequest {
                stream_id: wire_stream_id,
                topic_id: wire_topic_id,
                group_id: wire_group_id,
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn leave_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_stream_id = identifier_to_wire(stream_id)?;
        let wire_topic_id = identifier_to_wire(topic_id)?;
        let wire_group_id = identifier_to_wire(group_id)?;
        self.send_raw_with_response(
            LEAVE_CONSUMER_GROUP_CODE,
            LeaveConsumerGroupRequest {
                stream_id: wire_stream_id,
                topic_id: wire_topic_id,
                group_id: wire_group_id,
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }
}
