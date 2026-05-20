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
use crate::wire_conversions::{identifier_to_wire, topics_from_wire};
use crate::{
    BinaryClient, CompressionAlgorithm, Identifier, IggyError, IggyExpiry, MaxTopicSize, Topic,
    TopicClient, TopicDetails,
};
use iggy_binary_protocol::WireName;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::codes::{
    CREATE_TOPIC_CODE, DELETE_TOPIC_CODE, GET_TOPIC_CODE, GET_TOPICS_CODE, PURGE_TOPIC_CODE,
    UPDATE_TOPIC_CODE,
};
use iggy_binary_protocol::requests::topics::{
    CreateTopicRequest, DeleteTopicRequest, GetTopicRequest, GetTopicsRequest, PurgeTopicRequest,
    UpdateTopicRequest,
};
use iggy_binary_protocol::responses::topics::get_topic::GetTopicResponse;
use iggy_binary_protocol::responses::topics::get_topics::GetTopicsResponse;

#[async_trait::async_trait]
impl<B: BinaryClient> TopicClient for B {
    async fn get_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Option<TopicDetails>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_stream_id = identifier_to_wire(stream_id)?;
        let wire_topic_id = identifier_to_wire(topic_id)?;
        let response = self
            .send_raw_with_response(
                GET_TOPIC_CODE,
                GetTopicRequest {
                    stream_id: wire_stream_id,
                    topic_id: wire_topic_id,
                }
                .to_bytes(),
            )
            .await?;
        if response.is_empty() {
            return Ok(None);
        }
        let wire_resp = super::decode_response::<GetTopicResponse>(&response)?;
        Ok(Some(TopicDetails::try_from(wire_resp)?))
    }

    async fn get_topics(&self, stream_id: &Identifier) -> Result<Vec<Topic>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_stream_id = identifier_to_wire(stream_id)?;
        let response = self
            .send_raw_with_response(
                GET_TOPICS_CODE,
                GetTopicsRequest {
                    stream_id: wire_stream_id,
                }
                .to_bytes(),
            )
            .await?;
        if response.is_empty() {
            return Ok(Vec::new());
        }
        let wire_resp = super::decode_response::<GetTopicsResponse>(&response)?;
        Ok(topics_from_wire(wire_resp)?)
    }

    async fn create_topic(
        &self,
        stream_id: &Identifier,
        name: &str,
        partitions_count: u32,
        compression_algorithm: CompressionAlgorithm,
        replication_factor: Option<u8>,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
    ) -> Result<TopicDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_stream_id = identifier_to_wire(stream_id)?;
        let wire_name = WireName::new(name).map_err(|_| IggyError::InvalidFormat)?;
        let response = self
            .send_raw_with_response(
                CREATE_TOPIC_CODE,
                CreateTopicRequest {
                    stream_id: wire_stream_id,
                    partitions_count,
                    compression_algorithm: compression_algorithm.as_code(),
                    message_expiry: u64::from(message_expiry),
                    max_topic_size: u64::from(max_topic_size),
                    replication_factor: replication_factor.unwrap_or(0),
                    name: wire_name,
                }
                .to_bytes(),
            )
            .await?;
        let wire_resp = super::decode_response::<GetTopicResponse>(&response)?;
        Ok(TopicDetails::try_from(wire_resp)?)
    }

    async fn update_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        compression_algorithm: CompressionAlgorithm,
        replication_factor: Option<u8>,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_stream_id = identifier_to_wire(stream_id)?;
        let wire_topic_id = identifier_to_wire(topic_id)?;
        let wire_name = WireName::new(name).map_err(|_| IggyError::InvalidFormat)?;
        self.send_raw_with_response(
            UPDATE_TOPIC_CODE,
            UpdateTopicRequest {
                stream_id: wire_stream_id,
                topic_id: wire_topic_id,
                compression_algorithm: compression_algorithm.as_code(),
                message_expiry: u64::from(message_expiry),
                max_topic_size: u64::from(max_topic_size),
                replication_factor: replication_factor.unwrap_or(0),
                name: wire_name,
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn delete_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_stream_id = identifier_to_wire(stream_id)?;
        let wire_topic_id = identifier_to_wire(topic_id)?;
        self.send_raw_with_response(
            DELETE_TOPIC_CODE,
            DeleteTopicRequest {
                stream_id: wire_stream_id,
                topic_id: wire_topic_id,
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn purge_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_stream_id = identifier_to_wire(stream_id)?;
        let wire_topic_id = identifier_to_wire(topic_id)?;
        self.send_raw_with_response(
            PURGE_TOPIC_CODE,
            PurgeTopicRequest {
                stream_id: wire_stream_id,
                topic_id: wire_topic_id,
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }
}
