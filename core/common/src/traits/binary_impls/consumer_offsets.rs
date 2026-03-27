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
use crate::wire_conversions::{consumer_to_wire, identifier_to_wire};
use crate::{
    BinaryClient, Consumer, ConsumerOffsetClient, ConsumerOffsetInfo, Identifier, IggyError,
};
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::codes::{
    DELETE_CONSUMER_OFFSET_CODE, GET_CONSUMER_OFFSET_CODE, STORE_CONSUMER_OFFSET_CODE,
};
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffsetRequest, GetConsumerOffsetRequest, StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::responses::consumer_offsets::get_consumer_offset::ConsumerOffsetResponse;

#[async_trait::async_trait]
impl<B: BinaryClient> ConsumerOffsetClient for B {
    async fn store_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_consumer = consumer_to_wire(consumer)?;
        let wire_stream_id = identifier_to_wire(stream_id)?;
        let wire_topic_id = identifier_to_wire(topic_id)?;
        self.send_raw_with_response(
            STORE_CONSUMER_OFFSET_CODE,
            StoreConsumerOffsetRequest {
                consumer: wire_consumer,
                stream_id: wire_stream_id,
                topic_id: wire_topic_id,
                partition_id,
                offset,
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn get_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<Option<ConsumerOffsetInfo>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_consumer = consumer_to_wire(consumer)?;
        let wire_stream_id = identifier_to_wire(stream_id)?;
        let wire_topic_id = identifier_to_wire(topic_id)?;
        let response = self
            .send_raw_with_response(
                GET_CONSUMER_OFFSET_CODE,
                GetConsumerOffsetRequest {
                    consumer: wire_consumer,
                    stream_id: wire_stream_id,
                    topic_id: wire_topic_id,
                    partition_id,
                }
                .to_bytes(),
            )
            .await?;
        if response.is_empty() {
            return Ok(None);
        }
        let wire_resp = super::decode_response::<ConsumerOffsetResponse>(&response)?;
        Ok(Some(ConsumerOffsetInfo::from(wire_resp)))
    }

    async fn delete_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_consumer = consumer_to_wire(consumer)?;
        let wire_stream_id = identifier_to_wire(stream_id)?;
        let wire_topic_id = identifier_to_wire(topic_id)?;
        self.send_raw_with_response(
            DELETE_CONSUMER_OFFSET_CODE,
            DeleteConsumerOffsetRequest {
                consumer: wire_consumer,
                stream_id: wire_stream_id,
                topic_id: wire_topic_id,
                partition_id,
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }
}
