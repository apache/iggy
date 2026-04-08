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
use crate::BinaryClient;
use crate::traits::binary_auth::fail_if_not_authenticated;
use crate::wire_conversions::{
    consumer_to_wire, identifier_to_wire, partitioning_to_wire, polling_strategy_to_wire,
};
use crate::{
    Consumer, Identifier, IggyError, IggyMessage, MessageClient, Partitioning, PolledMessages,
    PollingStrategy,
};
use bytes::BytesMut;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::codes::{
    FLUSH_UNSAVED_BUFFER_CODE, POLL_MESSAGES_CODE, SEND_MESSAGES_CODE,
};
use iggy_binary_protocol::requests::messages::{
    FlushUnsavedBufferRequest, PollMessagesRequest, RawMessage, SendMessagesEncoder,
};

#[async_trait::async_trait]
impl<B: BinaryClient> MessageClient for B {
    async fn poll_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> Result<PolledMessages, IggyError> {
        fail_if_not_authenticated(self).await?;
        let req = PollMessagesRequest {
            consumer: consumer_to_wire(consumer)?,
            stream_id: identifier_to_wire(stream_id)?,
            topic_id: identifier_to_wire(topic_id)?,
            partition_id,
            strategy: polling_strategy_to_wire(strategy),
            count,
            auto_commit,
        };
        let response = self
            .send_raw_with_response(POLL_MESSAGES_CODE, req.to_bytes())
            .await?;
        PolledMessages::from_bytes(response)
    }

    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [IggyMessage],
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_stream_id = identifier_to_wire(stream_id)?;
        let wire_topic_id = identifier_to_wire(topic_id)?;
        let wire_partitioning = partitioning_to_wire(partitioning)?;
        let raw_messages: Vec<RawMessage<'_>> = messages
            .iter()
            .map(|m| RawMessage {
                id: m.header.id,
                origin_timestamp: m.header.origin_timestamp,
                headers: m.user_headers.as_deref(),
                payload: &m.payload,
            })
            .collect();
        let size = SendMessagesEncoder::encoded_size(
            &wire_stream_id,
            &wire_topic_id,
            &wire_partitioning,
            &raw_messages,
        );
        let mut buf = BytesMut::with_capacity(size);
        SendMessagesEncoder::encode(
            &mut buf,
            &wire_stream_id,
            &wire_topic_id,
            &wire_partitioning,
            &raw_messages,
        );
        self.send_raw_with_response(SEND_MESSAGES_CODE, buf.freeze())
            .await?;
        Ok(())
    }

    async fn flush_unsaved_buffer(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        fsync: bool,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let req = FlushUnsavedBufferRequest {
            stream_id: identifier_to_wire(stream_id)?,
            topic_id: identifier_to_wire(topic_id)?,
            partition_id,
            fsync,
        };
        self.send_raw_with_response(FLUSH_UNSAVED_BUFFER_CODE, req.to_bytes())
            .await?;
        Ok(())
    }
}
