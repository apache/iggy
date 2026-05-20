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
use crate::wire_conversions::{identifier_to_wire, streams_from_wire};
use crate::{BinaryClient, Identifier, IggyError, Stream, StreamClient, StreamDetails};
use iggy_binary_protocol::WireName;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::codes::{
    CREATE_STREAM_CODE, DELETE_STREAM_CODE, GET_STREAM_CODE, GET_STREAMS_CODE, PURGE_STREAM_CODE,
    UPDATE_STREAM_CODE,
};
use iggy_binary_protocol::requests::streams::{
    CreateStreamRequest, DeleteStreamRequest, GetStreamRequest, GetStreamsRequest,
    PurgeStreamRequest, UpdateStreamRequest,
};
use iggy_binary_protocol::responses::streams::get_stream::GetStreamResponse;
use iggy_binary_protocol::responses::streams::get_streams::GetStreamsResponse;

#[async_trait::async_trait]
impl<B: BinaryClient> StreamClient for B {
    async fn get_stream(&self, stream_id: &Identifier) -> Result<Option<StreamDetails>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_id = identifier_to_wire(stream_id)?;
        let response = self
            .send_raw_with_response(
                GET_STREAM_CODE,
                GetStreamRequest { stream_id: wire_id }.to_bytes(),
            )
            .await?;
        if response.is_empty() {
            return Ok(None);
        }
        let wire_resp = super::decode_response::<GetStreamResponse>(&response)?;
        Ok(Some(StreamDetails::try_from(wire_resp)?))
    }

    async fn get_streams(&self) -> Result<Vec<Stream>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_raw_with_response(GET_STREAMS_CODE, GetStreamsRequest.to_bytes())
            .await?;
        if response.is_empty() {
            return Ok(Vec::new());
        }
        let wire_resp = super::decode_response::<GetStreamsResponse>(&response)?;
        Ok(streams_from_wire(wire_resp))
    }

    async fn create_stream(&self, name: &str) -> Result<StreamDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_name = WireName::new(name).map_err(|_| IggyError::InvalidFormat)?;
        let response = self
            .send_raw_with_response(
                CREATE_STREAM_CODE,
                CreateStreamRequest { name: wire_name }.to_bytes(),
            )
            .await?;
        let wire_resp = super::decode_response::<GetStreamResponse>(&response)?;
        Ok(StreamDetails::try_from(wire_resp)?)
    }

    async fn update_stream(&self, stream_id: &Identifier, name: &str) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_id = identifier_to_wire(stream_id)?;
        let wire_name = WireName::new(name).map_err(|_| IggyError::InvalidFormat)?;
        self.send_raw_with_response(
            UPDATE_STREAM_CODE,
            UpdateStreamRequest {
                stream_id: wire_id,
                name: wire_name,
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn delete_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_id = identifier_to_wire(stream_id)?;
        self.send_raw_with_response(
            DELETE_STREAM_CODE,
            DeleteStreamRequest { stream_id: wire_id }.to_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn purge_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_id = identifier_to_wire(stream_id)?;
        self.send_raw_with_response(
            PURGE_STREAM_CODE,
            PurgeStreamRequest { stream_id: wire_id }.to_bytes(),
        )
        .await?;
        Ok(())
    }
}
