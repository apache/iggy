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

use crate::traits::binary_auth::fail_if_not_authenticated;
use crate::wire_conversions::{identifier_to_wire, page_from_wire};
use crate::{BinaryClient, Identifier, IggyError, Page, PageQuery, Partition, PartitionClient};
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::codes::{
    CREATE_PARTITIONS_CODE, DELETE_PARTITIONS_CODE, GET_PARTITIONS_CODE,
};
use iggy_binary_protocol::primitives::page::WirePageQuery;
use iggy_binary_protocol::requests::partitions::{
    CreatePartitionsRequest, DeletePartitionsRequest, GetPartitionsRequest,
};
use iggy_binary_protocol::responses::PageResponse;
use iggy_binary_protocol::responses::topics::get_topic::PartitionResponse;

#[async_trait::async_trait]
impl<B: BinaryClient> PartitionClient for B {
    async fn create_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_stream_id = identifier_to_wire(stream_id)?;
        let wire_topic_id = identifier_to_wire(topic_id)?;
        self.send_raw_with_response(
            CREATE_PARTITIONS_CODE,
            CreatePartitionsRequest {
                stream_id: wire_stream_id,
                topic_id: wire_topic_id,
                partitions_count,
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn delete_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_stream_id = identifier_to_wire(stream_id)?;
        let wire_topic_id = identifier_to_wire(topic_id)?;
        self.send_raw_with_response(
            DELETE_PARTITIONS_CODE,
            DeletePartitionsRequest {
                stream_id: wire_stream_id,
                topic_id: wire_topic_id,
                partitions_count,
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn get_partitions_page(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        query: &PageQuery,
    ) -> Result<Page<Partition>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let request = GetPartitionsRequest {
            stream_id: identifier_to_wire(stream_id)?,
            topic_id: identifier_to_wire(topic_id)?,
            page: WirePageQuery::try_from(query)?,
        };
        let response = self
            .send_raw_with_response(GET_PARTITIONS_CODE, request.to_bytes())
            .await?;
        let page = super::decode_response::<PageResponse<PartitionResponse>>(&response)?;
        page_from_wire(page, |partition| Ok(Partition::from(partition)))
    }
}
