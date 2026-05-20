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
use crate::wire_conversions::identifier_to_wire;
use crate::{BinaryClient, Identifier, IggyError, PartitionClient};
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::codes::{CREATE_PARTITIONS_CODE, DELETE_PARTITIONS_CODE};
use iggy_binary_protocol::requests::partitions::{
    CreatePartitionsRequest, DeletePartitionsRequest,
};

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
}
