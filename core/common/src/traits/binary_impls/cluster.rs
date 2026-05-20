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
use crate::wire_conversions::cluster_metadata_from_wire;
use crate::{BinaryClient, ClusterClient, ClusterMetadata, IggyError};
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::codes::GET_CLUSTER_METADATA_CODE;
use iggy_binary_protocol::requests::system::GetClusterMetadataRequest;
use iggy_binary_protocol::responses::system::get_cluster_metadata::ClusterMetadataResponse;

#[async_trait::async_trait]
impl<B: BinaryClient> ClusterClient for B {
    async fn get_cluster_metadata(&self) -> Result<ClusterMetadata, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_raw_with_response(
                GET_CLUSTER_METADATA_CODE,
                GetClusterMetadataRequest.to_bytes(),
            )
            .await?;
        let wire_resp = super::decode_response::<ClusterMetadataResponse>(&response)?;
        cluster_metadata_from_wire(wire_resp)
    }
}
