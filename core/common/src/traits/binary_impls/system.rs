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
use crate::wire_conversions::clients_from_wire;
use crate::{
    BinaryClient, ClientInfo, ClientInfoDetails, IggyDuration, IggyError, Snapshot,
    SnapshotCompression, Stats, SystemClient, SystemSnapshotType,
};
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::codes::{
    GET_CLIENT_CODE, GET_CLIENTS_CODE, GET_ME_CODE, GET_SNAPSHOT_FILE_CODE, GET_STATS_CODE,
    PING_CODE,
};
use iggy_binary_protocol::requests::system::{
    GetClientRequest, GetClientsRequest, GetMeRequest, GetSnapshotRequest, GetStatsRequest,
    PingRequest,
};
use iggy_binary_protocol::responses::clients::get_client::ClientDetailsResponse;
use iggy_binary_protocol::responses::clients::get_clients::GetClientsResponse;
use iggy_binary_protocol::responses::system::get_stats::StatsResponse;

#[async_trait::async_trait]
impl<B: BinaryClient> SystemClient for B {
    async fn get_stats(&self) -> Result<Stats, IggyError> {
        let response = self
            .send_raw_with_response(GET_STATS_CODE, GetStatsRequest.to_bytes())
            .await?;
        let wire_resp = super::decode_response::<StatsResponse>(&response)?;
        Ok(Stats::from(wire_resp))
    }

    async fn get_me(&self) -> Result<ClientInfoDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_raw_with_response(GET_ME_CODE, GetMeRequest.to_bytes())
            .await?;
        let wire_resp = super::decode_response::<ClientDetailsResponse>(&response)?;
        Ok(ClientInfoDetails::from(wire_resp))
    }

    async fn get_client(&self, client_id: u32) -> Result<Option<ClientInfoDetails>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_raw_with_response(GET_CLIENT_CODE, GetClientRequest { client_id }.to_bytes())
            .await?;
        if response.is_empty() {
            return Ok(None);
        }
        let wire_resp = super::decode_response::<ClientDetailsResponse>(&response)?;
        Ok(Some(ClientInfoDetails::from(wire_resp)))
    }

    async fn get_clients(&self) -> Result<Vec<ClientInfo>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_raw_with_response(GET_CLIENTS_CODE, GetClientsRequest.to_bytes())
            .await?;
        if response.is_empty() {
            return Ok(Vec::new());
        }
        let wire_resp = super::decode_response::<GetClientsResponse>(&response)?;
        Ok(clients_from_wire(wire_resp))
    }

    async fn ping(&self) -> Result<(), IggyError> {
        self.send_raw_with_response(PING_CODE, PingRequest.to_bytes())
            .await?;
        Ok(())
    }

    async fn heartbeat_interval(&self) -> IggyDuration {
        self.get_heartbeat_interval()
    }

    async fn snapshot(
        &self,
        compression: SnapshotCompression,
        snapshot_types: Vec<SystemSnapshotType>,
    ) -> Result<Snapshot, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_raw_with_response(
                GET_SNAPSHOT_FILE_CODE,
                GetSnapshotRequest {
                    compression: compression.as_code(),
                    snapshot_types: snapshot_types
                        .iter()
                        .map(SystemSnapshotType::as_code)
                        .collect(),
                }
                .to_bytes(),
            )
            .await?;
        let snapshot = Snapshot::new(response.to_vec());
        Ok(snapshot)
    }
}
