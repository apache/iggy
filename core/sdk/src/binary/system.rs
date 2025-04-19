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

use crate::binary::binary_client::BinaryClient;
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::client::SystemClient;
use crate::error::IggyError;
use crate::models::client_info::{ClientInfo, ClientInfoDetails};
use crate::models::snapshot::Snapshot;
use crate::models::stats::Stats;
use crate::snapshot::{SnapshotCompression, SystemSnapshotType};
use crate::system::get_client::GetClient;
use crate::system::get_clients::GetClients;
use crate::system::get_me::GetMe;
use crate::system::get_snapshot::GetSnapshot;
use crate::system::get_stats::GetStats;
use crate::system::ping::Ping;
use crate::utils::duration::IggyDuration;

#[async_trait::async_trait]
impl<B: BinaryClient> SystemClient for B {
    async fn get_stats(&self) -> Result<Stats, IggyError> {
        let response = self.send_with_response(&GetStats {}).await?;
        mapper::map_stats(response)
    }

    async fn get_me(&self) -> Result<ClientInfoDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self.send_with_response(&GetMe {}).await?;
        mapper::map_client(response)
    }

    async fn get_client(&self, client_id: u32) -> Result<Option<ClientInfoDetails>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self.send_with_response(&GetClient { client_id }).await?;
        if response.is_empty() {
            return Ok(None);
        }

        mapper::map_client(response).map(Some)
    }

    async fn get_clients(&self) -> Result<Vec<ClientInfo>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self.send_with_response(&GetClients {}).await?;
        mapper::map_clients(response)
    }

    async fn ping(&self) -> Result<(), IggyError> {
        self.send_with_response(&Ping {}).await?;
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
            .send_with_response(&GetSnapshot {
                compression,
                snapshot_types,
            })
            .await?;
        let snapshot = Snapshot::new(response.to_vec());
        Ok(snapshot)
    }
}
