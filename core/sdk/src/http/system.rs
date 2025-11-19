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

use crate::http::http_client::HttpClient;
use crate::http::http_transport::HttpTransport;
use crate::prelude::{IggyDuration, IggyError};
use async_trait::async_trait;
use iggy_binary_protocol::SystemClient;
use iggy_common::Snapshot;
use iggy_common::Stats;
use iggy_common::get_snapshot::GetSnapshot;
use iggy_common::{ClientInfo, ClientInfoDetails};
use iggy_common::{SnapshotCompression, SystemSnapshotType};

const PING: &str = "/ping";
const CLIENTS: &str = "/clients";
const STATS: &str = "/stats";
const SNAPSHOT: &str = "/snapshot";

#[async_trait]
impl SystemClient for HttpClient {
    async fn get_stats(&self) -> Result<Stats, IggyError> {
        let response = self.get(STATS).await?;
        let stats = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(stats)
    }

    async fn get_me(&self) -> Result<ClientInfoDetails, IggyError> {
        Err(IggyError::FeatureUnavailable)
    }

    async fn get_client(&self, client_id: u32) -> Result<Option<ClientInfoDetails>, IggyError> {
        let response = self.get(&format!("{CLIENTS}/{client_id}")).await;
        if let Err(error) = response {
            if matches!(error, IggyError::ResourceNotFound(_)) {
                return Ok(None);
            }

            return Err(error);
        }

        let client = response?
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(Some(client))
    }

    async fn get_clients(&self) -> Result<Vec<ClientInfo>, IggyError> {
        let response = self.get(CLIENTS).await?;
        let clients = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(clients)
    }

    async fn ping(&self) -> Result<(), IggyError> {
        self.get(PING).await?;
        Ok(())
    }

    async fn heartbeat_interval(&self) -> IggyDuration {
        self.heartbeat_interval
    }

    async fn snapshot(
        &self,
        compression: SnapshotCompression,
        snapshot_types: Vec<SystemSnapshotType>,
    ) -> Result<Snapshot, IggyError> {
        let response = self
            .post(
                SNAPSHOT,
                &GetSnapshot {
                    compression,
                    snapshot_types,
                },
            )
            .await?;
        let file = response
            .bytes()
            .await
            .map_err(|_| IggyError::InvalidBytesResponse)?;
        let snapshot = Snapshot::new(file.to_vec());
        Ok(snapshot)
    }
}
