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

use crate::prelude::IggyClient;
use async_trait::async_trait;
use iggy_binary_protocol::SystemClient;
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{
    ClientInfo, ClientInfoDetails, IggyDuration, IggyError, Snapshot, SnapshotCompression, Stats,
    SystemSnapshotType,
};

#[async_trait]
impl SystemClient for IggyClient {
    async fn get_stats(&self) -> Result<Stats, IggyError> {
        self.client.read().await.get_stats().await
    }

    async fn get_me(&self) -> Result<ClientInfoDetails, IggyError> {
        self.client.read().await.get_me().await
    }

    async fn get_client(&self, client_id: u32) -> Result<Option<ClientInfoDetails>, IggyError> {
        self.client.read().await.get_client(client_id).await
    }

    async fn get_clients(&self) -> Result<Vec<ClientInfo>, IggyError> {
        self.client.read().await.get_clients().await
    }

    async fn ping(&self) -> Result<(), IggyError> {
        self.client.read().await.ping().await
    }

    async fn heartbeat_interval(&self) -> IggyDuration {
        self.client.read().await.heartbeat_interval().await
    }

    async fn snapshot(
        &self,
        compression: SnapshotCompression,
        snapshot_types: Vec<SystemSnapshotType>,
    ) -> Result<Snapshot, IggyError> {
        self.client
            .read()
            .await
            .snapshot(compression, snapshot_types)
            .await
    }
}
