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

use crate::client_wrappers::client_wrapper::ClientWrapper;
use async_trait::async_trait;
use iggy_common::SystemClient;
use iggy_common::{
    ClientInfo, ClientInfoDetails, IggyError, MetadataChanges, MetadataChangesQuery,
    NonZeroIggyDuration, OptionSpec, OptionsScope, Snapshot, SnapshotCompression, Stats,
    SystemSnapshotType,
};

#[async_trait]
impl SystemClient for ClientWrapper {
    async fn get_stats(&self) -> Result<Stats, IggyError> {
        self.as_client().get_stats().await
    }

    async fn get_me(&self) -> Result<ClientInfoDetails, IggyError> {
        self.as_client().get_me().await
    }

    async fn get_client(&self, client_id: u32) -> Result<Option<ClientInfoDetails>, IggyError> {
        self.as_client().get_client(client_id).await
    }

    async fn get_clients(&self) -> Result<Vec<ClientInfo>, IggyError> {
        self.as_client().get_clients().await
    }

    async fn describe_options(&self, scope: OptionsScope) -> Result<Vec<OptionSpec>, IggyError> {
        self.as_client().describe_options(scope).await
    }

    async fn ping(&self) -> Result<(), IggyError> {
        self.as_client().ping().await
    }

    async fn heartbeat_interval(&self) -> NonZeroIggyDuration {
        self.as_client().heartbeat_interval().await
    }

    async fn refresh_consumer_group_assignments(&self) {
        self.as_client().refresh_consumer_group_assignments().await
    }

    async fn snapshot(
        &self,
        compression: SnapshotCompression,
        snapshot_types: Vec<SystemSnapshotType>,
    ) -> Result<Snapshot, IggyError> {
        self.as_client().snapshot(compression, snapshot_types).await
    }

    async fn get_metadata_changes(
        &self,
        query: &MetadataChangesQuery,
    ) -> Result<MetadataChanges, IggyError> {
        match self {
            ClientWrapper::Iggy(client) => client.get_metadata_changes(query).await,
            ClientWrapper::Http(client) => client.get_metadata_changes(query).await,
            ClientWrapper::Tcp(client) => client.get_metadata_changes(query).await,
            ClientWrapper::Quic(client) => client.get_metadata_changes(query).await,
            ClientWrapper::WebSocket(client) => client.get_metadata_changes(query).await,
        }
    }
}
