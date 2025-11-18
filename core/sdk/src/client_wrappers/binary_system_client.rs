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

use crate::client_wrappers::client_wrapper::ClientWrapper;
use async_trait::async_trait;
use iggy_binary_protocol::SystemClient;
use iggy_common::{
    ClientInfo, ClientInfoDetails, IggyDuration, IggyError, Snapshot, SnapshotCompression, Stats,
    SystemSnapshotType,
};

#[async_trait]
impl SystemClient for ClientWrapper {
    async fn get_stats(&self) -> Result<Stats, IggyError> {
        match self {
            ClientWrapper::Iggy(client) => client.get_stats().await,
            ClientWrapper::Http(client) => client.get_stats().await,
            ClientWrapper::Tcp(client) => client.get_stats().await,
            ClientWrapper::Quic(client) => client.get_stats().await,
            ClientWrapper::WebSocket(client) => client.get_stats().await,
        }
    }

    async fn get_me(&self) -> Result<ClientInfoDetails, IggyError> {
        match self {
            ClientWrapper::Iggy(client) => client.get_me().await,
            ClientWrapper::Http(client) => client.get_me().await,
            ClientWrapper::Tcp(client) => client.get_me().await,
            ClientWrapper::Quic(client) => client.get_me().await,
            ClientWrapper::WebSocket(client) => client.get_me().await,
        }
    }

    async fn get_client(&self, client_id: u32) -> Result<Option<ClientInfoDetails>, IggyError> {
        match self {
            ClientWrapper::Iggy(client) => client.get_client(client_id).await,
            ClientWrapper::Http(client) => client.get_client(client_id).await,
            ClientWrapper::Tcp(client) => client.get_client(client_id).await,
            ClientWrapper::Quic(client) => client.get_client(client_id).await,
            ClientWrapper::WebSocket(client) => client.get_client(client_id).await,
        }
    }

    async fn get_clients(&self) -> Result<Vec<ClientInfo>, IggyError> {
        match self {
            ClientWrapper::Iggy(client) => client.get_clients().await,
            ClientWrapper::Http(client) => client.get_clients().await,
            ClientWrapper::Tcp(client) => client.get_clients().await,
            ClientWrapper::Quic(client) => client.get_clients().await,
            ClientWrapper::WebSocket(client) => client.get_clients().await,
        }
    }

    async fn ping(&self) -> Result<(), IggyError> {
        match self {
            ClientWrapper::Iggy(client) => client.ping().await,
            ClientWrapper::Http(client) => client.ping().await,
            ClientWrapper::Tcp(client) => client.ping().await,
            ClientWrapper::Quic(client) => client.ping().await,
            ClientWrapper::WebSocket(client) => client.ping().await,
        }
    }

    async fn heartbeat_interval(&self) -> IggyDuration {
        match self {
            ClientWrapper::Iggy(client) => client.heartbeat_interval().await,
            ClientWrapper::Http(client) => client.heartbeat_interval().await,
            ClientWrapper::Tcp(client) => client.heartbeat_interval().await,
            ClientWrapper::Quic(client) => client.heartbeat_interval().await,
            ClientWrapper::WebSocket(client) => client.heartbeat_interval().await,
        }
    }

    async fn snapshot(
        &self,
        compression: SnapshotCompression,
        snapshot_types: Vec<SystemSnapshotType>,
    ) -> Result<Snapshot, IggyError> {
        match self {
            ClientWrapper::Iggy(client) => client.snapshot(compression, snapshot_types).await,
            ClientWrapper::Http(client) => client.snapshot(compression, snapshot_types).await,
            ClientWrapper::Tcp(client) => client.snapshot(compression, snapshot_types).await,
            ClientWrapper::Quic(client) => client.snapshot(compression, snapshot_types).await,
            ClientWrapper::WebSocket(client) => client.snapshot(compression, snapshot_types).await,
        }
    }
}
