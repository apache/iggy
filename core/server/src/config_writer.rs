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

use crate::server_error::ServerError;
use compio::fs::OpenOptions;
use compio::io::AsyncWriteAtExt;
use configs::cluster::TransportPorts;
use configs::server::ServerConfig;
use std::net::SocketAddr;

/// Every listener the server bound, as the OS reports it (`None` = not bound
/// on this node).
#[derive(Default)]
pub struct BoundAddresses {
    pub tcp: Option<SocketAddr>,
    pub tcp_tls: Option<SocketAddr>,
    pub quic: Option<SocketAddr>,
    pub websocket: Option<SocketAddr>,
    pub http: Option<SocketAddr>,
    pub replica: Option<SocketAddr>,
}

impl BoundAddresses {
    /// The listener a client's `tcp.address` reaches: the TLS one occupies the
    /// configured tcp slot, as the WSS one occupies the websocket slot.
    pub fn client_tcp(&self) -> Option<SocketAddr> {
        self.tcp_tls.or(self.tcp)
    }
}

/// The bound listeners as the roster ports the cluster metadata and
/// `current_config.toml` publish.
impl From<&BoundAddresses> for TransportPorts {
    fn from(bound: &BoundAddresses) -> Self {
        Self {
            tcp: bound.client_tcp().map(|addr| addr.port()),
            quic: bound.quic.map(|addr| addr.port()),
            http: bound.http.map(|addr| addr.port()),
            websocket: bound.websocket.map(|addr| addr.port()),
            tcp_replica: bound.replica.map(|addr| addr.port()),
        }
    }
}

/// Write the runtime `current_config.toml` file with the effective bound ports.
///
/// # Errors
///
/// Returns an error if the config cannot be serialized or if the runtime
/// config file cannot be written and synced.
pub async fn write_current_config(
    config: &ServerConfig,
    current_replica_id: Option<u8>,
    bound: &BoundAddresses,
) -> Result<(), ServerError> {
    let mut current_config = config.clone();

    if let Some(bound_client_tcp) = bound.client_tcp() {
        // Integration harnesses read the `*.address` fields from
        // `runtime/current_config.toml` to discover the actual port chosen by
        // the OS when binding to port 0.
        current_config.tcp.address = bound_client_tcp.to_string();
    }
    if let Some(bound_quic) = bound.quic {
        current_config.quic.address = bound_quic.to_string();
    }
    if let Some(bound_websocket) = bound.websocket {
        current_config.websocket.address = bound_websocket.to_string();
    }
    if let Some(bound_http) = bound.http {
        current_config.http.address = bound_http.to_string();
    }

    if current_config.cluster.enabled
        && let Some(replica_id) = current_replica_id
    {
        let node = current_config
            .cluster
            .nodes
            .iter_mut()
            .find(|node| node.replica_id == replica_id)
            .ok_or(ServerError::ClusterNodeNotFound { replica_id })?;
        // A transport this node did not bind keeps its configured port.
        let bound_ports = TransportPorts::from(bound);
        node.ports.tcp = bound_ports.tcp.or(node.ports.tcp);
        node.ports.tcp_replica = bound_ports.tcp_replica.or(node.ports.tcp_replica);
        node.ports.quic = bound_ports.quic.or(node.ports.quic);
        node.ports.websocket = bound_ports.websocket.or(node.ports.websocket);
        node.ports.http = bound_ports.http.or(node.ports.http);
    }

    let runtime_path = current_config.system.get_runtime_path();
    let config_path = format!("{runtime_path}/current_config.toml");
    let content = toml::to_string(&current_config).map_err(ServerError::CurrentConfigSerialize)?;

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&config_path)
        .await
        .map_err(|source| ServerError::CurrentConfigWrite {
            path: config_path.clone(),
            source,
        })?;

    file.write_all_at(content.into_bytes(), 0)
        .await
        .0
        .map_err(|source| ServerError::CurrentConfigWrite {
            path: config_path.clone(),
            source,
        })?;

    file.sync_all()
        .await
        .map_err(|source| ServerError::CurrentConfigWrite {
            path: config_path,
            source,
        })?;

    Ok(())
}
