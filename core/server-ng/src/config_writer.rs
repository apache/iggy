/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use crate::server_error::ServerNgError;
use compio::fs::OpenOptions;
use compio::io::AsyncWriteAtExt;
use configs::server::ServerConfig;
use std::net::SocketAddr;

/// Write the runtime `current_config.toml` file with the effective bound ports.
///
/// # Errors
///
/// Returns an error if the config cannot be serialized or if the runtime
/// config file cannot be written and synced.
pub async fn write_current_config(
    config: &ServerConfig,
    current_replica_id: Option<u8>,
    bound_tcp: Option<SocketAddr>,
    bound_replica: Option<SocketAddr>,
) -> Result<(), ServerNgError> {
    let mut current_config = config.clone();

    if let Some(bound_tcp) = bound_tcp {
        // Keep parity with the current server binary: integration harnesses
        // read `tcp.address` from `runtime/current_config.toml` to discover
        // the actual port chosen by the OS when binding to port 0.
        current_config.tcp.address = bound_tcp.to_string();
    }

    if current_config.cluster.enabled
        && let Some(replica_id) = current_replica_id
    {
        let node = current_config
            .cluster
            .nodes
            .iter_mut()
            .find(|node| node.replica_id == replica_id)
            .ok_or(ServerNgError::ClusterNodeNotFound { replica_id })?;
        if let Some(bound_tcp) = bound_tcp {
            node.ports.tcp = Some(bound_tcp.port());
        }
        if let Some(bound_replica) = bound_replica {
            node.ports.tcp_replica = Some(bound_replica.port());
        }
    }

    let runtime_path = current_config.system.get_runtime_path();
    let config_path = format!("{runtime_path}/current_config.toml");
    let content =
        toml::to_string(&current_config).map_err(ServerNgError::CurrentConfigSerialize)?;

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&config_path)
        .await
        .map_err(|source| ServerNgError::CurrentConfigWrite {
            path: config_path.clone(),
            source,
        })?;

    file.write_all_at(content.into_bytes(), 0)
        .await
        .0
        .map_err(|source| ServerNgError::CurrentConfigWrite {
            path: config_path.clone(),
            source,
        })?;

    file.sync_all()
        .await
        .map_err(|source| ServerNgError::CurrentConfigWrite {
            path: config_path,
            source,
        })?;

    Ok(())
}
