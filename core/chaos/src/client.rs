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

use crate::args::Transport;
use crate::scenarios::NamespacePrefixes;
use iggy::prelude::*;
use std::str::FromStr;
use std::sync::Arc;
use tracing::warn;

pub async fn create_client(
    server_address: &str,
    transport: Transport,
) -> Result<IggyClient, IggyError> {
    let client = match transport {
        Transport::Tcp => IggyClient::builder()
            .with_tcp()
            .with_server_address(server_address.to_owned())
            .build()?,
        Transport::Quic => IggyClient::builder()
            .with_quic()
            .with_server_address(server_address.to_owned())
            .build()?,
        Transport::Http => {
            let api_url = format!("http://{server_address}");
            IggyClient::builder()
                .with_http()
                .with_api_url(api_url)
                .build()?
        }
        Transport::WebSocket => IggyClient::builder()
            .with_websocket()
            .with_server_address(server_address.to_owned())
            .build()?,
    };
    client.connect().await?;
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await?;
    Ok(client)
}

/// Create a TCP client with a very long heartbeat interval (1 hour).
///
/// When connected to a server with `heartbeat.enabled = true` and a short
/// `heartbeat.interval` (e.g. 2s), this client will be evicted after
/// `server_interval * 1.2` because it never sends pings within the threshold.
#[allow(dead_code)]
pub async fn create_stale_client(server_address: &str) -> Result<IggyClient, IggyError> {
    let config = TcpClientConfig {
        server_address: server_address.to_string(),
        heartbeat_interval: IggyDuration::from_str("1h").unwrap(),
        ..TcpClientConfig::default()
    };
    let tcp = TcpClient::create(Arc::new(config))?;
    Client::connect(&tcp).await?;
    let client = IggyClient::create(ClientWrapper::Tcp(tcp), None, None);
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await?;
    Ok(client)
}

/// Delete all streams whose name starts with `prefix`.
pub async fn cleanup_prefixed(client: &IggyClient, prefix: &str) {
    let Ok(streams) = client.get_streams().await else {
        return;
    };
    for stream in streams.iter().filter(|s| s.name.starts_with(prefix)) {
        let id = Identifier::numeric(stream.id).unwrap();
        if let Err(e) = client.delete_stream(&id).await {
            warn!("Cleanup: failed to delete stream '{}': {e}", stream.name);
        }
    }
}

/// Delete all streams in both stable and churn namespaces.
pub async fn cleanup_all(client: &IggyClient, prefixes: &NamespacePrefixes) {
    cleanup_prefixed(client, &prefixes.stable).await;
    cleanup_prefixed(client, &prefixes.churn).await;
}
