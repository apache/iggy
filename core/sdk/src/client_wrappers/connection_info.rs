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
use iggy_common::TransportProtocol;

/// Connection information for the current client connection
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    /// The transport protocol being used
    pub protocol: TransportProtocol,
    /// The current server address the client is connected to
    pub server_address: String,
}

impl ClientWrapper {
    /// Returns the current connection information including protocol and server address
    pub async fn get_connection_info(&self) -> ConnectionInfo {
        match self {
            ClientWrapper::Iggy(_) => {
                // This variant should not be used in practice as IggyClient always wraps
                // one of the concrete transport clients. Return a default/placeholder.
                ConnectionInfo {
                    protocol: TransportProtocol::Tcp,
                    server_address: String::from("unknown"),
                }
            }
            ClientWrapper::Tcp(client) => ConnectionInfo {
                protocol: TransportProtocol::Tcp,
                server_address: client.current_server_address.lock().await.clone(),
            },
            ClientWrapper::Quic(client) => ConnectionInfo {
                protocol: TransportProtocol::Quic,
                server_address: client.current_server_address.lock().await.clone(),
            },
            ClientWrapper::Http(client) => ConnectionInfo {
                protocol: TransportProtocol::Http,
                server_address: client.api_url.to_string(),
            },
            ClientWrapper::WebSocket(client) => ConnectionInfo {
                protocol: TransportProtocol::WebSocket,
                server_address: client.current_server_address.lock().await.clone(),
            },
        }
    }
}
