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
use async_broadcast::Receiver;
use async_trait::async_trait;
use iggy_binary_protocol::Client;
use iggy_common::{DiagnosticEvent, IggyError};

#[async_trait]
impl Client for ClientWrapper {
    async fn connect(&self) -> Result<(), IggyError> {
        match self {
            ClientWrapper::Iggy(client) => client.connect().await,
            ClientWrapper::Http(client) => client.connect().await,
            ClientWrapper::Tcp(client) => client.connect().await,
            ClientWrapper::Quic(client) => client.connect().await,
            ClientWrapper::WebSocket(client) => client.connect().await,
        }
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        match self {
            ClientWrapper::Iggy(client) => client.disconnect().await,
            ClientWrapper::Http(client) => client.disconnect().await,
            ClientWrapper::Tcp(client) => client.disconnect().await,
            ClientWrapper::Quic(client) => client.disconnect().await,
            ClientWrapper::WebSocket(client) => client.disconnect().await,
        }
    }

    async fn shutdown(&self) -> Result<(), IggyError> {
        match self {
            ClientWrapper::Iggy(client) => client.shutdown().await,
            ClientWrapper::Http(client) => client.shutdown().await,
            ClientWrapper::Tcp(client) => client.shutdown().await,
            ClientWrapper::Quic(client) => client.shutdown().await,
            ClientWrapper::WebSocket(client) => client.shutdown().await,
        }
    }

    async fn subscribe_events(&self) -> Receiver<DiagnosticEvent> {
        match self {
            ClientWrapper::Iggy(client) => client.subscribe_events().await,
            ClientWrapper::Http(client) => client.subscribe_events().await,
            ClientWrapper::Tcp(client) => client.subscribe_events().await,
            ClientWrapper::Quic(client) => client.subscribe_events().await,
            ClientWrapper::WebSocket(client) => client.subscribe_events().await,
        }
    }
}
