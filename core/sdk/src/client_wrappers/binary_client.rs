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
use async_broadcast::Receiver;
use async_trait::async_trait;
use iggy_common::Client;
use iggy_common::{DiagnosticEvent, IggyError};
use std::time::Instant;

#[async_trait]
impl Client for ClientWrapper {
    async fn connect(&self) -> Result<(), IggyError> {
        self.as_client().connect().await
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        self.as_client().disconnect().await
    }

    async fn shutdown(&self) -> Result<(), IggyError> {
        self.as_client().shutdown().await
    }

    async fn subscribe_events(&self) -> Receiver<DiagnosticEvent> {
        self.as_client().subscribe_events().await
    }

    async fn resolve_operation(&self, op: u64, deadline: Instant) -> Result<(), IggyError> {
        self.as_client().resolve_operation(op, deadline).await
    }

    async fn abandon_operation(&self, op: u64) -> Result<(), IggyError> {
        self.as_client().abandon_operation(op).await
    }
}

impl ClientWrapper {
    /// Connects again through the transport's own `connect`, which leaves a
    /// caller's `disconnect()` in force: a reconnect the client starts on its
    /// own, such as after a leader redirect, is refused while it holds.
    pub(crate) async fn reconnect_keeping_intent(&self) -> Result<(), IggyError> {
        match self {
            ClientWrapper::Tcp(client) => client.connect().await,
            ClientWrapper::Quic(client) => client.connect().await,
            ClientWrapper::WebSocket(client) => client.connect().await,
            ClientWrapper::Iggy(client) => Client::connect(client).await,
            ClientWrapper::Http(client) => Client::connect(client).await,
        }
    }
}
