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
        match self {
            ClientWrapper::Iggy(client) => Client::connect(client).await,
            ClientWrapper::Http(client) => Client::connect(client).await,
            ClientWrapper::Tcp(client) => Client::connect(client).await,
            ClientWrapper::Quic(client) => Client::connect(client).await,
            ClientWrapper::WebSocket(client) => Client::connect(client).await,
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

    async fn resolve_operation(&self, op: u64, deadline: Instant) -> Result<(), IggyError> {
        match self {
            ClientWrapper::Iggy(client) => client.resolve_operation(op, deadline).await,
            ClientWrapper::Http(client) => client.resolve_operation(op, deadline).await,
            ClientWrapper::Tcp(client) => client.resolve_operation(op, deadline).await,
            ClientWrapper::Quic(client) => client.resolve_operation(op, deadline).await,
            ClientWrapper::WebSocket(client) => client.resolve_operation(op, deadline).await,
        }
    }

    async fn abandon_operation(&self, op: u64) -> Result<(), IggyError> {
        match self {
            ClientWrapper::Iggy(client) => client.abandon_operation(op).await,
            ClientWrapper::Http(client) => client.abandon_operation(op).await,
            ClientWrapper::Tcp(client) => client.abandon_operation(op).await,
            ClientWrapper::Quic(client) => client.abandon_operation(op).await,
            ClientWrapper::WebSocket(client) => client.abandon_operation(op).await,
        }
    }
}
