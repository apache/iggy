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

use crate::{
    ClusterClient, ConsumerGroupClient, ConsumerOffsetClient, MessageClient, PartitionClient,
    PersonalAccessTokenClient, SegmentClient, StreamClient, SystemClient, TopicClient, UserClient,
};
use crate::{DiagnosticEvent, IggyError};
use async_broadcast::Receiver;
use async_trait::async_trait;
use std::fmt::Debug;

/// The client trait which is the main interface to the Iggy server.
/// It consists of multiple modules, each of which is responsible for a specific set of commands.
/// Server operations require authentication, except ping and the login flows.
#[async_trait]
pub trait Client:
    ClusterClient
    + SystemClient
    + UserClient
    + PersonalAccessTokenClient
    + StreamClient
    + TopicClient
    + PartitionClient
    + SegmentClient
    + MessageClient
    + ConsumerOffsetClient
    + ConsumerGroupClient
    + Sync
    + Send
    + Debug
{
    /// Connect to the server. Depending on the selected transport and provided configuration it might also perform authentication, retry logic etc.
    /// If the client is already connected, it will do nothing.
    async fn connect(&self) -> Result<(), IggyError>;

    /// Disconnect from the server. Repeated calls are safe.
    ///
    /// On TCP, QUIC and WebSocket, the disconnect holds until the next `connect()`,
    /// also on a client that was not connected: nothing reconnects the client on
    /// its own, even with auto-login credentials. Until then, a request that needs
    /// a session fails with `IggyError::Disconnected`, and any other request with
    /// `IggyError::NotConnected`. A connection that drops without this call can
    /// still reconnect on its own. Over HTTP this call does nothing.
    async fn disconnect(&self) -> Result<(), IggyError>;

    /// Shut down the client and release all the resources. Repeated calls are safe.
    ///
    /// On TCP, QUIC and WebSocket, shutdown is final: `connect()` and every later
    /// request fail with `IggyError::ClientShutdown`, and a later `disconnect()`
    /// does not make the client usable again. HTTP keeps no connection, so there
    /// this call does nothing.
    async fn shutdown(&self) -> Result<(), IggyError>;

    /// Subscribe to diagnostic events.
    async fn subscribe_events(&self) -> Receiver<DiagnosticEvent>;
}
