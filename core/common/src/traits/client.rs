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
use std::time::Instant;

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
    /// On TCP, QUIC and WebSocket, the disconnect holds until the next `connect()`
    /// or explicit username/PAT login, also on a client that was not connected.
    /// Nothing reconnects the client on its own, even with auto-login credentials.
    /// Requests requiring a session fail with `IggyError::Disconnected`; other
    /// requests can return `IggyError::NotConnected`. A connection that drops
    /// without this call can still reconnect on its own. Over HTTP this call does
    /// nothing.
    async fn disconnect(&self) -> Result<(), IggyError>;

    /// Shut down the client and release all the resources. Repeated calls are safe.
    ///
    /// On TCP, QUIC and WebSocket, shutdown is final: `connect()` and every later
    /// request fail with `IggyError::ClientShutdown`, and a later `disconnect()`
    /// does not make the client usable again. HTTP keeps no connection, so there
    /// this call does nothing.
    async fn shutdown(&self) -> Result<(), IggyError>;

    /// Subscribe to diagnostic events.
    ///
    /// The event channel is bounded. When it is full, the client drops the
    /// oldest event, so publishing never waits for a subscriber. A subscriber
    /// that falls behind loses old events, not the newest ones.
    ///
    /// After a loss, the next `recv()` returns `RecvError::Overflowed` with the
    /// number of lost events, and the calls after it return the events that are
    /// left. Thus, after `Overflowed`, call `recv()` again, or read the receiver
    /// with `StreamExt::next()`, which skips that error.
    ///
    /// HTTP keeps no connection, so it publishes no events.
    async fn subscribe_events(&self) -> Receiver<DiagnosticEvent>;

    /// Replays the retained operation `op` until its committed result is proved
    /// or `deadline` passes. `op` is the request id that `OperationUncertain` or
    /// `LaneBlocked` carries.
    ///
    /// `Ok(())` means a successful commit was proved. A proved committed business
    /// rejection returns its original error. Both release the retained operation
    /// and its lane. Successful personal access token creation is an exception:
    /// replay returns `PersonalAccessTokenAlreadyExists` because its original
    /// secret is unrecoverable, even though the create committed successfully.
    /// `OperationUncertain(op)` keeps the original frame and lane:
    /// a later denial without committed-result proof cannot settle an earlier send.
    /// TCP, QUIC and WebSocket support recovery; HTTP returns `FeatureUnavailable`.
    ///
    /// The client keeps the proved outcomes of its last
    /// [`max_operations`](crate::PendingOperationsConfig::max_operations) replays,
    /// also across a session reset. A later resolve of `op` returns the same
    /// outcome without a request, also when a producer or a consumer of this
    /// client replayed `op` first.
    ///
    /// `OperationNotRetained(op)` means the outcome is unknown and can no longer
    /// be recovered through this client. Either a session reset or an abandon
    /// dropped `op` before a replay proved its outcome, or that outcome aged out.
    async fn resolve_operation(&self, _op: u64, _deadline: Instant) -> Result<(), IggyError> {
        Err(IggyError::FeatureUnavailable)
    }

    /// Frees the lane that the retained operation `op` blocks, and the outcome
    /// of `op` stays unknown. A replay of `op` that is already running still
    /// keeps the outcome it proves for [`Self::resolve_operation`]. An operation
    /// that is not retained gives `OperationNotRetained(op)`. A client that
    /// retains no operations returns `FeatureUnavailable`.
    async fn abandon_operation(&self, _op: u64) -> Result<(), IggyError> {
        Err(IggyError::FeatureUnavailable)
    }
}
