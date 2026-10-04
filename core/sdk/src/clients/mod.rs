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
use iggy_common::{BinaryTransport, ClientState};

mod binary_cluster;
mod binary_consumer_group;
mod binary_consumer_offset;
mod binary_message;
mod binary_partitions;
mod binary_personal_access_tokens;
mod binary_segments;
mod binary_streams;
mod binary_system;
mod binary_topics;
mod binary_users;
pub mod client;
pub mod client_builder;
pub mod consumer;
pub mod consumer_builder;
pub mod producer;
pub mod producer_builder;
pub mod producer_config;
pub mod producer_dispatcher;
pub mod producer_error_callback;
pub mod producer_sharding;

const ORDERING: std::sync::atomic::Ordering = std::sync::atomic::Ordering::SeqCst;

/// Whether the reconnect that followed a leader redirect already left this
/// client signed in as the user the redirected sign-in was for.
///
/// The connect flow signs in with the credentials that sign-in just
/// remembered, so on a client without a configured `AutoLogin` the session on
/// the leader is already the right one: signing in again would run a logout
/// plus a second login, an argon2 each on the server, to arrive where the
/// client already is. With `AutoLogin::Enabled(a)` the reconnect signed in the
/// configured user instead, who need not be the one signing in here, so the
/// sign-in still has to run.
pub(crate) async fn redirect_login_settled(client: &ClientWrapper) -> bool {
    let (state, auto_login_configured) = match client {
        ClientWrapper::Tcp(tcp_client) => (
            tcp_client.get_state().await,
            tcp_client.auto_login_configured(),
        ),
        ClientWrapper::Quic(quic_client) => (
            quic_client.get_state().await,
            quic_client.auto_login_configured(),
        ),
        ClientWrapper::WebSocket(ws_client) => (
            ws_client.get_state().await,
            ws_client.auto_login_configured(),
        ),
        _ => return false,
    };

    state == ClientState::Authenticated && !auto_login_configured
}
const MAX_BATCH_LENGTH: usize = 1000000;
const MIB: usize = 1_048_576;
