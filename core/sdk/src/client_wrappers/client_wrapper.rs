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

use crate::clients::client::IggyClient;
use crate::http::http_client::HttpClient;
use crate::quic::quic_client::QuicClient;
use crate::tcp::tcp_client::TcpClient;
use crate::websocket::websocket_client::WebSocketClient;
use iggy_common::NonZeroIggyDuration;
use std::fmt::Debug;
use std::sync::Arc;

pub(crate) trait ClientRequestPolicy: Debug + Send + Sync {
    fn timeout(&self) -> NonZeroIggyDuration;
    fn should_budget_connect(&self) -> bool;
    fn expire(&self);
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ClientWrapper {
    Iggy(IggyClient),
    Http(HttpClient),
    Tcp(TcpClient),
    Quic(QuicClient),
    WebSocket(WebSocketClient),
}

impl ClientWrapper {
    pub(crate) fn request_policy(&self) -> Option<Arc<dyn ClientRequestPolicy>> {
        match self {
            Self::Tcp(client) => Some(Arc::new(client.request_policy())),
            Self::Iggy(client) => client.request_policy.clone(),
            _ => None,
        }
    }
}
