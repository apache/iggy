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
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{
    BinarySendPartitioning, Identifier, IggyError, IggyMessage, MessageClient,
    SendMessagesResponse, SystemClient, send_binary_messages,
};

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
    pub(crate) async fn send_messages_with_partitioning(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        partitioning: &mut BinarySendPartitioning<'_>,
        messages: &mut [IggyMessage],
    ) -> Result<SendMessagesResponse, IggyError> {
        match self {
            Self::Iggy(client) => {
                Box::pin(client.send_messages_with_partitioning(
                    stream,
                    topic,
                    partitioning,
                    messages,
                ))
                .await
            }
            Self::Http(client) => {
                client
                    .send_messages(stream, topic, partitioning.original(), messages)
                    .await
            }
            Self::Tcp(client) => {
                send_binary_messages(client, stream, topic, partitioning, messages).await
            }
            Self::Quic(client) => {
                send_binary_messages(client, stream, topic, partitioning, messages).await
            }
            Self::WebSocket(client) => {
                send_binary_messages(client, stream, topic, partitioning, messages).await
            }
        }
    }

    pub(crate) async fn keepalive(&self) -> Result<bool, IggyError> {
        match self {
            Self::Iggy(client) => Box::pin(client.client.read().await.keepalive()).await,
            Self::Http(client) => client.ping().await.map(|()| true),
            Self::Tcp(client) => client.keepalive().await,
            Self::Quic(client) => client.keepalive().await,
            Self::WebSocket(client) => client.keepalive().await,
        }
    }

    pub(crate) async fn retains_operation(&self, op: u64) -> bool {
        match self {
            Self::Iggy(client) => Box::pin(client.client.read().await.retains_operation(op)).await,
            Self::Http(_) => false,
            Self::Tcp(client) => client.retains_operation(op),
            Self::Quic(client) => client.retains_operation(op),
            Self::WebSocket(client) => client.retains_operation(op),
        }
    }
}
