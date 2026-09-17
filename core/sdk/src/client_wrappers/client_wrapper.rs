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
use iggy_common::{BinaryTransport, Identifier, IggyError, sync_group_assignment};

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
    pub(crate) async fn deferred_group_state(
        &self,
    ) -> Result<std::sync::Arc<iggy_common::ConsumerGroupClientState>, IggyError> {
        match self {
            Self::Tcp(client) => Ok(client.consumer_group_state()),
            Self::Quic(client) => Ok(client.consumer_group_state()),
            Self::WebSocket(client) => Ok(client.consumer_group_state()),
            Self::Http(_) => Err(IggyError::FeatureUnavailable),
            Self::Iggy(client) => Box::pin(client.client.read().await.deferred_group_state()).await,
        }
    }

    pub(crate) async fn deferred_poll_partitions(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        group: &Identifier,
        refresh: bool,
    ) -> Result<(u64, u64, Vec<u32>), IggyError> {
        if let Self::Iggy(client) = self {
            return Box::pin(
                client
                    .client
                    .read()
                    .await
                    .deferred_poll_partitions(stream, topic, group, refresh),
            )
            .await;
        }
        let state = match self {
            Self::Tcp(client) => client.consumer_group_state(),
            Self::Quic(client) => client.consumer_group_state(),
            Self::WebSocket(client) => client.consumer_group_state(),
            Self::Http(_) => return Err(IggyError::FeatureUnavailable),
            Self::Iggy(_) => unreachable!("nested clients handled above"),
        };
        let key = format!("{stream}|{topic}|{group}");
        if refresh || !state.is_registered(&key) {
            match self {
                Self::Tcp(client) => sync_group_assignment(client, stream, topic, group).await?,
                Self::Quic(client) => sync_group_assignment(client, stream, topic, group).await?,
                Self::WebSocket(client) => {
                    sync_group_assignment(client, stream, topic, group).await?
                }
                Self::Http(_) | Self::Iggy(_) => unreachable!("binary transport selected above"),
            }
        }
        if !state.is_registered(&key) {
            return Err(IggyError::ConsumerGroupMemberNotFound(
                0,
                group.clone(),
                topic.clone(),
            ));
        }
        Ok(state.assignment_snapshot(&key))
    }
}
