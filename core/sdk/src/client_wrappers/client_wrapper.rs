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
use crate::http::http_transport::HttpTransport;
use crate::quic::quic_client::QuicClient;
use crate::tcp::tcp_client::TcpClient;
use crate::websocket::websocket_client::WebSocketClient;
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{
    AssignmentVersion, BinaryClient, BinaryOperationLineage, BinarySendPartitioning,
    BinaryTransport, ClientState, Consumer, DeferredPollOptions, FetchedSection, Identifier,
    IggyError, IggyMessage, MessageClient, Partitioning, PollingStrategy, SendMessagesResponse,
    SystemClient, fetch_binary_messages, group_cache_key, select_send_partition,
    send_binary_messages, sync_group_assignment,
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
    pub(crate) async fn operation_lineage(&self) -> Option<BinaryOperationLineage> {
        match self {
            Self::Iggy(client) => Box::pin(client.client.read().await.operation_lineage()).await,
            Self::Http(_) => None,
            Self::Tcp(client) => client.operation_lineage(),
            Self::Quic(client) => client.operation_lineage(),
            Self::WebSocket(client) => client.operation_lineage(),
        }
    }

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
            Self::Http(_) if partitioning.producer().is_some() => {
                Err(IggyError::FeatureUnavailable)
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

    /// The partition that a send with `partitioning` goes to. See
    /// [`select_send_partition`].
    pub(crate) async fn select_send_partition(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        partitioning: &Partitioning,
        fresh: bool,
    ) -> Result<u32, IggyError> {
        match self {
            Self::Iggy(client) => {
                Box::pin(client.client.read().await.select_send_partition(
                    stream,
                    topic,
                    partitioning,
                    fresh,
                ))
                .await
            }
            Self::Http(_) => Err(IggyError::FeatureUnavailable),
            Self::Tcp(client) => {
                select_send_partition(client, stream, topic, partitioning, fresh).await
            }
            Self::Quic(client) => {
                select_send_partition(client, stream, topic, partitioning, fresh).await
            }
            Self::WebSocket(client) => {
                select_send_partition(client, stream, topic, partitioning, fresh).await
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
            Self::Http(client) => client.retains_operation(op),
            Self::Tcp(client) => client.retains_operation(op),
            Self::Quic(client) => client.retains_operation(op),
            Self::WebSocket(client) => client.retains_operation(op),
        }
    }

    pub(crate) async fn is_authenticated(&self) -> bool {
        match self {
            Self::Iggy(client) => Box::pin(client.client.read().await.is_authenticated()).await,
            Self::Http(client) => client.is_authenticated().await,
            Self::Tcp(client) => client.get_state().await == ClientState::Authenticated,
            Self::Quic(client) => client.get_state().await == ClientState::Authenticated,
            Self::WebSocket(client) => client.get_state().await == ClientState::Authenticated,
        }
    }

    /// Whether the client is up or comes back without the caller.
    pub(crate) async fn can_recover(&self) -> bool {
        match self {
            Self::Iggy(client) => Box::pin(client.client.read().await.can_recover()).await,
            // HTTP keeps no link state: each request connects on its own.
            Self::Http(_) => true,
            Self::Tcp(client) => client.can_recover(),
            Self::Quic(client) => client.can_recover(),
            Self::WebSocket(client) => client.can_recover(),
        }
    }

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

    /// One `FetchMessages` (109) request for the `targets` of one readiness group, with the
    /// contract of [`fetch_binary_messages`]. The sections of a nested [`IggyClient`] decode
    /// with its encryptor. HTTP has no fetch and returns `FeatureUnavailable`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn fetch_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        targets: &[(u32, PollingStrategy)],
        consumer: &Consumer,
        count: u32,
        auto_commit: bool,
        options: DeferredPollOptions,
    ) -> Result<Vec<FetchedSection>, IggyError> {
        macro_rules! fetch {
            ($client:expr) => {
                fetch_binary_messages(
                    $client,
                    stream_id,
                    topic_id,
                    targets,
                    consumer,
                    count,
                    auto_commit,
                    options,
                )
                .await
            };
        }
        match self {
            Self::Tcp(client) => fetch!(client),
            Self::Quic(client) => fetch!(client),
            Self::WebSocket(client) => fetch!(client),
            Self::Http(_) => Err(IggyError::FeatureUnavailable),
            Self::Iggy(client) => {
                let sections = Box::pin(client.client.read().await.fetch_messages(
                    stream_id,
                    topic_id,
                    targets,
                    consumer,
                    count,
                    auto_commit,
                    options,
                ))
                .await?;
                Ok(sections
                    .into_iter()
                    .map(|section| section.with_encryptor(client.encryptor.clone()))
                    .collect())
            }
        }
    }

    /// The partitions grouped by primary endpoint and split to fit one request
    /// each, so that a consumer can keep one [`fetch_messages`](Self::fetch_messages)
    /// in flight per group. Each group keeps the order of `partitions`, and the first
    /// failed route lookup in that order is the error. HTTP has no groups and
    /// returns `FeatureUnavailable`.
    pub(crate) async fn readiness_groups(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        partitions: &[u32],
    ) -> Result<Vec<Vec<u32>>, IggyError> {
        match self {
            Self::Tcp(client) => {
                client
                    .driver
                    .readiness_groups(stream, topic, partitions)
                    .await
            }
            Self::Quic(client) => {
                client
                    .driver
                    .readiness_groups(stream, topic, partitions)
                    .await
            }
            Self::WebSocket(client) => {
                client
                    .driver
                    .readiness_groups(stream, topic, partitions)
                    .await
            }
            Self::Http(_) => Err(IggyError::FeatureUnavailable),
            Self::Iggy(client) => {
                Box::pin(
                    client
                        .client
                        .read()
                        .await
                        .readiness_groups(stream, topic, partitions),
                )
                .await
            }
        }
    }

    pub(crate) async fn deferred_poll_partitions(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        group: &Identifier,
    ) -> Result<(AssignmentVersion, Vec<u32>), IggyError> {
        match self {
            Self::Tcp(client) => group_assignment(client, stream, topic, group).await,
            Self::Quic(client) => group_assignment(client, stream, topic, group).await,
            Self::WebSocket(client) => group_assignment(client, stream, topic, group).await,
            Self::Http(_) => Err(IggyError::FeatureUnavailable),
            Self::Iggy(client) => {
                Box::pin(
                    client
                        .client
                        .read()
                        .await
                        .deferred_poll_partitions(stream, topic, group),
                )
                .await
            }
        }
    }
}

/// Syncs the assignment, then reads it.
async fn group_assignment<B: BinaryClient>(
    client: &B,
    stream: &Identifier,
    topic: &Identifier,
    group: &Identifier,
) -> Result<(AssignmentVersion, Vec<u32>), IggyError> {
    sync_group_assignment(client, stream, topic, group).await?;
    let state = client.consumer_group_state();
    let key = group_cache_key(stream, topic, group);
    if !state.is_registered(&key) {
        return Err(IggyError::ConsumerGroupMemberNotFound(
            0,
            group.clone(),
            topic.clone(),
        ));
    }
    Ok(state.assignment_snapshot(&key))
}
