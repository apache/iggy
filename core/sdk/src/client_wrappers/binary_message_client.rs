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
use async_trait::async_trait;
use iggy_binary_protocol::MessageClient;
use iggy_common::{
    Consumer, Identifier, IggyError, IggyMessage, Partitioning, PolledMessages, PollingStrategy,
};

#[async_trait]
impl MessageClient for ClientWrapper {
    async fn poll_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> Result<PolledMessages, IggyError> {
        match self {
            ClientWrapper::Iggy(client) => {
                client
                    .poll_messages(
                        stream_id,
                        topic_id,
                        partition_id,
                        consumer,
                        strategy,
                        count,
                        auto_commit,
                    )
                    .await
            }
            ClientWrapper::Http(client) => {
                client
                    .poll_messages(
                        stream_id,
                        topic_id,
                        partition_id,
                        consumer,
                        strategy,
                        count,
                        auto_commit,
                    )
                    .await
            }
            ClientWrapper::Tcp(client) => {
                client
                    .poll_messages(
                        stream_id,
                        topic_id,
                        partition_id,
                        consumer,
                        strategy,
                        count,
                        auto_commit,
                    )
                    .await
            }
            ClientWrapper::Quic(client) => {
                client
                    .poll_messages(
                        stream_id,
                        topic_id,
                        partition_id,
                        consumer,
                        strategy,
                        count,
                        auto_commit,
                    )
                    .await
            }
            ClientWrapper::WebSocket(client) => {
                client
                    .poll_messages(
                        stream_id,
                        topic_id,
                        partition_id,
                        consumer,
                        strategy,
                        count,
                        auto_commit,
                    )
                    .await
            }
        }
    }

    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [IggyMessage],
    ) -> Result<(), IggyError> {
        match self {
            ClientWrapper::Iggy(client) => {
                client
                    .send_messages(stream_id, topic_id, partitioning, messages)
                    .await
            }
            ClientWrapper::Http(client) => {
                client
                    .send_messages(stream_id, topic_id, partitioning, messages)
                    .await
            }
            ClientWrapper::Tcp(client) => {
                client
                    .send_messages(stream_id, topic_id, partitioning, messages)
                    .await
            }
            ClientWrapper::Quic(client) => {
                client
                    .send_messages(stream_id, topic_id, partitioning, messages)
                    .await
            }
            ClientWrapper::WebSocket(client) => {
                client
                    .send_messages(stream_id, topic_id, partitioning, messages)
                    .await
            }
        }
    }

    async fn flush_unsaved_buffer(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning_id: u32,
        fsync: bool,
    ) -> Result<(), IggyError> {
        match self {
            ClientWrapper::Iggy(client) => {
                client
                    .flush_unsaved_buffer(stream_id, topic_id, partitioning_id, fsync)
                    .await
            }
            ClientWrapper::Http(client) => {
                client
                    .flush_unsaved_buffer(stream_id, topic_id, partitioning_id, fsync)
                    .await
            }
            ClientWrapper::Tcp(client) => {
                client
                    .flush_unsaved_buffer(stream_id, topic_id, partitioning_id, fsync)
                    .await
            }
            ClientWrapper::Quic(client) => {
                client
                    .flush_unsaved_buffer(stream_id, topic_id, partitioning_id, fsync)
                    .await
            }
            ClientWrapper::WebSocket(client) => {
                client
                    .flush_unsaved_buffer(stream_id, topic_id, partitioning_id, fsync)
                    .await
            }
        }
    }
}
