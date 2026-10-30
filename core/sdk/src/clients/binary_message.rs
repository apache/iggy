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

use crate::prelude::IggyClient;
use async_trait::async_trait;
use bytes::Bytes;
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{
    BatchProducer, Consumer, DeferredPollOptions, Identifier, IggyError, IggyMessage, Partitioning,
    PolledMessages, PollingStrategy, SendMessagesResponse,
};
use iggy_common::{BinarySendPartitioning, MessageClient};

#[async_trait]
impl MessageClient for IggyClient {
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
        self.poll_messages_with_strategy_for(
            stream_id,
            topic_id,
            partition_id,
            consumer,
            &|_: u32| *strategy,
            count,
            auto_commit,
        )
        .await
    }

    async fn poll_messages_with_strategy_for(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        consumer: &Consumer,
        strategy_for: &(dyn Fn(u32) -> PollingStrategy + Send + Sync),
        count: u32,
        auto_commit: bool,
    ) -> Result<PolledMessages, IggyError> {
        self.poll_messages_with_strategy_for_and_options(
            stream_id,
            topic_id,
            partition_id,
            consumer,
            strategy_for,
            count,
            auto_commit,
            None,
        )
        .await
    }

    async fn poll_messages_with_strategy_for_and_options(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        consumer: &Consumer,
        strategy_for: &(dyn Fn(u32) -> PollingStrategy + Send + Sync),
        count: u32,
        auto_commit: bool,
        options: Option<iggy_common::DeferredPollOptions>,
    ) -> Result<PolledMessages, IggyError> {
        if count == 0 {
            return Err(IggyError::InvalidMessagesCount);
        }

        let mut polled_messages = super::with_client!(self, client => client.poll_messages_with_strategy_for_and_options(
                stream_id,
                topic_id,
                partition_id,
                consumer,
                strategy_for,
                count,
                auto_commit,
                options,
            ).await)?;

        if let Some(ref encryptor) = self.encryptor {
            for message in &mut polled_messages.messages {
                let payload = encryptor.decrypt(&message.payload)?;
                message.payload = Bytes::from(payload);
                message.header.payload_length = message.payload.len() as u32;

                if let Some(ref user_headers) = message.user_headers {
                    let decrypted_headers = encryptor.decrypt(user_headers)?;
                    message.header.user_headers_length = decrypted_headers.len() as u32;
                    message.user_headers = Some(Bytes::from(decrypted_headers));
                }
            }
        }

        Ok(polled_messages)
    }

    async fn wait_for_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions: &[(u32, PollingStrategy)],
        consumer: &Consumer,
        options: DeferredPollOptions,
    ) -> Result<Vec<u32>, IggyError> {
        self.client
            .read()
            .await
            .wait_for_messages(stream_id, topic_id, partitions, consumer, options)
            .await
    }

    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [IggyMessage],
    ) -> Result<SendMessagesResponse, IggyError> {
        self.send_messages_with_partitioning(
            stream_id,
            topic_id,
            &mut BinarySendPartitioning::new(partitioning),
            messages,
        )
        .await
    }

    async fn send_producer_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        producer: BatchProducer,
        messages: &mut [IggyMessage],
    ) -> Result<SendMessagesResponse, IggyError> {
        self.send_messages_with_partitioning(
            stream_id,
            topic_id,
            &mut BinarySendPartitioning::new(partitioning).with_producer(producer),
            messages,
        )
        .await
    }
}

impl IggyClient {
    pub(crate) async fn send_messages_with_partitioning(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &mut BinarySendPartitioning<'_>,
        messages: &mut [IggyMessage],
    ) -> Result<SendMessagesResponse, IggyError> {
        if messages.is_empty() {
            return Err(IggyError::InvalidMessagesCount);
        }

        if !partitioning.is_resolved()
            && let Some(encryptor) = &self.encryptor
        {
            for message in &mut *messages {
                message.payload = Bytes::from(encryptor.encrypt(&message.payload)?);
                message.header.payload_length = message.payload.len() as u32;

                if let Some(ref user_headers) = message.user_headers {
                    let encrypted_headers = encryptor.encrypt(user_headers)?;
                    message.header.user_headers_length = encrypted_headers.len() as u32;
                    message.user_headers = Some(Bytes::from(encrypted_headers));
                }
            }
        }

        super::with_client!(self, client => client.send_messages_with_partitioning(stream_id, topic_id, partitioning, messages).await)
    }
}
