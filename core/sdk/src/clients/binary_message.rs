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

use crate::prelude::IggyClient;
use async_trait::async_trait;
use bytes::Bytes;
use iggy_binary_protocol::MessageClient;
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{
    Consumer, Identifier, IggyError, IggyMessage, Partitioning, PolledMessages, PollingStrategy,
};

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
        if count == 0 {
            return Err(IggyError::InvalidMessagesCount);
        }

        let mut polled_messages = self
            .client
            .read()
            .await
            .poll_messages(
                stream_id,
                topic_id,
                partition_id,
                consumer,
                strategy,
                count,
                auto_commit,
            )
            .await?;

        if let Some(ref encryptor) = self.encryptor {
            for message in &mut polled_messages.messages {
                let payload = encryptor.decrypt(&message.payload)?;
                message.payload = Bytes::from(payload);
                message.header.payload_length = message.payload.len() as u32;
            }
        }

        Ok(polled_messages)
    }

    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [IggyMessage],
    ) -> Result<(), IggyError> {
        if messages.is_empty() {
            return Err(IggyError::InvalidMessagesCount);
        }

        if let Some(encryptor) = &self.encryptor {
            for message in &mut *messages {
                message.payload = Bytes::from(encryptor.encrypt(&message.payload)?);
                message.header.payload_length = message.payload.len() as u32;
            }
        }

        self.client
            .read()
            .await
            .send_messages(stream_id, topic_id, partitioning, messages)
            .await
    }

    async fn flush_unsaved_buffer(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        fsync: bool,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .flush_unsaved_buffer(stream_id, topic_id, partition_id, fsync)
            .await
    }
}
