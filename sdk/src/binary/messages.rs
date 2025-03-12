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

use crate::binary::binary_client::BinaryClient;
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::client::MessageClient;
use crate::command::{POLL_MESSAGES_CODE, SEND_MESSAGES_CODE};
use crate::consumer::Consumer;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::messages::flush_unsaved_buffer::FlushUnsavedBuffer;
use crate::messages::poll_messages::PollingStrategy;
use crate::messages::send_messages::{Message, Partitioning};
use crate::messages::{poll_messages, send_messages};
use crate::models::messages::PolledMessages;

#[async_trait::async_trait]
impl<B: BinaryClient> MessageClient for B {
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
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_raw_with_response(
                POLL_MESSAGES_CODE,
                poll_messages::as_bytes(
                    stream_id,
                    topic_id,
                    partition_id,
                    consumer,
                    strategy,
                    count,
                    auto_commit,
                ),
            )
            .await?;
        mapper::map_polled_messages(response)
    }

    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [Message],
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_raw_with_response(
            SEND_MESSAGES_CODE,
            send_messages::as_bytes(stream_id, topic_id, partitioning, messages),
        )
        .await?;
        Ok(())
    }

    async fn flush_unsaved_buffer(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        fsync: bool,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&FlushUnsavedBuffer {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
            partition_id,
            fsync,
        })
        .await?;
        Ok(())
    }
}
