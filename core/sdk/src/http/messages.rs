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

use crate::http::http_client::HttpClient;
use crate::http::http_transport::HttpTransport;
use crate::prelude::{
    Consumer, FlushUnsavedBuffer, Identifier, IggyError, IggyMessage, Partitioning, PollMessages,
    PolledMessages, PollingStrategy, SendMessages,
};
use async_trait::async_trait;
use iggy_binary_protocol::MessageClient;
use iggy_common::IggyMessagesBatch;

#[async_trait]
impl MessageClient for HttpClient {
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
        let response = self
            .get_with_query(
                &get_path(&stream_id.as_cow_str(), &topic_id.as_cow_str()),
                &PollMessages {
                    stream_id: stream_id.clone(),
                    topic_id: topic_id.clone(),
                    partition_id,
                    consumer: consumer.clone(),
                    strategy: *strategy,
                    count,
                    auto_commit,
                },
            )
            .await?;
        let messages = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(messages)
    }

    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [IggyMessage],
    ) -> Result<(), IggyError> {
        let batch = IggyMessagesBatch::from(&*messages);
        self.post(
            &get_path(&stream_id.as_cow_str(), &topic_id.as_cow_str()),
            &SendMessages {
                metadata_length: 0, // this field is used only for TCP/QUIC
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                partitioning: partitioning.clone(),
                batch,
            },
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
        let _ = self
            .get_with_query(
                &get_path_flush_unsaved_buffer(
                    &stream_id.as_cow_str(),
                    &topic_id.as_cow_str(),
                    partition_id,
                    fsync,
                ),
                &FlushUnsavedBuffer {
                    stream_id: stream_id.clone(),
                    topic_id: topic_id.clone(),
                    partition_id,
                    fsync,
                },
            )
            .await?;
        Ok(())
    }
}

fn get_path(stream_id: &str, topic_id: &str) -> String {
    format!("streams/{stream_id}/topics/{topic_id}/messages")
}

fn get_path_flush_unsaved_buffer(
    stream_id: &str,
    topic_id: &str,
    partition_id: u32,
    fsync: bool,
) -> String {
    format!("streams/{stream_id}/topics/{topic_id}/messages/flush/{partition_id}/fsync={fsync}")
}
