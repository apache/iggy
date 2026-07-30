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

use crate::http::http_client::HttpClient;
use crate::http::http_transport::HttpTransport;
use crate::prelude::{
    Consumer, Identifier, IggyError, IggyMessage, Partitioning, PollMessages, PolledMessages,
    PollingStrategy, SendMessages, SendMessagesConfirmationResponse, SendMessagesResponse,
};
use async_trait::async_trait;
use iggy_common::IggyMessagesBatch;
use iggy_common::MessageClient;
use iggy_common::decode_send_confirmations;
use iggy_common::flush_unsaved_buffer::FlushUnsavedBuffer;
use serde::Deserialize;

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
    ) -> Result<SendMessagesResponse, IggyError> {
        let batch = IggyMessagesBatch::from(&*messages);
        let response = self
            .post(
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
        let body = response
            .bytes()
            .await
            .map_err(|_| IggyError::InvalidBytesResponse)?;
        // The legacy server answers a successful send with no content; the
        // shared fallback turns that into a zeroed confirmation.
        if body.is_empty() {
            return decode_send_confirmations(&[]);
        }
        let confirmations: SendMessagesConfirmationsDto =
            serde_json::from_slice(&body).map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(SendMessagesResponse::from(confirmations))
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
                    partition_id,
                    fsync,
                },
            )
            .await?;
        Ok(())
    }
}

/// JSON shape of a successful `SendMessages` reply. Mirrors the binary
/// `SendMessagesResponse`, which carries no `serde` derives because wire types
/// stay codec-only.
#[derive(Deserialize)]
struct SendMessagesConfirmationsDto {
    confirmations: Vec<SendMessagesConfirmationDto>,
}

#[derive(Deserialize)]
struct SendMessagesConfirmationDto {
    stream_id: u32,
    topic_id: u32,
    partition_id: u32,
    base_offset: u64,
}

impl From<SendMessagesConfirmationsDto> for SendMessagesResponse {
    fn from(dto: SendMessagesConfirmationsDto) -> Self {
        let confirmations = dto
            .confirmations
            .into_iter()
            .map(|confirmation| SendMessagesConfirmationResponse {
                stream_id: confirmation.stream_id,
                topic_id: confirmation.topic_id,
                partition_id: confirmation.partition_id,
                base_offset: confirmation.base_offset,
            })
            .collect();
        Self { confirmations }
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

#[cfg(test)]
mod tests {
    use super::{SendMessagesConfirmationsDto, SendMessagesResponse};
    use crate::prelude::SendMessagesConfirmationResponse;

    fn parse(json: &str) -> SendMessagesResponse {
        let dto: SendMessagesConfirmationsDto =
            serde_json::from_str(json).expect("contract sample must parse");
        SendMessagesResponse::from(dto)
    }

    #[test]
    fn confirmation_converts_all_fields() {
        let response = parse(
            r#"{"confirmations":[{"stream_id":1,"topic_id":2,"partition_id":3,"base_offset":42}]}"#,
        );
        assert_eq!(
            response.confirmations,
            vec![SendMessagesConfirmationResponse {
                stream_id: 1,
                topic_id: 2,
                partition_id: 3,
                base_offset: 42,
            }]
        );
    }

    #[test]
    fn empty_list_converts_to_empty_confirmations() {
        let response = parse(r#"{"confirmations":[]}"#);
        assert!(response.confirmations.is_empty());
    }

    #[test]
    fn preserves_order_of_multiple_confirmations() {
        let response = parse(
            r#"{"confirmations":[
                {"stream_id":1,"topic_id":2,"partition_id":7,"base_offset":10},
                {"stream_id":1,"topic_id":2,"partition_id":3,"base_offset":20}]}"#,
        );
        let partitions: Vec<u32> = response
            .confirmations
            .iter()
            .map(|confirmation| confirmation.partition_id)
            .collect();
        assert_eq!(partitions, vec![7, 3]);
    }
}
