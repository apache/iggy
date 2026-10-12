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
    Consumer, Identifier, IggyError, IggyMessage, PartitionContext, Partitioning, PollMessages,
    PolledMessages, PollingStrategy, SendMessages, SendMessagesResponse,
};
use async_trait::async_trait;
use iggy_common::IggyMessagesBatch;
use iggy_common::MessageClient;
use iggy_common::PartitioningKind;
use iggy_common::SendMessagesConfirmations;

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
        crate::http::consumer_offsets::refuse_external_group(consumer)?;
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
        let response = self
            .post_messages(stream_id, topic_id, partitioning, messages)
            .await?;
        decode_send_response(response).await
    }
}

impl HttpClient {
    /// Send messages and expose the completion guarantee advertised by HTTP.
    /// An absent or unrecognized header returns None without turning a
    /// committed write into a retryable failure.
    ///
    /// # Errors
    /// Returns a request or confirmation-decoding error.
    pub async fn send_messages_with_durability(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [IggyMessage],
    ) -> Result<(SendMessagesResponse, Option<iggy_common::Durability>), IggyError> {
        let response = self
            .post_messages(stream_id, topic_id, partitioning, messages)
            .await?;
        let durability = response
            .headers()
            .get("iggy-durability")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok());
        Ok((decode_send_response(response).await?, durability))
    }

    /// A send to an explicit partition carries the context of the partition it targets, so a
    /// resend after the partition was deleted or recreated is refused with
    /// `HistoryUnavailable` instead of landing in the new partition. On that refusal the send
    /// reads a fresh context and is sent once more.
    async fn post_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [IggyMessage],
    ) -> Result<reqwest::Response, IggyError> {
        let path = get_path(&stream_id.as_cow_str(), &topic_id.as_cow_str());
        let partition_id = explicit_partition_id(partitioning)?;
        let mut command = SendMessages {
            metadata_length: 0, // this field is used only for TCP/QUIC
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
            partitioning: partitioning.clone(),
            batch: IggyMessagesBatch::from(&*messages),
            context: None,
        };
        // Without a partition there is no context of this client to refresh.
        let mut refresh_available = partition_id.is_some();
        loop {
            if let Some(partition_id) = partition_id {
                command.context = self.send_context(stream_id, topic_id, partition_id).await?;
            }
            match self.post(&path, &command).await {
                Err(IggyError::HistoryUnavailable) => {
                    self.send_contexts.invalidate_topic(stream_id, topic_id);
                    if !refresh_available {
                        return Err(IggyError::HistoryUnavailable);
                    }
                    refresh_available = false;
                }
                result => return result,
            }
        }
    }

    /// The topic details are the only HTTP source of a send context. A client allowed to send
    /// but not to read them sends without one, and the server fences the send with the context
    /// current when the send arrives.
    async fn send_context(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
    ) -> Result<Option<PartitionContext>, IggyError> {
        if let Some(context) =
            self.send_contexts
                .partition_context(stream_id, topic_id, partition_id)
        {
            return Ok(Some(context));
        }
        if self
            .send_contexts
            .send_contexts_refused(stream_id, topic_id)
        {
            return Ok(None);
        }
        match self.topic_details(stream_id, topic_id).await {
            Ok(topic) => topic
                .partitions
                .iter()
                .find(|partition| partition.id == partition_id)
                .map(|partition| Some(partition.context))
                .ok_or_else(|| {
                    IggyError::PartitionNotFound(
                        partition_id as usize,
                        topic_id.clone(),
                        stream_id.clone(),
                    )
                }),
            Err(IggyError::Unauthorized) => {
                self.send_contexts.refuse_send_contexts(stream_id, topic_id);
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
}

/// The partition a send names, or `None` when the server picks it.
fn explicit_partition_id(partitioning: &Partitioning) -> Result<Option<u32>, IggyError> {
    if partitioning.kind != PartitioningKind::PartitionId {
        return Ok(None);
    }
    <[u8; 4]>::try_from(partitioning.value.as_slice())
        .map(|bytes| Some(u32::from_le_bytes(bytes)))
        .map_err(|_| IggyError::InvalidCommand)
}

async fn decode_send_response(
    response: reqwest::Response,
) -> Result<SendMessagesResponse, IggyError> {
    let body = response
        .bytes()
        .await
        .map_err(|_| IggyError::InvalidBytesResponse)?;
    // The legacy server answers a successful send with 201 and no content
    // at all. That is not JSON, and it must not read as a decode failure on
    // a write that already committed: no body means the batch landed with
    // no offsets reported, which is an empty list.
    if body.is_empty() {
        return Ok(SendMessagesResponse {
            confirmations: Vec::new(),
        });
    }
    let confirmations: SendMessagesConfirmations =
        serde_json::from_slice(&body).map_err(|_| IggyError::InvalidJsonResponse)?;
    Ok(SendMessagesResponse::from(confirmations))
}

fn get_path(stream_id: &str, topic_id: &str) -> String {
    format!("streams/{stream_id}/topics/{topic_id}/messages")
}

#[cfg(test)]
mod tests {
    use super::{SendMessagesConfirmations, SendMessagesResponse};
    use crate::http::http_client::HttpClient;
    use crate::http::scripted_server::{Recorded, Reply, ScriptedServer};
    use crate::prelude::{
        Identifier, IggyError, IggyMessage, MessageClient, PartitionClient, PartitionContext,
        Partitioning, SendMessagesConfirmationResponse, StreamClient, TopicClient,
        TopicCreateOptions,
    };
    use bytes::Bytes;

    const PARTITION_ID: u32 = 2;
    const OTHER_PARTITION_ID: u32 = 3;
    const CURRENT: PartitionContext = PartitionContext {
        incarnation: 7,
        owner_generation: 0,
        metadata_op: 11,
    };
    const RECREATED: PartitionContext = PartitionContext {
        incarnation: 9,
        owner_generation: 0,
        metadata_op: 14,
    };
    const TOPIC_LINE: &str = "GET /streams/1/topics/1 HTTP/1.1";
    const SEND_LINE: &str = "POST /streams/1/topics/1/messages HTTP/1.1";

    fn ids() -> (Identifier, Identifier) {
        (
            Identifier::numeric(1).unwrap(),
            Identifier::numeric(1).unwrap(),
        )
    }

    async fn send(
        client: &HttpClient,
        partitioning: &Partitioning,
    ) -> Result<SendMessagesResponse, IggyError> {
        let (stream_id, topic_id) = ids();
        let mut messages = vec![
            IggyMessage::builder()
                .payload(Bytes::from_static(b"fenced"))
                .build()
                .unwrap(),
        ];
        client
            .send_messages(&stream_id, &topic_id, partitioning, &mut messages)
            .await
    }

    fn lines(requests: &[Recorded]) -> Vec<&str> {
        requests
            .iter()
            .map(|request| request.line.as_str())
            .collect()
    }

    #[tokio::test]
    async fn given_explicit_partition_when_sending_should_carry_its_cached_topic_context() {
        let server = ScriptedServer::start(vec![
            Reply::topic(&[(OTHER_PARTITION_ID, RECREATED), (PARTITION_ID, CURRENT)]),
            Reply::empty(201),
            Reply::empty(201),
        ])
        .await;
        let client = server.client();
        let partitioning = Partitioning::partition_id(PARTITION_ID);

        let first = send(&client, &partitioning).await;
        let second = send(&client, &partitioning).await;

        let requests = server.requests();
        assert_eq!(lines(&requests), [TOPIC_LINE, SEND_LINE, SEND_LINE]);
        assert_eq!(requests[1].context(), Some(CURRENT));
        assert_eq!(requests[2].context(), Some(CURRENT));
        first.unwrap();
        second.unwrap();
    }

    #[tokio::test]
    async fn given_history_unavailable_when_sending_should_refresh_the_context_and_resend_once() {
        let server = ScriptedServer::start(vec![
            Reply::topic(&[(PARTITION_ID, CURRENT)]),
            Reply::error(400, &IggyError::HistoryUnavailable),
            Reply::topic(&[(PARTITION_ID, RECREATED)]),
            Reply::empty(201),
        ])
        .await;
        let client = server.client();

        let sent = send(&client, &Partitioning::partition_id(PARTITION_ID)).await;

        let requests = server.requests();
        assert_eq!(
            lines(&requests),
            [TOPIC_LINE, SEND_LINE, TOPIC_LINE, SEND_LINE]
        );
        assert_eq!(requests[1].context(), Some(CURRENT));
        assert_eq!(requests[3].context(), Some(RECREATED));
        sent.unwrap();
    }

    #[tokio::test]
    async fn given_history_unavailable_after_the_refresh_when_sending_should_return_it() {
        let server = ScriptedServer::start(vec![
            Reply::topic(&[(PARTITION_ID, CURRENT)]),
            Reply::error(400, &IggyError::HistoryUnavailable),
            Reply::topic(&[(PARTITION_ID, CURRENT)]),
            Reply::error(400, &IggyError::HistoryUnavailable),
            Reply::topic(&[(PARTITION_ID, RECREATED)]),
            Reply::empty(201),
        ])
        .await;
        let client = server.client();
        let partitioning = Partitioning::partition_id(PARTITION_ID);

        let refused = send(&client, &partitioning).await;
        // The refused context is dropped, so the next send reads the details again.
        let sent = send(&client, &partitioning).await;

        let requests = server.requests();
        assert_eq!(
            lines(&requests),
            [
                TOPIC_LINE, SEND_LINE, TOPIC_LINE, SEND_LINE, TOPIC_LINE, SEND_LINE
            ]
        );
        assert_eq!(requests[5].context(), Some(RECREATED));
        assert!(
            matches!(refused, Err(IggyError::HistoryUnavailable)),
            "{refused:?}"
        );
        sent.unwrap();
    }

    /// Sending needs `append_messages`, reading the topic details needs a read permission. A
    /// producer with only the first sends without a context, and asks for the details once.
    #[tokio::test]
    async fn given_topic_details_forbidden_when_sending_should_send_without_a_context() {
        let server = ScriptedServer::start(vec![
            Reply::error(403, &IggyError::Unauthorized),
            Reply::empty(201),
            Reply::empty(201),
        ])
        .await;
        let client = server.client();
        let partitioning = Partitioning::partition_id(PARTITION_ID);

        let first = send(&client, &partitioning).await;
        let second = send(&client, &partitioning).await;

        let requests = server.requests();
        assert_eq!(lines(&requests), [TOPIC_LINE, SEND_LINE, SEND_LINE]);
        assert_eq!(requests[1].context(), None);
        assert_eq!(requests[2].context(), None);
        first.unwrap();
        second.unwrap();
    }

    #[tokio::test]
    async fn given_no_topic_context_when_sending_should_return_the_error_and_send_nothing() {
        let server = ScriptedServer::start(vec![
            Reply::error(401, &IggyError::Unauthenticated),
            Reply::topic(&[(OTHER_PARTITION_ID, CURRENT)]),
        ])
        .await;
        let client = server.client();
        let partitioning = Partitioning::partition_id(PARTITION_ID);

        let unauthenticated = send(&client, &partitioning).await;
        let missing = send(&client, &partitioning).await;

        assert_eq!(lines(&server.requests()), [TOPIC_LINE, TOPIC_LINE]);
        assert!(
            matches!(unauthenticated, Err(IggyError::Unauthenticated)),
            "{unauthenticated:?}"
        );
        assert!(
            matches!(missing, Err(IggyError::PartitionNotFound(id, _, _)) if id == PARTITION_ID as usize),
            "{missing:?}"
        );
    }

    /// The timed out attempt may have committed, and the resend is a new request, so its
    /// refusal cannot tell whether the batch landed.
    #[tokio::test(start_paused = true)]
    async fn given_resend_after_timeout_when_history_unavailable_should_report_not_committed() {
        let server = ScriptedServer::start(vec![
            Reply::topic(&[(PARTITION_ID, CURRENT)]),
            Reply::empty(504),
            Reply::error(400, &IggyError::HistoryUnavailable),
            Reply::empty(201),
        ])
        .await;
        let client = server.client();
        let partitioning = Partitioning::partition_id(PARTITION_ID);

        let unknown = send(&client, &partitioning).await;
        // Nothing was refreshed, so the next send keeps the cached context.
        let sent = send(&client, &partitioning).await;

        let requests = server.requests();
        assert_eq!(
            lines(&requests),
            [TOPIC_LINE, SEND_LINE, SEND_LINE, SEND_LINE]
        );
        assert!(
            requests[1..]
                .iter()
                .all(|request| request.context() == Some(CURRENT))
        );
        assert!(
            matches!(unknown, Err(IggyError::TransientNotCommitted)),
            "{unknown:?}"
        );
        sent.unwrap();
    }

    /// A 429 refuses the request before the server takes it, so the refusal of its resend is
    /// definitive.
    #[tokio::test(start_paused = true)]
    async fn given_resend_after_admission_refusal_when_history_unavailable_should_refresh() {
        let server = ScriptedServer::start(vec![
            Reply::topic(&[(PARTITION_ID, CURRENT)]),
            Reply::empty(429),
            Reply::error(400, &IggyError::HistoryUnavailable),
            Reply::topic(&[(PARTITION_ID, RECREATED)]),
            Reply::empty(201),
        ])
        .await;
        let client = server.client();

        let sent = send(&client, &Partitioning::partition_id(PARTITION_ID)).await;

        let requests = server.requests();
        assert_eq!(
            lines(&requests),
            [TOPIC_LINE, SEND_LINE, SEND_LINE, TOPIC_LINE, SEND_LINE]
        );
        assert_eq!(requests[4].context(), Some(RECREATED));
        sent.unwrap();
    }

    #[tokio::test]
    async fn given_server_picked_partition_when_sending_should_leave_the_context_to_the_server() {
        let server = ScriptedServer::start(vec![Reply::empty(201)]).await;
        let client = server.client();

        let sent = send(&client, &Partitioning::balanced()).await;

        let requests = server.requests();
        assert_eq!(lines(&requests), [SEND_LINE]);
        assert_eq!(requests[0].context(), None);
        sent.unwrap();
    }

    /// The client's own stream or topic delete, or partition change, can recreate the partition
    /// behind a cached context, so the next send reads the details again.
    #[tokio::test]
    async fn given_own_delete_or_partition_change_when_sending_should_read_a_fresh_context() {
        #[derive(Debug, Clone, Copy)]
        enum Change {
            DeleteStream,
            DeleteTopic,
            CreatePartitions,
            DeletePartitions,
        }
        let (stream_id, topic_id) = ids();
        for change in [
            Change::DeleteStream,
            Change::DeleteTopic,
            Change::CreatePartitions,
            Change::DeletePartitions,
        ] {
            let server = ScriptedServer::start(vec![
                Reply::topic(&[(PARTITION_ID, CURRENT)]),
                Reply::empty(201),
                Reply::empty(200),
                Reply::topic(&[(PARTITION_ID, RECREATED)]),
                Reply::empty(201),
            ])
            .await;
            let client = server.client();
            let partitioning = Partitioning::partition_id(PARTITION_ID);
            let first = send(&client, &partitioning).await;

            let changed = match change {
                Change::DeleteStream => client.delete_stream(&stream_id).await,
                Change::DeleteTopic => client.delete_topic(&stream_id, &topic_id).await,
                Change::CreatePartitions => {
                    client.create_partitions(&stream_id, &topic_id, 1).await
                }
                Change::DeletePartitions => {
                    client.delete_partitions(&stream_id, &topic_id, 1).await
                }
            };
            let second = send(&client, &partitioning).await;

            let requests = server.requests();
            let lines = lines(&requests);
            assert_eq!(lines.len(), 5, "{change:?}: {lines:?}");
            assert_eq!(
                [lines[0], lines[3]],
                [TOPIC_LINE, TOPIC_LINE],
                "{change:?}: {lines:?}"
            );
            assert_eq!(requests[4].context(), Some(RECREATED), "{change:?}");
            first.unwrap();
            changed.unwrap();
            second.unwrap();
        }
    }

    /// A create replaces no partition this client holds a context for, so the cache stays, as
    /// in the binary transports.
    #[tokio::test]
    async fn given_own_stream_or_topic_create_when_sending_should_keep_the_cached_context() {
        #[derive(Debug, Clone, Copy)]
        enum Change {
            CreateStream,
            CreateTopic,
        }
        let (stream_id, _) = ids();
        for change in [Change::CreateStream, Change::CreateTopic] {
            let answer = match change {
                Change::CreateStream => Reply::stream(),
                Change::CreateTopic => Reply::topic(&[(PARTITION_ID, RECREATED)]),
            };
            let server = ScriptedServer::start(vec![
                Reply::topic(&[(PARTITION_ID, CURRENT)]),
                Reply::empty(201),
                answer,
                Reply::empty(201),
            ])
            .await;
            let client = server.client();
            let partitioning = Partitioning::partition_id(PARTITION_ID);
            let first = send(&client, &partitioning).await;

            let changed = match change {
                Change::CreateStream => client.create_stream("stream").await.map(drop),
                Change::CreateTopic => client
                    .create_topic(&stream_id, "topic", &TopicCreateOptions::default())
                    .await
                    .map(drop),
            };
            let second = send(&client, &partitioning).await;

            let requests = server.requests();
            let lines = lines(&requests);
            assert_eq!(lines.len(), 4, "{change:?}: {lines:?}");
            assert_eq!(lines[3], SEND_LINE, "{change:?}: {lines:?}");
            assert_eq!(requests[3].context(), Some(CURRENT), "{change:?}");
            first.unwrap();
            changed.unwrap();
            second.unwrap();
        }
    }

    fn parse(json: &str) -> SendMessagesResponse {
        let confirmations: SendMessagesConfirmations =
            serde_json::from_str(json).expect("contract sample must parse");
        SendMessagesResponse::from(confirmations)
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
