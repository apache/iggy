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
use crate::http::path::encode_segment;
use crate::prelude::{
    Consumer, ConsumerKind, Identifier, IggyError, IggyMessage, PartitionContext, Partitioning,
    PollMessages, PolledMessages, PollingStrategy, SendMessages, SendMessagesResponse,
};
use async_trait::async_trait;
use iggy_common::IggyMessagesBatch;
use iggy_common::MessageClient;
use iggy_common::PartitioningKind;
use iggy_common::PollOptions;
use iggy_common::SendMessagesConfirmations;
use iggy_common::ServerCommitPolicy;

#[async_trait]
impl MessageClient for HttpClient {
    /// Every poll goes to the deferred endpoint, also with a zero wait, because only that
    /// endpoint honors `min_count`, `max_bytes` and the request timeout. A first message
    /// larger than `max_bytes` comes alone in its own read, except at a `Last` start with a
    /// count above 1.
    async fn poll_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        options: PollOptions,
    ) -> Result<PolledMessages, IggyError> {
        options.validate()?;
        strategy.validate_partition(partition_id)?;
        // HTTP cannot join a consumer group, and an external group only holds offsets.
        if consumer.kind != ConsumerKind::Consumer {
            return Err(IggyError::FeatureUnavailable);
        }
        let path = format!(
            "{}/deferred",
            get_path(&stream_id.as_cow_str(), &topic_id.as_cow_str())
        );
        options
            .poll_oversized_alone(strategy, |count, wire_options| {
                let poll = PollMessages {
                    stream_id: stream_id.clone(),
                    topic_id: topic_id.clone(),
                    partition_id: Some(partition_id),
                    consumer: consumer.clone(),
                    strategy: *strategy,
                    count,
                    auto_commit: options.server_commit == ServerCommitPolicy::OnRead,
                };
                let path = &path;
                async move { self.get_deferred_poll(path, &poll, wire_options).await }
            })
            .await
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
    let encoded_stream = encode_segment(stream_id);
    let encoded_topic = encode_segment(topic_id);
    format!("streams/{encoded_stream}/topics/{encoded_topic}/messages")
}

#[cfg(test)]
mod tests {
    use super::{PolledMessages, SendMessagesConfirmations, SendMessagesResponse};
    use crate::clients::reader::fixtures::message_at;
    use crate::http::http_client::HttpClient;
    use crate::http::scripted_server::{Recorded, Reply, ScriptedServer};
    use crate::prelude::{
        Client, Consumer, ConsumerOffsetClient, Identifier, IggyError, IggyMessage, MessageClient,
        PartitionClient, PartitionContext, Partitioning, PollingKind, PollingStrategy,
        SendMessagesConfirmationResponse, StreamClient, StreamCreateOptions, TopicClient,
        TopicCreateOptions,
    };
    use bytes::Bytes;
    use iggy_common::poll_messages::DEFAULT_PARTITION_ID;
    use iggy_common::{Checkpoint, DEFAULT_POLL_MAX_BYTES, PollOptions, ServerCommitPolicy};
    use std::time::Instant;

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
    const START: u64 = 5;
    const STREAM_NAME: &str = "stream";
    const TOPIC_NAME: &str = "topic";
    const TOPIC_LINE: &str = "GET /streams/1/topics/1 HTTP/1.1";
    const SEND_LINE: &str = "POST /streams/1/topics/1/messages HTTP/1.1";
    const DEFERRED_POLL_LINE: &str = "GET /streams/1/topics/1/messages/deferred?";

    fn ids() -> (Identifier, Identifier) {
        (
            Identifier::numeric(1).unwrap(),
            Identifier::numeric(1).unwrap(),
        )
    }

    /// The names of the stream and topic of [`ids`].
    fn named_ids() -> (Identifier, Identifier) {
        (
            Identifier::named(STREAM_NAME).unwrap(),
            Identifier::named(TOPIC_NAME).unwrap(),
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

    async fn poll(
        client: &HttpClient,
        strategy: &PollingStrategy,
        server_commit: ServerCommitPolicy,
    ) -> Result<PolledMessages, IggyError> {
        poll_partition(client, &ids(), PARTITION_ID, strategy, server_commit).await
    }

    async fn poll_partition(
        client: &HttpClient,
        (stream_id, topic_id): &(Identifier, Identifier),
        partition_id: u32,
        strategy: &PollingStrategy,
        server_commit: ServerCommitPolicy,
    ) -> Result<PolledMessages, IggyError> {
        client
            .poll_messages(
                stream_id,
                topic_id,
                partition_id,
                &Consumer::default(),
                strategy,
                PollOptions {
                    server_commit,
                    ..PollOptions::default()
                },
            )
            .await
    }

    fn lines(requests: &[Recorded]) -> Vec<&str> {
        requests
            .iter()
            .map(|request| request.line.as_str())
            .collect()
    }

    /// Only the deferred endpoint honors every poll option, so a poll that answers at once goes
    /// there too. An empty batch continues from the start of its request.
    #[tokio::test]
    async fn given_a_zero_wait_when_polling_should_use_the_deferred_endpoint_and_keep_its_start() {
        let server = ScriptedServer::start(vec![Reply::polled()]).await;

        let polled = poll(
            &server.client(),
            &PollingStrategy::offset(START),
            ServerCommitPolicy::Disabled,
        )
        .await
        .unwrap();

        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0].line.starts_with(DEFERRED_POLL_LINE),
            "{}",
            requests[0].line
        );
        assert_eq!(requests[0].query("wait_us").as_deref(), Some("0"));
        assert_eq!(
            requests[0].query("partition_id"),
            Some(PARTITION_ID.to_string())
        );
        assert_eq!(requests[0].query("auto_commit").as_deref(), Some("false"));
        let continuation = polled.continuation();
        assert_eq!(
            (continuation.kind, continuation.value),
            (PollingKind::Offset, START)
        );
    }

    /// HTTP cannot join a consumer group, and an external group only holds offsets.
    #[tokio::test]
    async fn given_a_group_consumer_when_polling_should_refuse_it_before_sending() {
        let server = ScriptedServer::start(vec![]).await;
        let client = server.client();
        let (stream_id, topic_id) = ids();
        let group_id = Identifier::numeric(1).unwrap();

        for consumer in [
            Consumer::group(group_id.clone()),
            Consumer::external_group(group_id.clone()),
        ] {
            let polled = client
                .poll_messages(
                    &stream_id,
                    &topic_id,
                    PARTITION_ID,
                    &consumer,
                    &PollingStrategy::next(),
                    PollOptions::default(),
                )
                .await;

            assert_eq!(
                polled.map(drop),
                Err(IggyError::FeatureUnavailable),
                "{consumer:?}"
            );
        }
        assert!(server.requests().is_empty());
    }

    /// A continuation of another partition would read this partition at an offset of the
    /// other one. A continuation of this partition fences the poll with its context.
    #[tokio::test]
    async fn given_a_continuation_when_polling_should_send_it_only_to_its_own_partition() {
        let server = ScriptedServer::start(vec![Reply::polled()]).await;
        let client = server.client();
        let other =
            PollingStrategy::continuation(OTHER_PARTITION_ID, PollingKind::Offset, START, CURRENT);
        let own = PollingStrategy::continuation(PARTITION_ID, PollingKind::Offset, START, CURRENT);

        let refused = poll(&client, &other, ServerCommitPolicy::Disabled).await;
        let polled = poll(&client, &own, ServerCommitPolicy::Disabled).await;

        assert_eq!(refused.map(drop), Err(IggyError::InvalidCommand));
        polled.unwrap();
        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        let context = requests[0]
            .query("context")
            .map(|context| serde_json::from_str::<PartitionContext>(&context).unwrap());
        assert_eq!(context, Some(CURRENT));
    }

    /// The server stores the offset of an `OnRead` poll before it answers, and it can still
    /// store it after the client gives up. So a poll whose answer does not prove the outcome
    /// is never sent again, and it blocks the partition. The server checks a poll again after
    /// the read, so a 4xx other than 404 settles nothing unless binary takes its error as proof
    /// too.
    #[tokio::test]
    async fn given_an_on_read_poll_when_its_outcome_is_unknown_should_block_the_partition() {
        for (case, unknown) in [
            ("lost reply", Reply::hang_up()),
            ("cut body", Reply::polled().cut()),
            ("401", Reply::error(401, &IggyError::Unauthenticated)),
            ("403", Reply::error(403, &IggyError::Unauthorized)),
            ("other 4xx", Reply::error(400, &IggyError::InvalidCommand)),
            ("4xx without an error id", Reply::empty(400)),
            ("500", Reply::empty(500)),
            ("502", Reply::empty(502)),
            ("504", Reply::error(504, &IggyError::TransientNotCommitted)),
            ("2xx that does not parse", Reply::empty(200)),
        ] {
            let server = ScriptedServer::start(vec![unknown, Reply::polled()]).await;
            let client = server.client();

            let polled = poll(
                &client,
                &PollingStrategy::next(),
                ServerCommitPolicy::OnRead,
            )
            .await;
            let next = poll(
                &client,
                &PollingStrategy::next(),
                ServerCommitPolicy::OnRead,
            )
            .await;

            let Err(IggyError::FetchProgressUncertain(op)) = polled else {
                panic!("{case}: {polled:?}");
            };
            assert!(client.retains_operation(op), "{case}");
            assert_eq!(
                next.map(drop),
                Err(IggyError::PartitionLaneBlocked(PARTITION_ID, op)),
                "{case}"
            );
            let requests = server.requests();
            assert_eq!(requests.len(), 1, "{case}");
            assert_eq!(
                requests[0].query("auto_commit").as_deref(),
                Some("true"),
                "{case}"
            );
        }
    }

    /// A 503 refuses a poll before the server takes it, and the server sends a 404 only before
    /// the read. Another 4xx proves the outcome when binary takes its error as proof too, also
    /// when the client returns it as `HttpResponseError`. None of them leaves a block.
    /// `HistoryUnavailable` after the read means that the history the poll read is gone. A
    /// non-admission refusal is sent again within the request timeout.
    #[tokio::test]
    async fn given_an_on_read_poll_when_its_refusal_proves_the_outcome_should_leave_no_block() {
        let server = ScriptedServer::start(vec![
            Reply::error(503, &IggyError::TransientNotAccepted),
            Reply::polled(),
            Reply::empty(503),
            Reply::error(400, &IggyError::HistoryUnavailable),
            Reply::error(400, &IggyError::InvalidOffset(START)),
            Reply::empty(404),
            Reply::polled(),
        ])
        .await;
        let client = server.client();

        let retried = poll(
            &client,
            &PollingStrategy::next(),
            ServerCommitPolicy::OnRead,
        )
        .await;
        let refused = poll(
            &client,
            &PollingStrategy::next(),
            ServerCommitPolicy::OnRead,
        )
        .await;
        let history_gone = poll(
            &client,
            &PollingStrategy::next(),
            ServerCommitPolicy::OnRead,
        )
        .await;
        let invalid_offset = poll(
            &client,
            &PollingStrategy::next(),
            ServerCommitPolicy::OnRead,
        )
        .await;
        let not_found = poll(
            &client,
            &PollingStrategy::next(),
            ServerCommitPolicy::OnRead,
        )
        .await;
        let next = poll(
            &client,
            &PollingStrategy::next(),
            ServerCommitPolicy::OnRead,
        )
        .await;

        retried.unwrap();
        assert!(
            matches!(refused, Err(IggyError::HttpResponseError(503, _))),
            "{refused:?}"
        );
        assert_eq!(history_gone.map(drop), Err(IggyError::HistoryUnavailable));
        assert!(
            matches!(invalid_offset, Err(IggyError::HttpResponseError(400, _))),
            "{invalid_offset:?}"
        );
        assert!(
            matches!(not_found, Err(IggyError::ResourceNotFound(_))),
            "{not_found:?}"
        );
        next.unwrap();
        assert_eq!(server.requests().len(), 7);
    }

    /// A first message larger than `max_bytes` comes alone in a second read with the consumer,
    /// the start and the server commit of the first one, so with `OnRead` the server stores the
    /// offset of that message only. A read without a byte limit is not repeated.
    #[tokio::test]
    async fn given_an_oversized_first_message_when_polled_should_read_it_alone_once() {
        for server_commit in [ServerCommitPolicy::Disabled, ServerCommitPolicy::OnRead] {
            for max_bytes in [DEFAULT_POLL_MAX_BYTES, u32::MAX] {
                let case = format!("{server_commit:?} max_bytes={max_bytes}");
                let alone =
                    PolledMessages::new(CURRENT, PARTITION_ID, START, 1, vec![message_at(START)]);
                let server = ScriptedServer::start(vec![
                    Reply::error(400, &IggyError::InvalidSizeBytes),
                    Reply::batch(&alone),
                ])
                .await;
                let client = server.client();
                let (stream_id, topic_id) = ids();
                let options = PollOptions {
                    max_bytes,
                    server_commit,
                    ..PollOptions::default()
                };

                let polled = client
                    .poll_messages(
                        &stream_id,
                        &topic_id,
                        PARTITION_ID,
                        &Consumer::default(),
                        &PollingStrategy::offset(START),
                        options,
                    )
                    .await;

                let requests = server.requests();
                if max_bytes == u32::MAX {
                    assert_eq!(polled.map(drop), Err(IggyError::InvalidSizeBytes), "{case}");
                    assert_eq!(requests.len(), 1, "{case}");
                    continue;
                }
                let offsets: Vec<u64> = polled
                    .unwrap()
                    .messages
                    .iter()
                    .map(|message| message.header.offset)
                    .collect();
                assert_eq!(offsets, [START], "{case}");
                let [first, second] = requests.as_slice() else {
                    panic!("{case}: one read alone after the refusal, got {requests:?}");
                };
                let expected = options.alone();
                for (key, value) in [
                    ("count", expected.count),
                    ("min_count", expected.min_count),
                    ("max_bytes", expected.max_bytes),
                ] {
                    assert_eq!(second.query(key), Some(value.to_string()), "{case}: {key}");
                }
                assert_eq!(
                    second.query("auto_commit"),
                    Some((server_commit == ServerCommitPolicy::OnRead).to_string()),
                    "{case}"
                );
                for key in ["consumer_id", "kind", "value"] {
                    assert_eq!(second.query(key), first.query(key), "{case}: {key}");
                }
            }
        }
    }

    /// With count 1, a `Last` start would begin at the last message and skip the messages
    /// before it. The refusal proves that the server stored nothing, so the partition polls on.
    #[tokio::test]
    async fn given_an_oversized_first_message_of_a_last_start_when_polled_should_return_the_refusal()
     {
        for server_commit in [ServerCommitPolicy::Disabled, ServerCommitPolicy::OnRead] {
            let server = ScriptedServer::start(vec![
                Reply::error(400, &IggyError::InvalidSizeBytes),
                Reply::polled(),
            ])
            .await;
            let client = server.client();
            let last = PollingStrategy::last();

            let refused = poll(&client, &last, server_commit).await;
            let next = poll(&client, &last, server_commit).await;

            assert_eq!(
                refused.map(drop),
                Err(IggyError::InvalidSizeBytes),
                "{server_commit:?}"
            );
            next.unwrap();
            let requests = server.requests();
            assert_eq!(requests.len(), 2, "{server_commit:?}: no read alone");
            for request in &requests {
                assert_eq!(
                    request.query("count"),
                    Some(PollOptions::default().count.to_string()),
                    "{server_commit:?}"
                );
            }
        }
    }

    /// The server can still store the offset of a sent `OnRead` poll that its caller dropped.
    #[tokio::test]
    async fn given_a_sent_on_read_poll_when_its_caller_drops_it_should_block_the_partition() {
        let server = ScriptedServer::start(vec![Reply::stall()]).await;
        let client = server.client();
        let strategy = PollingStrategy::next();

        tokio::select! {
            polled = poll(&client, &strategy, ServerCommitPolicy::OnRead) => {
                panic!("a poll without an answer ended: {polled:?}");
            }
            () = server.request_arrived() => {}
        }
        let next = poll(&client, &strategy, ServerCommitPolicy::Disabled).await;

        let Err(IggyError::PartitionLaneBlocked(PARTITION_ID, op)) = next else {
            panic!("{next:?}");
        };
        assert!(client.retains_operation(op));
        assert_eq!(server.requests().len(), 1);
    }

    /// A name and a numeric id spell the same partition, so the block refuses both. Offset
    /// reads and polls that neither store the offset nor start at it go on, and so do other
    /// partitions.
    #[tokio::test]
    async fn given_a_blocked_partition_when_reading_on_should_refuse_its_progress_calls() {
        let server = ScriptedServer::start(vec![
            Reply::hang_up(),
            Reply::empty(404),
            Reply::polled(),
            Reply::polled(),
        ])
        .await;
        let client = server.client();
        let consumer = Consumer::default();
        let (stream_id, topic_id) = ids();
        let named = named_ids();
        let checkpoint = Checkpoint::new(PARTITION_ID, START, CURRENT);
        let lost = poll(
            &client,
            &PollingStrategy::next(),
            ServerCommitPolicy::OnRead,
        )
        .await;
        let Err(IggyError::FetchProgressUncertain(op)) = lost else {
            panic!("{lost:?}");
        };
        let blocked = Err(IggyError::PartitionLaneBlocked(PARTITION_ID, op));

        let next = poll_partition(
            &client,
            &named,
            PARTITION_ID,
            &PollingStrategy::next(),
            ServerCommitPolicy::Disabled,
        )
        .await;
        let on_read = poll_partition(
            &client,
            &named,
            PARTITION_ID,
            &PollingStrategy::offset(START),
            ServerCommitPolicy::OnRead,
        )
        .await;
        let stored_position = client
            .store_consumer_position(&consumer, &named.0, &named.1, &checkpoint)
            .await;
        let stored_offset = client
            .store_consumer_offset(&consumer, &stream_id, &topic_id, Some(PARTITION_ID), START)
            .await;
        let deleted_offset = client
            .delete_consumer_offset(&consumer, &stream_id, &topic_id, Some(PARTITION_ID))
            .await;
        let deleted_position = client
            .delete_consumer_position(&consumer, &named.0, &named.1, &checkpoint)
            .await;
        let offset = client
            .get_consumer_offset(&consumer, &named.0, &named.1, Some(PARTITION_ID))
            .await;
        let read = poll_partition(
            &client,
            &named,
            PARTITION_ID,
            &PollingStrategy::offset(START),
            ServerCommitPolicy::Disabled,
        )
        .await;
        let other = poll_partition(
            &client,
            &named,
            OTHER_PARTITION_ID,
            &PollingStrategy::next(),
            ServerCommitPolicy::OnRead,
        )
        .await;

        assert_eq!(next.map(drop), blocked);
        assert_eq!(on_read.map(drop), blocked);
        assert_eq!(stored_position, blocked);
        assert_eq!(stored_offset, blocked);
        assert_eq!(deleted_offset, blocked);
        assert_eq!(deleted_position, blocked);
        assert!(matches!(offset, Ok(None)), "{offset:?}");
        read.unwrap();
        other.unwrap();
        assert_eq!(server.requests().len(), 4);
    }

    /// The server reads an offset call without a partition as one of `DEFAULT_PARTITION_ID`.
    #[tokio::test]
    async fn given_a_blocked_default_partition_when_an_offset_call_names_none_should_refuse_it() {
        let server = ScriptedServer::start(vec![Reply::hang_up()]).await;
        let client = server.client();
        let consumer = Consumer::default();
        let (stream_id, topic_id) = ids();
        let lost = poll_partition(
            &client,
            &ids(),
            DEFAULT_PARTITION_ID,
            &PollingStrategy::next(),
            ServerCommitPolicy::OnRead,
        )
        .await;
        let Err(IggyError::FetchProgressUncertain(op)) = lost else {
            panic!("{lost:?}");
        };

        let stored = client
            .store_consumer_offset(&consumer, &stream_id, &topic_id, None, START)
            .await;
        let deleted = client
            .delete_consumer_offset(&consumer, &stream_id, &topic_id, None)
            .await;

        let blocked = Err(IggyError::PartitionLaneBlocked(DEFAULT_PARTITION_ID, op));
        assert_eq!(stored, blocked);
        assert_eq!(deleted, blocked);
        assert_eq!(server.requests().len(), 1);
    }

    /// HTTP cannot learn the outcome of a lost poll, so only an abandon frees its partition.
    #[tokio::test]
    async fn given_a_blocked_partition_when_abandoned_should_poll_it_again() {
        let server = ScriptedServer::start(vec![Reply::hang_up(), Reply::polled()]).await;
        let client = server.client();
        let lost = poll(
            &client,
            &PollingStrategy::next(),
            ServerCommitPolicy::OnRead,
        )
        .await;
        let Err(IggyError::FetchProgressUncertain(op)) = lost else {
            panic!("{lost:?}");
        };

        let resolved = client.resolve_operation(op, Instant::now()).await;
        let retained_after_resolve = client.retains_operation(op);
        let abandoned = client.abandon_operation(op).await;
        let retained_after_abandon = client.retains_operation(op);
        let abandoned_again = client.abandon_operation(op).await;
        let polled = poll(
            &client,
            &PollingStrategy::next(),
            ServerCommitPolicy::OnRead,
        )
        .await;

        assert_eq!(resolved, Err(IggyError::FeatureUnavailable));
        assert!(retained_after_resolve);
        assert_eq!(abandoned, Ok(()));
        assert!(!retained_after_abandon);
        assert_eq!(abandoned_again, Err(IggyError::OperationNotRetained(op)));
        polled.unwrap();
        assert_eq!(server.requests().len(), 2);
    }

    /// A poll without `OnRead` stores nothing, so a failure leaves no block and keeps its error.
    /// It is not sent again, so a lost `Next` poll reads the same start at the next call.
    #[tokio::test]
    async fn given_a_poll_without_on_read_when_it_fails_should_keep_its_error_without_a_block() {
        for (strategy, failure, expected) in [
            (
                PollingStrategy::next(),
                Reply::hang_up(),
                IggyError::TransientNotCommitted,
            ),
            (
                PollingStrategy::offset(START),
                Reply::polled().cut(),
                IggyError::TransientNotCommitted,
            ),
            (
                PollingStrategy::offset(START),
                Reply::empty(200),
                IggyError::InvalidJsonResponse,
            ),
            (
                PollingStrategy::next(),
                Reply::empty(500),
                IggyError::HttpResponseError(500, String::new()),
            ),
        ] {
            let server = ScriptedServer::start(vec![failure, Reply::polled()]).await;
            let client = server.client();

            let polled = poll(&client, &strategy, ServerCommitPolicy::Disabled).await;
            let next = poll(
                &client,
                &PollingStrategy::next(),
                ServerCommitPolicy::Disabled,
            )
            .await;

            assert_eq!(polled.map(drop), Err(expected), "{strategy}");
            next.unwrap();
            assert_eq!(server.requests().len(), 2, "{strategy}");
        }
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

    /// The timed out attempt may have committed, and a resend would be a new request that
    /// lands the batch twice, so the send reports the unknown outcome without a resend.
    #[tokio::test(start_paused = true)]
    async fn given_timeout_when_sending_should_report_not_committed_without_resend() {
        let server = ScriptedServer::start(vec![
            Reply::topic(&[(PARTITION_ID, CURRENT)]),
            Reply::empty(504),
            Reply::empty(201),
        ])
        .await;
        let client = server.client();
        let partitioning = Partitioning::partition_id(PARTITION_ID);

        let unknown = send(&client, &partitioning).await;
        // Nothing was refreshed, so the next send keeps the cached context.
        let sent = send(&client, &partitioning).await;

        let requests = server.requests();
        assert_eq!(lines(&requests), [TOPIC_LINE, SEND_LINE, SEND_LINE]);
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
                Change::CreateStream => client
                    .create_stream("stream", &StreamCreateOptions::default())
                    .await
                    .map(drop),
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
