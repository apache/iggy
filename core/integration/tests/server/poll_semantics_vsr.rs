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

//! Poll semantics against the server (vsr): a poll aimed at a partition id the
//! topic does not have must surface a typed `PartitionNotFound`, not an empty
//! poll a consumer would read as end-of-partition; a poll whose stream or
//! topic does not resolve must surface the legacy `StreamIdNotFound` /
//! `TopicIdNotFound` the same way; the same three addressing errors on
//! `get_consumer_offset` must not decode as "no offset stored"; and a
//! timestamp poll must be at-or-after, including the message stamped exactly at
//! the queried timestamp (the timestamp replies report per message).

use futures::StreamExt;
use iggy::prelude::*;
use integration::iggy_harness;
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::{Duration, sleep};

#[iggy_harness(
    test_client_transport = [Tcp]
)]
async fn given_missing_partition_when_polling_should_reject_partition_not_found(
    harness: &TestHarness,
) {
    let client = harness.tcp_root_client().await.expect("tcp root client");
    client
        .create_stream("poll-stream")
        .await
        .expect("create stream");
    let stream_id = Identifier::from_str_value("poll-stream").expect("stream identifier");
    client
        .create_topic(
            &stream_id,
            "poll-topic",
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
    let topic_id = Identifier::from_str_value("poll-topic").expect("topic identifier");

    let result = client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(7),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await;

    let expected =
        IggyError::PartitionNotFound(7, Identifier::default(), Identifier::default()).as_code();
    assert!(
        matches!(&result, Err(error) if error.as_code() == expected),
        "polling partition 7 of a 1-partition topic must surface Err(PartitionNotFound), got {result:?}"
    );

    let valid = client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .expect("poll on the existing partition still succeeds");
    assert_eq!(valid.messages.len(), 0, "empty topic polls empty");
}

#[iggy_harness(
    test_client_transport = [Tcp]
)]
async fn given_missing_stream_when_polling_should_reject_stream_not_found(harness: &TestHarness) {
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let stream_id = Identifier::from_str_value("no-such-stream").expect("stream identifier");
    let topic_id = Identifier::from_str_value("no-such-topic").expect("topic identifier");

    let result = client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await;

    let expected = IggyError::StreamIdNotFound(Identifier::default()).as_code();
    assert!(
        matches!(&result, Err(error) if error.as_code() == expected),
        "polling a missing stream must surface Err(StreamIdNotFound), got {result:?}"
    );
}

#[iggy_harness(
    test_client_transport = [Tcp]
)]
async fn given_missing_topic_when_polling_should_reject_topic_not_found(harness: &TestHarness) {
    let client = harness.tcp_root_client().await.expect("tcp root client");
    client
        .create_stream("topicless-stream")
        .await
        .expect("create stream");
    let stream_id = Identifier::from_str_value("topicless-stream").expect("stream identifier");
    let topic_id = Identifier::from_str_value("no-such-topic").expect("topic identifier");

    let result = client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await;

    let expected =
        IggyError::TopicIdNotFound(Identifier::default(), Identifier::default()).as_code();
    assert!(
        matches!(&result, Err(error) if error.as_code() == expected),
        "polling a missing topic of an existing stream must surface Err(TopicIdNotFound), \
         got {result:?}"
    );
}

/// `get_consumer_offset` answered an unknown partition with an empty body,
/// which the SDK decodes as `None` - the same value a consumer that simply has
/// no stored offset yet gets back, so a client could not tell a typo from a
/// fresh consumer. Legacy swallows this one too; the server surfaces the code
/// the poll path already surfaces for the identical addressing error.
#[iggy_harness(
    test_client_transport = [Tcp]
)]
async fn given_missing_partition_when_getting_consumer_offset_should_reject_partition_not_found(
    harness: &TestHarness,
) {
    let client = harness.tcp_root_client().await.expect("tcp root client");
    client
        .create_stream("offset-stream")
        .await
        .expect("create stream");
    let stream_id = Identifier::from_str_value("offset-stream").expect("stream identifier");
    client
        .create_topic(
            &stream_id,
            "offset-topic",
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
    let topic_id = Identifier::from_str_value("offset-topic").expect("topic identifier");
    let consumer = Consumer::default();

    let result = client
        .get_consumer_offset(&consumer, &stream_id, &topic_id, Some(7))
        .await;

    let expected =
        IggyError::PartitionNotFound(7, Identifier::default(), Identifier::default()).as_code();
    assert!(
        matches!(&result, Err(error) if error.as_code() == expected),
        "get_consumer_offset on partition 7 of a 1-partition topic must surface \
         Err(PartitionNotFound), got {result:?}"
    );

    // The existing partition still answers "no offset stored" as `None`.
    let stored = client
        .get_consumer_offset(&consumer, &stream_id, &topic_id, Some(0))
        .await
        .expect("get_consumer_offset on the existing partition still succeeds");
    assert!(
        stored.is_none(),
        "a consumer with no stored offset reads back as None, not an error"
    );
}

/// The same swallow one level up: an unresolved STREAM or TOPIC also answered
/// the empty body, so a typo'd or deleted target read back as a fresh
/// consumer. The consumer then resumes from its configured default and
/// silently reprocesses, with no error anywhere. The poll path denies both
/// codes, and the identical read over REST 404s.
#[iggy_harness(
    test_client_transport = [Tcp]
)]
async fn given_missing_stream_when_getting_consumer_offset_should_reject_stream_not_found(
    harness: &TestHarness,
) {
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let stream_id = Identifier::from_str_value("no-such-stream").expect("stream identifier");
    let topic_id = Identifier::from_str_value("no-such-topic").expect("topic identifier");

    let result = client
        .get_consumer_offset(&Consumer::default(), &stream_id, &topic_id, Some(0))
        .await;

    let expected = IggyError::StreamIdNotFound(Identifier::default()).as_code();
    assert!(
        matches!(&result, Err(error) if error.as_code() == expected),
        "get_consumer_offset on a missing stream must surface Err(StreamIdNotFound), \
         got {result:?}"
    );
}

#[iggy_harness(
    test_client_transport = [Tcp]
)]
async fn given_missing_topic_when_getting_consumer_offset_should_reject_topic_not_found(
    harness: &TestHarness,
) {
    let client = harness.tcp_root_client().await.expect("tcp root client");
    client
        .create_stream("offset-topicless-stream")
        .await
        .expect("create stream");
    let stream_id =
        Identifier::from_str_value("offset-topicless-stream").expect("stream identifier");
    let topic_id = Identifier::from_str_value("no-such-topic").expect("topic identifier");

    let result = client
        .get_consumer_offset(&Consumer::default(), &stream_id, &topic_id, Some(0))
        .await;

    let expected =
        IggyError::TopicIdNotFound(Identifier::default(), Identifier::default()).as_code();
    assert!(
        matches!(&result, Err(error) if error.as_code() == expected),
        "get_consumer_offset on a missing topic of an existing stream must surface \
         Err(TopicIdNotFound), got {result:?}"
    );
}

#[iggy_harness(
    test_client_transport = [Tcp]
)]
async fn given_message_at_polled_timestamp_when_polling_should_include_it(harness: &TestHarness) {
    let client = harness.tcp_root_client().await.expect("tcp root client");
    client
        .create_stream("ts-stream")
        .await
        .expect("create stream");
    let stream_id = Identifier::from_str_value("ts-stream").expect("stream identifier");
    client
        .create_topic(
            &stream_id,
            "ts-topic",
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
    let topic_id = Identifier::from_str_value("ts-topic").expect("topic identifier");

    // Two sends spaced apart so the broker stamps distinct batch timestamps.
    let mut first = vec![
        IggyMessage::builder()
            .payload("first".into())
            .build()
            .expect("message"),
    ];
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut first,
        )
        .await
        .expect("send first");
    sleep(Duration::from_millis(20)).await;
    let mut second = vec![
        IggyMessage::builder()
            .payload("second".into())
            .build()
            .expect("message"),
    ];
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut second,
        )
        .await
        .expect("send second");

    let all = client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            10,
            false,
        )
        .await
        .expect("poll all");
    assert_eq!(all.messages.len(), 2, "both messages are readable");
    let second_timestamp = all.messages[1].header.timestamp;
    assert!(
        second_timestamp > all.messages[0].header.timestamp,
        "spaced sends must carry distinct broker timestamps"
    );

    // Poll at the exact reported timestamp of the second message: at-or-after
    // semantics must return it, not skip past it.
    let polled = client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::timestamp(second_timestamp.into()),
            10,
            false,
        )
        .await
        .expect("poll by timestamp");
    assert_eq!(
        polled.messages.len(),
        1,
        "timestamp poll at the second message's own timestamp returns exactly it"
    );
    assert_eq!(
        polled.messages[0].header.offset, all.messages[1].header.offset,
        "the message at the queried timestamp is the one returned"
    );
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic, Http])]
async fn given_deferred_poll_when_messages_arrive_should_fill_count_and_keep_control_available(
    harness: &TestHarness,
) {
    let client = harness.root_client().await.unwrap();
    client.create_stream("deferred-stream").await.unwrap();
    let stream = Identifier::from_str_value("deferred-stream").unwrap();
    client
        .create_topic(
            &stream,
            "deferred-topic",
            &TopicCreateOptions {
                partitions_count: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let topic = Identifier::from_str_value("deferred-topic").unwrap();
    let consumer = Consumer::default();
    let strategy = PollingStrategy::first();
    let mut poll = Box::pin(client.poll_messages_deferred(
        &stream,
        &topic,
        Some(0),
        &consumer,
        &strategy,
        2,
        false,
        DeferredPollOptions {
            max_wait: 2_000_000.into(),
            min_count: 2,
            ..Default::default()
        },
    ));
    tokio::select! {
        result = &mut poll => panic!("empty poll completed before arrivals: {result:?}"),
        () = sleep(Duration::from_millis(50)) => {}
    }
    let mut first = vec![
        IggyMessage::builder()
            .payload("first".into())
            .build()
            .unwrap(),
    ];
    client
        .send_messages(&stream, &topic, &Partitioning::partition_id(0), &mut first)
        .await
        .unwrap();
    client
        .ping()
        .await
        .expect("control connection remains available");
    tokio::select! {
        result = &mut poll => panic!("partial poll completed before count or deadline: {result:?}"),
        () = sleep(Duration::from_millis(50)) => {}
    }
    let mut second = vec![
        IggyMessage::builder()
            .payload("second".into())
            .build()
            .unwrap(),
    ];
    client
        .send_messages(&stream, &topic, &Partitioning::partition_id(0), &mut second)
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), poll)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        result
            .messages
            .iter()
            .map(|message| message.header.offset)
            .collect::<Vec<_>>(),
        [0, 1]
    );
    let started = tokio::time::Instant::now();
    let partial = client
        .poll_messages_deferred(
            &stream,
            &topic,
            Some(0),
            &consumer,
            &strategy,
            3,
            false,
            DeferredPollOptions {
                max_wait: 200_000.into(),
                min_count: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(partial.messages.len(), 2);
    assert!(
        started.elapsed() >= Duration::from_millis(150),
        "partial result returned before deadline"
    );
    let empty = client
        .poll_messages_deferred(
            &stream,
            &topic,
            Some(0),
            &consumer,
            &PollingStrategy::offset(2),
            1,
            false,
            DeferredPollOptions {
                max_wait: 50_000.into(),
                min_count: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(empty.messages.is_empty());
    let immediate = client
        .poll_messages_deferred(
            &stream,
            &topic,
            Some(0),
            &consumer,
            &strategy,
            3,
            false,
            DeferredPollOptions {
                max_wait: 0.into(),
                min_count: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(immediate.messages.len(), 2);
}

#[iggy_harness(
    test_client_transport = [Tcp, WebSocket, Quic, Http],
    cluster_nodes = 3,
    server(
        http.jwt.encoding_secret = "0123456789abcdef0123456789abcdef",
        http.jwt.decoding_secret = "0123456789abcdef0123456789abcdef"
    )
)]
async fn given_deferred_auto_commit_when_cancelled_should_preserve_unseen_messages(
    harness: &TestHarness,
) {
    let client = if harness.transport().unwrap() == TransportProtocol::Http {
        harness
            .node(1)
            .http_client()
            .unwrap()
            .with_root_login()
            .connect()
            .await
            .unwrap()
    } else {
        harness.root_client().await.unwrap()
    };
    client.create_stream("cancel-stream").await.unwrap();
    let stream = Identifier::from_str_value("cancel-stream").unwrap();
    client
        .create_topic(
            &stream,
            "cancel-topic",
            &TopicCreateOptions {
                partitions_count: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let topic = Identifier::from_str_value("cancel-topic").unwrap();
    let consumer = Consumer::default();
    let strategy = PollingStrategy::next();
    let mut messages = vec![
        IggyMessage::builder()
            .payload("unseen".into())
            .build()
            .unwrap(),
    ];
    client
        .send_messages(
            &stream,
            &topic,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();
    let mut cancelled = Box::pin(client.poll_messages_deferred(
        &stream,
        &topic,
        Some(0),
        &consumer,
        &strategy,
        2,
        true,
        DeferredPollOptions {
            max_wait: 200_000.into(),
            min_count: 2,
            ..Default::default()
        },
    ));
    tokio::select! {
        result = &mut cancelled => panic!("partial result completed before cancellation: {result:?}"),
        () = sleep(Duration::from_millis(50)) => {}
    }
    drop(cancelled);
    sleep(Duration::from_millis(250)).await;
    let next = client
        .poll_messages_deferred(
            &stream,
            &topic,
            Some(0),
            &consumer,
            &strategy,
            1,
            false,
            DeferredPollOptions {
                max_wait: 200_000.into(),
                min_count: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        next.messages.len(),
        1,
        "cancelled poll must not accept its partial result"
    );
    assert_eq!(next.messages[0].header.offset, 0);

    let committed = client
        .poll_messages_deferred(
            &stream,
            &topic,
            Some(0),
            &consumer,
            &strategy,
            1,
            true,
            DeferredPollOptions {
                max_wait: 500_000.into(),
                min_count: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(committed.messages[0].header.offset, 0);
    let next = client
        .poll_messages_deferred(
            &stream,
            &topic,
            Some(0),
            &consumer,
            &strategy,
            1,
            true,
            DeferredPollOptions {
                max_wait: 200_000.into(),
                min_count: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(
        next.messages.is_empty(),
        "accepted automatic commit advances Next"
    );
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
async fn given_deferred_group_when_one_partition_is_empty_should_deliver_other_partition(
    harness: &TestHarness,
) {
    let client = harness.root_client().await.unwrap();
    client.create_stream("group-stream").await.unwrap();
    let stream = Identifier::from_str_value("group-stream").unwrap();
    client
        .create_topic(
            &stream,
            "group-topic",
            &TopicCreateOptions {
                partitions_count: Some(2),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let topic = Identifier::from_str_value("group-topic").unwrap();
    let mut consumer = client
        .consumer_group("deferred-group", "group-stream", "group-topic")
        .unwrap()
        .create_consumer_group_if_not_exists()
        .auto_join_consumer_group()
        .auto_commit(AutoCommit::Disabled)
        .polling_strategy(PollingStrategy::first())
        .batch_length(1)
        .poll_options(DeferredPollOptions {
            max_wait: Duration::from_secs(2).into(),
            ..Default::default()
        })
        .build();
    consumer.init().await.unwrap();
    let mut messages = vec![
        IggyMessage::builder()
            .payload("ready".into())
            .build()
            .unwrap(),
    ];
    client
        .send_messages(
            &stream,
            &topic,
            &Partitioning::partition_id(1),
            &mut messages,
        )
        .await
        .unwrap();
    let message = tokio::time::timeout(Duration::from_secs(1), consumer.next())
        .await
        .expect("empty partition must not hold up the ready partition")
        .unwrap()
        .unwrap();
    assert_eq!(message.partition_id, 1);
    consumer.shutdown().await.unwrap();
}

#[iggy_harness(
    test_client_transport = [Tcp],
    server(heartbeat.enabled = true, heartbeat.interval = "500ms")
)]
async fn given_deferred_poll_when_wait_exceeds_heartbeat_window_should_keep_sessions_alive(
    harness: &TestHarness,
) {
    let client = TcpClient::create(Arc::new(TcpClientConfig {
        server_address: harness.server().raw_tcp_addr().unwrap(),
        heartbeat_interval: NonZeroIggyDuration::from_str("100ms").unwrap(),
        nodelay: true,
        ..TcpClientConfig::default()
    }))
    .unwrap();
    Client::connect(&client).await.unwrap();
    let client = IggyClient::create(ClientWrapper::Tcp(client), None, None);
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    client.create_stream("live-stream").await.unwrap();
    let stream = Identifier::named("live-stream").unwrap();
    client
        .create_topic(
            &stream,
            "live-topic",
            &TopicCreateOptions {
                partitions_count: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let topic = Identifier::named("live-topic").unwrap();
    client
        .create_consumer_group(&stream, &topic, "live-group")
        .await
        .unwrap();
    let group = Identifier::named("live-group").unwrap();
    client
        .join_consumer_group(&stream, &topic, &group)
        .await
        .unwrap();

    let started = tokio::time::Instant::now();
    let result = client
        .poll_messages_deferred(
            &stream,
            &topic,
            None,
            &Consumer::group(group.clone()),
            &PollingStrategy::first(),
            1,
            false,
            DeferredPollOptions {
                max_wait: 2_000_000.into(),
                min_count: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(result.partition_id, 0);
    assert!(result.messages.is_empty());
    assert!(started.elapsed() >= Duration::from_millis(1800));
    client.ping().await.unwrap();
    assert_eq!(
        client
            .get_consumer_group(&stream, &topic, &group)
            .await
            .unwrap()
            .unwrap()
            .members_count,
        1,
        "the parent session must remain a group member throughout the deferred wait"
    );
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic, Http])]
async fn given_large_batch_cap_when_one_message_arrives_should_return_without_waiting_for_batch(
    harness: &TestHarness,
) {
    let client = harness.root_client().await.unwrap();
    client.create_stream("ready-stream").await.unwrap();
    let stream = Identifier::named("ready-stream").unwrap();
    client
        .create_topic(
            &stream,
            "topic",
            &TopicCreateOptions {
                partitions_count: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let topic = Identifier::named("topic").unwrap();
    let consumer = Consumer::default();
    let strategy = PollingStrategy::offset(0);
    let mut poll = Box::pin(client.poll_messages_deferred(
        &stream,
        &topic,
        Some(0),
        &consumer,
        &strategy,
        100,
        false,
        DeferredPollOptions {
            max_wait: Duration::from_secs(5).into(),
            request_timeout: Duration::from_secs(5).into(),
            ..Default::default()
        },
    ));
    tokio::select! {
        result = &mut poll => panic!("completed without an arrival: {result:?}"),
        () = sleep(Duration::from_millis(50)) => {}
    }
    let mut messages = vec![IggyMessage::from_str("ready").unwrap()];
    client
        .send_messages(
            &stream,
            &topic,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();
    let polled = tokio::time::timeout(Duration::from_secs(2), poll)
        .await
        .expect("minimum is one, not 100")
        .unwrap();
    assert_eq!(polled.messages.len(), 1);
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic, Http])]
async fn given_byte_limit_when_batch_is_larger_should_return_prefix_without_skipping_offsets(
    harness: &TestHarness,
) {
    const PAYLOAD: &str = "payload";
    let client = harness.root_client().await.unwrap();
    client.create_stream("bytes-stream").await.unwrap();
    let stream = Identifier::named("bytes-stream").unwrap();
    client
        .create_topic(
            &stream,
            "topic",
            &TopicCreateOptions {
                partitions_count: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let topic = Identifier::named("topic").unwrap();
    let mut messages = (0..3)
        .map(|_| IggyMessage::from_str(PAYLOAD).unwrap())
        .collect::<Vec<_>>();
    client
        .send_messages(
            &stream,
            &topic,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();
    let max_bytes =
        (iggy_binary_protocol::responses::messages::poll_messages::POLL_RESPONSE_HEADER_SIZE
            + iggy_binary_protocol::batch::BATCH_HEADER_SIZE
            + iggy_binary_protocol::batch::BATCH_MESSAGE_HEADER_SIZE
            + PAYLOAD.len()) as u32;
    let options = DeferredPollOptions {
        min_count: 3,
        max_bytes,
        ..Default::default()
    };
    for offset in 0..3 {
        let polled = tokio::time::timeout(
            Duration::from_millis(700),
            client.poll_messages_deferred(
                &stream,
                &topic,
                Some(0),
                &Consumer::default(),
                &PollingStrategy::next(),
                3,
                true,
                options,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(polled.messages.len(), 1);
        assert_eq!(polled.messages[0].header.offset, offset);
    }
    let oversized = client
        .poll_messages_deferred(
            &stream,
            &topic,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            3,
            false,
            DeferredPollOptions {
                max_bytes: max_bytes - 1,
                ..options
            },
        )
        .await;
    let error = oversized.expect_err("an oversized first record must fail, never disappear");
    let code = if let IggyError::HttpResponseError(_, body) = &error {
        serde_json::from_str::<serde_json::Value>(body).unwrap()["id"]
            .as_u64()
            .unwrap()
    } else {
        u64::from(error.as_code())
    };
    assert_eq!(
        code,
        u64::from(IggyError::InvalidSizeBytes.as_code()),
        "{error:?}"
    );
    let replay = client
        .poll_messages_deferred(
            &stream,
            &topic,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            3,
            false,
            options,
        )
        .await
        .unwrap();
    assert_eq!(replay.messages[0].header.offset, 0);
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic, Http])]
async fn given_default_consumer_when_restarted_before_commit_should_replay_unprocessed_messages(
    harness: &TestHarness,
) {
    let client = harness.root_client().await.unwrap();
    client.create_stream("safe-stream").await.unwrap();
    let stream = Identifier::named("safe-stream").unwrap();
    client
        .create_topic(
            &stream,
            "topic",
            &TopicCreateOptions {
                partitions_count: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let topic = Identifier::named("topic").unwrap();
    let mut messages = (0..6)
        .map(|_| IggyMessage::from_str("uncommitted").unwrap())
        .collect::<Vec<_>>();
    client
        .send_messages(
            &stream,
            &topic,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();
    let mut consumer = client
        .consumer("safe-reader", "safe-stream", "topic", 0)
        .unwrap()
        .batch_length(2)
        .prefetch_messages(4)
        .build();
    consumer.init().await.unwrap();
    for offset in 0..6 {
        let received = tokio::time::timeout(Duration::from_secs(2), consumer.next())
            .await
            .expect("explicit fetch position must advance without a commit")
            .unwrap()
            .unwrap();
        assert_eq!(received.message.header.offset, offset);
    }
    drop(consumer);
    let identity = Consumer::new(Identifier::named("safe-reader").unwrap());
    assert!(
        client
            .get_consumer_offset(&identity, &stream, &topic, Some(0))
            .await
            .unwrap()
            .is_none()
    );
    let mut restarted = client
        .consumer("safe-reader", "safe-stream", "topic", 0)
        .unwrap()
        .batch_length(2)
        .build();
    restarted.init().await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), restarted.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.message.header.offset, 0);
    restarted.store_offset(0, Some(0)).await.unwrap();
    drop(restarted);
    let mut committed = client
        .consumer("safe-reader", "safe-stream", "topic", 0)
        .unwrap()
        .build();
    committed.init().await.unwrap();
    let next = tokio::time::timeout(Duration::from_secs(2), committed.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(next.message.header.offset, 1);
    committed.shutdown().await.unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
async fn given_buffered_group_batch_when_membership_is_revoked_should_discard_queued_messages(
    harness: &TestHarness,
) {
    let client = harness.root_client().await.unwrap();
    client.create_stream("revoke-stream").await.unwrap();
    let stream = Identifier::named("revoke-stream").unwrap();
    client
        .create_topic(
            &stream,
            "topic",
            &TopicCreateOptions {
                partitions_count: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let topic = Identifier::named("topic").unwrap();
    let group = Identifier::named("readers").unwrap();
    client
        .create_consumer_group(&stream, &topic, "readers")
        .await
        .unwrap();
    client
        .join_consumer_group(&stream, &topic, &group)
        .await
        .unwrap();
    let mut messages = (0..10)
        .map(|_| IggyMessage::from_str("buffered").unwrap())
        .collect::<Vec<_>>();
    client
        .send_messages(
            &stream,
            &topic,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();
    let mut consumer = client
        .consumer_group("readers", "revoke-stream", "topic")
        .unwrap()
        .do_not_auto_join_consumer_group()
        .batch_length(10)
        .prefetch_messages(10)
        .build();
    consumer.init().await.unwrap();
    assert_eq!(
        consumer
            .next()
            .await
            .unwrap()
            .unwrap()
            .message
            .header
            .offset,
        0
    );
    client
        .leave_consumer_group(&stream, &topic, &group)
        .await
        .unwrap();
    sleep(Duration::from_millis(1200)).await;
    let received = tokio::time::timeout(Duration::from_secs(2), consumer.next())
        .await
        .unwrap()
        .unwrap();
    assert!(
        received.is_err(),
        "revoked prefetched messages must never reach the application"
    );
    consumer.shutdown().await.unwrap();
}
