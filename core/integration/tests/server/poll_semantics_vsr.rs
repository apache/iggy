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
use integration::harness::{fetch_sections, partition_primary, wait_for_consumer_group_assignment};
use integration::iggy_harness;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::collections::BTreeMap;
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
        .create_stream("poll-stream", &StreamCreateOptions::default())
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
            7,
            &Consumer::default(),
            &PollingStrategy::offset(0),
            PollOptions {
                count: 1,
                ..PollOptions::default()
            },
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
            0,
            &Consumer::default(),
            &PollingStrategy::offset(0),
            PollOptions {
                count: 1,
                ..PollOptions::default()
            },
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
            0,
            &Consumer::default(),
            &PollingStrategy::offset(0),
            PollOptions {
                count: 1,
                ..PollOptions::default()
            },
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
        .create_stream("topicless-stream", &StreamCreateOptions::default())
        .await
        .expect("create stream");
    let stream_id = Identifier::from_str_value("topicless-stream").expect("stream identifier");
    let topic_id = Identifier::from_str_value("no-such-topic").expect("topic identifier");

    let result = client
        .poll_messages(
            &stream_id,
            &topic_id,
            0,
            &Consumer::default(),
            &PollingStrategy::offset(0),
            PollOptions {
                count: 1,
                ..PollOptions::default()
            },
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
        .create_stream("offset-stream", &StreamCreateOptions::default())
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
        .create_stream("offset-topicless-stream", &StreamCreateOptions::default())
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
        .create_stream("ts-stream", &StreamCreateOptions::default())
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
            0,
            &Consumer::default(),
            &PollingStrategy::offset(0),
            PollOptions {
                count: 10,
                ..PollOptions::default()
            },
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
            0,
            &Consumer::default(),
            &PollingStrategy::timestamp(second_timestamp.into()),
            PollOptions {
                count: 10,
                ..PollOptions::default()
            },
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
    client
        .create_stream("deferred-stream", &StreamCreateOptions::default())
        .await
        .unwrap();
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
    let mut poll = Box::pin(client.poll_messages(
        &stream,
        &topic,
        0,
        &consumer,
        &strategy,
        PollOptions {
            count: 2,
            min_count: 2,
            // Far above the run time, so a slow runner cannot end the wait before the second send.
            ..PollOptions::wait(30_000_000.into())
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
        .poll_messages(
            &stream,
            &topic,
            0,
            &consumer,
            &strategy,
            PollOptions {
                count: 3,
                min_count: 3,
                ..PollOptions::wait(200_000.into())
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
        .poll_messages(
            &stream,
            &topic,
            0,
            &consumer,
            &PollingStrategy::offset(2),
            PollOptions {
                count: 1,
                ..PollOptions::wait(50_000.into())
            },
        )
        .await
        .unwrap();
    assert!(empty.messages.is_empty());
    let immediate = client
        .poll_messages(
            &stream,
            &topic,
            0,
            &consumer,
            &strategy,
            PollOptions {
                count: 3,
                min_count: 3,
                ..PollOptions::default()
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
async fn given_committing_poll_when_dropped_should_settle_through_its_one_reply(
    harness: &TestHarness,
) {
    let http = harness.transport().unwrap() == TransportProtocol::Http;
    let client = if http {
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
    client
        .create_stream("cancel-stream", &StreamCreateOptions::default())
        .await
        .unwrap();
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
    // Over HTTP the server reads after the wait, so the poll ends before the next steps.
    let max_wait = if http {
        Duration::from_millis(200)
    } else {
        Duration::from_secs(10)
    };
    let mut dropped = Box::pin(client.poll_messages(
        &stream,
        &topic,
        0,
        &consumer,
        &strategy,
        PollOptions {
            count: 2,
            min_count: 2,
            commit: CommitPolicy::OnRead,
            ..PollOptions::wait(max_wait.into())
        },
    ));
    tokio::select! {
        result = &mut dropped => panic!("partial result completed before the drop: {result:?}"),
        () = sleep(Duration::from_millis(50)) => {}
    }
    drop(dropped);
    if !http {
        // The poll waits for a second message, so the cancel wins before any store. The server
        // answers it once with nothing stored, and that answer opens the partition.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let polled = loop {
            let polled = client
                .poll_messages(
                    &stream,
                    &topic,
                    0,
                    &consumer,
                    &strategy,
                    PollOptions {
                        count: 1,
                        ..PollOptions::default()
                    },
                )
                .await;
            match polled {
                Err(IggyError::PartitionLaneBlocked(0, _))
                    if tokio::time::Instant::now() < deadline =>
                {
                    sleep(Duration::from_millis(50)).await;
                }
                polled => break polled,
            }
        };
        let polled = polled.expect("the answer to the cancel opens the partition");
        assert_eq!(
            polled
                .messages
                .iter()
                .map(|message| message.header.offset)
                .collect::<Vec<_>>(),
            [0],
            "a cancelled poll stores nothing"
        );
        return;
    }
    // HTTP has no cancel, so the server can still store the offset after the drop.
    sleep(Duration::from_millis(250)).await;
    let blocked = client
        .poll_messages(
            &stream,
            &topic,
            0,
            &consumer,
            &strategy,
            PollOptions {
                count: 1,
                ..PollOptions::wait(200_000.into())
            },
        )
        .await;
    let operation = match blocked {
        Err(IggyError::PartitionLaneBlocked(0, operation)) => operation,
        other => panic!("a dropped committing poll must block its partition, got {other:?}"),
    };
    client.abandon_operation(operation).await.unwrap();

    // The server may have stored offset 0 for the dropped poll, so read from an explicit offset.
    let committed = client
        .poll_messages(
            &stream,
            &topic,
            0,
            &consumer,
            &PollingStrategy::offset(0),
            PollOptions {
                count: 1,
                commit: CommitPolicy::OnRead,
                ..PollOptions::wait(500_000.into())
            },
        )
        .await
        .expect("an abandoned operation must release its partition");
    assert_eq!(committed.messages[0].header.offset, 0);
    let next = client
        .poll_messages(
            &stream,
            &topic,
            0,
            &consumer,
            &strategy,
            PollOptions {
                count: 1,
                commit: CommitPolicy::OnRead,
                ..PollOptions::wait(200_000.into())
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
    client
        .create_stream("group-stream", &StreamCreateOptions::default())
        .await
        .unwrap();
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
        .polling_strategy(PollingStrategy::first())
        .poll_options(PollOptions {
            count: 1,
            ..PollOptions::wait(Duration::from_secs(2).into())
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

#[iggy_harness(cluster_nodes = 1, test_client_transport = [Tcp, WebSocket, Quic])]
async fn given_group_fetch_when_one_partition_holds_messages_should_not_wait_on_idle_ones(
    harness: &TestHarness,
) {
    const MAX_WAIT: Duration = Duration::from_secs(10);
    const PROMPT_REPLY: Duration = Duration::from_secs(2);
    const PARTITIONS: u32 = 3;
    const FETCHES: u64 = 3;
    const READY_PARTITION: u32 = 1;
    let client = harness.root_client().await.unwrap();
    let (stream, topic, group) =
        create_sole_group_member(&client, "ready-stream", PARTITIONS).await;
    let consumer = Consumer::group(group);
    let targets: Vec<_> = (0..PARTITIONS)
        .map(|partition| (partition, PollingStrategy::next()))
        .collect();
    let options = PollOptions {
        count: 1,
        commit: CommitPolicy::OnRead,
        ..PollOptions::wait(MAX_WAIT.into())
    };
    for offset in 0..FETCHES {
        send_to_partition(&client, &stream, &topic, READY_PARTITION, 1).await;
        let sections = tokio::time::timeout(
            PROMPT_REPLY,
            fetch_sections(&client, &stream, &topic, &targets, &consumer, options),
        )
        .await
        .expect("an idle partition must not hold the fetch for max_wait")
        .unwrap();
        assert_eq!(
            sections.len(),
            PARTITIONS as usize,
            "every Next target resolves"
        );
        let delivered: Vec<_> = sections
            .iter()
            .flat_map(|section| {
                section
                    .decode()
                    .unwrap()
                    .messages
                    .into_iter()
                    .map(|message| (section.partition_id, message.header.offset))
            })
            .collect();
        assert_eq!(delivered, [(READY_PARTITION, offset)]);
        assert_eq!(
            client
                .get_consumer_offset(&consumer, &stream, &topic, Some(READY_PARTITION))
                .await
                .unwrap()
                .unwrap()
                .stored_offset,
            offset,
        );
    }
}

#[iggy_harness(cluster_nodes = 1, test_client_transport = [Tcp, WebSocket, Quic])]
async fn given_group_fetch_when_every_partition_is_idle_should_resolve_next_after_max_wait(
    harness: &TestHarness,
) {
    const MAX_WAIT: Duration = Duration::from_secs(2);
    const EARLY_TOLERANCE: Duration = Duration::from_millis(100);
    const LATE_TOLERANCE: Duration = Duration::from_secs(1);
    const PARTITIONS: u32 = 3;
    let client = harness.root_client().await.unwrap();
    let (stream, topic, group) = create_sole_group_member(&client, "idle-stream", PARTITIONS).await;
    let targets: Vec<_> = (0..PARTITIONS)
        .map(|partition| (partition, PollingStrategy::next()))
        .collect();
    let started = tokio::time::Instant::now();
    let sections = fetch_sections(
        &client,
        &stream,
        &topic,
        &targets,
        &Consumer::group(group),
        PollOptions {
            count: 1,
            commit: CommitPolicy::OnRead,
            ..PollOptions::wait(MAX_WAIT.into())
        },
    )
    .await
    .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(sections.len(), PARTITIONS as usize);
    for (partition, section) in sections.iter().enumerate() {
        assert_eq!(section.partition_id, partition as u32);
        assert_eq!(section.next_offset, 0);
        assert!(section.decode().unwrap().messages.is_empty());
    }
    assert!(
        elapsed >= MAX_WAIT - EARLY_TOLERANCE && elapsed < MAX_WAIT + LATE_TOLERANCE,
        "an empty fetch returned after {elapsed:?}, expected about {MAX_WAIT:?}"
    );
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
async fn given_group_fetch_when_one_partition_keeps_a_backlog_should_read_another_within_max_wait(
    harness: &TestHarness,
) {
    const MAX_WAIT: Duration = Duration::from_secs(2);
    const LATE_TOLERANCE: Duration = Duration::from_secs(1);
    const PARTITIONS: u32 = 2;
    const BACKLOG_PARTITION: u32 = 0;
    const LATE_PARTITION: u32 = 1;
    const BACKLOG_BATCH: usize = 2;
    let client = harness.root_client().await.unwrap();
    let (stream, topic, _) = create_sole_group_member(&client, "backlog-stream", PARTITIONS).await;
    let mut consumer = client
        .consumer_group(GROUP, "backlog-stream", TOPIC)
        .unwrap()
        .polling_strategy(PollingStrategy::next())
        .poll_options(PollOptions {
            count: 1,
            commit: CommitPolicy::OnRead,
            ..PollOptions::wait(MAX_WAIT.into())
        })
        .build();
    consumer.init().await.unwrap();
    send_to_partition(&client, &stream, &topic, BACKLOG_PARTITION, BACKLOG_BATCH).await;
    let first = tokio::time::timeout(MAX_WAIT + LATE_TOLERANCE, consumer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.partition_id, BACKLOG_PARTITION);
    send_to_partition(&client, &stream, &topic, LATE_PARTITION, 1).await;
    let late = tokio::time::timeout(MAX_WAIT + LATE_TOLERANCE, async {
        loop {
            send_to_partition(&client, &stream, &topic, BACKLOG_PARTITION, BACKLOG_BATCH).await;
            let delivered = consumer.next().await.unwrap().unwrap();
            if delivered.partition_id == LATE_PARTITION {
                return delivered;
            }
            assert_eq!(delivered.partition_id, BACKLOG_PARTITION);
        }
    })
    .await
    .expect("a backlog on one partition must not hide another for longer than max_wait");
    assert_eq!(late.message.header.offset, 0);
    consumer.shutdown().await.unwrap();
}

#[iggy_harness(cluster_nodes = 1, test_client_transport = [Tcp, WebSocket, Quic])]
async fn given_fetch_when_a_message_arrives_should_return_its_section_without_committing(
    harness: &TestHarness,
) {
    const READY_PARTITION: u32 = 1;
    const PARK_TIME: Duration = Duration::from_millis(50);
    const REPLY_TIMEOUT: Duration = Duration::from_secs(3);
    let client = harness.root_client().await.unwrap();
    let (stream, topic, _) = create_sole_group_member(&client, "fetch-stream", 2).await;
    let consumer = Consumer::default();
    let targets = [
        (0, PollingStrategy::next()),
        (READY_PARTITION, PollingStrategy::next()),
    ];
    let options = PollOptions {
        count: 1,
        ..PollOptions::wait(Duration::from_secs(2).into())
    };
    let mut fetch = Box::pin(fetch_sections(
        &client, &stream, &topic, &targets, &consumer, options,
    ));
    tokio::select! {
        result = &mut fetch => panic!("fetch on empty partitions completed early: {result:?}"),
        () = sleep(PARK_TIME) => {}
    }
    send_to_partition(&client, &stream, &topic, READY_PARTITION, 1).await;
    let sections = tokio::time::timeout(REPLY_TIMEOUT, fetch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        sections.len(),
        targets.len(),
        "every Next target has a resolved start"
    );
    let idle = &sections[0];
    assert_eq!(
        (idle.partition_id, idle.next_offset, idle.messages_count),
        (0, 0, 0)
    );
    let ready = &sections[1];
    assert_eq!(
        (ready.partition_id, ready.next_offset, ready.messages_count),
        (READY_PARTITION, 1, 1)
    );
    assert_eq!(ready.decode().unwrap().messages[0].header.offset, 0);
    assert!(
        client
            .get_consumer_offset(&consumer, &stream, &topic, Some(READY_PARTITION))
            .await
            .unwrap()
            .is_none(),
        "a non-committing fetch stores no offset"
    );

    let immediate = PollOptions {
        count: 1,
        ..PollOptions::default()
    };
    let continued: Vec<_> = sections
        .iter()
        .map(|section| {
            (
                section.partition_id,
                PollingStrategy::continuation(
                    section.partition_id,
                    PollingKind::Offset,
                    section.next_offset,
                    section.context,
                ),
            )
        })
        .collect();
    assert!(
        fetch_sections(&client, &stream, &topic, &continued, &consumer, immediate)
            .await
            .unwrap()
            .is_empty(),
        "continuing from the section table must not replay records"
    );
    let replayed = fetch_sections(
        &client,
        &stream,
        &topic,
        &[(READY_PARTITION, PollingStrategy::next())],
        &consumer,
        immediate,
    )
    .await
    .unwrap();
    assert_eq!(replayed[0].decode().unwrap().messages[0].header.offset, 0);
    assert!(matches!(
        fetch_sections(
            &client,
            &stream,
            &topic,
            &[(READY_PARTITION, PollingStrategy::first())],
            &consumer,
            immediate
        )
        .await,
        Err(IggyError::InvalidCommand)
    ));
}

#[iggy_harness(cluster_nodes = 1, test_client_transport = [Tcp, WebSocket, Quic])]
async fn given_repeated_fetches_when_each_partition_receives_a_message_should_return_it(
    harness: &TestHarness,
) {
    const PARTITIONS: u32 = 8;
    const PARK_TIME: Duration = Duration::from_millis(50);
    const REPLY_TIMEOUT: Duration = Duration::from_secs(3);
    let client = harness.root_client().await.unwrap();
    let (stream, topic, _) = create_sole_group_member(&client, "watch-stream", PARTITIONS).await;
    let consumer = Consumer::default();
    let options = PollOptions {
        count: 1,
        ..PollOptions::wait(Duration::from_secs(2).into())
    };
    let mut targets: Vec<_> = (0..PARTITIONS)
        .map(|partition| (partition, PollingStrategy::offset(0)))
        .collect();
    for partition in 0..PARTITIONS {
        let mut fetch = Box::pin(fetch_sections(
            &client, &stream, &topic, &targets, &consumer, options,
        ));
        tokio::select! {
            result = &mut fetch => panic!("fetch completed before its message: {result:?}"),
            () = sleep(PARK_TIME) => {}
        }
        send_to_partition(&client, &stream, &topic, partition, 1).await;
        let sections = tokio::time::timeout(REPLY_TIMEOUT, fetch)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(sections.len(), 1);
        let section = &sections[0];
        assert_eq!((section.partition_id, section.next_offset), (partition, 1));
        assert_eq!(section.decode().unwrap().messages[0].header.offset, 0);
        targets[partition as usize].1 = PollingStrategy::continuation(
            section.partition_id,
            PollingKind::Offset,
            section.next_offset,
            section.context,
        );
    }
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
async fn given_commit_on_poll_group_when_the_application_pauses_should_commit_only_delivered_messages(
    harness: &TestHarness,
) {
    const SENT: u64 = 5;
    const READ: u64 = 2;
    let client = harness.root_client().await.unwrap();
    client
        .create_stream("on-demand-stream", &StreamCreateOptions::default())
        .await
        .unwrap();
    let stream = Identifier::from_str_value("on-demand-stream").unwrap();
    client
        .create_topic(
            &stream,
            "on-demand-topic",
            &TopicCreateOptions {
                partitions_count: Some(2),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let topic = Identifier::from_str_value("on-demand-topic").unwrap();
    let mut consumer = client
        .consumer_group("on-demand-group", "on-demand-stream", "on-demand-topic")
        .unwrap()
        .create_consumer_group_if_not_exists()
        .polling_strategy(PollingStrategy::next())
        .poll_options(PollOptions {
            count: 1,
            commit: CommitPolicy::OnRead,
            ..PollOptions::wait(Duration::from_secs(2).into())
        })
        .build();
    consumer.init().await.unwrap();
    let mut first = Box::pin(consumer.next());
    tokio::select! {
        _ = &mut first => panic!("consumer read from empty partitions"),
        () = sleep(Duration::from_millis(50)) => {}
    }
    let mut messages: Vec<_> = (0..SENT)
        .map(|index| {
            IggyMessage::builder()
                .payload(format!("message-{index}").into())
                .build()
                .unwrap()
        })
        .collect();
    client
        .send_messages(
            &stream,
            &topic,
            &Partitioning::partition_id(1),
            &mut messages,
        )
        .await
        .unwrap();
    let mut delivered = vec![
        tokio::time::timeout(Duration::from_secs(3), first)
            .await
            .expect("a readiness wait must wake the consumer")
            .unwrap()
            .unwrap(),
    ];
    while (delivered.len() as u64) < READ {
        delivered.push(
            tokio::time::timeout(Duration::from_secs(3), consumer.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(
        delivered
            .iter()
            .map(|message| (message.partition_id, message.message.header.offset))
            .collect::<Vec<_>>(),
        [(1, 0), (1, 1)]
    );
    sleep(Duration::from_millis(300)).await;
    let group = Consumer::group(Identifier::from_str_value("on-demand-group").unwrap());
    let stored = client
        .get_consumer_offset(&group, &stream, &topic, Some(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.stored_offset,
        READ - 1,
        "a paused application leaves the rest of the partition uncommitted"
    );
    for offset in READ..SENT {
        let message = tokio::time::timeout(Duration::from_secs(3), consumer.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            (message.partition_id, message.message.header.offset),
            (1, offset)
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(300), consumer.next())
            .await
            .is_err(),
        "every message is delivered once"
    );
    consumer.shutdown().await.unwrap();
}

#[iggy_harness(test_client_transport = [Tcp])]
async fn given_commit_on_poll_group_when_shut_down_mid_batch_should_store_the_last_delivered_offset(
    harness: &TestHarness,
) {
    const SENT: u32 = 10;
    const READ: u64 = 3;
    let client = harness.root_client().await.unwrap();
    let (stream, topic) = create_partition_with_messages(&client, "shutdown-stream", SENT).await;
    let mut consumer = client
        .consumer_group("shutdown-group", "shutdown-stream", TOPIC)
        .unwrap()
        .create_consumer_group_if_not_exists()
        .poll_options(PollOptions {
            count: SENT,
            commit: CommitPolicy::OnRead,
            ..PollOptions::wait(IggyDuration::ONE_SECOND)
        })
        .build();
    consumer.init().await.unwrap();
    read_offsets(&mut consumer, READ).await;
    let group = Consumer::group(Identifier::named("shutdown-group").unwrap());
    wait_for_stored_offset(&client, &group, &stream, &topic, u64::from(SENT) - 1).await;

    consumer.shutdown().await.unwrap();

    wait_for_stored_offset(&client, &group, &stream, &topic, READ - 1).await;
}

#[iggy_harness(test_client_transport = [Tcp])]
async fn given_commit_on_poll_consumer_when_shut_down_mid_batch_should_store_the_last_delivered_offset(
    harness: &TestHarness,
) {
    const SENT: u32 = 10;
    const READ: u64 = 3;
    let client = harness.root_client().await.unwrap();
    let (stream, topic) = create_partition_with_messages(&client, "on-read-stream", SENT).await;
    let mut consumer = client
        .consumer("on-read-reader", "on-read-stream", TOPIC)
        .unwrap()
        .partitions([0])
        .poll_options(PollOptions {
            count: SENT,
            commit: CommitPolicy::OnRead,
            ..PollOptions::wait(IggyDuration::ONE_SECOND)
        })
        .build();
    consumer.init().await.unwrap();

    read_offsets(&mut consumer, READ).await;

    let reader = Consumer::new(Identifier::named("on-read-reader").unwrap());
    wait_for_stored_offset(&client, &reader, &stream, &topic, u64::from(SENT) - 1).await;
    consumer.shutdown().await.unwrap();
    wait_for_stored_offset(&client, &reader, &stream, &topic, READ - 1).await;
}

#[iggy_harness(
    test_client_transport = [Tcp],
    server(heartbeat.enabled = true, heartbeat.interval = "7s", consumer_group.heartbeat_interval = "500ms")
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
    let client = IggyClient::create(ClientWrapper::Tcp(client), None, None);
    client.connect().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    client
        .create_stream("live-stream", &StreamCreateOptions::default())
        .await
        .unwrap();
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
    wait_for_consumer_group_assignment(&client, &stream, &topic, &group, 1, Duration::from_secs(5))
        .await;

    let started = tokio::time::Instant::now();
    let result = client
        .poll_messages(
            &stream,
            &topic,
            0,
            &Consumer::group(group.clone()),
            &PollingStrategy::first(),
            PollOptions {
                count: 1,
                ..PollOptions::wait(10_000_000.into())
            },
        )
        .await
        .unwrap();
    assert_eq!(result.partition_id, 0);
    assert!(result.messages.is_empty());
    assert!(started.elapsed() >= Duration::from_secs(9));
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
    client
        .create_stream("ready-stream", &StreamCreateOptions::default())
        .await
        .unwrap();
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
    let mut poll = Box::pin(client.poll_messages(
        &stream,
        &topic,
        0,
        &consumer,
        &strategy,
        PollOptions {
            count: 100,
            ..PollOptions::wait(Duration::from_secs(5).into())
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
    client
        .create_stream("bytes-stream", &StreamCreateOptions::default())
        .await
        .unwrap();
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
    let options = PollOptions {
        count: 3,
        min_count: 3,
        max_bytes,
        ..PollOptions::wait(IggyDuration::ONE_SECOND)
    };
    for offset in 0..3 {
        let polled = tokio::time::timeout(
            Duration::from_millis(700),
            client.poll_messages(
                &stream,
                &topic,
                0,
                &Consumer::default(),
                &PollingStrategy::next(),
                PollOptions {
                    commit: CommitPolicy::OnRead,
                    ..options
                },
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(polled.messages.len(), 1);
        assert_eq!(polled.messages[0].header.offset, offset);
    }
    // The server returns a first record larger than `max_bytes` alone, in the one reply of the
    // request.
    let oversized = client
        .poll_messages(
            &stream,
            &topic,
            0,
            &Consumer::default(),
            &PollingStrategy::offset(0),
            PollOptions {
                max_bytes: max_bytes - 1,
                ..options
            },
        )
        .await
        .expect("an oversized first record must come alone, never fail or disappear");
    assert_eq!(
        oversized
            .messages
            .iter()
            .map(|message| message.header.offset)
            .collect::<Vec<_>>(),
        [0]
    );
    // A `Last` start counts back from the end of the partition, and its first record comes alone
    // in the same way.
    let last = client
        .poll_messages(
            &stream,
            &topic,
            0,
            &Consumer::default(),
            &PollingStrategy::last(),
            PollOptions {
                count: 2,
                min_count: 1,
                max_bytes: max_bytes - 1,
                ..options
            },
        )
        .await
        .expect("an oversized first record of a `Last` start must come alone");
    assert_eq!(
        last.messages
            .iter()
            .map(|message| message.header.offset)
            .collect::<Vec<_>>(),
        [1]
    );
    let replay = client
        .poll_messages(
            &stream,
            &topic,
            0,
            &Consumer::default(),
            &PollingStrategy::offset(0),
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
    client
        .create_stream("safe-stream", &StreamCreateOptions::default())
        .await
        .unwrap();
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
        .consumer("safe-reader", "safe-stream", "topic")
        .unwrap()
        .partitions([0])
        .poll_options(PollOptions {
            count: 2,
            ..PollOptions::wait(IggyDuration::ONE_SECOND)
        })
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
        .consumer("safe-reader", "safe-stream", "topic")
        .unwrap()
        .partitions([0])
        .poll_options(PollOptions {
            count: 2,
            ..PollOptions::wait(IggyDuration::ONE_SECOND)
        })
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
        .consumer("safe-reader", "safe-stream", "topic")
        .unwrap()
        .partitions([0])
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
    client
        .create_stream("revoke-stream", &StreamCreateOptions::default())
        .await
        .unwrap();
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
        .poll_options(PollOptions {
            count: 10,
            ..PollOptions::wait(IggyDuration::ONE_SECOND)
        })
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
    // The reader joins again at an assignment refresh and reads the group position after a retry
    // interval, so the next message comes from a new read.
    let received = tokio::time::timeout(REJOIN_READ_TIMEOUT, consumer.next())
        .await
        .unwrap()
        .unwrap()
        .expect("the member joins again and reads on");
    assert_eq!(
        received.message.header.offset, 0,
        "revoked prefetched messages must never reach the application"
    );
    consumer.shutdown().await.unwrap();
}

#[iggy_harness(cluster_nodes = 1, test_client_transport = [Tcp, WebSocket, Quic])]
async fn given_one_prefetch_slot_when_a_fetch_has_many_sections_should_deliver_every_section(
    harness: &TestHarness,
) {
    const PARTITIONS: u32 = 3;
    const PARK_TIME: Duration = Duration::from_millis(100);
    const REPLY_TIMEOUT: Duration = Duration::from_secs(3);
    let client = harness.root_client().await.unwrap();
    let (stream, topic, _) = create_sole_group_member(&client, "prefetch-stream", PARTITIONS).await;
    let options = PollOptions {
        count: PARTITIONS,
        min_count: PARTITIONS,
        ..PollOptions::wait(Duration::from_secs(2).into())
    };
    let mut consumer = client
        .consumer_group(GROUP, "prefetch-stream", TOPIC)
        .unwrap()
        .polling_strategy(PollingStrategy::offset(0))
        .prefetch_messages(PARTITIONS)
        .prefetch_bytes(options.max_bytes)
        .poll_options(options)
        .build();
    consumer.init().await.unwrap();
    let mut first = Box::pin(consumer.next());
    tokio::select! {
        result = &mut first => panic!("consumer completed before its messages: {result:?}"),
        () = sleep(PARK_TIME) => {}
    }
    for partition in 0..PARTITIONS {
        send_to_partition(&client, &stream, &topic, partition, 1).await;
    }
    let first = tokio::time::timeout(REPLY_TIMEOUT, first)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let mut delivered = vec![(first.partition_id, first.message.header.offset)];
    for _ in 1..PARTITIONS {
        let message = tokio::time::timeout(REPLY_TIMEOUT, consumer.next())
            .await
            .expect("the remaining sections must survive one-slot backpressure")
            .unwrap()
            .unwrap();
        delivered.push((message.partition_id, message.message.header.offset));
    }
    delivered.sort_unstable();
    assert_eq!(delivered, [(0, 0), (1, 0), (2, 0)]);
    consumer.shutdown().await.unwrap();
}

#[iggy_harness(cluster_nodes = 3)]
async fn given_seeded_fetches_when_replies_are_lost_or_cancelled_should_preserve_partition_progress(
    harness: &TestHarness,
) {
    const SEED: u64 = 109;
    const PARTITIONS: u32 = 6;
    const ROUNDS: usize = 12;
    const MAX_BATCH: usize = 3;
    const MAX_FETCHES: usize = ROUNDS * MAX_BATCH;
    const FETCH_BYTES: u32 = 512;
    const CANCEL_EVERY: usize = 3;
    const DROP_EVERY: usize = 2;
    const PARK_TIME: Duration = Duration::from_millis(50);
    let client = harness.tcp_root_client().await.unwrap();
    let (stream, topic, _) =
        create_sole_group_member(&client, "fetch-model-stream", PARTITIONS).await;
    let consumer = Consumer::default();
    let mut primaries = BTreeMap::<u8, Vec<u32>>::new();
    for partition in 0..PARTITIONS {
        let primary = partition_primary(&client, &stream, &topic, partition)
            .await
            .unwrap();
        primaries.entry(primary).or_default().push(partition);
    }
    let groups: Vec<_> = primaries.into_values().collect();
    assert!(
        groups.len() > 1,
        "the sequence must exercise more than one primary"
    );
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut produced = [0_u64; PARTITIONS as usize];
    let mut positions = [(0_u64, None); PARTITIONS as usize];
    let mut dropped_replies = 0;
    let options = PollOptions {
        max_bytes: FETCH_BYTES,
        ..PollOptions::default()
    };
    for round in 0..ROUNDS {
        let partition = rng.random_range(0..PARTITIONS);
        if round % CANCEL_EVERY == 0 {
            let targets = [(
                partition,
                PollingStrategy::offset(produced[partition as usize]),
            )];
            let mut parked = Box::pin(fetch_sections(
                &client,
                &stream,
                &topic,
                &targets,
                &consumer,
                PollOptions {
                    count: 1,
                    max_wait: Duration::from_secs(2).into(),
                    ..options
                },
            ));
            tokio::select! {
                result = &mut parked => panic!("seed {SEED}, round {round}: empty fetch did not park: {result:?}"),
                () = sleep(PARK_TIME) => {}
            }
            drop(parked);
        }
        let batch = rng.random_range(1..=MAX_BATCH);
        send_to_partition(&client, &stream, &topic, partition, batch).await;
        produced[partition as usize] += batch as u64;
        let mut caught_up = false;
        for fetch_round in 0..MAX_FETCHES {
            for group in &groups {
                let mut targets: Vec<_> = group
                    .iter()
                    .map(|&partition| {
                        let (offset, context) = positions[partition as usize];
                        let strategy = match context {
                            Some(context) => PollingStrategy::continuation(
                                partition,
                                PollingKind::Offset,
                                offset,
                                context,
                            ),
                            None => PollingStrategy::offset(offset),
                        };
                        (partition, strategy)
                    })
                    .collect();
                let rotation = rng.random_range(0..targets.len());
                targets.rotate_left(rotation);
                let count = rng.random_range(1..=MAX_BATCH as u32);
                let fetch_options = PollOptions { count, ..options };
                let mut sections =
                    fetch_sections(&client, &stream, &topic, &targets, &consumer, fetch_options)
                        .await
                        .unwrap();
                if round % DROP_EVERY == 0 && fetch_round == 0 && !sections.is_empty() {
                    let lost: Vec<_> = sections
                        .iter()
                        .map(|section| {
                            (
                                section.partition_id,
                                section.next_offset,
                                section.records.clone(),
                            )
                        })
                        .collect();
                    sections = fetch_sections(
                        &client,
                        &stream,
                        &topic,
                        &targets,
                        &consumer,
                        fetch_options,
                    )
                    .await
                    .unwrap();
                    let replayed: Vec<_> = sections
                        .iter()
                        .map(|section| {
                            (
                                section.partition_id,
                                section.next_offset,
                                section.records.clone(),
                            )
                        })
                        .collect();
                    assert_eq!(
                        replayed, lost,
                        "seed {SEED}, round {round}: a lost non-committing reply must replay"
                    );
                    dropped_replies += 1;
                }
                assert!(
                    sections
                        .iter()
                        .map(|section| section.messages_count)
                        .sum::<u32>()
                        <= count
                );
                assert!(
                    sections
                        .iter()
                        .map(|section| section.records.len())
                        .sum::<usize>()
                        <= FETCH_BYTES as usize
                );
                for section in sections {
                    let polled = section.decode().unwrap();
                    let position = &mut positions[section.partition_id as usize];
                    for message in polled.messages {
                        assert_eq!(
                            message.header.offset, position.0,
                            "seed {SEED}, round {round}, partition {}: no gap or unrequested duplicate",
                            section.partition_id
                        );
                        position.0 += 1;
                    }
                    assert_eq!(section.next_offset, position.0);
                    position.1 = Some(section.context);
                }
            }
            if positions.iter().map(|position| position.0).eq(produced) {
                caught_up = true;
                break;
            }
        }
        assert!(
            caught_up,
            "seed {SEED}, round {round}: positions {positions:?}, produced {produced:?}"
        );
    }
    assert!(dropped_replies > 0, "the seed must cover discarded replies");
}

const TOPIC: &str = "topic";
const GROUP: &str = "group";
const ASSIGNMENT_TIMEOUT: Duration = Duration::from_secs(5);
const RECEIVE_TIMEOUT: Duration = Duration::from_secs(3);
const REJOIN_READ_TIMEOUT: Duration = Duration::from_secs(5);
const STORED_OFFSET_TIMEOUT: Duration = Duration::from_secs(3);
const STORED_OFFSET_RECHECK: Duration = Duration::from_millis(20);

/// Creates `stream_name` with a one-partition topic that holds `count` messages.
async fn create_partition_with_messages(
    client: &IggyClient,
    stream_name: &str,
    count: u32,
) -> (Identifier, Identifier) {
    client
        .create_stream(stream_name, &StreamCreateOptions::default())
        .await
        .unwrap();
    let stream = Identifier::named(stream_name).unwrap();
    client
        .create_topic(
            &stream,
            TOPIC,
            &TopicCreateOptions {
                partitions_count: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let topic = Identifier::named(TOPIC).unwrap();
    let mut messages = (0..count)
        .map(|index| IggyMessage::from_str(&format!("message-{index}")).unwrap())
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
    (stream, topic)
}

/// Creates `stream_name` with a topic of `partitions` partitions and a group
/// that the client joins as its only member.
async fn create_sole_group_member(
    client: &IggyClient,
    stream_name: &str,
    partitions: u32,
) -> (Identifier, Identifier, Identifier) {
    client
        .create_stream(stream_name, &StreamCreateOptions::default())
        .await
        .unwrap();
    let stream = Identifier::named(stream_name).unwrap();
    client
        .create_topic(
            &stream,
            TOPIC,
            &TopicCreateOptions {
                partitions_count: Some(partitions),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let topic = Identifier::named(TOPIC).unwrap();
    client
        .create_consumer_group(&stream, &topic, GROUP)
        .await
        .unwrap();
    let group = Identifier::named(GROUP).unwrap();
    client
        .join_consumer_group(&stream, &topic, &group)
        .await
        .unwrap();
    wait_for_consumer_group_assignment(client, &stream, &topic, &group, 1, ASSIGNMENT_TIMEOUT)
        .await;
    (stream, topic, group)
}

/// Sends `count` messages to `partition`.
async fn send_to_partition(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
    partition: u32,
    count: usize,
) {
    let mut messages = (0..count)
        .map(|index| IggyMessage::from_str(&format!("message-{index}")).unwrap())
        .collect::<Vec<_>>();
    client
        .send_messages(
            stream,
            topic,
            &Partitioning::partition_id(partition),
            &mut messages,
        )
        .await
        .unwrap();
}

/// Reads offsets `0..count` of partition 0, in order.
async fn read_offsets(consumer: &mut IggyConsumer, count: u64) {
    for offset in 0..count {
        let received = tokio::time::timeout(RECEIVE_TIMEOUT, consumer.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            (received.partition_id, received.message.header.offset),
            (0, offset)
        );
    }
}

/// Waits until the server stores `expected` for `consumer` on partition 0.
async fn wait_for_stored_offset(
    client: &IggyClient,
    consumer: &Consumer,
    stream: &Identifier,
    topic: &Identifier,
    expected: u64,
) {
    let deadline = tokio::time::Instant::now() + STORED_OFFSET_TIMEOUT;
    loop {
        let stored = client
            .get_consumer_offset(consumer, stream, topic, Some(0))
            .await
            .unwrap()
            .map(|offset| offset.stored_offset);
        if stored == Some(expected) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "stored offset {stored:?}, expected {expected}"
        );
        sleep(STORED_OFFSET_RECHECK).await;
    }
}
