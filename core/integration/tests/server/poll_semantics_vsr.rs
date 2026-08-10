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

//! Poll semantics against server-ng (vsr): a poll aimed at a partition id the
//! topic does not have must surface a typed `PartitionNotFound`, not an empty
//! poll a consumer would read as end-of-partition; a poll whose stream or
//! topic does not resolve must surface the legacy `StreamIdNotFound` /
//! `TopicIdNotFound` the same way; the partition addressing error on
//! `get_consumer_offset` must not decode as "no offset stored"; and a
//! timestamp poll must be at-or-after, including the message stamped exactly at
//! the queried timestamp (the timestamp replies report per message).

use iggy::prelude::*;
use integration::iggy_harness;
use tokio::time::{Duration, sleep};

#[iggy_harness(
    test_client_transport = [Tcp],
    server(tcp.socket.override_defaults = true, tcp.socket.nodelay = true)
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
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
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
    test_client_transport = [Tcp],
    server(tcp.socket.override_defaults = true, tcp.socket.nodelay = true)
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
    test_client_transport = [Tcp],
    server(tcp.socket.override_defaults = true, tcp.socket.nodelay = true)
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
/// fresh consumer. Legacy swallows this one too; server-ng surfaces the code
/// the poll path already surfaces for the identical addressing error.
#[iggy_harness(
    test_client_transport = [Tcp],
    server(tcp.socket.override_defaults = true, tcp.socket.nodelay = true)
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
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
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

#[iggy_harness(
    test_client_transport = [Tcp],
    server(tcp.socket.override_defaults = true, tcp.socket.nodelay = true)
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
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
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
