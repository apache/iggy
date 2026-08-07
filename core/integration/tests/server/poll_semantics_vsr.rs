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
//! poll a consumer would read as end-of-partition; a timestamp poll must be
//! at-or-after, including the message stamped exactly at the queried
//! timestamp (the timestamp replies report per message).

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
