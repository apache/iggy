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

//! Topic admission and echo semantics against server-ng (vsr). Create bounds
//! must be rejected with typed errors before consensus: partitions count above
//! `MAX_PARTITIONS_PER_REQUEST` denies with `TooManyPartitions` (for create
//! topic, create partitions and delete partitions alike); a custom
//! `max_topic_size` below the configured segment size denies with
//! `InvalidTopicSize`; `ServerDefault` and `Unlimited` sizes pass. Update
//! stores `max_topic_size` and `message_expiry` verbatim and gets echo the
//! stored value (never the node default frozen at update time), matching
//! legacy wire behavior.

use std::str::FromStr;

use iggy::prelude::*;
use integration::iggy_harness;

const PARTITIONS_LIMIT: u32 = 1000;

async fn create_topic_with(
    client: &IggyClient,
    stream_id: &Identifier,
    name: &str,
    partitions_count: u32,
    max_topic_size: MaxTopicSize,
) -> Result<TopicDetails, IggyError> {
    client
        .create_topic(
            stream_id,
            name,
            partitions_count,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            max_topic_size,
        )
        .await
}

#[iggy_harness(
    test_client_transport = [Tcp],
    server(tcp.socket.override_defaults = true, tcp.socket.nodelay = true)
)]
async fn given_out_of_bounds_topic_when_creating_should_reject_typed(harness: &TestHarness) {
    let client = harness.tcp_root_client().await.expect("tcp root client");
    client
        .create_stream("admission-stream")
        .await
        .expect("create stream");
    let stream_id = Identifier::from_str_value("admission-stream").expect("stream identifier");

    let too_many = IggyError::TooManyPartitions.as_code();
    for partitions_count in [PARTITIONS_LIMIT + 1, 10_000] {
        let result = create_topic_with(
            &client,
            &stream_id,
            "too-many-partitions",
            partitions_count,
            MaxTopicSize::ServerDefault,
        )
        .await;
        assert!(
            matches!(&result, Err(error) if error.as_code() == too_many),
            "{partitions_count} partitions must deny with TooManyPartitions, got {result:?}"
        );
    }

    // Below the default segment size (1 GiB) => rejected before consensus.
    let tiny = MaxTopicSize::Custom(IggyByteSize::from_str("10KiB").expect("byte size"));
    let result = create_topic_with(&client, &stream_id, "tiny-topic", 1, tiny).await;
    let invalid_size = IggyError::InvalidTopicSize(tiny, IggyByteSize::default()).as_code();
    assert!(
        matches!(&result, Err(error) if error.as_code() == invalid_size),
        "max_topic_size below segment size must deny with InvalidTopicSize, got {result:?}"
    );

    create_topic_with(
        &client,
        &stream_id,
        "boundary-partitions",
        PARTITIONS_LIMIT,
        MaxTopicSize::ServerDefault,
    )
    .await
    .expect("exactly MAX_PARTITIONS_PER_REQUEST partitions is accepted");

    create_topic_with(
        &client,
        &stream_id,
        "unlimited-topic",
        1,
        MaxTopicSize::Unlimited,
    )
    .await
    .expect("unlimited max_topic_size is accepted");
}

#[iggy_harness(
    test_client_transport = [Tcp],
    server(tcp.socket.override_defaults = true, tcp.socket.nodelay = true)
)]
async fn given_updated_topic_when_getting_topic_should_echo_stored_values(harness: &TestHarness) {
    let client = harness.tcp_root_client().await.expect("tcp root client");
    client
        .create_stream("echo-stream")
        .await
        .expect("create stream");
    let stream_id = Identifier::from_str_value("echo-stream").expect("stream identifier");
    create_topic_with(
        &client,
        &stream_id,
        "echo-topic",
        1,
        MaxTopicSize::Custom(IggyByteSize::from_str("2GiB").expect("byte size")),
    )
    .await
    .expect("create topic");
    let topic_id = Identifier::from_str_value("echo-topic").expect("topic identifier");

    let update_topic = |max_topic_size: MaxTopicSize, message_expiry: IggyExpiry| {
        let client = &client;
        let stream_id = &stream_id;
        let topic_id = &topic_id;
        async move {
            client
                .update_topic(
                    stream_id,
                    topic_id,
                    "echo-topic",
                    CompressionAlgorithm::None,
                    None,
                    message_expiry,
                    max_topic_size,
                )
                .await
                .expect("update topic");
            let topic = client
                .get_topic(stream_id, topic_id)
                .await
                .expect("get topic")
                .expect("topic exists");
            (topic.max_topic_size, topic.message_expiry)
        }
    };

    // The server echoes both stored sentinels as wire 0 (legacy parity). The
    // SDK decodes a topic-response size 0 as `ServerDefault` but an expiry 0
    // as `NeverExpire` (`wire_conversions`), so that is the legacy-identical
    // client-visible read-back; the node default must NOT leak into either.
    assert_eq!(
        update_topic(MaxTopicSize::ServerDefault, IggyExpiry::ServerDefault).await,
        (MaxTopicSize::ServerDefault, IggyExpiry::NeverExpire),
        "an update to ServerDefault must echo the stored sentinel, \
         not the node default frozen at update time"
    );
    let custom_size = MaxTopicSize::Custom(IggyByteSize::from_str("3GiB").expect("byte size"));
    let custom_expiry = IggyExpiry::ExpireDuration(IggyDuration::from_str("5s").expect("duration"));
    assert_eq!(
        update_topic(custom_size, custom_expiry).await,
        (custom_size, custom_expiry),
        "explicit custom values echo verbatim"
    );
    assert_eq!(
        update_topic(MaxTopicSize::Unlimited, IggyExpiry::NeverExpire).await,
        (MaxTopicSize::Unlimited, IggyExpiry::NeverExpire),
        "unlimited size and never-expire echo verbatim"
    );
}

#[iggy_harness(
    test_client_transport = [Tcp],
    server(tcp.socket.override_defaults = true, tcp.socket.nodelay = true)
)]
async fn given_out_of_bounds_partitions_count_when_mutating_should_reject_typed(
    harness: &TestHarness,
) {
    let client = harness.tcp_root_client().await.expect("tcp root client");
    client
        .create_stream("partitions-stream")
        .await
        .expect("create stream");
    let stream_id = Identifier::from_str_value("partitions-stream").expect("stream identifier");
    create_topic_with(
        &client,
        &stream_id,
        "partitions-topic",
        1,
        MaxTopicSize::ServerDefault,
    )
    .await
    .expect("create topic");
    let topic_id = Identifier::from_str_value("partitions-topic").expect("topic identifier");

    let too_many = IggyError::TooManyPartitions.as_code();
    let result = client
        .create_partitions(&stream_id, &topic_id, PARTITIONS_LIMIT + 1)
        .await;
    assert!(
        matches!(&result, Err(error) if error.as_code() == too_many),
        "oversized create_partitions must deny with TooManyPartitions, got {result:?}"
    );
    let result = client
        .delete_partitions(&stream_id, &topic_id, PARTITIONS_LIMIT + 1)
        .await;
    assert!(
        matches!(&result, Err(error) if error.as_code() == too_many),
        "oversized delete_partitions must deny with TooManyPartitions, got {result:?}"
    );

    // Zero is a no-op that still burns a replicated log entry, bumps the
    // metadata revision and forces a rebalance pass. Legacy rejects it with the
    // same code in both handlers (`1..=MAX` on create, `== 0` on delete); note
    // that create_topic is deliberately NOT included, since a zero-partition
    // topic is legal there in legacy too.
    let result = client.create_partitions(&stream_id, &topic_id, 0).await;
    assert!(
        matches!(&result, Err(error) if error.as_code() == too_many),
        "create_partitions with 0 must deny with TooManyPartitions, got {result:?}"
    );
    let result = client.delete_partitions(&stream_id, &topic_id, 0).await;
    assert!(
        matches!(&result, Err(error) if error.as_code() == too_many),
        "delete_partitions with 0 must deny with TooManyPartitions, got {result:?}"
    );

    client
        .create_partitions(&stream_id, &topic_id, 2)
        .await
        .expect("in-bounds create_partitions is accepted");
    let topic = client
        .get_topic(&stream_id, &topic_id)
        .await
        .expect("get topic")
        .expect("topic exists");
    assert_eq!(
        topic.partitions_count, 3,
        "the in-bounds add lands after the oversized denies"
    );
}
