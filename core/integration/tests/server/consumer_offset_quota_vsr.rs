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

use iggy::prelude::*;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::consensus::Operation;
use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
use iggy_binary_protocol::{AckLevel, WireConsumer, WireIdentifier};
use iggy_common::store_consumer_offset::StoreConsumerOffset;
use integration::harness::TestBinary;
use integration::iggy_harness;
use reqwest::StatusCode;
use std::collections::BTreeMap;
use std::fs;

use super::http_client::HttpClient;
use super::raw_tcp;

const STREAM_NAME: &str = "consumer-offset-quota-stream";
const TOPIC_NAME: &str = "consumer-offset-quota-topic";
const PARTITION_ID: u32 = 0;
const LIMIT: u32 = 4;

#[iggy_harness(
    cluster_nodes = 1,
    server(partition.consumer_offsets_max = "4")
)]
async fn given_full_consumer_offset_table_when_creating_another_should_reject_without_new_file(
    harness: &TestHarness,
) {
    let client = harness.tcp_root_client().await.expect("TCP root client");
    let stream = Identifier::named(STREAM_NAME).expect("stream identifier");
    let topic = Identifier::named(TOPIC_NAME).expect("topic identifier");
    let stream_details = client
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream");
    let topic_details = client
        .create_topic(
            &stream,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
    let mut messages = vec![
        IggyMessage::builder()
            .payload("offset-quota".into())
            .build()
            .expect("build message"),
    ];
    client
        .send_messages(
            &stream,
            &topic,
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .expect("seed non-empty partition");

    client
        .create_user(
            "offset-poll-only",
            "password123",
            UserStatus::Active,
            Some(Permissions {
                global: GlobalPermissions::default(),
                streams: Some(BTreeMap::from([(
                    stream_details.id as usize,
                    StreamPermissions {
                        topics: Some(BTreeMap::from([(
                            topic_details.id as usize,
                            TopicPermissions {
                                poll_messages: true,
                                ..Default::default()
                            },
                        )])),
                        ..Default::default()
                    },
                )])),
            }),
        )
        .await
        .expect("create a topic-scoped consumer");
    let client = harness.tcp_new_client().await.expect("consumer TCP client");
    client
        .login_user("offset-poll-only", "password123")
        .await
        .expect("consumer login");

    let first_consumer = Consumer::new(Identifier::numeric(1).unwrap());
    let polled = client
        .poll_messages(
            &stream,
            &topic,
            Some(PARTITION_ID),
            &first_consumer,
            &PollingStrategy::first(),
            1,
            true,
        )
        .await
        .expect("new auto-commit consumer fits");
    assert_eq!(polled.messages.len(), 1);
    let first_file = harness.server().data_path().join(format!(
        "streams/{}/topics/{}/partitions/{PARTITION_ID}/offsets/consumers/1",
        stream_details.id, topic_details.id
    ));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while !first_file.is_file() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "auto-commit never reached its file"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        client
            .poll_messages(
                &stream,
                &topic,
                Some(PARTITION_ID),
                &first_consumer,
                &PollingStrategy::next(),
                1,
                true
            )
            .await
            .expect("next poll")
            .messages
            .is_empty()
    );

    for consumer_id in 1..=LIMIT {
        client
            .store_consumer_offset(
                &Consumer::new(Identifier::numeric(consumer_id).expect("consumer identifier")),
                &stream,
                &topic,
                Some(PARTITION_ID),
                0,
            )
            .await
            .expect("store offset within limit");
    }

    let rejected_consumer =
        Consumer::new(Identifier::numeric(LIMIT + 1).expect("consumer identifier"));
    let rejected = client
        .store_consumer_offset(&rejected_consumer, &stream, &topic, Some(PARTITION_ID), 0)
        .await;
    assert!(
        matches!(rejected, Err(IggyError::TooManyConsumerOffsets)),
        "the first key above the limit must receive the typed capacity error"
    );

    client
        .store_consumer_offset(
            &Consumer::new(Identifier::numeric(1).expect("consumer identifier")),
            &stream,
            &topic,
            Some(PARTITION_ID),
            0,
        )
        .await
        .expect("existing key remains writable at the limit");

    let poll_rejected = client
        .poll_messages(
            &stream,
            &topic,
            Some(PARTITION_ID),
            &rejected_consumer,
            &PollingStrategy::first(),
            1,
            true,
        )
        .await;
    assert!(
        matches!(poll_rejected, Err(IggyError::TooManyConsumerOffsets)),
        "auto-commit must not return data when its new key cannot be admitted"
    );
    client
        .poll_messages(
            &stream,
            &topic,
            Some(PARTITION_ID),
            &rejected_consumer,
            &PollingStrategy::first(),
            1,
            false,
        )
        .await
        .expect("the same poll succeeds when auto-commit is disabled");

    client
        .delete_consumer_offset(
            &Consumer::new(Identifier::numeric(1).expect("consumer identifier")),
            &stream,
            &topic,
            Some(PARTITION_ID),
        )
        .await
        .expect("delete one accepted offset");
    client
        .store_consumer_offset(&rejected_consumer, &stream, &topic, Some(PARTITION_ID), 0)
        .await
        .expect("delete releases one durable slot");

    let mut raw = raw_tcp::connect(harness).await;
    let raw_client_id = 0xC0FF_EE03;
    let session = raw_tcp::register_root(&mut raw, raw_client_id).await;
    let unresolved_group = StoreConsumerOffsetRequest {
        consumer: WireConsumer::consumer_group(WireIdentifier::Numeric(999)),
        stream_id: WireIdentifier::Numeric(stream_details.id),
        topic_id: WireIdentifier::Numeric(topic_details.id),
        partition_id: Some(PARTITION_ID),
        offset: 0,
        ack: AckLevel::Quorum,
    }
    .to_bytes();
    let header = raw_tcp::request_header(
        Operation::StoreConsumerOffset,
        raw_client_id,
        session,
        1,
        unresolved_group.len(),
    );
    let (reply, _) = raw_tcp::exchange(&mut raw, &header, &unresolved_group).await;
    assert_eq!(
        raw_tcp::reply_status(&reply),
        IggyError::ConsumerGroupIdNotFound(Identifier::numeric(999).unwrap(), topic.clone())
            .as_code()
    );

    let offsets_dir = harness.server().data_path().join(format!(
        "streams/{}/topics/{}/partitions/{PARTITION_ID}/offsets/consumers",
        stream_details.id, topic_details.id
    ));
    let file_count = integration::harness::disk::consumer_offset_file_ids(
        &harness.server().data_path(),
        stream_details.id,
        topic_details.id,
        PARTITION_ID,
        ConsumerKind::Consumer,
    )
    .expect("consumer offsets directory")
    .len();
    assert_eq!(file_count, LIMIT as usize);
    let groups_dir = offsets_dir
        .parent()
        .expect("consumer offset directory has offsets parent")
        .join("groups");
    let group_file_count = fs::read_dir(groups_dir)
        .map(|entries| entries.filter_map(Result::ok).count())
        .unwrap_or_default();
    assert_eq!(group_file_count, 0);

    let named_group = StoreConsumerOffsetRequest {
        consumer: WireConsumer::consumer_group(WireIdentifier::named("unknown-group").unwrap()),
        stream_id: WireIdentifier::Numeric(stream_details.id),
        topic_id: WireIdentifier::Numeric(topic_details.id),
        partition_id: Some(PARTITION_ID),
        offset: 0,
        ack: AckLevel::Quorum,
    }
    .to_bytes();
    let header = raw_tcp::request_header(
        Operation::StoreConsumerOffset,
        raw_client_id,
        session,
        2,
        named_group.len(),
    );
    let (reply, _) = raw_tcp::exchange(&mut raw, &header, &named_group).await;
    assert_eq!(
        raw_tcp::reply_status(&reply),
        IggyError::ConsumerGroupNameNotFound("unknown-group".to_owned(), topic.clone()).as_code()
    );

    let unknown_stream = StoreConsumerOffsetRequest {
        consumer: WireConsumer::consumer_group(WireIdentifier::Numeric(999)),
        stream_id: WireIdentifier::Numeric(999_999),
        topic_id: WireIdentifier::Numeric(topic_details.id),
        partition_id: Some(PARTITION_ID),
        offset: 0,
        ack: AckLevel::Quorum,
    }
    .to_bytes();
    let header = raw_tcp::request_header(
        Operation::StoreConsumerOffset,
        raw_client_id,
        session,
        3,
        unknown_stream.len(),
    );
    let (reply, _) = raw_tcp::exchange(&mut raw, &header, &unknown_stream).await;
    assert_eq!(
        raw_tcp::reply_status(&reply),
        IggyError::ResourceNotFound(String::new()).as_code(),
        "a missing stream is not reported as a missing group"
    );

    let http = HttpClient::login_root(harness).await;
    let response = http
        .client
        .put(http.url(&format!(
            "/streams/{STREAM_NAME}/topics/{TOPIC_NAME}/consumer-offsets"
        )))
        .bearer_auth(&http.token)
        .json(&StoreConsumerOffset {
            consumer: Consumer::new(Identifier::numeric(6).expect("consumer identifier")),
            partition_id: Some(PARTITION_ID),
            offset: 0,
        })
        .send()
        .await
        .expect("HTTP capacity request");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = response.json().await.expect("HTTP error body");
    assert_eq!(body["id"], 3024);
    assert_eq!(body["code"], "too_many_consumer_offsets");

    let metrics = http
        .client
        .get(http.url("/metrics"))
        .bearer_auth(&http.token)
        .send()
        .await
        .expect("metrics response")
        .text()
        .await
        .expect("metrics text");
    let denied: u64 = metrics
        .lines()
        .filter(|line| line.starts_with("partition_consumer_offsets_denied_total{"))
        .map(|line| {
            line.split_whitespace()
                .last()
                .expect("counter value")
                .parse::<u64>()
                .expect("numeric counter")
        })
        .sum();
    assert_eq!(denied, 3, "one explicit TCP, one poll, and one HTTP denial");
}

#[iggy_harness(
    cluster_nodes = 1,
    server(partition.consumer_offsets_max = "2")
)]
async fn given_full_consumer_offset_table_when_server_restarts_should_preserve_admission_state(
    harness: &mut TestHarness,
) {
    let client = harness.tcp_root_client().await.expect("TCP root client");
    let stream = Identifier::named("consumer-offset-restart-stream").expect("stream identifier");
    let topic = Identifier::named("consumer-offset-restart-topic").expect("topic identifier");
    let stream_details = client
        .create_stream("consumer-offset-restart-stream")
        .await
        .expect("create stream");
    let topic_details = client
        .create_topic(
            &stream,
            "consumer-offset-restart-topic",
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
    let mut messages = vec![
        IggyMessage::builder()
            .payload("offset-restart".into())
            .build()
            .expect("build message"),
    ];
    client
        .send_messages(
            &stream,
            &topic,
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .expect("seed non-empty partition");
    for consumer_id in 1..=2 {
        client
            .store_consumer_offset(
                &Consumer::new(Identifier::numeric(consumer_id).expect("consumer identifier")),
                &stream,
                &topic,
                Some(PARTITION_ID),
                0,
            )
            .await
            .expect("store offset before restart");
    }
    drop(client);
    harness.server_mut().stop().expect("stop server");
    let offsets_dir = harness.server().data_path().join(format!(
        "streams/{}/topics/{}/partitions/{PARTITION_ID}/offsets/consumers",
        stream_details.id, topic_details.id
    ));
    fs::copy(offsets_dir.join("1"), offsets_dir.join("3"))
        .expect("create first historical over-limit offset");
    fs::copy(offsets_dir.join("1"), offsets_dir.join("4"))
        .expect("create second historical over-limit offset");
    harness.server_mut().start().expect("restart server");
    let client = harness
        .root_client()
        .await
        .expect("post-restart root client");

    let rejected = client
        .store_consumer_offset(
            &Consumer::new(Identifier::numeric(5).expect("consumer identifier")),
            &stream,
            &topic,
            Some(PARTITION_ID),
            0,
        )
        .await;
    assert!(matches!(rejected, Err(IggyError::TooManyConsumerOffsets)));
    client
        .store_consumer_offset(
            &Consumer::new(Identifier::numeric(4).expect("consumer identifier")),
            &stream,
            &topic,
            Some(PARTITION_ID),
            0,
        )
        .await
        .expect("recovered existing key remains writable");
    client
        .delete_consumer_offset(
            &Consumer::new(Identifier::numeric(4).expect("consumer identifier")),
            &stream,
            &topic,
            Some(PARTITION_ID),
        )
        .await
        .expect("delete recovered key");
    client
        .delete_consumer_offset(
            &Consumer::new(Identifier::numeric(3).expect("consumer identifier")),
            &stream,
            &topic,
            Some(PARTITION_ID),
        )
        .await
        .expect("delete second historical key");
    client
        .delete_consumer_offset(
            &Consumer::new(Identifier::numeric(2).expect("consumer identifier")),
            &stream,
            &topic,
            Some(PARTITION_ID),
        )
        .await
        .expect("delete below configured limit");
    client
        .store_consumer_offset(
            &Consumer::new(Identifier::numeric(5).expect("consumer identifier")),
            &stream,
            &topic,
            Some(PARTITION_ID),
            0,
        )
        .await
        .expect("deleting below the limit releases a slot");
}
