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

//! A server over its disk usage limit refuses new messages, new consumer
//! offset keys, new topics and new partitions, and keeps serving reads,
//! existing offsets and deletes.
//!
//! The test restarts the same server with a limit its volume is already over,
//! so no disk has to fill and nothing depends on what other tests write. The
//! data directory survives the restart, so the offset stored before it is an
//! existing key after it.

use iggy::prelude::*;
use iggy_common::IggyMessagesBatch;
use integration::harness::TestHarness;
use integration::harness::seeds::{self, names};
use reqwest::StatusCode;
use serial_test::parallel;

use super::http_client::HttpClient;

const MESSAGES: u32 = 10;
const CONSUMER_WITH_OFFSET: u32 = 1;
const NEW_CONSUMER: u32 = 2;
/// Created under the limit and deleted over it.
const SPARE_TOPIC: &str = "spare";
/// Any volume holding a server's data is more than a millionth used.
const LIMIT_REACHED_PERCENT: &str = "0.0001";

#[tokio::test]
#[parallel]
async fn given_a_disk_over_the_limit_when_writing_should_refuse_new_data_and_keep_serving_reads() {
    let mut harness = start_server().await;
    let stream = Identifier::named(names::STREAM).expect("stream identifier");
    let topic = Identifier::named(names::TOPIC).expect("topic identifier");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    send(&client, &stream, &topic)
        .await
        .expect("send under the limit");
    client
        .store_consumer_offset(&consumer(CONSUMER_WITH_OFFSET), &stream, &topic, Some(0), 4)
        .await
        .expect("store an offset under the limit");
    create_topic(&client, &stream, SPARE_TOPIC)
        .await
        .expect("create a topic under the limit");
    drop(client);

    harness
        .server_mut()
        .add_env("IGGY_DISK_USAGE_LIMIT_ENABLED", "true");
    harness
        .server_mut()
        .add_env("IGGY_DISK_USAGE_LIMIT_PERCENT", LIMIT_REACHED_PERCENT);
    harness
        .restart_server()
        .await
        .expect("restart over the limit");
    let client = harness.tcp_root_client().await.expect("tcp root client");

    assert_eq!(
        send(&client, &stream, &topic).await,
        Err(IggyError::DiskUsageLimitReached),
        "new messages must be refused"
    );
    let polled = client
        .poll_messages(
            &stream,
            &topic,
            0,
            &consumer(CONSUMER_WITH_OFFSET),
            &PollingStrategy::offset(0),
            PollOptions {
                count: MESSAGES,
                ..PollOptions::default()
            },
        )
        .await
        .expect("reads must keep working");
    assert_eq!(polled.messages.len(), MESSAGES as usize);
    client
        .store_consumer_offset(&consumer(CONSUMER_WITH_OFFSET), &stream, &topic, Some(0), 9)
        .await
        .expect("an existing offset key must stay writable");
    assert_eq!(
        client
            .store_consumer_offset(&consumer(NEW_CONSUMER), &stream, &topic, Some(0), 0)
            .await,
        Err(IggyError::DiskUsageLimitReached),
        "a new offset key must be refused"
    );
    client
        .delete_consumer_offset(&consumer(CONSUMER_WITH_OFFSET), &stream, &topic, Some(0))
        .await
        .expect("a delete frees space, so it must be admitted");
    assert_eq!(
        create_topic(&client, &stream, "new").await.map(drop),
        Err(IggyError::DiskUsageLimitReached),
        "a new topic must be refused"
    );
    assert_eq!(
        client.create_partitions(&stream, &topic, 1).await,
        Err(IggyError::DiskUsageLimitReached),
        "new partitions must be refused"
    );
    client
        .delete_topic(
            &stream,
            &Identifier::named(SPARE_TOPIC).expect("topic identifier"),
        )
        .await
        .expect("deleting a topic frees space, so it must be admitted");

    let http = HttpClient::login_root(&harness).await;
    let response = http_send(&http).await;
    assert_eq!(
        response.status(),
        StatusCode::INSUFFICIENT_STORAGE,
        "HTTP must refuse new messages with 507"
    );
    let metrics = http
        .get("/metrics")
        .await
        .text()
        .await
        .expect("metrics text");
    assert!(
        metrics.contains("\ndisk_usage_limit_reached 1\n"),
        "the gauge must report the limit reached:\n{metrics}"
    );
    drop(client);

    harness
        .server_mut()
        .add_env("IGGY_DISK_USAGE_LIMIT_ENABLED", "false");
    harness
        .restart_server()
        .await
        .expect("restart without the limit");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    send(&client, &stream, &topic)
        .await
        .expect("without the limit, sends must be admitted again");
}

async fn create_topic(
    client: &IggyClient,
    stream: &Identifier,
    name: &str,
) -> Result<TopicDetails, IggyError> {
    client
        .create_topic(
            stream,
            name,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
}

async fn send(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
) -> Result<(), IggyError> {
    let mut messages = messages();
    client
        .send_messages(stream, topic, &Partitioning::partition_id(0), &mut messages)
        .await
        .map(drop)
}

async fn http_send(http: &HttpClient) -> reqwest::Response {
    let body = SendMessages {
        metadata_length: 0,
        stream_id: Identifier::default(),
        topic_id: Identifier::default(),
        partitioning: Partitioning::partition_id(0),
        batch: IggyMessagesBatch::from(&messages()),
        context: None,
    };
    http.client
        .post(http.url(&format!(
            "/streams/{}/topics/{}/messages",
            names::STREAM,
            names::TOPIC
        )))
        .bearer_auth(&http.token)
        .json(&body)
        .send()
        .await
        .expect("HTTP send request")
}

fn messages() -> Vec<IggyMessage> {
    (0..MESSAGES)
        .map(|_| {
            IggyMessage::builder()
                .payload(names::MESSAGE_PAYLOAD.into())
                .build()
                .expect("build message")
        })
        .collect()
}

fn consumer(id: u32) -> Consumer {
    Consumer::new(Identifier::numeric(id).expect("consumer identifier"))
}

/// One node: the harness defaults to a three-replica cluster, and the limit
/// under test is the one node's.
async fn start_server() -> TestHarness {
    let mut harness = TestHarness::builder()
        .default_server()
        .cluster_nodes(1)
        .build()
        .expect("build harness");
    harness
        .start_with_seed(|client| async move { seeds::stream_with_topic(&client).await })
        .await
        .expect("start server");
    harness
}
