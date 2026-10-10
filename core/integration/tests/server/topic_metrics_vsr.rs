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

//! `/metrics` per topic: an operator alerting on a topic's growth (against its
//! `max_topic_size`, or behind a stopped consumer that holds retention back)
//! needs the topic's size and message count as labelled series. The cluster
//! totals (`messages`, `segments`) can't say which topic is growing.

use iggy::prelude::*;
use integration::harness::TestHarness;
use integration::iggy_harness;

use super::http_client::HttpClient;

const STREAM: &str = "metrics-stream";
const TOPIC: &str = "metrics-topic";
const METRICS_USER: &str = "metrics-reader";
const METRICS_PASSWORD: &str = "metrics-reader-pass";
const TOPIC_SERIES: [&str; 3] = ["topic_size_bytes", "topic_messages", "topic_max_size_bytes"];

/// The value of the unlabelled `name` gauge.
fn gauge(exposition: &str, name: &str) -> Option<u64> {
    let series_prefix = format!("{name} ");
    exposition
        .lines()
        .find_map(|line| line.strip_prefix(&series_prefix))
        .and_then(|value| value.trim().parse().ok())
}

/// The value of the `name` series labelled with this test's stream and topic.
fn topic_series(exposition: &str, name: &str) -> Option<u64> {
    let stream_label = format!("stream=\"{STREAM}\"");
    let topic_label = format!("topic=\"{TOPIC}\"");
    let series_prefix = format!("{name}{{");
    exposition
        .lines()
        .filter(|line| line.starts_with(&series_prefix))
        .find(|line| line.contains(&stream_label) && line.contains(&topic_label))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse().ok())
}

#[iggy_harness(cluster_nodes = 1)]
async fn given_topic_with_messages_when_scraping_metrics_should_report_its_size_and_count(
    harness: &TestHarness,
) {
    let client = harness.tcp_root_client().await.expect("tcp root client");
    client.create_stream(STREAM).await.expect("create stream");
    let stream_id = Identifier::from_str_value(STREAM).expect("stream identifier");
    let topic_id = Identifier::from_str_value(TOPIC).expect("topic identifier");
    client
        .create_topic(
            &stream_id,
            TOPIC,
            &TopicCreateOptions {
                partitions_count: Some(2),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");

    for partition in 0..2 {
        let mut messages: Vec<IggyMessage> = (0..10)
            .map(|index| {
                IggyMessage::builder()
                    .payload(format!("partition-{partition}-message-{index}").into())
                    .build()
                    .expect("build message")
            })
            .collect();
        client
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(partition),
                &mut messages,
            )
            .await
            .expect("send messages");
    }

    let topic = client
        .get_topic(&stream_id, &topic_id)
        .await
        .expect("get topic")
        .expect("topic exists");
    assert_eq!(
        topic.messages_count, 20,
        "GetTopic must count both partitions' sends, or the comparison below proves nothing"
    );
    assert!(topic.size.as_bytes_u64() > 0);

    let exposition = scrape(harness).await;

    assert_eq!(
        topic_series(&exposition, "topic_size_bytes"),
        Some(topic.size.as_bytes_u64()),
        "/metrics must report the topic's size as GetTopic does:\n{exposition}"
    );
    assert_eq!(
        topic_series(&exposition, "topic_messages"),
        Some(topic.messages_count),
        "/metrics must report the topic's message count as GetTopic does:\n{exposition}"
    );
    assert_eq!(
        topic_series(&exposition, "topic_max_size_bytes"),
        None,
        "an uncapped topic must not export a cap series:\n{exposition}"
    );
}

#[iggy_harness(cluster_nodes = 1)]
async fn given_topic_with_max_size_when_scraping_metrics_should_report_its_cap(
    harness: &TestHarness,
) {
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let max_topic_size = MaxTopicSize::from(2 * 1024 * 1024 * 1024u64);
    create_topic(&client, max_topic_size).await;

    let exposition = scrape(harness).await;

    assert_eq!(
        topic_series(&exposition, "topic_max_size_bytes"),
        Some(max_topic_size.as_bytes_u64()),
        "/metrics must report the topic's configured cap:\n{exposition}"
    );
}

#[iggy_harness(cluster_nodes = 1)]
async fn given_scraped_topic_when_deleted_should_drop_its_series_on_next_scrape(
    harness: &TestHarness,
) {
    let client = harness.tcp_root_client().await.expect("tcp root client");
    create_topic(&client, MaxTopicSize::from(2 * 1024 * 1024 * 1024u64)).await;

    let before = scrape(harness).await;
    for name in TOPIC_SERIES {
        assert!(
            topic_series(&before, name).is_some(),
            "the capped topic must export its {name} series before the delete:\n{before}"
        );
    }

    let stream_id = Identifier::from_str_value(STREAM).expect("stream identifier");
    let topic_id = Identifier::from_str_value(TOPIC).expect("topic identifier");
    client
        .delete_topic(&stream_id, &topic_id)
        .await
        .expect("delete topic");

    let after = scrape(harness).await;
    for name in TOPIC_SERIES {
        assert_eq!(
            topic_series(&after, name),
            None,
            "a deleted topic must not keep its {name} series:\n{after}"
        );
    }
}

#[iggy_harness(cluster_nodes = 1)]
async fn given_user_without_server_read_when_scraping_metrics_should_omit_topic_series(
    harness: &TestHarness,
) {
    let client = harness.tcp_root_client().await.expect("tcp root client");
    create_topic(&client, MaxTopicSize::from(2 * 1024 * 1024 * 1024u64)).await;
    // Topic reads alone must not unlock the per-topic series: the gate is the
    // server-read rule `/stats` uses, not visibility of any one topic.
    let permissions = Permissions {
        global: GlobalPermissions {
            read_streams: true,
            read_topics: true,
            ..GlobalPermissions::default()
        },
        streams: None,
    };
    client
        .create_user(
            METRICS_USER,
            METRICS_PASSWORD,
            UserStatus::Active,
            Some(permissions),
        )
        .await
        .expect("create metrics user");
    let root = HttpClient::login_root(harness).await;
    let user = root.login(METRICS_USER, METRICS_PASSWORD).await;

    // A privileged scrape first, so a denied scrape that re-encoded the
    // previous snapshot would leak these series.
    let privileged_before = scrape_as(&root).await;
    for name in TOPIC_SERIES {
        assert!(
            topic_series(&privileged_before, name).is_some(),
            "root must see the {name} series:\n{privileged_before}"
        );
    }

    let restricted = scrape_as(&user).await;
    assert_eq!(
        gauge(&restricted, "streams"),
        Some(1),
        "aggregate gauges stay login-only:\n{restricted}"
    );
    assert_eq!(
        gauge(&restricted, "topics"),
        Some(1),
        "aggregate gauges stay login-only:\n{restricted}"
    );
    for name in TOPIC_SERIES {
        assert!(
            !restricted
                .lines()
                .any(|line| line.starts_with(&format!("{name}{{"))),
            "a user without server read must not see any {name} series:\n{restricted}"
        );
    }

    let privileged_after = scrape_as(&root).await;
    for name in TOPIC_SERIES {
        assert!(
            topic_series(&privileged_after, name).is_some(),
            "a denied scrape must not empty the next privileged one ({name}):\n{privileged_after}"
        );
    }
}

async fn create_topic(client: &IggyClient, max_topic_size: MaxTopicSize) {
    client.create_stream(STREAM).await.expect("create stream");
    let stream_id = Identifier::from_str_value(STREAM).expect("stream identifier");
    client
        .create_topic(
            &stream_id,
            TOPIC,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                max_topic_size: Some(max_topic_size),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
}

async fn scrape(harness: &TestHarness) -> String {
    scrape_as(&HttpClient::login_root(harness).await).await
}

async fn scrape_as(session: &HttpClient) -> String {
    let response = session.get("/metrics").await;
    assert!(
        response.status().is_success(),
        "/metrics scrape failed: {}",
        response.status()
    );
    response.text().await.expect("metrics text")
}
