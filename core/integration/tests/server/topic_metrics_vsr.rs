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
use integration::iggy_harness;

use super::http_client::HttpClient;

const STREAM: &str = "metrics-stream";
const TOPIC: &str = "metrics-topic";

/// The value of the `name` series labelled with this test's stream and topic.
fn topic_series(exposition: &str, name: &str) -> Option<u64> {
    let stream_label = format!("stream=\"{STREAM}\"");
    let topic_label = format!("topic=\"{TOPIC}\"");
    exposition
        .lines()
        .filter(|line| line.starts_with(&format!("{name}{{")))
        .find(|line| line.contains(&stream_label) && line.contains(&topic_label))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse().ok())
}

#[iggy_harness(cluster_nodes = 1)]
#[ignore = "#4473: /metrics has no per-topic series yet"]
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

    let http = HttpClient::login_root(harness).await;
    let exposition = http
        .get("/metrics")
        .await
        .text()
        .await
        .expect("metrics text");

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
}
