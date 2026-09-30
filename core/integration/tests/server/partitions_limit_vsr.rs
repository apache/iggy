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

//! The node-wide `[metadata] partitions_max` cap. A create-topic or
//! create-partitions that would push the committed partition count past it
//! denies typed with `PartitionsLimitReached` over TCP and HTTP. Creates up to
//! the cap succeed, and deleting partitions frees room again.

use iggy::prelude::*;
use iggy_common::create_topic::CreateTopic;
use integration::iggy_harness;
use reqwest::StatusCode;
use serde_json::json;

use super::http_client::HttpClient;

const STREAM_NAME: &str = "partitions-limit-stream";

async fn create_topic(
    client: &IggyClient,
    stream_id: &Identifier,
    name: &str,
    partitions_count: u32,
) -> Result<TopicDetails, IggyError> {
    client
        .create_topic(
            stream_id,
            name,
            &TopicCreateOptions {
                partitions_count: Some(partitions_count),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
}

fn assert_limit_reached<T: std::fmt::Debug>(result: &Result<T, IggyError>, context: &str) {
    let limit_reached = IggyError::PartitionsLimitReached.as_code();
    assert!(
        matches!(result, Err(error) if error.as_code() == limit_reached),
        "{context} must deny with PartitionsLimitReached, got {result:?}"
    );
}

#[iggy_harness(
    cluster_nodes = 1,
    server(metadata.partitions_max = "4")
)]
async fn given_partitions_cap_when_creating_past_it_should_reject_typed(harness: &TestHarness) {
    let client = harness.tcp_root_client().await.expect("TCP root client");
    client
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream");
    let stream_id = Identifier::named(STREAM_NAME).expect("stream identifier");
    let topic_id = Identifier::named("first").expect("topic identifier");

    create_topic(&client, &stream_id, "first", 3)
        .await
        .expect("3 of 4 partitions fit under the cap");
    assert_limit_reached(
        &create_topic(&client, &stream_id, "second", 2).await,
        "a topic of 2 partitions on top of 3",
    );
    assert_limit_reached(
        &client.create_partitions(&stream_id, &topic_id, 2).await,
        "adding 2 partitions on top of 3",
    );
    client
        .create_partitions(&stream_id, &topic_id, 1)
        .await
        .expect("the cap itself is admissible");
    assert_limit_reached(
        &create_topic(&client, &stream_id, "second", 1).await,
        "a topic of 1 partition at the cap",
    );

    let http = HttpClient::login_root(harness).await;
    let topic_body = serde_json::to_value(CreateTopic {
        name: "second".to_owned(),
        partitions_count: 1,
        ..CreateTopic::default()
    })
    .expect("serialize create topic");
    for (path, body) in [
        (format!("/streams/{STREAM_NAME}/topics"), topic_body),
        (
            format!("/streams/{STREAM_NAME}/topics/first/partitions"),
            json!({ "partitions_count": 1 }),
        ),
    ] {
        let response = http.post_json(&path, &body).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "POST {path}");
        let body: serde_json::Value = response.json().await.expect("HTTP error body");
        assert_eq!(body["id"], 2022, "POST {path}");
        assert_eq!(body["code"], "partitions_limit_reached", "POST {path}");
    }

    client
        .delete_partitions(&stream_id, &topic_id, 2)
        .await
        .expect("delete 2 partitions");
    create_topic(&client, &stream_id, "second", 2)
        .await
        .expect("deleted partitions free room under the cap");
}
