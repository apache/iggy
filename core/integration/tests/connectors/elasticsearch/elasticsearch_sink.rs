/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::connectors::create_test_messages;
use crate::connectors::fixtures::{ElasticsearchFixture, ElasticsearchOps};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_binary_protocol::MessageClient;
use iggy_common::Identifier;
use integration::harness::seeds;
use integration::iggy_harness;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/elasticsearch/config.toml")),
    seed = seeds::connector_stream
)]
async fn given_json_messages_should_index_documents(
    harness: &TestHarness,
    fixture: ElasticsearchFixture,
) {
    let client = harness.client();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let message_count = 10;
    let test_messages = create_test_messages(message_count);
    let payloads: Vec<Bytes> = test_messages
        .iter()
        .map(|m| Bytes::from(serde_json::to_vec(m).expect("serialize")))
        .collect();

    let mut messages: Vec<IggyMessage> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| {
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(p.clone())
                .build()
                .expect("build message")
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send messages");

    let docs = fixture
        .fetch_documents_with_count(seeds::names::TOPIC, message_count)
        .await
        .expect("fetch documents");

    assert_eq!(docs.len(), message_count);
    for (doc, original) in docs.iter().zip(test_messages.iter()) {
        assert_eq!(doc["id"].as_u64(), Some(original.id));
        assert_eq!(doc["name"].as_str(), Some(original.name.as_str()));
        assert_eq!(doc["count"].as_u64(), Some(original.count as u64));
        assert!(doc["_iggy_offset"].is_number());
        assert_eq!(doc["_iggy_stream"].as_str(), Some(seeds::names::STREAM));
        assert_eq!(doc["_iggy_topic"].as_str(), Some(seeds::names::TOPIC));
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/elasticsearch/config.toml")),
    seed = seeds::connector_stream
)]
async fn given_bulk_messages_should_index_all(
    harness: &TestHarness,
    fixture: ElasticsearchFixture,
) {
    let client = harness.client();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let message_count = 500;
    let test_messages = create_test_messages(message_count);
    let payloads: Vec<Bytes> = test_messages
        .iter()
        .map(|m| Bytes::from(serde_json::to_vec(m).expect("serialize")))
        .collect();

    let mut messages: Vec<IggyMessage> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| {
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(p.clone())
                .build()
                .expect("build message")
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send messages");

    let docs = fixture
        .fetch_documents_with_count(seeds::names::TOPIC, message_count)
        .await
        .expect("fetch documents");

    assert_eq!(docs.len(), message_count);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/elasticsearch/config.toml")),
    seed = seeds::connector_stream
)]
async fn given_messages_should_have_iggy_metadata(
    harness: &TestHarness,
    fixture: ElasticsearchFixture,
) {
    let client = harness.client();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let message_count = 3;
    let test_messages = create_test_messages(message_count);
    let payloads: Vec<Bytes> = test_messages
        .iter()
        .map(|m| Bytes::from(serde_json::to_vec(m).expect("serialize")))
        .collect();

    let mut messages: Vec<IggyMessage> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| {
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(p.clone())
                .build()
                .expect("build message")
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send messages");

    let docs = fixture
        .fetch_documents_with_count(seeds::names::TOPIC, message_count)
        .await
        .expect("fetch documents");

    assert_eq!(docs.len(), message_count);
    for doc in docs.iter() {
        assert!(doc.get("_iggy_offset").is_some());
        assert!(doc.get("_iggy_stream").is_some());
        assert!(doc.get("_iggy_topic").is_some());
        assert!(doc.get("_iggy_partition").is_some());
        assert!(doc.get("_iggy_timestamp").is_some());
    }
}
