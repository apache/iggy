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

use super::TEST_MESSAGE_COUNT;
use crate::connectors::create_test_messages;
use crate::connectors::fixtures::ElasticsearchSinkFixture;
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use integration::harness::seeds;
use integration::iggy_harness;

/// A `proto_convert` transform with no descriptor falls back to proto text, so
/// the batch is tagged `Schema::Proto` and the sink is handed `Payload::Proto`
/// holding the JSON it was given. The sink indexes that as the document it
/// holds, field by field, the same way the bytes indexed when the batch was
/// tagged `json`. Indexing it as one opaque `text` field would pass while
/// losing every original field, so the assertions read the fields back.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/elasticsearch/proto_text.toml")),
    seed = seeds::connector_stream
)]
async fn given_a_proto_convert_transform_when_the_sink_consumes_should_index_the_document(
    harness: &TestHarness,
    fixture: ElasticsearchSinkFixture,
) {
    let client = harness.root_client().await.unwrap();

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let messages_data = create_test_messages(TEST_MESSAGE_COUNT);
    let mut messages: Vec<IggyMessage> = messages_data
        .iter()
        .enumerate()
        .map(|(i, msg)| {
            let payload = serde_json::to_vec(msg).expect("Failed to serialize message");
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(payload))
                .build()
                .expect("Failed to build message")
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
        .expect("Failed to send messages");

    fixture
        .wait_for_documents(TEST_MESSAGE_COUNT)
        .await
        .expect("the proto text batch must be indexed, not dropped");

    fixture
        .refresh_index()
        .await
        .expect("Failed to refresh index");

    let search_result = fixture
        .search_documents()
        .await
        .expect("Failed to search documents");

    assert_eq!(
        search_result.hits.total.value, TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} documents in Elasticsearch"
    );

    for (i, hit) in search_result.hits.hits.iter().enumerate() {
        assert!(
            hit.source.get("data_type").is_none(),
            "a proto payload holding JSON must index as a document, not a text blob: {}",
            hit.source
        );
        let name = hit
            .source
            .get("name")
            .and_then(|value| value.as_str())
            .unwrap_or_else(|| {
                panic!(
                    "the original fields must survive the transform: {}",
                    hit.source
                )
            });
        assert!(
            name.starts_with("user_"),
            "unexpected name at hit {i}: {}",
            hit.source
        );
        assert!(
            hit.source
                .get("amount")
                .and_then(|value| value.as_f64())
                .is_some(),
            "amount must be indexed as a number, got {}",
            hit.source
        );
        assert!(
            hit.source.get("_iggy_offset").is_some(),
            "metadata must still be injected into the document, got {}",
            hit.source
        );
    }
}
