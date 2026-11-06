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

use crate::connectors::fixtures::{OpenSearchOps, OpenSearchSinkFixture};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::{Identifier, MessageClient};
use integration::harness::seeds;
use integration::iggy_harness;

const MESSAGE_COUNT: usize = 3;

/// Descriptor-less `proto_convert` hands the sink `Payload::Proto` holding JSON, which must index field by field.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/proto_text.toml")),
    seed = seeds::connector_multi_topic_stream
)]
async fn given_a_proto_convert_transform_when_the_sink_consumes_should_index_the_document(
    harness: &TestHarness,
    fixture: OpenSearchSinkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let mut messages: Vec<IggyMessage> = (0..MESSAGE_COUNT)
        .map(|index| {
            let payload = serde_json::json!({
                "order_id": format!("P-{index}"),
                "name": format!("user_{index}"),
                "amount": 2.5,
            });
            IggyMessage::builder()
                .id((index + 1) as u128)
                .payload(Bytes::from(
                    serde_json::to_vec(&payload).expect("serialize"),
                ))
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

    fixture
        .wait_for_document_count(MESSAGE_COUNT as u64)
        .await
        .expect("the proto text batch was not indexed");

    let documents = fixture
        .search_all(fixture.index())
        .await
        .expect("search index");
    let hits = documents["hits"]["hits"].as_array().expect("hits array");
    assert_eq!(hits.len(), MESSAGE_COUNT);

    for hit in hits {
        let source = &hit["_source"];
        assert!(
            source.get("data_type").is_none(),
            "a proto payload holding JSON must index as a document, not a text blob: {source}"
        );
        assert!(
            source["name"]
                .as_str()
                .is_some_and(|name| name.starts_with("user_")),
            "the original fields must survive the transform: {source}"
        );
        assert_eq!(source["amount"], 2.5);
        assert_eq!(hit["_id"], source["order_id"]);
        assert!(
            source.get("iggy_offset").is_some(),
            "metadata must still be injected into the document: {source}"
        );
    }
}
