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
use base64::{Engine as _, engine::general_purpose};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::{HeaderKey, HeaderValue, Identifier, MessageClient};
use integration::harness::seeds;
use integration::iggy_harness;
use std::collections::BTreeMap;

// Covers the natural-key path: `send_messages` always assigns fresh offsets,
// so generated-ID replay idempotency is unit-tested instead.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/sink.toml")),
    seed = seeds::connector_multi_topic_stream
)]
async fn given_json_messages_when_sink_consumes_should_index_documents_and_upsert_by_natural_key(
    harness: &TestHarness,
    fixture: OpenSearchSinkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let send = |payload: serde_json::Value, message_id: u128| {
        let stream_id = stream_id.clone();
        let topic_id = topic_id.clone();
        let client = &client;
        async move {
            let mut messages = vec![
                IggyMessage::builder()
                    .id(message_id)
                    .payload(Bytes::from(
                        serde_json::to_vec(&payload).expect("serialize"),
                    ))
                    .build()
                    .expect("build message"),
            ];
            client
                .send_messages(
                    &stream_id,
                    &topic_id,
                    &Partitioning::partition_id(0),
                    &mut messages,
                )
                .await
                .expect("send message");
        }
    };

    send(
        serde_json::json!({
            "order_id": "A-1",
            "name": "first",
            "nested": { "deep": { "value": 1 } },
            "tags": ["rust", "iggy"],
        }),
        1,
    )
    .await;
    send(
        serde_json::json!({ "order_id": "A-2", "name": "second" }),
        2,
    )
    .await;

    let count_after_first = fixture
        .wait_for_document_count(2)
        .await
        .expect("wait for documents after first send");
    assert_eq!(count_after_first, 2);

    let documents = fixture
        .search_all(fixture.index())
        .await
        .expect("search index");
    let hits = documents["hits"]["hits"]
        .as_array()
        .expect("hits array")
        .clone();
    let first_hit = hits
        .iter()
        .find(|hit| hit["_source"]["order_id"] == "A-1")
        .expect("order A-1 indexed");
    assert_eq!(first_hit["_source"]["name"], "first");
    assert_eq!(first_hit["_source"]["nested"]["deep"]["value"], 1);
    assert_eq!(
        first_hit["_source"]["tags"],
        serde_json::json!(["rust", "iggy"])
    );
    assert_eq!(first_hit["_id"], "A-1");
    assert!(
        hits.iter()
            .all(|hit| hit["_source"]["iggy_stream"] == seeds::names::STREAM)
    );

    // Resend order A-1 at a new offset: the natural key must upsert even when
    // the offset differs.
    send(
        serde_json::json!({ "order_id": "A-1", "name": "first-updated" }),
        3,
    )
    .await;

    // Poll on the actual updated content rather than a fixed sleep.
    // `refresh=wait_for` blocks the sink's bulk response until the *next*
    // scheduled OpenSearch refresh, not an immediate one, so the connector's
    // own indexing latency for this batch can approach a full refresh
    // interval (~1s default) even though the config is applied correctly.
    // The document count alone cannot detect this: an upsert leaves the
    // count at 2 whether or not the new content has landed yet, so only a
    // content-based poll can distinguish "still stale" from "genuinely
    // failed to upsert."
    let updated_first_hit = wait_for_updated_name(&fixture, "first-updated").await;
    assert_eq!(updated_first_hit["_source"]["order_id"], "A-1");

    let count_after_update = fixture
        .count_documents(fixture.index())
        .await
        .expect("count after natural-key update");
    assert_eq!(
        count_after_update, 2,
        "resending the same order_id at a new offset must upsert, not duplicate"
    );

    // A message carrying both a string and a raw binary header, proving
    // iggy_headers is actually indexed against a live server. The conversion
    // itself (headers_to_json) is unit-tested in isolation; only a real
    // send_messages round trip proves the wire-decoded headers survive all
    // the way into the indexed document.
    let string_header_key = HeaderKey::try_from("x-correlation-id").expect("header key");
    let string_header_value = HeaderValue::try_from("abc-123").expect("header value");
    let raw_header_key = HeaderKey::try_from("x-raw").expect("header key");
    let raw_header_value = HeaderValue::try_from([1u8, 2, 3].as_slice()).expect("header value");
    let user_headers = BTreeMap::from([
        (string_header_key, string_header_value),
        (raw_header_key, raw_header_value),
    ]);

    let mut messages_with_headers = vec![
        IggyMessage::builder()
            .id(4)
            .payload(Bytes::from(
                serde_json::to_vec(&serde_json::json!({ "order_id": "A-3", "name": "third" }))
                    .expect("serialize"),
            ))
            .user_headers(user_headers)
            .build()
            .expect("build message"),
    ];
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages_with_headers,
        )
        .await
        .expect("send message with headers");

    let count_after_headers = fixture
        .wait_for_document_count(3)
        .await
        .expect("wait for documents after header send");
    assert_eq!(count_after_headers, 3);

    let documents = fixture
        .search_all(fixture.index())
        .await
        .expect("search index");
    let header_hit = documents["hits"]["hits"]
        .as_array()
        .expect("hits array")
        .iter()
        .find(|hit| hit["_source"]["order_id"] == "A-3")
        .expect("order A-3 indexed")
        .clone();
    // Both header kinds share the {data, data_encoding} shape. Asserted against
    // a live server because a divergent shape per kind is only rejected once
    // OpenSearch has pinned the dynamic mapping for iggy_headers.<key>.
    let headers = &header_hit["_source"]["iggy_headers"];
    assert_eq!(headers["x-correlation-id"]["data"], "abc-123");
    assert_eq!(headers["x-correlation-id"]["data_encoding"], "utf8");
    assert_eq!(
        headers["x-raw"]["data"],
        general_purpose::STANDARD.encode([1u8, 2, 3])
    );
    assert_eq!(headers["x-raw"]["data_encoding"], "base64");

    // Payload::Raw coverage: stream 1 subscribes to TOPIC_2 under the `raw`
    // schema, proving `document_from_raw`'s two branches against a live
    // server instead of only the hand-built bytes the unit tests use.
    let topic_2_id: Identifier = seeds::names::TOPIC_2.try_into().unwrap();

    // Raw bytes that happen to parse as JSON: document_from_raw hands the
    // parsed object to document_from_json exactly like the Payload::Json
    // path, so the natural-key document ID must still resolve from
    // `order_id` even though the message arrived through the raw decoder.
    let mut raw_json_message = vec![
        IggyMessage::builder()
            .id(5)
            .payload(Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "order_id": "R-1",
                    "name": "raw-json-object",
                }))
                .expect("serialize"),
            ))
            .build()
            .expect("build message"),
    ];
    client
        .send_messages(
            &stream_id,
            &topic_2_id,
            &Partitioning::partition_id(0),
            &mut raw_json_message,
        )
        .await
        .expect("send raw JSON message");

    // Raw bytes that are not valid JSON: the simd_json parse fails, falling
    // back to base64-encoding the bytes verbatim. This message has no
    // `order_id` field, so it also exercises the generated-document-ID
    // fallback, which previously had no live-server coverage either.
    let non_json_bytes: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF];
    let mut raw_binary_message = vec![
        IggyMessage::builder()
            .id(6)
            .payload(Bytes::from(non_json_bytes.clone()))
            .build()
            .expect("build message"),
    ];
    client
        .send_messages(
            &stream_id,
            &topic_2_id,
            &Partitioning::partition_id(0),
            &mut raw_binary_message,
        )
        .await
        .expect("send raw binary message");

    let count_after_raw = fixture
        .wait_for_document_count(5)
        .await
        .expect("wait for documents after raw sends");
    assert_eq!(count_after_raw, 5);

    let documents = fixture
        .search_all(fixture.index())
        .await
        .expect("search index");
    let hits = documents["hits"]["hits"]
        .as_array()
        .expect("hits array")
        .clone();

    let raw_json_hit = hits
        .iter()
        .find(|hit| hit["_source"]["order_id"] == "R-1")
        .expect("raw JSON document indexed");
    assert_eq!(raw_json_hit["_source"]["name"], "raw-json-object");
    assert_eq!(
        raw_json_hit["_id"], "R-1",
        "raw bytes that parse as JSON must still resolve the natural key"
    );

    let raw_binary_hit = hits
        .iter()
        .find(|hit| {
            hit["_source"]["data_type"] == "raw" && hit["_source"]["data_encoding"] == "base64"
        })
        .expect("raw binary document indexed via base64 fallback");
    assert_eq!(
        raw_binary_hit["_source"]["data"],
        general_purpose::STANDARD.encode(&non_json_bytes)
    );
    assert!(
        !raw_binary_hit["_id"]
            .as_str()
            .unwrap_or_default()
            .is_empty(),
        "fallback document must still get a generated ID"
    );
}

async fn wait_for_updated_name(
    fixture: &OpenSearchSinkFixture,
    expected_name: &str,
) -> serde_json::Value {
    const POLL_ATTEMPTS: usize = 40;
    const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

    for _ in 0..POLL_ATTEMPTS {
        if let Ok(documents) = fixture.search_all(fixture.index()).await
            && let Some(hits) = documents["hits"]["hits"].as_array()
            && let Some(hit) = hits.iter().find(|hit| hit["_source"]["order_id"] == "A-1")
            && hit["_source"]["name"] == expected_name
        {
            return hit.clone();
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    panic!(
        "order A-1 did not show name={expected_name:?} within {} attempts",
        POLL_ATTEMPTS
    );
}
