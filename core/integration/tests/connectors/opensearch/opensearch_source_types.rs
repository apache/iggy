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

use super::{
    TEST_MESSAGE_COUNT, expected_ids, json_payloads, poll_next, read_from_start,
    sorted_document_ids,
};
use crate::connectors::fixtures::{
    OpenSearchSourcePreCreatedFixture, OpenSearchSourceTypedFieldsFixture,
};
use iggy_common::{HeaderKey, IggyTimestamp};
use integration::harness::seeds;
use integration::iggy_harness;

const DOCUMENT_ID_HEADER: &str = "opensearch.document_id";
const INDEX_HEADER: &str = "opensearch.index";
const ORIGIN_TIMESTAMP_TOLERANCE_MICROS: u64 = 3_600_000_000;

// Mirrors the connector's id derivation on purpose: the id is a contract consumers dedupe on.
fn expected_message_id(index: &str, document_id: &str) -> u128 {
    let mut hasher = blake3::Hasher::new();
    for part in [index, document_id] {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    let mut id = [0u8; 16];
    hasher.finalize_xof().fill(&mut id);
    u128::from_le_bytes(id)
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_document_in_index_when_connector_polls_should_publish_payload_id_headers_and_timestamp(
    harness: &TestHarness,
    fixture: OpenSearchSourcePreCreatedFixture,
) {
    fixture
        .insert_document(1, "identity_doc", 42)
        .await
        .expect("insert document");
    fixture.refresh_index().await.expect("refresh index");

    let client = harness.root_client().await.unwrap();
    let messages = poll_next(&client, "single_document_consumer", 1).await;

    assert_eq!(messages.len(), 1, "expected one message");
    let message = &messages[0];

    let payload = &json_payloads(&messages)[0];
    assert_eq!(payload["id"], 1);
    assert_eq!(payload["name"], "identity_doc");
    assert_eq!(payload["value"], 42);
    assert!(
        payload.get("timestamp").is_some(),
        "missing timestamp field"
    );

    assert_eq!(
        message.header.id,
        expected_message_id(fixture.index_name(), "1")
    );
    let now_micros = IggyTimestamp::now().as_micros();
    assert!(
        message.header.origin_timestamp.abs_diff(now_micros) < ORIGIN_TIMESTAMP_TOLERANCE_MICROS,
        "origin_timestamp {} must be the document timestamp in microseconds (now: {now_micros})",
        message.header.origin_timestamp
    );

    let headers = message
        .user_headers_map()
        .expect("headers decode")
        .expect("document id header present");
    let document_id = headers
        .get(&HeaderKey::try_from(DOCUMENT_ID_HEADER).unwrap())
        .expect("document id header");
    assert_eq!(document_id.as_str().expect("string header"), "1");
    let index = headers
        .get(&HeaderKey::try_from(INDEX_HEADER).unwrap())
        .expect("index header");
    assert_eq!(index.as_str().expect("string header"), fixture.index_name());
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_typed_fields_document_when_connector_polls_should_round_trip_payload(
    harness: &TestHarness,
    fixture: OpenSearchSourceTypedFieldsFixture,
) {
    fixture
        .insert_typed_sample_document()
        .await
        .expect("insert typed document");

    let client = harness.root_client().await.unwrap();
    let messages = poll_next(&client, "typed_fields_consumer", 1).await;
    let payloads = json_payloads(&messages);

    assert_eq!(payloads.len(), 1, "expected one typed message");
    let record = &payloads[0];
    assert_eq!(record["title"], "OpenSearch typed field coverage");
    assert_eq!(record["status"], "active");
    assert_eq!(record["count"].as_i64(), Some(9_223_372_036_854_775_807));
    assert!((record["score"].as_f64().unwrap() - 98.6).abs() < 0.01);
    assert!((record["ratio"].as_f64().unwrap() - 0.125).abs() < f64::EPSILON);
    assert_eq!(record["active"], true);
    assert_eq!(record["client_ip"], "192.168.1.42");
    assert_eq!(record["location"]["lat"], 40.12);
    assert_eq!(record["location"]["lon"], -71.34);
    assert!(record["tags"].is_array());
    assert!(record["optional_note"].is_null());
    assert!(record.get("timestamp").is_some());
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn document_without_timestamp_source_keeps_delivering_later_documents(
    harness: &TestHarness,
    fixture: OpenSearchSourcePreCreatedFixture,
) {
    fixture
        .insert_document(1, "dated_doc", 10)
        .await
        .expect("insert dated document");
    fixture
        .insert_document_without_timestamp(2, "undated_doc")
        .await
        .expect("insert undated document");
    fixture.refresh_index().await.expect("refresh index");

    let client = harness.root_client().await.unwrap();
    let first = poll_next(&client, "undated_consumer", 2).await;
    assert_eq!(
        sorted_document_ids(&json_payloads(&first)),
        expected_ids(1, 2),
        "a document without the timestamp field is still published"
    );

    fixture
        .insert_documents_from(3, 1)
        .await
        .expect("insert later document");

    let later = poll_next(&client, "undated_consumer", 1).await;
    assert_eq!(
        sorted_document_ids(&json_payloads(&later)),
        expected_ids(3, 1),
        "a document without the timestamp field must not park the cursor ahead of later ones"
    );

    let on_stream = read_from_start(&client, "undated_audit", 3).await;
    assert_eq!(
        sorted_document_ids(&json_payloads(&on_stream)),
        expected_ids(1, 3)
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opensearch/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_documents_added_after_first_poll_when_connector_runs_should_deliver_only_new_ones(
    harness: &TestHarness,
    fixture: OpenSearchSourcePreCreatedFixture,
) {
    fixture
        .insert_documents(TEST_MESSAGE_COUNT)
        .await
        .expect("insert first batch");

    let client = harness.root_client().await.unwrap();
    let first_batch = poll_next(&client, "cursor_consumer", TEST_MESSAGE_COUNT).await;
    assert_eq!(
        sorted_document_ids(&json_payloads(&first_batch)),
        expected_ids(1, TEST_MESSAGE_COUNT)
    );

    fixture
        .insert_documents_from(TEST_MESSAGE_COUNT + 1, TEST_MESSAGE_COUNT)
        .await
        .expect("insert second batch");

    let second_batch = poll_next(&client, "cursor_consumer", TEST_MESSAGE_COUNT).await;
    assert_eq!(
        sorted_document_ids(&json_payloads(&second_batch)),
        expected_ids(TEST_MESSAGE_COUNT + 1, TEST_MESSAGE_COUNT),
        "the consumer must see only the documents added after its first poll"
    );

    let on_stream = read_from_start(&client, "no_dup_audit", TEST_MESSAGE_COUNT * 2).await;
    assert_eq!(
        sorted_document_ids(&json_payloads(&on_stream)),
        expected_ids(1, TEST_MESSAGE_COUNT * 2),
        "a cursor reset would have re-published the first batch"
    );
}
