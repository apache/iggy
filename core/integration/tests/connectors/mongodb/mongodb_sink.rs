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

use super::{POLL_ATTEMPTS, POLL_INTERVAL_MS, TEST_MESSAGE_COUNT};
use crate::connectors::fixtures::{
    MongoDbOps, MongoDbSinkAutoCreateFixture, MongoDbSinkBatchFixture, MongoDbSinkFailpointFixture,
    MongoDbSinkFixture, MongoDbSinkJsonFixture, MongoDbSinkWriteConcernFixture,
};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use iggy_connector_sdk::api::{ConnectorRuntimeStats, ConnectorStatus, SinkInfoResponse};
use integration::harness::seeds;
use integration::iggy_harness;
use mongodb::bson::{Document, doc};
use reqwest::Client as HttpClient;
use std::time::Duration;
use tokio::time::sleep;

const DEFAULT_TEST_DATABASE: &str = "iggy_test";
const DEFAULT_SINK_COLLECTION: &str = "iggy_messages";
const LARGE_BATCH_COUNT: usize = 50;
const MONGODB_SINK_KEY: &str = "mongodb";

fn build_expected_document_id(message_id: u128) -> String {
    build_expected_document_id_for_topic(seeds::names::TOPIC, message_id)
}

fn build_expected_document_id_for_topic(topic_name: &str, message_id: u128) -> String {
    format!("{}:{}:{}:{message_id}", seeds::names::STREAM, topic_name, 0)
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mongodb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn json_messages_sink_to_mongodb(harness: &TestHarness, fixture: MongoDbSinkJsonFixture) {
    let client = harness.root_client().await.unwrap();
    let mongo_client = fixture
        .create_client()
        .await
        .expect("Failed to create MongoDB client");

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let json_payloads: Vec<serde_json::Value> = vec![
        serde_json::json!({"name": "Alice", "age": 30}),
        serde_json::json!({"name": "Bob", "score": 99}),
        serde_json::json!({"name": "Carol", "active": true}),
    ];

    let mut messages: Vec<IggyMessage> = json_payloads
        .iter()
        .enumerate()
        .map(|(i, payload)| {
            let bytes = serde_json::to_vec(payload).expect("Failed to serialize");
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(bytes))
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

    // Wait for connector to consume and insert into MongoDB.
    let docs = fixture
        .wait_for_documents(&mongo_client, DEFAULT_SINK_COLLECTION, TEST_MESSAGE_COUNT)
        .await
        .expect("Documents did not appear in MongoDB");

    assert_eq!(docs.len(), TEST_MESSAGE_COUNT);

    // Verify metadata fields are present on first document.
    let first = &docs[0];
    assert!(
        first.contains_key("iggy_offset"),
        "Expected iggy_offset field"
    );
    assert!(
        first.contains_key("iggy_stream"),
        "Expected iggy_stream field"
    );
    assert!(
        first.contains_key("iggy_topic"),
        "Expected iggy_topic field"
    );
    assert!(
        first.contains_key("iggy_partition_id"),
        "Expected iggy_partition_id field"
    );
    assert!(
        first.contains_key("iggy_timestamp"),
        "Expected iggy_timestamp field"
    );

    // Verify offset sequence is contiguous.
    for (i, doc) in docs.iter().enumerate() {
        let offset = doc.get_i64("iggy_offset").expect("iggy_offset missing");
        assert_eq!(offset, i as i64, "Offset mismatch at document {i}");
    }

    // Verify payload is stored as a BSON Document (queryable) not Binary.
    let payload = first.get("payload").expect("payload field missing");
    assert!(
        matches!(payload, mongodb::bson::Bson::Document(_)),
        "Expected payload to be BSON Document for json format, got: {payload:?}"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mongodb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn binary_messages_sink_as_bson_binary(harness: &TestHarness, fixture: MongoDbSinkFixture) {
    let client = harness.root_client().await.unwrap();
    let mongo_client = fixture
        .create_client()
        .await
        .expect("Failed to create MongoDB client");

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let raw_payloads: Vec<Vec<u8>> = vec![
        b"plain text message".to_vec(),
        vec![0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD],
        vec![0xDE, 0xAD, 0xBE, 0xEF],
    ];

    let mut messages: Vec<IggyMessage> = raw_payloads
        .iter()
        .enumerate()
        .map(|(i, payload)| {
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(payload.clone()))
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

    let docs = fixture
        .wait_for_documents(&mongo_client, DEFAULT_SINK_COLLECTION, raw_payloads.len())
        .await
        .expect("Documents did not appear");

    assert_eq!(docs.len(), raw_payloads.len());

    for (i, doc) in docs.iter().enumerate() {
        let payload = doc.get("payload").expect("payload field missing");
        match payload {
            mongodb::bson::Bson::Binary(bin) => {
                assert_eq!(
                    bin.subtype,
                    mongodb::bson::spec::BinarySubtype::Generic,
                    "Expected Generic subtype at doc {i}"
                );
                assert_eq!(
                    bin.bytes, raw_payloads[i],
                    "Payload bytes mismatch at doc {i}"
                );
            }
            other => panic!("Expected Binary, got {other:?} at doc {i}"),
        }
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mongodb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn large_batch_processed_correctly(harness: &TestHarness, fixture: MongoDbSinkBatchFixture) {
    let client = harness.root_client().await.unwrap();
    let mongo_client = fixture
        .create_client()
        .await
        .expect("Failed to create MongoDB client");

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let mut messages: Vec<IggyMessage> = (0..LARGE_BATCH_COUNT)
        .map(|i| {
            let payload =
                serde_json::to_vec(&serde_json::json!({"idx": i})).expect("Failed to serialize");
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

    let docs = fixture
        .wait_for_documents(&mongo_client, DEFAULT_SINK_COLLECTION, LARGE_BATCH_COUNT)
        .await
        .expect("Not all documents appeared");

    assert!(
        docs.len() >= LARGE_BATCH_COUNT,
        "Expected at least {LARGE_BATCH_COUNT} documents, got {}",
        docs.len()
    );

    // Verify offsets are contiguous (0..N).
    for (i, doc) in docs.iter().enumerate() {
        let offset = doc.get_i64("iggy_offset").expect("iggy_offset missing");
        assert_eq!(offset, i as i64, "Offset gap detected at position {i}");
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mongodb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn duplicate_key_is_idempotent_replay_not_sink_error(
    harness: &TestHarness,
    fixture: MongoDbSinkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = HttpClient::new();
    let mongo_client = fixture
        .create_client()
        .await
        .expect("Failed to create MongoDB client");

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let collection = mongo_client
        .database(DEFAULT_TEST_DATABASE)
        .collection::<Document>(DEFAULT_SINK_COLLECTION);

    let preseeded_document = doc! {
        "_id": build_expected_document_id(2),
        "seed_marker": "preseed-unchanged",
        "payload": "preseeded"
    };

    collection
        .insert_one(preseeded_document)
        .await
        .expect("Failed to pre-seed duplicate _id document");

    // Create deterministic insert set:
    // - "1" should be inserted
    // - "2" collides with pre-seeded _id
    // - "3" should be inserted because unordered insert_many continues past duplicates.
    let mut messages: Vec<IggyMessage> = vec![
        IggyMessage::builder()
            .id(1)
            .payload(Bytes::from_static(b"message-1"))
            .build()
            .expect("Failed to build message 1"),
        IggyMessage::builder()
            .id(2)
            .payload(Bytes::from_static(b"message-2"))
            .build()
            .expect("Failed to build message 2"),
        IggyMessage::builder()
            .id(3)
            .payload(Bytes::from_static(b"message-3"))
            .build()
            .expect("Failed to build message 3"),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");

    // Wait until the sink processes the batch and at least the first non-conflicting message appears.
    let mut first_message_inserted = false;
    for _ in 0..POLL_ATTEMPTS {
        if collection
            .find_one(doc! { "_id": build_expected_document_id(1) })
            .await
            .expect("Failed to query _id=1")
            .is_some()
        {
            first_message_inserted = true;
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(
        first_message_inserted,
        "Expected first non-conflicting message (_id=1) to be inserted"
    );

    let preseeded_after = collection
        .find_one(doc! { "_id": build_expected_document_id(2) })
        .await
        .expect("Failed to query pre-seeded document")
        .expect("Pre-seeded _id=2 document should still exist");

    assert_eq!(
        preseeded_after
            .get_str("seed_marker")
            .expect("seed_marker missing from pre-seeded document"),
        "preseed-unchanged",
        "Pre-seeded document must remain unchanged"
    );

    let duplicate_count = collection
        .count_documents(doc! { "_id": build_expected_document_id(2) })
        .await
        .expect("Failed to count _id=2 documents");
    assert_eq!(
        duplicate_count, 1,
        "Duplicate _id must not create extra documents"
    );

    let mut sink_error_reported = false;
    let mut processed_messages = u64::MAX;
    let mut sink_errors = 0_u64;
    let mut third_message_inserted = false;

    for _ in 0..POLL_ATTEMPTS {
        third_message_inserted = collection
            .find_one(doc! { "_id": build_expected_document_id(3) })
            .await
            .expect("Failed to query _id=3")
            .is_some();

        let sinks_response = http_client
            .get(format!("{}/sinks", api_address))
            .send()
            .await
            .expect("Failed to get sinks");
        assert_eq!(sinks_response.status(), 200);
        let sinks: Vec<SinkInfoResponse> = sinks_response.json().await.expect("Invalid sinks JSON");
        let sink = sinks
            .iter()
            .find(|s| s.key == MONGODB_SINK_KEY)
            .expect("MongoDB sink not found in /sinks response");
        sink_error_reported = sink.last_error.is_some();

        let stats_response = http_client
            .get(format!("{}/stats", api_address))
            .send()
            .await
            .expect("Failed to get stats");
        assert_eq!(stats_response.status(), 200);
        let stats: ConnectorRuntimeStats = stats_response.json().await.expect("Invalid stats JSON");
        let sink_stats = stats
            .connectors
            .iter()
            .find(|c| c.key == MONGODB_SINK_KEY)
            .expect("MongoDB sink stats not found");
        processed_messages = sink_stats
            .messages_processed
            .expect("messages_processed should be present for sink stats");
        sink_errors = sink_stats.errors;

        if third_message_inserted
            && !sink_error_reported
            && sink_errors == 0
            && processed_messages == 3
        {
            break;
        }

        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(
        third_message_inserted,
        "Message after duplicate conflict should still be inserted with unordered insert_many"
    );
    assert!(
        !sink_error_reported,
        "Duplicate key collision should be idempotent"
    );
    assert_eq!(
        sink_errors, 0,
        "Idempotent duplicate handling should not count as sink error"
    );
    assert_eq!(
        processed_messages, 3,
        "messages_processed should reflect the successful runtime batch processing count"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mongodb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn ordered_duplicate_partial_insert_has_exact_accounting(
    harness: &TestHarness,
    fixture: MongoDbSinkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = HttpClient::new();
    let mongo_client = fixture
        .create_client()
        .await
        .expect("Failed to create MongoDB client");

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let collection = mongo_client
        .database(DEFAULT_TEST_DATABASE)
        .collection::<Document>(DEFAULT_SINK_COLLECTION);

    collection
        .insert_one(doc! {
            "_id": build_expected_document_id(2),
            "seed_marker": "preseeded-stable",
            "payload": "preseeded"
        })
        .await
        .expect("Failed to pre-seed duplicate key baseline");

    let mut messages: Vec<IggyMessage> = vec![
        IggyMessage::builder()
            .id(1)
            .payload(Bytes::from_static(b"ordered-accounting-1"))
            .build()
            .expect("Failed to build message 1"),
        IggyMessage::builder()
            .id(2)
            .payload(Bytes::from_static(b"ordered-accounting-2"))
            .build()
            .expect("Failed to build message 2"),
        IggyMessage::builder()
            .id(3)
            .payload(Bytes::from_static(b"ordered-accounting-3"))
            .build()
            .expect("Failed to build message 3"),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send ordered duplicate batch");

    let mut id1_inserted = false;
    let mut id3_inserted = false;
    let mut sink_last_error = false;
    let mut sink_status = ConnectorStatus::Stopped;
    let mut processed_messages = u64::MAX;
    let mut sink_errors = 0_u64;

    for _ in 0..POLL_ATTEMPTS {
        id1_inserted = collection
            .find_one(doc! { "_id": build_expected_document_id(1) })
            .await
            .expect("Failed to query _id=1")
            .is_some();
        id3_inserted = collection
            .find_one(doc! { "_id": build_expected_document_id(3) })
            .await
            .expect("Failed to query _id=3")
            .is_some();

        let sinks_response = http_client
            .get(format!("{}/sinks", api_address))
            .send()
            .await
            .expect("Failed to fetch /sinks");
        assert_eq!(sinks_response.status(), 200);
        let sinks: Vec<SinkInfoResponse> = sinks_response.json().await.expect("Invalid sinks JSON");
        let sink = sinks
            .iter()
            .find(|s| s.key == MONGODB_SINK_KEY)
            .expect("MongoDB sink not found in /sinks");
        sink_last_error = sink.last_error.is_some();
        sink_status = sink.status;

        let stats_response = http_client
            .get(format!("{}/stats", api_address))
            .send()
            .await
            .expect("Failed to fetch /stats");
        assert_eq!(stats_response.status(), 200);
        let stats: ConnectorRuntimeStats = stats_response.json().await.expect("Invalid stats JSON");
        let sink_stats = stats
            .connectors
            .iter()
            .find(|c| c.key == MONGODB_SINK_KEY)
            .expect("MongoDB sink stats not found");
        processed_messages = sink_stats
            .messages_processed
            .expect("messages_processed must be present for sink stats");
        sink_errors = sink_stats.errors;

        if id1_inserted
            && id3_inserted
            && !sink_last_error
            && sink_status == ConnectorStatus::Running
        {
            break;
        }

        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(id1_inserted, "Prefix write (_id=1) must be inserted");
    assert!(
        id3_inserted,
        "Suffix write (_id=3) should be inserted with unordered duplicate handling"
    );

    let preseeded_after = collection
        .find_one(doc! { "_id": build_expected_document_id(2) })
        .await
        .expect("Failed to query pre-seeded _id=2")
        .expect("Pre-seeded _id=2 document must remain");
    assert_eq!(
        preseeded_after
            .get_str("seed_marker")
            .expect("seed_marker missing on pre-seeded document"),
        "preseeded-stable",
        "Pre-seeded duplicate target must remain unchanged"
    );

    let total_docs = collection
        .count_documents(doc! {})
        .await
        .expect("Failed to count total collection documents");
    assert_eq!(
        total_docs, 3,
        "Expected exact collection accounting (preseed + two successful inserts)"
    );

    let duplicate_docs = collection
        .count_documents(doc! { "_id": build_expected_document_id(2) })
        .await
        .expect("Failed to count duplicate _id entries");
    assert_eq!(duplicate_docs, 1, "Duplicate _id must remain single");

    assert!(
        !sink_last_error,
        "Duplicate collision should not produce sink last_error"
    );
    assert_eq!(
        sink_status,
        ConnectorStatus::Running,
        "Sink should remain Running on idempotent duplicate handling"
    );
    assert_eq!(
        sink_errors, 0,
        "Duplicate handling should not increment runtime errors"
    );
    assert_eq!(
        processed_messages, 3,
        "Idempotent duplicate handling should keep runtime batch processed count"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mongodb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn schema_validation_mid_batch_surfaces_hard_error_and_partial_prefix(
    harness: &TestHarness,
    fixture: MongoDbSinkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let mongo_client = fixture
        .create_client()
        .await
        .expect("Failed to create MongoDB client");

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let database = mongo_client.database(DEFAULT_TEST_DATABASE);
    let collection = database.collection::<Document>(DEFAULT_SINK_COLLECTION);

    // Enforce that only the first message in this batch is valid.
    // iggy_offset=0 passes; iggy_offset>=1 fails validation.
    database
        .run_command(doc! {
            "create": DEFAULT_SINK_COLLECTION,
            "validator": {
                "iggy_offset": { "$lt": 1 }
            },
            "validationLevel": "strict",
            "validationAction": "error"
        })
        .await
        .expect("Failed to create collection with schema validator");

    let mut messages: Vec<IggyMessage> = vec![
        IggyMessage::builder()
            .id(11)
            .payload(Bytes::from_static(b"schema-validation-1"))
            .build()
            .expect("Failed to build message 11"),
        IggyMessage::builder()
            .id(12)
            .payload(Bytes::from_static(b"schema-validation-2"))
            .build()
            .expect("Failed to build message 12"),
        IggyMessage::builder()
            .id(13)
            .payload(Bytes::from_static(b"schema-validation-3"))
            .build()
            .expect("Failed to build message 13"),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send schema-validation batch");

    let mut id11_inserted = false;
    let mut id12_inserted = true;
    let mut id13_inserted = true;

    for _ in 0..POLL_ATTEMPTS {
        id11_inserted = collection
            .find_one(doc! { "_id": build_expected_document_id(11) })
            .await
            .expect("Failed to query _id=11")
            .is_some();
        id12_inserted = collection
            .find_one(doc! { "_id": build_expected_document_id(12) })
            .await
            .expect("Failed to query _id=12")
            .is_some();
        id13_inserted = collection
            .find_one(doc! { "_id": build_expected_document_id(13) })
            .await
            .expect("Failed to query _id=13")
            .is_some();

        if id11_inserted && !id12_inserted && !id13_inserted {
            break;
        }

        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(
        id11_inserted,
        "Expected prefix message (_id=11) to be inserted before schema validation failure"
    );
    assert!(
        !id12_inserted,
        "Expected mid-batch message (_id=12) to fail schema validation and not insert"
    );
    assert!(
        !id13_inserted,
        "Expected suffix message (_id=13) to be skipped after ordered schema validation failure"
    );

    let total_docs = collection
        .count_documents(doc! {})
        .await
        .expect("Failed to count documents after schema validation test");
    assert_eq!(
        total_docs, 1,
        "Expected exact prefix-only write accounting under schema validation failure"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mongodb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn write_concern_timeout_does_not_report_full_success(
    harness: &TestHarness,
    fixture: MongoDbSinkWriteConcernFixture,
) {
    let client = harness.root_client().await.unwrap();
    let mongo_client = fixture
        .create_client()
        .await
        .expect("Failed to create MongoDB client");

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let collection = mongo_client
        .database(DEFAULT_TEST_DATABASE)
        .collection::<Document>(DEFAULT_SINK_COLLECTION);

    let mut messages: Vec<IggyMessage> = vec![
        IggyMessage::builder()
            .id(101)
            .payload(Bytes::from_static(b"wc-timeout-1"))
            .build()
            .expect("Failed to build message 101"),
        IggyMessage::builder()
            .id(102)
            .payload(Bytes::from_static(b"wc-timeout-2"))
            .build()
            .expect("Failed to build message 102"),
        IggyMessage::builder()
            .id(103)
            .payload(Bytes::from_static(b"wc-timeout-3"))
            .build()
            .expect("Failed to build message 103"),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send write concern timeout batch");

    let mut total_docs = 0_u64;
    let mut id101_inserted = false;
    let mut id102_inserted = false;
    let mut id103_inserted = false;

    for _ in 0..POLL_ATTEMPTS {
        total_docs = collection
            .count_documents(doc! {})
            .await
            .expect("Failed to count documents after write concern timeout");
        id101_inserted = collection
            .find_one(doc! { "_id": build_expected_document_id(101) })
            .await
            .expect("Failed to query _id=101")
            .is_some();
        id102_inserted = collection
            .find_one(doc! { "_id": build_expected_document_id(102) })
            .await
            .expect("Failed to query _id=102")
            .is_some();
        id103_inserted = collection
            .find_one(doc! { "_id": build_expected_document_id(103) })
            .await
            .expect("Failed to query _id=103")
            .is_some();

        if total_docs == 3 && id101_inserted && id102_inserted && id103_inserted {
            break;
        }

        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert_eq!(
        total_docs, 3,
        "write concern timeout should still leave exactly one persisted document per message"
    );
    assert!(
        id101_inserted && id102_inserted && id103_inserted,
        "write concern timeout should not lose the primary-side writes for this batch"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mongodb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn retryable_write_failover_keeps_single_doc_per_id(
    harness: &TestHarness,
    fixture: MongoDbSinkFailpointFixture,
) {
    let client = harness.root_client().await.unwrap();
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = HttpClient::new();
    let mongo_client = fixture
        .create_client()
        .await
        .expect("Failed to create MongoDB client");

    fixture
        .configure_fail_command_once(
            &mongo_client,
            doc! {
                "failCommands": ["insert"],
                "closeConnection": true
            },
        )
        .await
        .expect("Failed to configure failpoint for retryable failover simulation");

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let collection = mongo_client
        .database(DEFAULT_TEST_DATABASE)
        .collection::<Document>(DEFAULT_SINK_COLLECTION);

    let mut messages: Vec<IggyMessage> = vec![
        IggyMessage::builder()
            .id(201)
            .payload(Bytes::from_static(b"retryable-failover-1"))
            .build()
            .expect("Failed to build message 201"),
        IggyMessage::builder()
            .id(202)
            .payload(Bytes::from_static(b"retryable-failover-2"))
            .build()
            .expect("Failed to build message 202"),
        IggyMessage::builder()
            .id(203)
            .payload(Bytes::from_static(b"retryable-failover-3"))
            .build()
            .expect("Failed to build message 203"),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send retryable-failover batch");

    let mut sink_last_error = true;
    let mut sink_status = ConnectorStatus::Stopped;
    let mut processed_messages = 0_u64;
    let mut sink_errors = u64::MAX;
    let mut total_docs = 0_u64;

    for _ in 0..POLL_ATTEMPTS {
        total_docs = collection
            .count_documents(doc! {})
            .await
            .expect("Failed to count documents after retryable failover");

        let sinks_response = http_client
            .get(format!("{}/sinks", api_address))
            .send()
            .await
            .expect("Failed to fetch /sinks");
        assert_eq!(sinks_response.status(), 200);
        let sinks: Vec<SinkInfoResponse> = sinks_response.json().await.expect("Invalid sinks JSON");
        let sink = sinks
            .iter()
            .find(|s| s.key == MONGODB_SINK_KEY)
            .expect("MongoDB sink not found in /sinks");
        sink_last_error = sink.last_error.is_some();
        sink_status = sink.status;

        let stats_response = http_client
            .get(format!("{}/stats", api_address))
            .send()
            .await
            .expect("Failed to fetch /stats");
        assert_eq!(stats_response.status(), 200);
        let stats: ConnectorRuntimeStats = stats_response.json().await.expect("Invalid stats JSON");
        let sink_stats = stats
            .connectors
            .iter()
            .find(|c| c.key == MONGODB_SINK_KEY)
            .expect("MongoDB sink stats not found");
        processed_messages = sink_stats
            .messages_processed
            .expect("messages_processed must be present for sink stats");
        sink_errors = sink_stats.errors;

        if total_docs == 3 && !sink_last_error && processed_messages == 3 && sink_errors == 0 {
            break;
        }

        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    let _ = fixture.disable_fail_command(&mongo_client).await;

    assert_eq!(
        total_docs, 3,
        "expected exact document count after retryable failover retry"
    );
    assert!(
        !sink_last_error,
        "retryable failover path must recover without leaving sink in error state"
    );
    assert_eq!(
        sink_status,
        ConnectorStatus::Running,
        "sink should remain Running after successful retryable failover recovery"
    );
    assert_eq!(
        sink_errors, 0,
        "retryable failover recovery should not increment runtime errors"
    );
    assert_eq!(
        processed_messages, 3,
        "successful retryable failover recovery must count full batch as processed"
    );

    for id in [201_u128, 202, 203] {
        let count = collection
            .count_documents(doc! { "_id": build_expected_document_id(id) })
            .await
            .expect("Failed to count per-id documents after retryable failover");
        assert_eq!(count, 1, "Expected exactly one document for _id={id}");
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mongodb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn no_writes_performed_label_path_preserves_state_accuracy(
    harness: &TestHarness,
    fixture: MongoDbSinkFailpointFixture,
) {
    let client = harness.root_client().await.unwrap();
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = HttpClient::new();
    let mongo_client = fixture
        .create_client()
        .await
        .expect("Failed to create MongoDB client");

    fixture
        .configure_fail_command_once(
            &mongo_client,
            doc! {
                "failCommands": ["insert"],
                "errorCode": 13,
                "errorLabels": ["RetryableWriteError", "NoWritesPerformed"]
            },
        )
        .await
        .expect("Failed to configure failpoint with NoWritesPerformed label");

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let collection = mongo_client
        .database(DEFAULT_TEST_DATABASE)
        .collection::<Document>(DEFAULT_SINK_COLLECTION);

    let mut messages: Vec<IggyMessage> = vec![
        IggyMessage::builder()
            .id(301)
            .payload(Bytes::from_static(b"no-writes-performed-1"))
            .build()
            .expect("Failed to build message 301"),
        IggyMessage::builder()
            .id(302)
            .payload(Bytes::from_static(b"no-writes-performed-2"))
            .build()
            .expect("Failed to build message 302"),
        IggyMessage::builder()
            .id(303)
            .payload(Bytes::from_static(b"no-writes-performed-3"))
            .build()
            .expect("Failed to build message 303"),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send NoWritesPerformed label batch");

    let mut sink_last_error = true;
    let mut sink_status = ConnectorStatus::Stopped;
    let mut processed_messages = 0_u64;
    let mut sink_errors = u64::MAX;
    let mut total_docs = 0_u64;

    for _ in 0..POLL_ATTEMPTS {
        total_docs = collection
            .count_documents(doc! {})
            .await
            .expect("Failed to count documents for NoWritesPerformed path");

        let sinks_response = http_client
            .get(format!("{}/sinks", api_address))
            .send()
            .await
            .expect("Failed to fetch /sinks");
        assert_eq!(sinks_response.status(), 200);
        let sinks: Vec<SinkInfoResponse> = sinks_response.json().await.expect("Invalid sinks JSON");
        let sink = sinks
            .iter()
            .find(|s| s.key == MONGODB_SINK_KEY)
            .expect("MongoDB sink not found in /sinks");
        sink_last_error = sink.last_error.is_some();
        sink_status = sink.status;

        let stats_response = http_client
            .get(format!("{}/stats", api_address))
            .send()
            .await
            .expect("Failed to fetch /stats");
        assert_eq!(stats_response.status(), 200);
        let stats: ConnectorRuntimeStats = stats_response.json().await.expect("Invalid stats JSON");
        let sink_stats = stats
            .connectors
            .iter()
            .find(|c| c.key == MONGODB_SINK_KEY)
            .expect("MongoDB sink stats not found");
        processed_messages = sink_stats
            .messages_processed
            .expect("messages_processed must be present for sink stats");
        sink_errors = sink_stats.errors;

        if total_docs == 3 && !sink_last_error && processed_messages == 3 && sink_errors == 0 {
            break;
        }

        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    let _ = fixture.disable_fail_command(&mongo_client).await;

    assert_eq!(
        total_docs, 3,
        "NoWritesPerformed path should result in exactly one committed batch after retry"
    );
    assert!(
        !sink_last_error,
        "NoWritesPerformed retry path should not leave sink in error state"
    );
    assert_eq!(
        sink_status,
        ConnectorStatus::Running,
        "sink should remain Running after NoWritesPerformed retry recovery"
    );
    assert_eq!(
        sink_errors, 0,
        "NoWritesPerformed retry recovery should not increment runtime errors"
    );
    assert_eq!(
        processed_messages, 3,
        "NoWritesPerformed retry recovery must count full batch as processed"
    );

    for id in [301_u128, 302, 303] {
        let count = collection
            .count_documents(doc! { "_id": build_expected_document_id(id) })
            .await
            .expect("Failed to count per-id documents for NoWritesPerformed path");
        assert_eq!(count, 1, "Expected exactly one document for _id={id}");
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mongodb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn auto_create_collection_on_open(
    harness: &TestHarness,
    fixture: MongoDbSinkAutoCreateFixture,
) {
    let mongo_client = fixture
        .create_client()
        .await
        .expect("Failed to create MongoDB client");

    // The connector's open() creates the collection. Poll until it appears.
    // No messages are sent in this test.
    let mut found = false;
    for _ in 0..POLL_ATTEMPTS {
        if fixture
            .collection_exists(&mongo_client, DEFAULT_SINK_COLLECTION)
            .await
            .unwrap_or(false)
        {
            found = true;
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(
        found,
        "Collection '{DEFAULT_SINK_COLLECTION}' was not created by open() within timeout"
    );

    // No messages sent -- collection should be empty.
    let count = fixture
        .count_documents_in_collection(&mongo_client, DEFAULT_SINK_COLLECTION)
        .await
        .expect("Failed to count");
    assert_eq!(
        count, 0,
        "Collection should be empty after open() with no messages"
    );

    // Suppress unused harness warning.
    let _ = harness;
}
