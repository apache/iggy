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

use super::{LARGE_BATCH_COUNT, POLL_ATTEMPTS, POLL_INTERVAL_MS, TEST_MESSAGE_COUNT};
use crate::connectors::fixtures::{
    SurrealDbOps, SurrealDbSinkBatchFixture, SurrealDbSinkFixture, SurrealDbSinkJsonFixture,
    SurrealDbSinkRawFixture,
};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use integration::harness::seeds;
use integration::iggy_harness;
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;

fn build_expected_record_id(message_id: u128, offset: u64) -> String {
    let mut id = String::new();
    id.push('s');
    push_hex_component(&mut id, seeds::names::STREAM.as_bytes());
    id.push_str("_t");
    push_hex_component(&mut id, seeds::names::TOPIC.as_bytes());
    id.push_str("_p0_o");
    id.push_str(&offset.to_string());
    id.push_str("_m");
    id.push_str(&format!("{message_id:032x}"));
    id
}

fn push_hex_component(out: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/surrealdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn json_messages_sink_to_surrealdb(harness: &TestHarness, fixture: SurrealDbSinkJsonFixture) {
    let client = harness.root_client().await.unwrap();
    let surreal_client = fixture
        .create_client()
        .await
        .expect("Failed to create SurrealDB client");

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let payloads = [
        serde_json::json!({"name": "Alice", "score": 10}),
        serde_json::json!({"name": "Bob", "score": 20}),
        serde_json::json!({"name": "Carol", "score": 30}),
    ];
    let mut messages: Vec<IggyMessage> = payloads
        .iter()
        .enumerate()
        .map(|(idx, payload)| {
            IggyMessage::builder()
                .id((idx + 1) as u128)
                .payload(Bytes::from(
                    serde_json::to_vec(payload).expect("Failed to serialize payload"),
                ))
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

    let records = fixture
        .wait_for_records(&surreal_client, TEST_MESSAGE_COUNT)
        .await
        .expect("Records did not appear in SurrealDB");

    assert_eq!(records.len(), TEST_MESSAGE_COUNT);
    for (idx, record) in records.iter().enumerate() {
        assert_eq!(
            record["iggy_message_id"],
            Value::String((idx + 1).to_string())
        );
        assert_eq!(
            record["iggy_stream"],
            Value::String(seeds::names::STREAM.to_string())
        );
        assert_eq!(
            record["iggy_topic"],
            Value::String(seeds::names::TOPIC.to_string())
        );
        assert_eq!(record["iggy_partition_id"], Value::String("0".to_string()));
        assert_eq!(record["iggy_offset"], Value::String(idx.to_string()));
        assert_eq!(
            record["payload_encoding"],
            Value::String("json".to_string())
        );
        assert_eq!(record["payload"], payloads[idx]);
        assert_eq!(
            record["id"].as_str().expect("record id should be string"),
            format!(
                "iggy_messages:{}",
                build_expected_record_id((idx + 1) as u128, idx as u64)
            )
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/surrealdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn raw_messages_sink_as_base64(harness: &TestHarness, fixture: SurrealDbSinkRawFixture) {
    let client = harness.root_client().await.unwrap();
    let surreal_client = fixture
        .create_client()
        .await
        .expect("Failed to create SurrealDB client");

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let payloads: Vec<Vec<u8>> = vec![
        b"plain text".to_vec(),
        vec![0x00, 0x01, 0x02, 0xff],
        vec![0xde, 0xad, 0xbe, 0xef],
    ];
    let mut messages: Vec<IggyMessage> = payloads
        .iter()
        .enumerate()
        .map(|(idx, payload)| {
            IggyMessage::builder()
                .id((idx + 1) as u128)
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

    let records = fixture
        .wait_for_records(&surreal_client, payloads.len())
        .await
        .expect("Records did not appear in SurrealDB");

    assert_eq!(records.len(), payloads.len());
    let expected_payloads = ["cGxhaW4gdGV4dA==", "AAEC/w==", "3q2+7w=="];
    for (idx, record) in records.iter().enumerate() {
        assert_eq!(
            record["payload_encoding"],
            Value::String("base64".to_string())
        );
        assert_eq!(
            record["payload"],
            Value::String(expected_payloads[idx].to_string())
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/surrealdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn large_batch_processed_in_chunks(
    harness: &TestHarness,
    fixture: SurrealDbSinkBatchFixture,
) {
    let client = harness.root_client().await.unwrap();
    let surreal_client = fixture
        .create_client()
        .await
        .expect("Failed to create SurrealDB client");

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let mut messages: Vec<IggyMessage> = (0..LARGE_BATCH_COUNT)
        .map(|idx| {
            IggyMessage::builder()
                .id((idx + 1) as u128)
                .payload(Bytes::from(
                    serde_json::to_vec(&serde_json::json!({"idx": idx}))
                        .expect("Failed to serialize payload"),
                ))
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

    let records = fixture
        .wait_for_records(&surreal_client, LARGE_BATCH_COUNT)
        .await
        .expect("Records did not appear in SurrealDB");

    assert_eq!(records.len(), LARGE_BATCH_COUNT);
    for (idx, record) in records.iter().enumerate() {
        assert_eq!(record["iggy_offset"], Value::String(idx.to_string()));
        assert_eq!(record["payload"], serde_json::json!({"idx": idx}));
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/surrealdb/sink.toml")),
    seed = seeds::connector_stream
)]
async fn duplicate_record_id_is_idempotent_replay_not_overwrite(
    harness: &TestHarness,
    fixture: SurrealDbSinkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let surreal_client = fixture
        .create_client()
        .await
        .expect("Failed to create SurrealDB client");

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    fixture
        .insert_preseeded_record(&surreal_client, &build_expected_record_id(2, 1), 2)
        .await
        .expect("Failed to preseed duplicate record");

    let mut messages: Vec<IggyMessage> = vec![
        IggyMessage::builder()
            .id(1)
            .payload(Bytes::from_static(br#"{"message":"one"}"#))
            .build()
            .expect("Failed to build message 1"),
        IggyMessage::builder()
            .id(2)
            .payload(Bytes::from_static(br#"{"message":"two"}"#))
            .build()
            .expect("Failed to build message 2"),
        IggyMessage::builder()
            .id(3)
            .payload(Bytes::from_static(br#"{"message":"three"}"#))
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
        .expect("Failed to send duplicate batch");

    let mut id1_inserted = false;
    let mut id3_inserted = false;

    for _ in 0..POLL_ATTEMPTS {
        id1_inserted = !fixture
            .select_records_by_message_id(&surreal_client, 1)
            .await
            .expect("Failed to query id 1")
            .is_empty();
        id3_inserted = !fixture
            .select_records_by_message_id(&surreal_client, 3)
            .await
            .expect("Failed to query id 3")
            .is_empty();

        if id1_inserted && id3_inserted {
            break;
        }

        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(
        id1_inserted,
        "Expected first non-duplicate record to be inserted"
    );
    assert!(
        id3_inserted,
        "Expected suffix record after duplicate to be inserted"
    );

    let duplicate_records = fixture
        .select_records_by_message_id(&surreal_client, 2)
        .await
        .expect("Failed to query duplicate record");
    assert_eq!(
        duplicate_records.len(),
        1,
        "Duplicate replay should not create extra records"
    );
    assert_eq!(
        duplicate_records[0]["seed_marker"],
        Value::String("preseed-unchanged".to_string()),
        "Existing record must not be overwritten by replay"
    );
    assert_eq!(
        duplicate_records[0]["payload"],
        Value::String("preseeded".to_string()),
        "Existing record payload must remain unchanged"
    );
    assert_eq!(
        fixture
            .select_all_records(&surreal_client)
            .await
            .expect("Failed to select all records")
            .len(),
        3
    );
}
