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

//! Temporary developer harness against a QuestDB reachable at
//! `QUESTDB_CONF`. Ignored by default. Replaced by the
//! `core/integration/tests/connectors/questdb/` testcontainers suite.
//!
//!   QUESTDB_CONF='ws::addr=127.0.0.1:9000;username=admin;password=quest;' \
//!     cargo test -p iggy_connector_questdb_sink --test live_smoke -- --ignored --nocapture

use iggy_connector_questdb_sink::{QuestDbSink, QuestDbSinkConfig};
use iggy_connector_sdk::{ConsumedMessage, MessagesMetadata, Payload, Schema, Sink, TopicMetadata};
use secrecy::SecretString;

fn message(offset: u64, json: &str) -> ConsumedMessage {
    let mut bytes = json.as_bytes().to_vec();
    ConsumedMessage {
        id: offset as u128,
        offset,
        checksum: 0,
        timestamp: 1_788_523_200_000_000 + offset,
        origin_timestamp: 1_788_523_200_000_000 + offset,
        headers: None,
        payload: Payload::Json(simd_json::to_owned_value(&mut bytes).unwrap()),
    }
}

#[tokio::test]
#[ignore = "requires a running QuestDB"]
async fn given_live_questdb_when_consuming_should_write_typed_rows() {
    // The FFI entry point installs a subscriber; calling the trait directly
    // does not, so rejection logs would otherwise go nowhere.
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    let conf = std::env::var("QUESTDB_CONF").expect("QUESTDB_CONF must be set");
    let config = QuestDbSinkConfig {
        connection_string: SecretString::from(conf),
        table: "iggy_sink_smoke".to_owned(),
        timestamp_source: Some("payload".to_owned()),
        timestamp_field: Some("event_time".to_owned()),
        timestamp_unit: Some("micros".to_owned()),
        symbol_columns: Some(vec!["side".to_owned()]),
        uuid_columns: Some(vec!["trade_id".to_owned()]),
        include_stream_column: Some(true),
        include_topic_column: Some(true),
        include_partition_column: Some(true),
        include_offset_column: Some(true),
        include_headers: None,
        ack_level: Some("ok".to_owned()),
        flush_timeout: Some("15s".to_owned()),
        batch_size: Some(500),
        max_flush_bytes: None,
        log_rejected_payload: Some(true),
        verbose_logging: Some(true),
    };

    let mut sink = QuestDbSink::new(1, config);
    sink.open().await.expect("open");

    let topic_metadata = TopicMetadata {
        stream: "user_events".to_owned(),
        topic: "trades".to_owned(),
    };
    let messages_metadata = MessagesMetadata {
        partition_id: 3,
        current_offset: 41,
        schema: Schema::Json,
    };

    let messages = vec![
        message(
            0,
            r#"{"event_time":1788523200000000,"side":"buy","price":2615.54,
                "amount":7,"active":true,"note":"hello",
                "trade_id":"123e4567-e89b-12d3-a456-426614174000",
                "embedding":[1.5,2.5,3.5],"matrix":[[1.0,2.0],[3.0,4.0]],
                "meta":{"venue":"nyse"},"missing":null}"#,
        ),
        message(
            1,
            r#"{"event_time":1788523200000001,"side":"sell","price":2616.0,
                "amount":8,"active":false,"note":"world",
                "trade_id":"00000000-0000-0000-0000-000000000001",
                "embedding":[9.5],"matrix":[[5.0,6.0]],
                "meta":{"venue":"lse"}}"#,
        ),
        // Rejected before any row bytes are written.
        ConsumedMessage {
            id: 98,
            offset: 98,
            checksum: 0,
            timestamp: 1_788_523_200_000_098,
            origin_timestamp: 1_788_523_200_000_098,
            headers: None,
            payload: Payload::Json(simd_json::to_owned_value(&mut b"[1,2,3]".to_vec()).unwrap()),
        },
        // Rejected MID-ROW: the table, symbol and several columns are already
        // encoded before the bad UUID is reached, so this is the case
        // rewind_to_marker exists for. Without it the whole batch would fail.
        message(
            99,
            r#"{"event_time":1788523200000099,"side":"buy","price":1.0,
                "amount":1,"active":true,"note":"bad-uuid-row",
                "trade_id":"definitely-not-a-uuid"}"#,
        ),
        // Must still land, proving the buffer recovered from the row above.
        message(
            100,
            r#"{"event_time":1788523200000100,"side":"sell","price":2.0,
                "amount":2,"active":false,"note":"after-rejection",
                "trade_id":"00000000-0000-0000-0000-000000000002"}"#,
        ),
    ];

    sink.consume(&topic_metadata, messages_metadata, messages)
        .await
        .expect("consume");
    sink.close().await.expect("close");
}
