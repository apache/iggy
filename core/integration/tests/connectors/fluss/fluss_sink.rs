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

use crate::connectors::fixtures::FlussSinkFixture;
use crate::connectors::{TestMessage, create_test_messages};
use bytes::Bytes;
use fluss::metadata::{Column, DataTypes, Schema};
use fluss::row::{DataGetters, Decimal, TimestampLtz};
use iggy::prelude::{Consumer, IggyMessage, Partitioning, PollingStrategy};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use integration::harness::seeds;
use integration::iggy_harness;

const TEST_MESSAGE_COUNT: usize = 10;
const ROW_COMPARISON_MESSAGE_COUNT: u32 = 3;
const ID_COLUMN_INDEX: usize = 0;
const CHECKSUM_COLUMN_INDEX: usize = 1;
const IGGY_OFFSET_COLUMN_INDEX: usize = 2;
const IGGY_TIMESTAMP_COLUMN_INDEX: usize = 3;
const IGGY_STREAM_COLUMN_INDEX: usize = 4;
const IGGY_TOPIC_COLUMN_INDEX: usize = 5;
const IGGY_PARTITION_ID_COLUMN_INDEX: usize = 6;
const IGGY_ORIGIN_TIMESTAMP_COLUMN_INDEX: usize = 7;
const PAYLOAD_COLUMN_INDEX: usize = 8;
const UNSIGNED_64_DECIMAL_PRECISION: u32 = 20;
const TIMESTAMP_PRECISION: u32 = 6;
const WAIT_TIMEOUT_S: u64 = 10;

fn expected_decimal(value: u64) -> Decimal {
    Decimal::from_arrow_decimal128(i128::from(value), 0, UNSIGNED_64_DECIMAL_PRECISION, 0)
        .expect("Unsigned 64-bit value should fit in DECIMAL(20, 0)")
}

fn expected_timestamp(value: u64) -> TimestampLtz {
    let epoch_micros = i64::try_from(value).expect("Test timestamp should fit in i64");
    TimestampLtz::from_millis_nanos(
        epoch_micros / 1_000,
        ((epoch_micros % 1_000) * 1_000) as i32,
    )
    .expect("Test timestamp should convert to Fluss TIMESTAMP_LTZ(6)")
}

fn expected_sink_schema() -> Schema {
    Schema::builder()
        .with_columns(vec![
            Column::new("id", DataTypes::string()).with_comment("Apache Iggy message ID"),
            Column::new("checksum", DataTypes::decimal(20, 0))
                .with_comment("Apache Iggy message checksum"),
            Column::new("iggy_offset", DataTypes::decimal(20, 0))
                .with_comment("Apache Iggy message offset"),
            Column::new("iggy_timestamp", DataTypes::timestamp_ltz_with_precision(6))
                .with_comment("Apache Iggy message timestamp"),
            Column::new("iggy_stream", DataTypes::string()).with_comment("Apache Iggy stream name"),
            Column::new("iggy_topic", DataTypes::string()).with_comment("Apache Iggy topic name"),
            Column::new("iggy_partition_id", DataTypes::bigint())
                .with_comment("Apache Iggy partition ID"),
            Column::new(
                "iggy_origin_timestamp",
                DataTypes::timestamp_ltz_with_precision(6),
            )
            .with_comment("Apache Iggy message origin timestamp"),
            Column::new("payload", DataTypes::string()).with_comment("Apache Iggy message payload"),
        ])
        .build()
        .expect("Expected Fluss sink schema should build")
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/fluss/sink.toml")),
    seed = seeds::connector_stream
)]
async fn sink_should_create_test_table_with_expected_schema(
    harness: &TestHarness,
    fixture: FlussSinkFixture,
) {
    let client = harness
        .root_client()
        .await
        .expect("Root client should be available");

    let stream_id: Identifier = seeds::names::STREAM
        .try_into()
        .expect("Stream identifier should be valid");
    let topic_id: Identifier = seeds::names::TOPIC
        .try_into()
        .expect("Topic identifier should be valid");

    let messages_data = create_test_messages(1);
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
        .wait_for_test_table(WAIT_TIMEOUT_S)
        .await
        .expect("Fluss test table should be created");
    let table = fixture
        .get_test_table()
        .await
        .expect("Fluss test table should be available");

    assert_eq!(table.schema, expected_sink_schema());
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/fluss/sink.toml")),
    seed = seeds::connector_stream
)]
async fn sink_should_write_message_to_test_table(harness: &TestHarness, fixture: FlussSinkFixture) {
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
        .wait_for_test_table(WAIT_TIMEOUT_S)
        .await
        .expect("Table has not found in time");

    let messages = fixture
        .read_from_test_table(WAIT_TIMEOUT_S)
        .await
        .expect("read messages");

    assert_eq!(messages.len(), TEST_MESSAGE_COUNT);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/fluss/sink.toml")),
    seed = seeds::connector_stream
)]
async fn sink_should_preserve_extra_fields_and_payload_in_rows(
    harness: &TestHarness,
    fixture: FlussSinkFixture,
) {
    let client = harness
        .root_client()
        .await
        .expect("Root client should be available");

    let stream_id: Identifier = seeds::names::STREAM
        .try_into()
        .expect("Stream identifier should be valid");
    let topic_id: Identifier = seeds::names::TOPIC
        .try_into()
        .expect("Topic identifier should be valid");

    let expected_messages = create_test_messages(ROW_COMPARISON_MESSAGE_COUNT as usize);
    let mut messages: Vec<IggyMessage> = expected_messages
        .iter()
        .enumerate()
        .map(|(index, message)| {
            let payload =
                serde_json::to_vec(message).expect("Test message payload should serialize");
            IggyMessage::builder()
                .id((index + 1) as u128)
                .payload(Bytes::from(payload))
                .build()
                .expect("Iggy message should build")
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
        .expect("Messages should be sent");

    let polled_messages = client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            ROW_COMPARISON_MESSAGE_COUNT,
            false,
        )
        .await
        .expect("Messages should be readable from Iggy");

    fixture
        .wait_for_test_table(WAIT_TIMEOUT_S)
        .await
        .expect("Fluss test table should be created");
    let rows = fixture
        .read_from_test_table(WAIT_TIMEOUT_S)
        .await
        .expect("Fluss test table rows should be readable");

    assert_eq!(rows.len(), expected_messages.len());
    assert_eq!(polled_messages.messages.len(), expected_messages.len());

    for ((row, source_message), expected_message) in rows
        .iter()
        .zip(&polled_messages.messages)
        .zip(&expected_messages)
    {
        assert_eq!(
            row.get_string(ID_COLUMN_INDEX)
                .expect("ID column should contain a string"),
            format!("{:032x}", source_message.header.id)
        );
        assert_eq!(
            row.get_decimal(CHECKSUM_COLUMN_INDEX, 20, 0)
                .expect("Checksum column should contain a decimal"),
            expected_decimal(source_message.header.checksum)
        );
        assert_eq!(
            row.get_decimal(IGGY_OFFSET_COLUMN_INDEX, 20, 0)
                .expect("Iggy offset column should contain a decimal"),
            expected_decimal(source_message.header.offset)
        );
        assert_eq!(
            row.get_timestamp_ltz(IGGY_TIMESTAMP_COLUMN_INDEX, TIMESTAMP_PRECISION)
                .expect("Iggy timestamp column should contain a timestamp"),
            expected_timestamp(source_message.header.timestamp)
        );
        assert_eq!(
            row.get_string(IGGY_STREAM_COLUMN_INDEX)
                .expect("Iggy stream column should contain a string"),
            seeds::names::STREAM
        );
        assert_eq!(
            row.get_string(IGGY_TOPIC_COLUMN_INDEX)
                .expect("Iggy topic column should contain a string"),
            seeds::names::TOPIC
        );
        assert_eq!(
            row.get_long(IGGY_PARTITION_ID_COLUMN_INDEX)
                .expect("Iggy partition ID column should contain a bigint"),
            0
        );
        assert_eq!(
            row.get_timestamp_ltz(IGGY_ORIGIN_TIMESTAMP_COLUMN_INDEX, TIMESTAMP_PRECISION)
                .expect("Iggy origin timestamp column should contain a timestamp"),
            expected_timestamp(source_message.header.origin_timestamp)
        );

        let payload = row
            .get_string(PAYLOAD_COLUMN_INDEX)
            .expect("Payload column should contain a string");
        let actual_message: TestMessage =
            serde_json::from_str(payload).expect("Payload should contain a test message");
        assert_eq!(&actual_message, expected_message);
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/fluss/sink.toml")),
    seed = seeds::connector_stream
)]
async fn sink_should_use_arrow_for_appending_messages(
    harness: &TestHarness,
    fixture: FlussSinkFixture,
) {
    let client = harness
        .root_client()
        .await
        .expect("Root client should be available");

    let stream_id: Identifier = seeds::names::STREAM
        .try_into()
        .expect("Stream identifier should be valid");
    let topic_id: Identifier = seeds::names::TOPIC
        .try_into()
        .expect("Topic identifier should be valid");

    let expected_messages = create_test_messages(ROW_COMPARISON_MESSAGE_COUNT as usize);
    let mut messages: Vec<IggyMessage> = expected_messages
        .iter()
        .enumerate()
        .map(|(index, message)| {
            let payload =
                serde_json::to_vec(message).expect("Test message payload should serialize");
            IggyMessage::builder()
                .id((index + 1) as u128)
                .payload(Bytes::from(payload))
                .build()
                .expect("Iggy message should build")
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
        .expect("Messages should be sent");

    fixture
        .wait_for_test_table(WAIT_TIMEOUT_S)
        .await
        .expect("Fluss test table should be created");
    let rows = fixture
        .read_from_test_table_arrow_batch(WAIT_TIMEOUT_S)
        .await
        .expect("Fluss test table rows should be readable");

    let records_number: usize = rows.iter().map(|row| row.num_records()).sum();
    assert_eq!(records_number, expected_messages.len());
}
