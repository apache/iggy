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

use std::time::{Duration, SystemTime};

use bytes::Bytes;
use fluss::{
    client::EARLIEST_OFFSET,
    metadata::{Column, DataType, DataTypes, Schema, TablePath},
    row::{DataGetters, Decimal, InternalArray, TimestampLtz},
};
use iggy::prelude::{Consumer, IggyMessage, Partitioning, PollingStrategy};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use integration::harness::seeds;
use integration::iggy_harness;
use tokio::time::{Instant, timeout_at};

use crate::connectors::fixtures::{
    FlussMultiSinkFixture, FlussPartitionedSinkFixture, FlussPrimaryKeySinkFixture,
    FlussSinkFixture,
};
use crate::connectors::{TestMessage, create_test_messages};

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
const WAIT_TIMEOUT_S: u64 = 5;
const AUTO_PARTITION_WAIT_TIMEOUT_S: u64 = 30;
const MULTI_TABLE_DATABASE: &str = "fluss";
const MULTI_TABLE_ROUTES: [&str; 2] = ["multi_table_events_a", "multi_table_events_b"];

fn primary_key_message(
    message_id: u128,
    table_path: &TablePath,
    id: i64,
    name: Option<&str>,
    op: &str,
) -> IggyMessage {
    let payload = serde_json::json!({
        "table": table_path.to_string(),
        "id": id,
        "name": name,
        "op": op,
    });

    IggyMessage::builder()
        .id(message_id)
        .payload(Bytes::from(
            serde_json::to_vec(&payload).expect("Primary-key message should serialize"),
        ))
        .build()
        .expect("Primary-key message should build")
}

async fn wait_for_primary_key_value(
    fixture: &FlussPrimaryKeySinkFixture,
    id: i64,
    expected: Option<&str>,
) {
    let deadline = Instant::now() + Duration::from_secs(WAIT_TIMEOUT_S);
    timeout_at(deadline, async {
        loop {
            let actual = fixture
                .lookup_test_value(id)
                .await
                .expect("Primary-key row should be readable");
            if actual.as_deref() == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Primary-key row {id} should become {expected:?} before timeout"));
}

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

fn row_field<'a>(data_type: &'a DataType, field_name: &str) -> (usize, &'a DataType) {
    let DataType::Row(row_type) = data_type else {
        panic!("{field_name} should belong to an inferred row type");
    };
    let field_index = row_type
        .get_field_index(field_name)
        .unwrap_or_else(|| panic!("Inferred row should contain {field_name}"));

    (field_index, row_type.fields()[field_index].data_type())
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
    let client = harness.root_client().await.expect("Client creation failed");

    let stream_id: Identifier = seeds::names::STREAM
        .try_into()
        .expect("stream to identifier should not fail");
    let topic_id: Identifier = seeds::names::TOPIC
        .try_into()
        .expect("topic to identifier should not fail");

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
        .read_from_test_table(TEST_MESSAGE_COUNT, WAIT_TIMEOUT_S)
        .await
        .expect("read messages");

    assert_eq!(messages.len(), TEST_MESSAGE_COUNT);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/fluss/sink.toml")),
    seed = seeds::connector_stream
)]
async fn partitioned_table_sink_should_ingest_messages(
    harness: &TestHarness,
    fixture: FlussPartitionedSinkFixture,
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
    let table_path = fixture.test_table_path();
    let current_time = humantime::format_rfc3339(SystemTime::now()).to_string();
    let event_day = current_time
        .split_once('T')
        .expect("RFC 3339 timestamp should contain a date")
        .0;
    let messages_data = create_test_messages(TEST_MESSAGE_COUNT);
    let mut messages: Vec<IggyMessage> = messages_data
        .iter()
        .enumerate()
        .map(|(index, message)| {
            let payload = serde_json::to_vec(&serde_json::json!({
                "table": table_path.to_string(),
                "event_day": event_day,
                "event": message,
            }))
            .expect("Test message should serialize");
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
        .expect("Partitioned Fluss test table should be created");
    let table_info = fixture
        .get_test_table()
        .await
        .expect("Partitioned Fluss test table should be available");
    assert!(table_info.is_partitioned());
    assert!(table_info.is_auto_partitioned());
    assert_eq!(table_info.get_partition_keys().as_ref(), ["event_day"]);

    let connection = fixture
        .get_fluss_connection()
        .await
        .expect("Fluss connection should be available");
    let admin = connection
        .get_admin()
        .expect("Fluss admin client should be available");
    let deadline = Instant::now() + Duration::from_secs(AUTO_PARTITION_WAIT_TIMEOUT_S);
    timeout_at(deadline, async {
        loop {
            let partitions = admin
                .list_partition_infos(&table_path)
                .await
                .expect("Fluss partitions should be readable");
            if partitions
                .iter()
                .any(|partition| partition.get_partition_name() == event_day)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("Fluss should create the partition automatically");

    let rows = fixture
        .read_from_partitioned_test_table(TEST_MESSAGE_COUNT, WAIT_TIMEOUT_S)
        .await
        .expect("Partitioned Fluss test table rows should be readable");

    assert_eq!(rows.len(), TEST_MESSAGE_COUNT);
    let event_day_index = table_info
        .schema
        .columns()
        .iter()
        .position(|column| column.name() == "event_day")
        .expect("Partitioned Fluss table should contain event_day");
    assert!(rows.iter().all(|row| {
        row.get_string(event_day_index)
            .is_ok_and(|day| day == event_day)
    }));
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
        .read_from_test_table(expected_messages.len(), WAIT_TIMEOUT_S)
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
async fn multi_table_sink_should_upsert_primary_key_rows(
    harness: &TestHarness,
    fixture: FlussPrimaryKeySinkFixture,
) {
    let table_info = fixture
        .get_test_table()
        .await
        .expect("Primary-key Fluss test table should be available");
    assert!(table_info.has_primary_key());

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
    let table_path = fixture.test_table_path();
    let mut messages = vec![primary_key_message(1, &table_path, 42, Some("before"), "u")];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Initial primary-key message should be sent");
    wait_for_primary_key_value(&fixture, 42, Some("before")).await;

    let mut messages = vec![primary_key_message(2, &table_path, 42, Some("after"), "u")];
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Updated primary-key message should be sent");
    wait_for_primary_key_value(&fixture, 42, Some("after")).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/fluss/sink.toml")),
    seed = seeds::connector_stream
)]
async fn multi_table_sink_should_delete_primary_key_rows(
    harness: &TestHarness,
    fixture: FlussPrimaryKeySinkFixture,
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
    let table_path = fixture.test_table_path();
    let mut messages = vec![primary_key_message(
        1,
        &table_path,
        42,
        Some("delete-me"),
        "u",
    )];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Initial primary-key message should be sent");
    wait_for_primary_key_value(&fixture, 42, Some("delete-me")).await;

    let mut messages = vec![primary_key_message(2, &table_path, 42, None, "d")];
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Delete primary-key message should be sent");
    wait_for_primary_key_value(&fixture, 42, None).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/fluss/sink.toml")),
    seed = seeds::connector_stream
)]
async fn json_messages_sink_creates_inferred_tables_and_routes_rows(
    harness: &TestHarness,
    fixture: FlussMultiSinkFixture,
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
    let table_paths = MULTI_TABLE_ROUTES.map(|route| TablePath::new(MULTI_TABLE_DATABASE, route));

    let mut messages: Vec<IggyMessage> = (0..TEST_MESSAGE_COUNT)
        .map(|index| {
            let event_id = i64::try_from(index).expect("Test event ID should fit in i64");
            let score =
                f64::from(u32::try_from(index).expect("Test score should fit in u32")) + 0.5;
            let payload = serde_json::json!({
                "table": table_paths[index % table_paths.len()].to_string(),
                "event": {
                    "envelope": {
                        "context": {
                            "pipeline": {
                                "stage": {
                                    "processor": {
                                        "runtime": {
                                            "node": {
                                                "location": {
                                                    "details": {
                                                        "event_id": event_id,
                                                        "event_type": format!("event-{index}"),
                                                        "active": index % 2 == 0,
                                                        "score": score,
                                                        "tags": [
                                                            "streaming",
                                                            null,
                                                            format!("event-{index}"),
                                                        ],
                                                    },
                                                },
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            });
            let payload =
                serde_json::to_vec(&payload).expect("Multi-table message should serialize");
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

    let connection = fixture
        .get_fluss_connection()
        .await
        .expect("Fluss connection should be available");
    let admin = connection
        .get_admin()
        .expect("Fluss admin client should be available");
    let table_creation_deadline = Instant::now() + Duration::from_secs(WAIT_TIMEOUT_S);
    for table_path in &table_paths {
        loop {
            let table_exists = timeout_at(table_creation_deadline, admin.table_exists(table_path))
                .await
                .expect("Multi-table target should be created before the timeout")
                .expect("Multi-table target existence should be readable");
            if table_exists {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    let table_info = admin
        .get_table_info(&table_paths[0])
        .await
        .expect("Multi-table target metadata should be available");
    for table_path in &table_paths[1..] {
        let routed_table_info = admin
            .get_table_info(table_path)
            .await
            .expect("Routed table metadata should be available");
        assert_eq!(routed_table_info.schema, table_info.schema);
    }
    let columns = table_info.schema.columns();
    let event_column_index = columns
        .iter()
        .position(|column| column.name() == "event")
        .expect("Inferred schema should contain event");

    let event_data_type = columns[event_column_index].data_type();
    let (envelope_field_index, envelope_data_type) = row_field(event_data_type, "envelope");
    let (context_field_index, context_data_type) = row_field(envelope_data_type, "context");
    let (pipeline_field_index, pipeline_data_type) = row_field(context_data_type, "pipeline");
    let (stage_field_index, stage_data_type) = row_field(pipeline_data_type, "stage");
    let (processor_field_index, processor_data_type) = row_field(stage_data_type, "processor");
    let (runtime_field_index, runtime_data_type) = row_field(processor_data_type, "runtime");
    let (node_field_index, node_data_type) = row_field(runtime_data_type, "node");
    let (location_field_index, location_data_type) = row_field(node_data_type, "location");
    let (details_field_index, details_data_type) = row_field(location_data_type, "details");
    let (event_id_field_index, event_id_data_type) = row_field(details_data_type, "event_id");
    let (event_type_field_index, event_type_data_type) = row_field(details_data_type, "event_type");
    let (active_field_index, active_data_type) = row_field(details_data_type, "active");
    let (score_field_index, score_data_type) = row_field(details_data_type, "score");
    let (tags_field_index, tags_data_type) = row_field(details_data_type, "tags");

    assert_eq!(event_id_data_type, &DataTypes::bigint());
    assert_eq!(event_type_data_type, &DataTypes::string());
    assert_eq!(active_data_type, &DataTypes::boolean());
    assert_eq!(score_data_type, &DataTypes::double());
    assert_eq!(tags_data_type, &DataTypes::array(DataTypes::string()));

    let expected_rows_per_table = TEST_MESSAGE_COUNT / MULTI_TABLE_ROUTES.len();
    for (route_index, table_path) in table_paths.iter().enumerate() {
        let table = connection
            .get_table(table_path)
            .await
            .expect("Multi-table target should be readable");
        let log_scanner = table
            .new_scan()
            .create_log_scanner()
            .expect("Multi-table log scanner should be created");
        log_scanner
            .subscribe(0, EARLIEST_OFFSET)
            .await
            .expect("Multi-table log scanner should subscribe");

        let row_read_deadline = Instant::now() + Duration::from_secs(WAIT_TIMEOUT_S);
        let mut rows = Vec::with_capacity(expected_rows_per_table);
        while rows.len() < expected_rows_per_table {
            let records = timeout_at(row_read_deadline, log_scanner.poll(Duration::from_secs(1)))
                .await
                .expect("All routed rows should arrive before the timeout")
                .expect("Routed rows should be readable");
            rows.extend(records.into_iter().map(|record| record.row));
        }

        assert_eq!(rows.len(), expected_rows_per_table);
        rows.sort_by_key(|row| {
            row.get_row(event_column_index)
                .expect("event should contain a row")
                .get_row(envelope_field_index)
                .expect("envelope should contain a row")
                .get_row(context_field_index)
                .expect("context should contain a row")
                .get_row(pipeline_field_index)
                .expect("pipeline should contain a row")
                .get_row(stage_field_index)
                .expect("stage should contain a row")
                .get_row(processor_field_index)
                .expect("processor should contain a row")
                .get_row(runtime_field_index)
                .expect("runtime should contain a row")
                .get_row(node_field_index)
                .expect("node should contain a row")
                .get_row(location_field_index)
                .expect("location should contain a row")
                .get_row(details_field_index)
                .expect("details should contain a row")
                .get_long(event_id_field_index)
                .expect("event_id should contain a bigint")
        });
        for (row_index, row) in rows.iter().enumerate() {
            let expected_index = route_index + row_index * MULTI_TABLE_ROUTES.len();
            let event = row
                .get_row(event_column_index)
                .expect("event should contain a row");
            let envelope = event
                .get_row(envelope_field_index)
                .expect("envelope should contain a row");
            let context = envelope
                .get_row(context_field_index)
                .expect("context should contain a row");
            let pipeline = context
                .get_row(pipeline_field_index)
                .expect("pipeline should contain a row");
            let stage = pipeline
                .get_row(stage_field_index)
                .expect("stage should contain a row");
            let processor = stage
                .get_row(processor_field_index)
                .expect("processor should contain a row");
            let runtime = processor
                .get_row(runtime_field_index)
                .expect("runtime should contain a row");
            let node = runtime
                .get_row(node_field_index)
                .expect("node should contain a row");
            let location = node
                .get_row(location_field_index)
                .expect("location should contain a row");
            let details = location
                .get_row(details_field_index)
                .expect("details should contain a row");
            assert_eq!(
                details
                    .get_long(event_id_field_index)
                    .expect("event_id should contain a bigint"),
                i64::try_from(expected_index).expect("Test event ID should fit in i64")
            );
            assert_eq!(
                details
                    .get_string(event_type_field_index)
                    .expect("event_type should contain a string"),
                format!("event-{expected_index}")
            );
            assert_eq!(
                details
                    .get_boolean(active_field_index)
                    .expect("active should contain a boolean"),
                expected_index.is_multiple_of(2)
            );
            assert_eq!(
                details
                    .get_double(score_field_index)
                    .expect("score should contain a double"),
                f64::from(u32::try_from(expected_index).expect("Test score should fit in u32"))
                    + 0.5
            );
            let tags = details
                .get_array(tags_field_index)
                .expect("tags should contain an array");
            assert_eq!(tags.size(), 3);
            assert_eq!(
                tags.get_string(0).expect("First tag should be a string"),
                "streaming"
            );
            assert!(
                tags.is_null_at(1)
                    .expect("Second tag nullability should be readable")
            );
            assert_eq!(
                tags.get_string(2).expect("Third tag should be a string"),
                format!("event-{expected_index}")
            );
        }
    }
}
