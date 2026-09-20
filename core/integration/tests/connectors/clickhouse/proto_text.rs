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
use crate::connectors::fixtures::ClickHouseSinkFixture;
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use integration::harness::seeds;
use integration::iggy_harness;

/// A `proto_convert` transform with no descriptor falls back to proto text, so
/// every message reaches the sink as `Payload::Proto` holding the JSON it was
/// given. The default JSONEachRow builder reads that as the document it holds.
/// A builder that only accepts `Payload::Json` skips every row, sends nothing,
/// and returns success, so the offset is committed and the rows are gone; this
/// test times out on that with zero rows and zero errors counted.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/clickhouse/proto_text.toml")),
    seed = seeds::connector_stream
)]
async fn given_a_proto_convert_transform_when_the_sink_consumes_should_store_the_rows(
    harness: &TestHarness,
    fixture: ClickHouseSinkFixture,
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
        .wait_for_rows(TEST_MESSAGE_COUNT)
        .await
        .expect("the proto text batch must be inserted, not skipped");

    let rows = fixture
        .fetch_rows()
        .await
        .expect("Failed to fetch rows from ClickHouse");

    assert_eq!(
        rows.len(),
        TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} rows in ClickHouse"
    );

    for (i, row) in rows.iter().enumerate() {
        let expected = &messages_data[i];

        let id = row["id"]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| row["id"].as_u64())
            .unwrap_or_else(|| panic!("Missing 'id' at row {i}"));
        assert_eq!(id, expected.id, "id mismatch at row {i}");

        let name = row["name"]
            .as_str()
            .unwrap_or_else(|| panic!("Missing 'name' at row {i}"));
        assert_eq!(name, expected.name, "name mismatch at row {i}");

        let amount = row["amount"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .or_else(|| row["amount"].as_f64())
            .unwrap_or_else(|| panic!("Missing 'amount' at row {i}"));
        assert!(
            (amount - expected.amount).abs() < 1e-6,
            "amount mismatch at row {i}: got {amount}, expected {}",
            expected.amount
        );
    }
}
