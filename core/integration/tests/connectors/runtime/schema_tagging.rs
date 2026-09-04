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

use apache_avro::Schema as AvroSchema;
use apache_avro::types::Value as AvroValue;
use apache_avro::writer::datum::GenericDatumWriter;
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use integration::harness::{TestHarness, seeds};
use integration::iggy_harness;
use std::time::{Duration, Instant};
use tokio::time::sleep;

// The runtime tags each batch with a `Schema` and the SDK rebuilds the sink's
// `Payload` from that tag alone. The Avro decoder returns JSON, so the tag has
// to say `json` even though the stream is configured `avro`. Asserting on the
// stdout sink's own log lines is the cheapest way to see what actually crossed
// the FFI boundary, and it needs no container.

const MESSAGE_COUNT: usize = 5;
const SCHEMA: &str = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/runtime/schema_tagging.toml")),
    seed = seeds::connector_stream
)]
async fn given_an_avro_stream_when_the_sink_consumes_should_receive_json_payloads(
    harness: &TestHarness,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let schema = AvroSchema::parse_str(SCHEMA).expect("failed to parse the Avro schema");
    let mut messages: Vec<IggyMessage> = (0..MESSAGE_COUNT)
        .map(|index| {
            IggyMessage::builder()
                .id((index + 1) as u128)
                .payload(Bytes::from(avro_datum(&schema, index as i64)))
                .build()
                .expect("failed to build the message")
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
        .expect("failed to send the messages");

    let logs = wait_for_sink_batch(harness).await;

    assert!(
        logs.contains("schema: json,"),
        "the sink should be told the batch is JSON, got:\n{logs}"
    );
    assert!(
        !logs.contains("schema: avro,"),
        "the batch must not be tagged with the decoder's schema, got:\n{logs}"
    );
    assert!(
        !logs.contains("Avro("),
        "the sink must not receive a Payload::Avro, got:\n{logs}"
    );
    assert!(
        logs.contains("Json("),
        "the sink should receive a Payload::Json, got:\n{logs}"
    );
    for index in 0..MESSAGE_COUNT {
        let name = format!("row-{index}");
        assert!(
            logs.contains(&name),
            "decoded payload should carry '{name}', got:\n{logs}"
        );
    }
}

/// Bare Avro datum, not an object container file: the decoder reads with
/// `GenericDatumReader::read_value` and rejects trailing bytes.
fn avro_datum(schema: &AvroSchema, id: i64) -> Vec<u8> {
    let record = AvroValue::Record(vec![
        ("id".to_owned(), AvroValue::Long(id)),
        ("name".to_owned(), AvroValue::String(format!("row-{id}"))),
    ]);
    GenericDatumWriter::builder(schema)
        .build()
        .expect("failed to build the Avro writer")
        .write_value_to_vec(record)
        .expect("failed to encode the Avro datum")
}

async fn wait_for_sink_batch(harness: &TestHarness) -> String {
    let runtime = harness
        .connectors_runtime()
        .expect("connector runtime should be available");
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut logs = String::new();

    while Instant::now() < deadline {
        let (stdout, stderr) = runtime.collect_logs();
        logs = format!("{stdout}\n{stderr}");
        if logs.contains("Stdout sink with ID:") && logs.contains("received:") {
            return logs;
        }
        sleep(Duration::from_millis(200)).await;
    }

    panic!("the stdout sink never reported a batch. logs:\n{logs}");
}
