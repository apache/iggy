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

use crate::connectors::create_test_messages;
use crate::connectors::fixtures::{DorisOps, DorisSinkPreCreatedFixture};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use integration::harness::seeds;
use integration::iggy_harness;

const TEST_TABLE: &str = "test_topic";

/// A `proto_convert` transform with no descriptor falls back to proto text, so
/// every message reaches the sink as `Payload::Proto` holding the JSON it was
/// given. The sink loads that as the document it holds. A sink that only accepts
/// `Payload::Json` aborts the whole poll on the first message, so no chunk is
/// written and the committed offset is not replayed; this test times out on
/// that with zero rows.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/doris/proto_text.toml")),
    seed = seeds::connector_stream
)]
async fn given_proto_text_messages_should_store(
    harness: &TestHarness,
    fixture: DorisSinkPreCreatedFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let message_count = 10;
    let test_messages = create_test_messages(message_count);
    let mut messages: Vec<IggyMessage> = test_messages
        .iter()
        .enumerate()
        .map(|(i, m)| {
            let payload = Bytes::from(serde_json::to_vec(m).expect("serialize"));
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(payload)
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

    let count = fixture
        .wait_for_rows(fixture.database(), TEST_TABLE, message_count as i64)
        .await
        .expect("the proto text batch must be loaded, not aborted");
    assert_eq!(count, message_count as i64);
}
