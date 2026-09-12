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

use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::{Identifier, MessageClient};
use integration::{harness::seeds, iggy_harness};

use crate::connectors::fixtures::OpenDalSinkFixture;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/opendal/sink.toml")),
    seed = seeds::connector_stream
)]
async fn json_messages_sink_to_opendal(harness: &TestHarness, fixture: OpenDalSinkFixture) {
    let client = harness.root_client().await.expect("client should connect");
    let stream_id: Identifier = seeds::names::STREAM
        .try_into()
        .expect("stream identifier should be valid");
    let topic_id: Identifier = seeds::names::TOPIC
        .try_into()
        .expect("topic identifier should be valid");
    let payloads = [
        Bytes::from_static(br#"{"id":1}"#),
        Bytes::from_static(br#"{"id":2}"#),
    ];
    let mut messages: Vec<IggyMessage> = payloads
        .iter()
        .enumerate()
        .map(|(index, payload)| {
            IggyMessage::builder()
                .id((index + 1) as u128)
                .payload(payload.clone())
                .build()
                .expect("message should build")
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
        .expect("messages should be sent");

    for (offset, expected) in payloads.iter().enumerate() {
        let stored = fixture
            .wait_for_object(offset as u64)
            .await
            .expect("OpenDAL object should be written");
        assert_eq!(stored, expected.as_ref());
    }
}
