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

//! Airflow sink connector integration tests (WireMock as Airflow REST).

use super::TEST_MESSAGE_COUNT;
use crate::connectors::fixtures::AirflowSinkFixture;
use bytes::Bytes;
use iggy_common::{Identifier, IggyMessage, MessageClient, Partitioning};
use integration::harness::seeds;
use integration::iggy_harness;

/// Publishes JSON messages and asserts the poll becomes one DAG-run POST with conf.messages.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/airflow/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_json_messages_when_consumed_should_post_one_batch_dag_run(
    harness: &TestHarness,
    fixture: AirflowSinkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let json_payloads: Vec<serde_json::Value> = vec![
        serde_json::json!({"order_id": 1, "status": "new"}),
        serde_json::json!({"order_id": 2, "status": "paid"}),
        serde_json::json!({"order_id": 3, "status": "shipped"}),
    ];

    let mut messages: Vec<IggyMessage> = json_payloads
        .iter()
        .enumerate()
        .map(|(i, payload)| {
            let bytes = serde_json::to_vec(payload).expect("serialize");
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(bytes))
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

    // Batch design: N messages in one poll => one DAG-run POST, not N.
    let requests = fixture
        .container()
        .wait_for_dag_run_requests(1)
        .await
        .expect("WireMock did not receive expected DAG-run POST");

    assert_eq!(requests.len(), 1);

    let req = &requests[0];
    assert_eq!(req.method, "POST");
    assert!(
        req.url.contains("/api/v1/dags/example_dag/dagRuns"),
        "unexpected url: {}",
        req.url
    );

    let body = req.body_as_json().expect("JSON body");
    assert!(
        body.get("dag_run_id")
            .and_then(|v| v.as_str())
            .is_some_and(|id| id.starts_with("iggy-")),
        "missing deterministic batch dag_run_id: {body}"
    );

    let conf = body
        .get("conf")
        .expect("conf field required")
        .as_object()
        .expect("conf object");

    let batch = conf
        .get("messages")
        .and_then(|v| v.as_array())
        .expect("conf.messages array required for batch trigger");
    assert_eq!(
        batch.len(),
        TEST_MESSAGE_COUNT,
        "expected all polled messages in one DAG run conf: {body}"
    );
    assert_eq!(batch[0]["payload"]["order_id"], 1);
    assert_eq!(batch[1]["payload"]["order_id"], 2);
    assert_eq!(batch[2]["payload"]["order_id"], 3);

    assert!(
        conf.get("iggy").is_some(),
        "include_iggy_metadata_in_conf should nest conf.iggy: {body}"
    );
    assert_eq!(conf["iggy"]["message_count"], TEST_MESSAGE_COUNT as u64);
}
