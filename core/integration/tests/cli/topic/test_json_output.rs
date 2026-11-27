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

use crate::cli::common::json_output::{JsonCase, JsonOutputTarget, run_json_output_cases};
use async_trait::async_trait;
use iggy::prelude::{Client, StreamCreateOptions, TopicCreateOptions};
use serde_json::Value;
use serial_test::parallel;

#[derive(Clone, Copy)]
struct TopicTarget;

#[async_trait]
impl JsonOutputTarget for TopicTarget {
    const ITEM_NAME: &'static str = "json-topic";

    async fn prepare(&self, case: JsonCase, client: &dyn Client) {
        client
            .create_stream("json-stream", &StreamCreateOptions::default())
            .await
            .unwrap();
        if !matches!(
            case,
            JsonCase::Empty | JsonCase::Missing | JsonCase::Conflict
        ) {
            client
                .create_topic(
                    &"json-stream".try_into().unwrap(),
                    "json-topic",
                    &TopicCreateOptions {
                        partitions_count: Some(1),
                        ..TopicCreateOptions::default()
                    },
                )
                .await
                .unwrap();
        }
    }

    async fn expected(&self, case: JsonCase, client: &dyn Client) -> Option<Value> {
        match case {
            JsonCase::Get => Some(
                serde_json::to_value(
                    client
                        .get_topic(
                            &"json-stream".try_into().unwrap(),
                            &"json-topic".try_into().unwrap(),
                        )
                        .await
                        .unwrap()
                        .unwrap(),
                )
                .unwrap(),
            ),
            JsonCase::List | JsonCase::Empty => Some(
                serde_json::to_value(
                    client
                        .get_topics(&"json-stream".try_into().unwrap())
                        .await
                        .unwrap(),
                )
                .unwrap(),
            ),
            _ => None,
        }
    }

    fn args(&self, case: JsonCase) -> Vec<&'static str> {
        let args = match case {
            JsonCase::Get => vec!["get", "json-stream", "json-topic", "-o", "json"],
            JsonCase::Missing => vec!["get", "json-stream", "missing-topic", "-o", "json"],
            JsonCase::List | JsonCase::Empty => vec!["list", "json-stream", "-o", "json"],
            JsonCase::Conflict => vec!["list", "json-stream", "-l", "list", "-o", "json"],
            JsonCase::Table => vec!["list", "json-stream", "-o", "table"],
            JsonCase::TextList => vec!["list", "json-stream", "-o", "list"],
        };
        [vec!["topic"], args].concat()
    }

    fn align_expected(&self, case: JsonCase, actual: &Value, expected: &mut Value) {
        if !matches!(case, JsonCase::Get) {
            return;
        }
        for (actual_partition, expected_partition) in actual["partitions"]
            .as_array()
            .unwrap()
            .iter()
            .zip(expected["partitions"].as_array_mut().unwrap())
        {
            let metadata_op = actual_partition["context"]["metadata_op"].as_u64().unwrap();
            assert!(
                metadata_op
                    >= expected_partition["context"]["metadata_op"]
                        .as_u64()
                        .unwrap()
            );
            expected_partition["context"]["metadata_op"] = metadata_op.into();

            // Replicas can join the ISR between the server read and the CLI run.
            let members = expected_partition["members"].as_array().unwrap();
            let in_sync_members = &actual_partition["in_sync_members"];
            assert!(
                in_sync_members
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|member| members.contains(member))
            );
            expected_partition["in_sync_members"] = in_sync_members.clone();
        }
    }

    async fn cleanup(&self, case: JsonCase, client: &dyn Client) {
        if !matches!(
            case,
            JsonCase::Empty | JsonCase::Missing | JsonCase::Conflict
        ) {
            client
                .delete_topic(
                    &"json-stream".try_into().unwrap(),
                    &"json-topic".try_into().unwrap(),
                )
                .await
                .unwrap();
        }
        client
            .delete_stream(&"json-stream".try_into().unwrap())
            .await
            .unwrap();
    }
}

#[tokio::test]
#[parallel]
async fn given_topic_output_when_requested_should_preserve_json_and_output_modes() {
    run_json_output_cases(TopicTarget).await;
}
