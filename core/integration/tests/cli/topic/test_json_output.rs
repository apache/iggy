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

use crate::cli::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::{Client, StreamCreateOptions, TopicCreateOptions};
use serde_json::Value;
use serial_test::parallel;

#[derive(Clone, Copy)]
enum Case {
    Get,
    List,
    Empty,
    Missing,
    Conflict,
    Table,
    TextList,
}

struct JsonCase {
    case: Case,
    quiet: bool,
    expected: Option<Value>,
}

#[async_trait]
impl IggyCmdTestCase for JsonCase {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        client
            .create_stream("json-stream", &StreamCreateOptions::default())
            .await
            .unwrap();
        if !matches!(self.case, Case::Empty | Case::Missing | Case::Conflict) {
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
        self.expected = match self.case {
            Case::Get => Some(
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
            Case::List | Case::Empty => Some(
                serde_json::to_value(
                    client
                        .get_topics(&"json-stream".try_into().unwrap())
                        .await
                        .unwrap(),
                )
                .unwrap(),
            ),
            _ => None,
        };
    }

    fn get_command(&self) -> IggyCmdCommand {
        let mut command = IggyCmdCommand::new().with_env_credentials();
        if self.quiet {
            command = command.opt("-q");
        }
        command = command.arg("topic");
        command = match self.case {
            Case::Get => command.args(vec!["get", "json-stream", "json-topic", "-o", "json"]),
            Case::Missing => {
                command.args(vec!["get", "json-stream", "missing-topic", "-o", "json"])
            }
            Case::List | Case::Empty => command.args(vec!["list", "json-stream", "-o", "json"]),
            Case::Conflict => command.args(vec!["list", "json-stream", "-l", "list", "-o", "json"]),
            Case::Table => command.args(vec!["list", "json-stream", "-o", "table"]),
            Case::TextList => command.args(vec!["list", "json-stream", "-o", "list"]),
        };
        command
    }

    fn verify_command(&self, command_state: Assert) {
        if matches!(self.case, Case::Missing | Case::Conflict) {
            let output = command_state.failure();
            assert!(output.get_output().stdout.is_empty());
            let stderr = String::from_utf8_lossy(&output.get_output().stderr);
            if matches!(self.case, Case::Missing) {
                assert!(stderr.contains("not found"), "{stderr}");
            } else {
                assert!(stderr.contains("cannot be used with"), "{stderr}");
            }
            return;
        }
        let output = command_state.success();
        let stdout = &output.get_output().stdout;
        if matches!(self.case, Case::Table | Case::TextList) {
            assert!(String::from_utf8_lossy(stdout).contains("json-topic"));
            return;
        }
        let value: Value = serde_json::from_slice(stdout).expect("stdout must be one JSON value");
        let mut expected = self.expected.as_ref().unwrap().clone();
        if matches!(self.case, Case::Get) {
            for (actual_partition, expected_partition) in value["partitions"]
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
            }
        }
        assert_eq!(value, expected);
        match self.case {
            Case::Get => assert_eq!(value["name"], "json-topic"),
            Case::List => assert!(
                value
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|item| item["name"] == "json-topic")
            ),
            Case::Empty => assert_eq!(value, serde_json::json!([])),
            _ => unreachable!(),
        }
        let stderr = String::from_utf8_lossy(&output.get_output().stderr);
        assert_eq!(stderr.contains("Executing "), !self.quiet, "{stderr}");
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        if !matches!(self.case, Case::Empty | Case::Missing | Case::Conflict) {
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
    let mut test = IggyCmdTest::default();
    test.setup().await;
    for case in [
        Case::Get,
        Case::List,
        Case::Empty,
        Case::Missing,
        Case::Conflict,
        Case::Table,
        Case::TextList,
    ] {
        for quiet in [false, true] {
            if quiet && matches!(case, Case::Table | Case::TextList) {
                continue;
            }
            test.execute_test(JsonCase {
                case,
                quiet,
                expected: None,
            })
            .await;
        }
    }
}
