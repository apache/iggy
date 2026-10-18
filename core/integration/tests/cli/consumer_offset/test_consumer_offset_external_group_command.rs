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
use iggy::prelude::*;
use predicates::str::{contains, starts_with};
use serial_test::parallel;

const STREAM_NAME: &str = "kafka";
const TOPIC_NAME: &str = "orders";
const GROUP_NAME: &str = "billing";
const PARTITION_ID: u32 = 0;
// The partition holds no message, and an external group offset has no range check.
const STORED_OFFSET: u64 = 1 << 40;

enum Step {
    Set,
    Get,
}

/// One step of a set-then-get round trip through `--kind external-group`. The
/// set step creates the stream, topic and Iggy consumer group the offset is
/// keyed by, and the get step removes them.
struct TestExternalGroupOffsetCmd {
    step: Step,
}

fn stream_id() -> Identifier {
    Identifier::named(STREAM_NAME).unwrap()
}

fn topic_id() -> Identifier {
    Identifier::named(TOPIC_NAME).unwrap()
}

#[async_trait]
impl IggyCmdTestCase for TestExternalGroupOffsetCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        if matches!(self.step, Step::Get) {
            return;
        }
        client.create_stream(STREAM_NAME).await.unwrap();
        client
            .create_topic(
                &stream_id(),
                TOPIC_NAME,
                &TopicCreateOptions {
                    partitions_count: Some(1),
                    message_expiry: Some(IggyExpiry::NeverExpire),
                    ..TopicCreateOptions::default()
                },
            )
            .await
            .unwrap();
        client
            .create_consumer_group(&stream_id(), &topic_id(), GROUP_NAME)
            .await
            .unwrap();
    }

    fn get_command(&self) -> IggyCmdCommand {
        let partition_id = PARTITION_ID.to_string();
        let offset = STORED_OFFSET.to_string();
        let args = match self.step {
            Step::Set => vec![
                "set",
                GROUP_NAME,
                STREAM_NAME,
                TOPIC_NAME,
                &partition_id,
                &offset,
            ],
            Step::Get => vec!["get", GROUP_NAME, STREAM_NAME, TOPIC_NAME, &partition_id],
        };
        IggyCmdCommand::new()
            .arg("consumer-offset")
            .args(args)
            .arg("--kind")
            .arg("external-group")
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        match self.step {
            Step::Set => {
                command_state.success();
            }
            Step::Get => {
                command_state
                    .success()
                    .stdout(starts_with(format!(
                        "Executing get consumer offset for external group with ID: {GROUP_NAME}"
                    )))
                    .stdout(contains(format!("Stored offset  | {STORED_OFFSET}")));
            }
        }
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        match self.step {
            Step::Set => {
                let offset = client
                    .get_consumer_offset(
                        &Consumer::external_group(Identifier::named(GROUP_NAME).unwrap()),
                        &stream_id(),
                        &topic_id(),
                        Some(PARTITION_ID),
                    )
                    .await
                    .unwrap()
                    .expect("the CLI stored the external group offset");
                assert_eq!(offset.stored_offset, STORED_OFFSET);
            }
            Step::Get => {
                client.delete_stream(&stream_id()).await.unwrap();
            }
        }
    }
}

#[tokio::test]
#[parallel]
pub async fn given_external_group_kind_when_set_offset_is_read_back_should_show_it() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestExternalGroupOffsetCmd { step: Step::Set })
        .await;
    iggy_cmd_test
        .execute_test(TestExternalGroupOffsetCmd { step: Step::Get })
        .await;
}
