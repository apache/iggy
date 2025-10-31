/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::cli::common::{
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, TestStreamId,
    TestTopicId, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use iggy::prelude::IggyExpiry;
use iggy::prelude::MaxTopicSize;
use predicates::str::{contains, starts_with};
use serial_test::parallel;

struct TestTopicGetCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    using_stream_id: TestStreamId,
    using_topic_id: TestTopicId,
}

impl TestTopicGetCmd {
    fn new(
        stream_id: u32,
        stream_name: String,
        topic_id: u32,
        topic_name: String,
        using_stream_id: TestStreamId,
        using_topic_id: TestTopicId,
    ) -> Self {
        Self {
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            using_stream_id,
            using_topic_id,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut command = match self.using_stream_id {
            TestStreamId::Numeric => vec![format!("{}", self.stream_id)],
            TestStreamId::Named => vec![self.stream_name.clone()],
        };

        command.push(match self.using_topic_id {
            TestTopicId::Numeric => format!("{}", self.topic_id),
            TestTopicId::Named => self.topic_name.clone(),
        });

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestTopicGetCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client.create_stream(&self.stream_name).await;
        assert!(stream.is_ok());

        let topic = client
            .create_topic(
                &self.stream_name.clone().try_into().unwrap(),
                &self.topic_name,
                1,
                Default::default(),
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await;
        assert!(topic.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("topic")
            .arg("get")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let stream_id = match self.using_stream_id {
            TestStreamId::Numeric => format!("{}", self.stream_id),
            TestStreamId::Named => self.stream_name.clone(),
        };

        let topic_id = match self.using_topic_id {
            TestTopicId::Numeric => format!("{}", self.topic_id),
            TestTopicId::Named => self.topic_name.clone(),
        };

        let start_message =
            format!("Executing get topic with ID: {topic_id} from stream with ID: {stream_id}");

        command_state
            .success()
            .stdout(starts_with(start_message))
            .stdout(contains("Topic id            | 0"))
            .stdout(contains(format!(
                "Topic name          | {}",
                self.topic_name
            )))
            .stdout(contains("Topic size          | 0"))
            .stdout(contains("Message expiry      | unlimited"))
            .stdout(contains("Max topic size      | unlimited"))
            .stdout(contains("Topic message count | 0"))
            .stdout(contains("Partitions count    | 1"));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let topic = client
            .delete_topic(
                &self.stream_name.clone().try_into().unwrap(),
                &self.topic_name.clone().try_into().unwrap(),
            )
            .await;
        assert!(topic.is_ok());

        let stream = client
            .delete_stream(&self.stream_name.clone().try_into().unwrap())
            .await;
        assert!(stream.is_ok());
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestTopicGetCmd::new(
            0,
            String::from("main"),
            0,
            String::from("sync"),
            TestStreamId::Named,
            TestTopicId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicGetCmd::new(
            1,
            String::from("customer"),
            3,
            String::from("probe"),
            TestStreamId::Named,
            TestTopicId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicGetCmd::new(
            1,
            String::from("development"),
            2,
            String::from("testing"),
            TestStreamId::Named,
            TestTopicId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicGetCmd::new(
            6,
            String::from("other"),
            1,
            String::from("topic"),
            TestStreamId::Named,
            TestTopicId::Named,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["topic", "get", "--help"],
            format!(
                r#"Get topic detail for given topic ID and stream ID

Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID

Examples
 iggy topic get 1 1
 iggy topic get prod 2
 iggy topic get test debugs
 iggy topic get 2 debugs

{USAGE_PREFIX} topic get <STREAM_ID> <TOPIC_ID>

Arguments:
  <STREAM_ID>
          Stream ID to get topic
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          Topic ID to get
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

Options:
  -h, --help
          Print help (see a summary with '-h')
"#,
            ),
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_short_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["topic", "get", "-h"],
            format!(
                r#"Get topic detail for given topic ID and stream ID

{USAGE_PREFIX} topic get <STREAM_ID> <TOPIC_ID>

Arguments:
  <STREAM_ID>  Stream ID to get topic
  <TOPIC_ID>   Topic ID to get

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
