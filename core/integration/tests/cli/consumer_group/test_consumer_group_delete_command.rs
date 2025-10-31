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
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestConsumerGroupId, TestHelpCmd,
    TestStreamId, TestTopicId, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use iggy::prelude::IggyExpiry;
use iggy::prelude::MaxTopicSize;
use predicates::str::diff;
use serial_test::parallel;

struct TestConsumerGroupDeleteCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    group_id: u32,
    group_name: String,
    using_stream_id: TestStreamId,
    using_topic_id: TestTopicId,
    using_group_id: TestConsumerGroupId,
}

impl TestConsumerGroupDeleteCmd {
    #[allow(clippy::too_many_arguments)]
    fn new(
        stream_id: u32,
        stream_name: String,
        topic_id: u32,
        topic_name: String,
        group_id: u32,
        group_name: String,
        using_stream_id: TestStreamId,
        using_topic_id: TestTopicId,
        using_group_id: TestConsumerGroupId,
    ) -> Self {
        Self {
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            group_id,
            group_name,
            using_stream_id,
            using_topic_id,
            using_group_id,
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

        command.push(match self.using_group_id {
            TestConsumerGroupId::Numeric => format!("{}", self.group_id),
            TestConsumerGroupId::Named => self.group_name.clone(),
        });

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestConsumerGroupDeleteCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client.create_stream(&self.stream_name).await;
        assert!(stream.is_ok());
        let stream_details = stream.unwrap();
        self.stream_id = stream_details.id;

        let topic = client
            .create_topic(
                &self.stream_id.try_into().unwrap(),
                &self.topic_name,
                1,
                Default::default(),
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await;
        assert!(topic.is_ok());
        let topic_details = topic.unwrap();
        self.topic_id = topic_details.id;

        let consumer_group = client
            .create_consumer_group(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
                &self.group_name,
            )
            .await;
        assert!(consumer_group.is_ok());
        let consumer_group_details = consumer_group.unwrap();
        self.group_id = consumer_group_details.id;
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("consumer-group")
            .arg("delete")
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

        let group_id = match self.using_group_id {
            TestConsumerGroupId::Numeric => format!("{}", self.group_id),
            TestConsumerGroupId::Named => self.group_name.clone(),
        };

        let message = format!(
            "Executing delete consumer group with ID: {group_id} for topic with ID: {topic_id} and stream with ID: {stream_id}\nConsumer group with ID: {group_id} deleted for topic with ID: {topic_id} and stream with ID: {stream_id}\n"
        );

        command_state.success().stdout(diff(message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let consumer_groups = client
            .get_consumer_groups(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
            )
            .await;
        assert!(consumer_groups.is_ok());
        let consumer_groups_details = consumer_groups.unwrap();
        assert_eq!(consumer_groups_details.len(), 0);

        let topic = client
            .delete_topic(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
            )
            .await;
        assert!(topic.is_ok());

        let stream = client
            .delete_stream(&self.stream_id.try_into().unwrap())
            .await;
        assert!(stream.is_ok());
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    let test_parameters = vec![
        (
            TestStreamId::Numeric,
            TestTopicId::Numeric,
            TestConsumerGroupId::Numeric,
        ),
        (
            TestStreamId::Numeric,
            TestTopicId::Numeric,
            TestConsumerGroupId::Named,
        ),
        (
            TestStreamId::Numeric,
            TestTopicId::Named,
            TestConsumerGroupId::Numeric,
        ),
        (
            TestStreamId::Numeric,
            TestTopicId::Named,
            TestConsumerGroupId::Named,
        ),
        (
            TestStreamId::Named,
            TestTopicId::Numeric,
            TestConsumerGroupId::Numeric,
        ),
        (
            TestStreamId::Named,
            TestTopicId::Numeric,
            TestConsumerGroupId::Named,
        ),
        (
            TestStreamId::Named,
            TestTopicId::Named,
            TestConsumerGroupId::Numeric,
        ),
        (
            TestStreamId::Named,
            TestTopicId::Named,
            TestConsumerGroupId::Named,
        ),
    ];

    iggy_cmd_test.setup().await;
    for (using_stream_id, using_topic_id, using_group_id) in test_parameters {
        iggy_cmd_test
            .execute_test(TestConsumerGroupDeleteCmd::new(
                1,
                String::from("stream"),
                2,
                String::from("topic"),
                3,
                String::from("consumer-group"),
                using_stream_id,
                using_topic_id,
                using_group_id,
            ))
            .await;
    }
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["consumer-group", "delete", "--help"],
            format!(
                r#"Delete consumer group with given ID for given stream ID and topic ID

Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID
Consumer group ID can be specified as a consumer group name or ID

Examples:
 iggy consumer-group delete 1 2 3
 iggy consumer-group delete stream 2 3
 iggy consumer-group delete 1 topic 3
 iggy consumer-group delete 1 2 group
 iggy consumer-group delete stream topic 3
 iggy consumer-group delete 1 topic group
 iggy consumer-group delete stream 2 group
 iggy consumer-group delete stream topic group

{USAGE_PREFIX} consumer-group delete <STREAM_ID> <TOPIC_ID> <GROUP_ID>

Arguments:
  <STREAM_ID>
          Stream ID to delete consumer group
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          Topic ID to delete consumer group
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

  <GROUP_ID>
          Consumer group ID to delete
{CLAP_INDENT}
          Consumer group ID can be specified as a consumer group name or ID

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
            vec!["consumer-group", "delete", "-h"],
            format!(
                r#"Delete consumer group with given ID for given stream ID and topic ID

{USAGE_PREFIX} consumer-group delete <STREAM_ID> <TOPIC_ID> <GROUP_ID>

Arguments:
  <STREAM_ID>  Stream ID to delete consumer group
  <TOPIC_ID>   Topic ID to delete consumer group
  <GROUP_ID>   Consumer group ID to delete

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
