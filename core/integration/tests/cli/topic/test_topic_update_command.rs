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

use crate::cli::common::{
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, TestStreamId,
    TestTopicId, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use humantime::Duration as HumanDuration;
use iggy::prelude::{
    Client, HeaderKey, IggyByteSize, IggyExpiry, MaxTopicSize, OptionKey, StreamCreateOptions,
    TopicCreateOptions,
};
use predicates::str::diff;
use serial_test::parallel;
use std::str::FromStr;
use std::time::Duration;

struct TestTopicUpdateCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    message_expiry: Option<Vec<String>>,
    max_topic_size: MaxTopicSize,
    new_name: Option<String>,
    new_message_expiry: Option<Vec<String>>,
    new_max_topic_size: Option<MaxTopicSize>,
    reset: Vec<OptionKey>,
    using_stream_id: TestStreamId,
    using_topic_id: TestTopicId,
}

impl TestTopicUpdateCmd {
    fn new(stream_id: u32, stream_name: &str, topic_id: u32, topic_name: &str) -> Self {
        Self {
            stream_id,
            stream_name: stream_name.to_owned(),
            topic_id,
            topic_name: topic_name.to_owned(),
            message_expiry: None,
            max_topic_size: MaxTopicSize::ServerDefault,
            new_name: None,
            new_message_expiry: None,
            new_max_topic_size: None,
            reset: Vec::new(),
            using_stream_id: TestStreamId::Named,
            using_topic_id: TestTopicId::Named,
        }
    }

    fn final_name(&self) -> &str {
        self.new_name.as_deref().unwrap_or(&self.topic_name)
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
        if let Some(name) = &self.new_name {
            command.push(format!("--name={name}"));
        }
        if let Some(max_topic_size) = &self.new_max_topic_size {
            command.push(format!(
                "--max-topic-size={}",
                max_topic_size.as_bytes_u64()
            ));
        }
        if let Some(message_expiry) = &self.new_message_expiry {
            command.push("--message-expiry".to_owned());
            command.extend(message_expiry.clone());
        }
        for key in &self.reset {
            command.push(format!("--reset={key}"));
        }
        command
    }

    fn changes(&self) -> String {
        let mut changes = Vec::new();
        if let Some(name) = &self.new_name {
            changes.push(format!("name: {name}"));
        }
        if let Some(message_expiry) = &self.new_message_expiry {
            changes.push(format!("message expiry: {}", expiry(message_expiry)));
        }
        if let Some(max_topic_size) = &self.new_max_topic_size {
            changes.push(format!("max topic size: {max_topic_size}"));
        }
        for key in &self.reset {
            changes.push(format!("reset: {key}"));
        }
        changes.join(", ")
    }
}

fn expiry(parts: &[String]) -> IggyExpiry {
    let duration: Duration = *parts.join(" ").parse::<HumanDuration>().unwrap();
    IggyExpiry::ExpireDuration(duration.into())
}

#[async_trait]
impl IggyCmdTestCase for TestTopicUpdateCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client
            .create_stream(&self.stream_name, &StreamCreateOptions::default())
            .await;
        assert!(stream.is_ok());

        let topic = client
            .create_topic(
                &self.stream_name.clone().try_into().unwrap(),
                &self.topic_name,
                &TopicCreateOptions {
                    partitions_count: Some(1),
                    message_expiry: self.message_expiry.as_deref().map(expiry),
                    max_topic_size: (self.max_topic_size != MaxTopicSize::ServerDefault)
                        .then_some(self.max_topic_size),
                    ..TopicCreateOptions::default()
                },
            )
            .await;
        assert!(topic.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("topic")
            .arg("update")
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
        let changes = self.changes();
        let expected_message = format!(
            "Executing update topic with ID: {topic_id} ({changes}) in stream with ID: {stream_id}\n\
             Topic with ID: {topic_id} updated ({changes}) in stream with ID: {stream_id}\n"
        );

        command_state.success().stdout(diff(expected_message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let topic = client
            .get_topic(
                &self.stream_name.clone().try_into().unwrap(),
                &self.final_name().try_into().unwrap(),
            )
            .await;
        assert!(topic.is_ok());
        let topic_details = topic.unwrap().expect("Failed to get topic");
        assert_eq!(topic_details.name, self.final_name());
        let resolved = topic_details.resolved_options();

        let expected_expiry = self
            .new_message_expiry
            .as_deref()
            .or(self.message_expiry.as_deref())
            .filter(|_| !self.reset.contains(&OptionKey::MessageExpiry))
            .map_or(IggyExpiry::NeverExpire, expiry);
        assert_eq!(resolved.message_expiry, expected_expiry);
        let expected_max_topic_size = self
            .new_max_topic_size
            .or((self.max_topic_size != MaxTopicSize::ServerDefault).then_some(self.max_topic_size))
            .filter(|_| !self.reset.contains(&OptionKey::MaxTopicSize))
            .unwrap_or(MaxTopicSize::Unlimited);
        assert_eq!(resolved.max_topic_size, expected_max_topic_size);
        for key in &self.reset {
            let option = topic_details
                .options
                .get(&HeaderKey::from(*key))
                .expect("a reset key holds its default");
            assert!(!option.explicit, "{key} must read back as derived");
        }

        let topic = client
            .delete_topic(
                &self.stream_name.clone().try_into().unwrap(),
                &self.final_name().try_into().unwrap(),
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
        .execute_test(TestTopicUpdateCmd {
            new_name: Some("new_name".to_owned()),
            new_max_topic_size: Some(MaxTopicSize::Custom(
                IggyByteSize::from_str("2GiB").unwrap(),
            )),
            ..TestTopicUpdateCmd::new(0, "main", 0, "sync")
        })
        .await;
    iggy_cmd_test
        .execute_test(TestTopicUpdateCmd {
            new_name: Some("testing".to_owned()),
            new_message_expiry: Some(vec!["1day".to_owned(), "1hour".to_owned()]),
            ..TestTopicUpdateCmd::new(1, "production", 0, "topic")
        })
        .await;
}

#[tokio::test]
#[parallel]
pub async fn given_topic_reset_when_using_cli_should_send_only_requested_keys() {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestTopicUpdateCmd {
            message_expiry: Some(vec!["10m".to_owned()]),
            max_topic_size: MaxTopicSize::Custom(IggyByteSize::from_str("5GiB").unwrap()),
            reset: vec![OptionKey::MaxTopicSize],
            ..TestTopicUpdateCmd::new(2, "reset-stream", 0, "reset-topic")
        })
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["topic", "update", "--help"],
            format!(
                r#"Update topic name, message expiry or max topic size for given topic ID in given stream ID

Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID
Only the settings passed change, every other setting keeps its value.
Compression, segment size, durability and the other create-time settings cannot change.

Examples
 iggy topic update 1 1 --name sensor3
 iggy topic update prod sensor3 --name old-sensor --message-expiry 15days
 iggy topic update test debugs --max-topic-size 15GB
 iggy topic update 1 2 --message-expiry 1day 1hour 1min 1sec
 iggy topic update 1 2 --reset message_expiry --reset max_topic_size

{USAGE_PREFIX} topic update [OPTIONS] <STREAM_ID> <TOPIC_ID>

Arguments:
  <STREAM_ID>
          Stream ID to update topic
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          Topic ID to update
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

Options:
  -n, --name <NAME>
          New name for the topic

  -m, --max-topic-size <MAX_TOPIC_SIZE>
          New max topic size in human-readable format like "unlimited" or "15GB"
{CLAP_INDENT}
          A finite size cannot be lower than the topic segment size.

  -e, --message-expiry <MESSAGE_EXPIRY>...
          New message expiry time in human-readable format like "unlimited" or "15days 2min 2s"

      --set <KEY=VALUE>
          Updatable topic option as key=value, repeatable
{CLAP_INDENT}
          Values are sent as strings and parsed server-side.
          Run "iggy options topic" to list the keys and which ones can change.

  -r, --reset <RESET>
          Setting to return to its default, like message_expiry or max_topic_size
{CLAP_INDENT}
          Repeat the flag to reset several settings.
          The server refuses a key that cannot be reset.

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
            vec!["topic", "update", "-h"],
            format!(
                r#"Update topic name, message expiry or max topic size for given topic ID in given stream ID

{USAGE_PREFIX} topic update [OPTIONS] <STREAM_ID> <TOPIC_ID>

Arguments:
  <STREAM_ID>  Stream ID to update topic
  <TOPIC_ID>   Topic ID to update

Options:
  -n, --name <NAME>                         New name for the topic
  -m, --max-topic-size <MAX_TOPIC_SIZE>     New max topic size in human-readable format like "unlimited" or "15GB"
  -e, --message-expiry <MESSAGE_EXPIRY>...  New message expiry time in human-readable format like "unlimited" or "15days 2min 2s"
      --set <KEY=VALUE>                     Updatable topic option as key=value, repeatable
  -r, --reset <RESET>                       Setting to return to its default, like message_expiry or max_topic_size
  -h, --help                                Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
