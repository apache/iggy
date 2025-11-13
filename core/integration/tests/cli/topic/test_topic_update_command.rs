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
use humantime::Duration as HumanDuration;
use iggy::prelude::Client;
use iggy::prelude::CompressionAlgorithm;
use iggy::prelude::IggyByteSize;
use iggy::prelude::IggyExpiry;
use iggy::prelude::MaxTopicSize;
use predicates::str::diff;
use serial_test::parallel;
use std::str::FromStr;
use std::time::Duration;

struct TestTopicUpdateCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    compression_algorithm: CompressionAlgorithm,
    message_expiry: Option<Vec<String>>,
    max_topic_size: MaxTopicSize,
    replication_factor: u8,
    topic_new_name: String,
    topic_new_compression_algorithm: CompressionAlgorithm,
    topic_new_message_expiry: Option<Vec<String>>,
    topic_new_max_size: MaxTopicSize,
    topic_new_replication_factor: u8,
    using_stream_id: TestStreamId,
    using_topic_id: TestTopicId,
}

impl TestTopicUpdateCmd {
    #[allow(clippy::too_many_arguments)]
    fn new(
        stream_id: u32,
        stream_name: String,
        topic_id: u32,
        topic_name: String,
        compression_algorithm: CompressionAlgorithm,
        message_expiry: Option<Vec<String>>,
        max_topic_size: MaxTopicSize,
        replication_factor: u8,
        topic_new_name: String,
        topic_new_compression_algorithm: CompressionAlgorithm,
        topic_new_message_expiry: Option<Vec<String>>,
        topic_new_max_size: MaxTopicSize,
        topic_new_replication_factor: u8,
        using_stream_id: TestStreamId,
        using_topic_id: TestTopicId,
    ) -> Self {
        Self {
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            compression_algorithm,
            message_expiry,
            max_topic_size,
            replication_factor,
            topic_new_name,
            topic_new_compression_algorithm,
            topic_new_message_expiry,
            topic_new_max_size,
            topic_new_replication_factor,
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

        command.push(self.topic_new_name.clone());
        command.push(self.topic_new_compression_algorithm.to_string());
        command.push(format!("--max-topic-size={}", self.topic_new_max_size));

        if self.topic_new_replication_factor != 1 {
            command.push(format!(
                "--replication-factor={}",
                self.topic_new_replication_factor
            ));
        }

        if let Some(message_expiry) = &self.topic_new_message_expiry {
            command.extend(message_expiry.clone());
        }

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestTopicUpdateCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client.create_stream(&self.stream_name).await;
        assert!(stream.is_ok());

        let message_expiry = match &self.message_expiry {
            None => IggyExpiry::ServerDefault,
            Some(message_expiry) => {
                let duration: Duration =
                    *message_expiry.join(" ").parse::<HumanDuration>().unwrap();

                IggyExpiry::ExpireDuration(duration.into())
            }
        };

        let topic = client
            .create_topic(
                &self.stream_name.clone().try_into().unwrap(),
                &self.topic_name,
                1,
                self.compression_algorithm,
                Some(self.replication_factor),
                message_expiry,
                self.max_topic_size,
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

        let compression_algorithm = &self.topic_new_compression_algorithm;

        let message_expiry = (match &self.topic_new_message_expiry {
            Some(value) => value.join(" "),
            None => IggyExpiry::ServerDefault.to_string(),
        })
        .to_string();

        let replication_factor = self.topic_new_replication_factor;
        let new_topic_name = &self.topic_new_name;
        let new_max_topic_size = self.topic_new_max_size.to_string();

        let expected_message = format!(
            "Executing update topic with ID: {topic_id}, name: {new_topic_name}, \
                                message expiry: {message_expiry}, compression algorithm: {compression_algorithm}, max topic size: {new_max_topic_size}, \
                                replication factor: {replication_factor}, in stream with ID: {stream_id}\n\
                                Topic with ID: {topic_id} updated name: {new_topic_name}, updated message expiry: {message_expiry}, \
                                updated compression algorithm: {compression_algorithm}, updated max topic size: {new_max_topic_size}, \
                                updated replication factor: {replication_factor} in stream with ID: {stream_id}\n"
        );

        command_state.success().stdout(diff(expected_message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let topic = client
            .get_topic(
                &self.stream_name.clone().try_into().unwrap(),
                &self.topic_new_name.clone().try_into().unwrap(),
            )
            .await;
        assert!(topic.is_ok());
        let topic_details = topic.unwrap().expect("Failed to get topic");
        assert_eq!(topic_details.name, self.topic_new_name);
        assert_eq!(topic_details.messages_count, 0);

        if self.topic_new_message_expiry.is_some() {
            let duration: Duration = *self
                .topic_new_message_expiry
                .clone()
                .unwrap()
                .join(" ")
                .parse::<HumanDuration>()
                .unwrap();
            assert_eq!(
                topic_details.message_expiry,
                IggyExpiry::ExpireDuration(duration.into())
            );
        }

        let topic = client
            .delete_topic(
                &self.stream_name.clone().try_into().unwrap(),
                &self.topic_new_name.clone().try_into().unwrap(),
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
        .execute_test(TestTopicUpdateCmd::new(
            0,
            String::from("main"),
            0,
            String::from("sync"),
            Default::default(),
            None,
            MaxTopicSize::ServerDefault,
            1,
            String::from("new_name"),
            CompressionAlgorithm::Gzip,
            None,
            MaxTopicSize::Custom(IggyByteSize::from_str("2GiB").unwrap()),
            1,
            TestStreamId::Named,
            TestTopicId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicUpdateCmd::new(
            1,
            String::from("production"),
            0,
            String::from("topic"),
            Default::default(),
            None,
            MaxTopicSize::ServerDefault,
            1,
            String::from("testing"),
            CompressionAlgorithm::Gzip,
            None,
            MaxTopicSize::Unlimited,
            1,
            TestStreamId::Named,
            TestTopicId::Named,
        ))
        .await;
    // There used to be 3 more test cases, but they don't fit anymore the test scenario
    // Previously we allowed to update topic name to an already existing one, which is now forbidden.

    // Imagine a scenario where user tries to use any of our APIs with the `Identifier::named` variant for topic identifier
    // How to distinguish between topics with same name.
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["topic", "update", "--help"],
            format!(
                r#"Update topic name, compression algorithm and message expiry time for given topic ID in given stream ID

Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID

Examples
 iggy update 1 1 sensor3 none
 iggy update prod sensor3 old-sensor none
 iggy update test debugs ready gzip 15days
 iggy update 1 1 new-name gzip
 iggy update 1 2 new-name none 1day 1hour 1min 1sec

{USAGE_PREFIX} topic update [OPTIONS] <STREAM_ID> <TOPIC_ID> <NAME> <COMPRESSION_ALGORITHM> [MESSAGE_EXPIRY]...

Arguments:
  <STREAM_ID>
          Stream ID to update topic
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          Topic ID to update
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

  <NAME>
          New name for the topic

  <COMPRESSION_ALGORITHM>
          Compression algorithm for the topic, set to "none" for no compression

  [MESSAGE_EXPIRY]...
          New message expiry time in human-readable format like "unlimited" or "15days 2min 2s"
{CLAP_INDENT}
          "server_default" or skipping parameter makes CLI to use server default (from current server config) expiry time
{CLAP_INDENT}
          [default: server_default]

Options:
  -m, --max-topic-size <MAX_TOPIC_SIZE>
          New max topic size in human-readable format like "unlimited" or "15GB"
{CLAP_INDENT}
          "server_default" or skipping parameter makes CLI to use server default (from current server config) max topic size
          Can't be lower than segment size in the config.
{CLAP_INDENT}
          [default: server_default]

  -r, --replication-factor <REPLICATION_FACTOR>
          New replication factor for the topic
{CLAP_INDENT}
          [default: 1]

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
                r#"Update topic name, compression algorithm and message expiry time for given topic ID in given stream ID

{USAGE_PREFIX} topic update [OPTIONS] <STREAM_ID> <TOPIC_ID> <NAME> <COMPRESSION_ALGORITHM> [MESSAGE_EXPIRY]...

Arguments:
  <STREAM_ID>              Stream ID to update topic
  <TOPIC_ID>               Topic ID to update
  <NAME>                   New name for the topic
  <COMPRESSION_ALGORITHM>  Compression algorithm for the topic, set to "none" for no compression
  [MESSAGE_EXPIRY]...      New message expiry time in human-readable format like "unlimited" or "15days 2min 2s" [default: server_default]

Options:
  -m, --max-topic-size <MAX_TOPIC_SIZE>          New max topic size in human-readable format like "unlimited" or "15GB" [default: server_default]
  -r, --replication-factor <REPLICATION_FACTOR>  New replication factor for the topic [default: 1]
  -h, --help                                     Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
