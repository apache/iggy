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

use super::test_stream_create_command::{SERVER_KEY, encrypted_server};
use crate::cli::common::{
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, TestStreamId,
    USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use iggy::prelude::{StreamCreateOptions, StreamEncryptionKey};
use iggy_common::ServerKeyContext;
use predicates::str::{contains, starts_with};
use serial_test::parallel;

struct TestStreamGetCmd {
    stream_id: u32,
    stream_name: String,
    using_identifier: TestStreamId,
}

impl TestStreamGetCmd {
    fn new(stream_id: u32, name: String, using_identifier: TestStreamId) -> Self {
        Self {
            stream_id,
            stream_name: name,
            using_identifier,
        }
    }

    fn to_arg(&self) -> String {
        match self.using_identifier {
            TestStreamId::Named => self.stream_name.clone(),
            TestStreamId::Numeric => format!("{}", self.stream_id),
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestStreamGetCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client
            .create_stream(&self.stream_name, &StreamCreateOptions::default())
            .await;
        assert!(stream.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("stream")
            .arg("get")
            .arg(self.to_arg())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let start_message = match self.using_identifier {
            TestStreamId::Named => format!(
                "Executing get stream with ID: {}\n",
                self.stream_name.clone()
            ),
            TestStreamId::Numeric => format!("Executing get stream with ID: {}\n", self.stream_id),
        };

        command_state
            .success()
            .stdout(starts_with(start_message))
            .stdout(contains(format!(
                "Stream name          | {}",
                self.stream_name
            )))
            .stdout(contains("Stream size          | 0"))
            .stdout(contains("Stream message count | 0"))
            .stdout(contains("Stream topics count  | 0"))
            .stdout(contains("Encryption source    | none"));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

struct TestStreamGetEncryptedCmd {
    stream_name: String,
    key: StreamEncryptionKey,
}

#[async_trait]
impl IggyCmdTestCase for TestStreamGetEncryptedCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        client
            .create_stream(
                &self.stream_name,
                &StreamCreateOptions {
                    encryption_key: Some(self.key.clone()),
                    ..StreamCreateOptions::default()
                },
            )
            .await
            .expect("create encrypted stream");
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("stream")
            .arg("get")
            .arg(&self.stream_name)
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let kek_fingerprint = ServerKeyContext::from_config(SERVER_KEY, &[], true)
            .expect("server key")
            .kek_fingerprint()
            .expect("KEK fingerprint");
        command_state
            .success()
            .stdout(contains("Encryption source    | stream"))
            .stdout(contains(format!(
                "Key fingerprint      | {}",
                self.key.fingerprint()
            )))
            .stdout(contains(format!(
                "KEK fingerprints     | {kek_fingerprint}"
            )));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        client
            .delete_stream(&self.stream_name.clone().try_into().unwrap())
            .await
            .expect("delete");
    }
}

#[tokio::test]
#[parallel]
pub async fn given_encrypted_stream_when_getting_should_show_source_and_fingerprints() {
    let mut iggy_cmd_test = IggyCmdTest::with_server(encrypted_server());

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestStreamGetEncryptedCmd {
            stream_name: String::from("secured"),
            key: StreamEncryptionKey::try_from([3u8; 32].as_slice()).expect("valid key"),
        })
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestStreamGetCmd::new(
            0,
            String::from("production"),
            TestStreamId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestStreamGetCmd::new(
            0,
            String::from("testing"),
            TestStreamId::Named,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["stream", "get", "--help"],
            format!(
                r#"Get details of a single stream with given ID

Stream ID can be specified as a stream name or ID

Examples:
 iggy stream get 1
 iggy stream get test

{USAGE_PREFIX} stream get <STREAM_ID>

Arguments:
  <STREAM_ID>
          Stream ID to get
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

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
            vec!["stream", "get", "-h"],
            format!(
                r#"Get details of a single stream with given ID

{USAGE_PREFIX} stream get <STREAM_ID>

Arguments:
  <STREAM_ID>  Stream ID to get

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
