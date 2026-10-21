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
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use iggy::prelude::{Client, StreamEncryptionKey, StreamEncryptionSource};
use iggy_common::ServerKeyContext;
use integration::harness::{EncryptionConfig, TestServerConfig};
use predicates::prelude::PredicateBooleanExt;
use predicates::str::{contains, diff};
use serial_test::parallel;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

pub(super) const SERVER_KEY: &str = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=";
const STREAM_KEY_ENV: &str = "IGGY_STREAM_ENCRYPTION_KEY";

struct TestStreamCreateCmd {
    stream_id: Option<u32>,
    stream_name: String,
}

impl TestStreamCreateCmd {
    fn new(stream_id: Option<u32>, name: String) -> Self {
        Self {
            stream_id,
            stream_name: name,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut args = Vec::new();

        if let Some(stream_id) = self.stream_id {
            args.push("-s".to_string());
            args.push(format!("{stream_id}"));
        }

        args.push(self.stream_name.clone());

        args
    }
}

#[async_trait]
impl IggyCmdTestCase for TestStreamCreateCmd {
    async fn prepare_server_state(&mut self, _client: &dyn Client) {}

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("stream")
            .arg("create")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let stream_id = "ID auto incremented";

        let message = format!(
            "Executing create stream with name: {} and {}\nStream with name: {} and {} created\n",
            self.stream_name, stream_id, self.stream_name, stream_id
        );

        command_state.success().stdout(diff(message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let stream = client
            .get_stream(&self.stream_name.clone().try_into().unwrap())
            .await;
        assert!(stream.is_ok());
        let stream = stream.unwrap().expect("Stream not found");
        assert_eq!(stream.name, self.stream_name);

        let delete = client
            .delete_stream(&self.stream_name.clone().try_into().unwrap())
            .await;
        assert!(delete.is_ok());
    }
}

struct TestStreamCreateWithKeyCmd {
    stream_name: String,
    _key_directory: TempDir,
    key_file: Option<PathBuf>,
    file_key: Option<String>,
    environment_key: Option<String>,
}

impl TestStreamCreateWithKeyCmd {
    fn new(stream_name: &str, file_key: Option<String>, environment_key: Option<String>) -> Self {
        let key_directory = TempDir::new().expect("key directory");
        let key_file = file_key.as_ref().map(|key| {
            let key_file = key_directory.path().join("stream.key");
            fs::write(&key_file, format!("{key}\n")).expect("key file");
            key_file
        });
        Self {
            stream_name: stream_name.to_owned(),
            _key_directory: key_directory,
            key_file,
            file_key,
            environment_key,
        }
    }

    /// The key the server must hold: the file wins over the environment, and a malformed one is refused.
    fn applied_key(&self) -> Option<StreamEncryptionKey> {
        self.file_key
            .as_ref()
            .or(self.environment_key.as_ref())
            .and_then(|text| text.parse().ok())
    }

    fn secrets(&self) -> Vec<String> {
        self.file_key
            .iter()
            .chain(self.environment_key.iter())
            .cloned()
            .collect()
    }
}

#[async_trait]
impl IggyCmdTestCase for TestStreamCreateWithKeyCmd {
    async fn prepare_server_state(&mut self, _client: &dyn Client) {}

    fn get_command(&self) -> IggyCmdCommand {
        let mut command = IggyCmdCommand::new().arg("stream").arg("create");
        if let Some(key_file) = &self.key_file {
            command = command
                .arg("--encryption-key-file")
                .arg(key_file.to_str().expect("UTF-8 path"));
        }
        let command = command.arg(&self.stream_name).with_env_credentials();
        match &self.environment_key {
            Some(key) => command.env(STREAM_KEY_ENV, key),
            None => command,
        }
    }

    fn verify_command(&self, command_state: Assert) {
        let mut command_state = match self.applied_key() {
            Some(key) => {
                let kek_fingerprint = ServerKeyContext::from_config(SERVER_KEY, &[], true)
                    .expect("server key")
                    .kek_fingerprint()
                    .expect("KEK fingerprint");
                let name = &self.stream_name;
                let message = format!(
                    "Executing create stream with name: {name} and ID auto incremented\n\
                     Stream with name: {name} and ID auto incremented created\n\
                     Stream encryption key source: stream, fingerprint: {}, KEK fingerprints: {kek_fingerprint}\n",
                    key.fingerprint()
                );
                command_state.success().stdout(diff(message))
            }
            None => command_state.failure().stderr(contains(
                "Stream encryption key must be standard base64 of exactly 32 bytes",
            )),
        };
        for secret in self.secrets() {
            command_state = command_state
                .stdout(contains(secret.as_str()).not())
                .stderr(contains(secret).not());
        }
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let stream_id = self.stream_name.clone().try_into().unwrap();
        let stream = client.get_stream(&stream_id).await.expect("get stream");
        let Some(key) = self.applied_key() else {
            assert!(stream.is_none(), "a refused key must not create the stream");
            return;
        };
        let stream = stream.expect("stream exists");
        assert_eq!(
            stream.encryption_key_source(),
            Some(StreamEncryptionSource::Stream)
        );
        assert_eq!(stream.encryption_key_fingerprint(), Some(key.fingerprint()));
        client.delete_stream(&stream_id).await.expect("delete");
    }
}

#[tokio::test]
#[parallel]
pub async fn given_key_file_when_creating_stream_then_should_show_only_projections() {
    let mut iggy_cmd_test = IggyCmdTest::with_server(encrypted_server());

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestStreamCreateWithKeyCmd::new(
            "secured",
            Some(STANDARD.encode([7; 32])),
            None,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn given_environment_key_only_when_creating_stream_then_should_apply_it() {
    let mut iggy_cmd_test = IggyCmdTest::with_server(encrypted_server());

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestStreamCreateWithKeyCmd::new(
            "from-environment",
            None,
            Some(STANDARD.encode([9; 32])),
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn given_malformed_key_when_creating_stream_then_should_refuse_without_echoing_it() {
    let mut iggy_cmd_test = IggyCmdTest::with_server(encrypted_server());

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestStreamCreateWithKeyCmd::new(
            "short-key",
            None,
            Some(STANDARD.encode([5; 31])),
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestStreamCreateWithKeyCmd::new(
            "short-key-file",
            Some(STANDARD.encode([5; 31])),
            None,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn given_key_file_and_environment_when_creating_then_should_prefer_file() {
    let mut iggy_cmd_test = IggyCmdTest::with_server(encrypted_server());

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestStreamCreateWithKeyCmd::new(
            "preferred",
            Some(STANDARD.encode([7; 32])),
            Some(STANDARD.encode([9; 32])),
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestStreamCreateCmd::new(None, String::from("main")))
        .await;

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestStreamCreateCmd::new(None, String::from("prod")))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["stream", "create", "--help"],
            format!(
                r#"Create stream with given name

The server assigns the stream ID. The legacy --stream-id flag is ignored.

Examples:
 iggy stream create prod
 iggy stream create -s 1 test
 iggy stream create --encryption-key-file stream.key payments

{USAGE_PREFIX} stream create [OPTIONS] <NAME>

Arguments:
  <NAME>
          Name of the stream

Options:
  -s, --stream-id <STREAM_ID>
          Legacy stream ID flag (ignored)

      --encryption-key-file <PATH>
          File holding the stream's own at-rest encryption key
{CLAP_INDENT}
          Standard base64 of exactly 32 bytes. Without this flag the key is read
          from the IGGY_STREAM_ENCRYPTION_KEY environment variable, and without
          either the server encryption setting applies. The server stores the
          key wrapped and reads report only its fingerprint. Unrelated to
          --encryption-key, which encrypts message payloads on the client.

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
            vec!["stream", "create", "-h"],
            format!(
                r#"Create stream with given name

{USAGE_PREFIX} stream create [OPTIONS] <NAME>

Arguments:
  <NAME>  Name of the stream

Options:
  -s, --stream-id <STREAM_ID>       Legacy stream ID flag (ignored)
      --encryption-key-file <PATH>  File holding the stream's own at-rest encryption key
  -h, --help                        Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}

pub(super) fn encrypted_server() -> TestServerConfig {
    TestServerConfig::builder()
        .encryption(EncryptionConfig::new(SERVER_KEY))
        .build()
}
