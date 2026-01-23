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

pub(crate) mod command;
pub(crate) mod help;
pub(crate) use crate::cli::common::command::IggyCmdCommand;
pub(crate) use crate::cli::common::help::{CLAP_INDENT, TestHelpCmd, USAGE_PREFIX};
use assert_cmd::assert::{Assert, OutputAssertExt};
use assert_cmd::prelude::CommandCargoExt;
use async_trait::async_trait;
use iggy::prelude::Client;
use integration::harness::handle::ServerHandle;
use integration::harness::{TestBinary, TestHarness, TestServerConfig};
use std::fmt::{Display, Formatter, Result};
use std::io::Write;
use std::process::{Command, Stdio};

pub(crate) enum TestIdentifier {
    Numeric,
    Named,
}

pub(crate) type TestStreamId = TestIdentifier;

pub(crate) type TestTopicId = TestIdentifier;

pub(crate) type TestUserId = TestIdentifier;

pub(crate) type TestConsumerGroupId = TestIdentifier;

pub(crate) type TestConsumerId = TestIdentifier;

pub(crate) enum OutputFormat {
    Default,
    List,
    Table,
}

impl Display for OutputFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Self::Default => write!(f, "table"),
            Self::List => write!(f, "list"),
            Self::Table => write!(f, "table"),
        }
    }
}

impl OutputFormat {
    pub(crate) fn to_args(&self) -> Vec<&str> {
        match self {
            Self::Default => vec![],
            Self::List => vec!["--list-mode", "list"],
            Self::Table => vec!["--list-mode", "table"],
        }
    }
}

#[async_trait]
pub(crate) trait IggyCmdTestCase {
    async fn prepare_server_state(&mut self, client: &dyn Client);
    fn get_command(&self) -> IggyCmdCommand;
    fn provide_stdin_input(&self) -> Option<Vec<String>> {
        None
    }
    fn verify_command(&self, command_state: Assert);
    async fn verify_server_state(&self, client: &dyn Client);
    fn protocol(&self, server: &ServerHandle) -> Vec<String> {
        vec![
            "--tcp-server-address".into(),
            server.raw_tcp_addr().unwrap(),
        ]
    }
}

pub(crate) struct IggyCmdTest {
    harness: TestHarness,
    started: bool,
}

impl IggyCmdTest {
    pub(crate) fn new(start_server: bool) -> Self {
        let harness = TestHarness::builder()
            .server(TestServerConfig::default())
            .build()
            .expect("Failed to build test harness");

        Self {
            harness,
            started: start_server,
        }
    }

    pub(crate) fn help_message() -> Self {
        Self::new(false)
    }

    pub(crate) async fn setup(&mut self) {
        if self.started && !self.harness.server().is_running() {
            self.harness
                .start()
                .await
                .expect("Failed to start test harness");
        }
    }

    pub(crate) async fn execute_test(&mut self, mut test_case: impl IggyCmdTestCase) {
        assert!(
            self.harness.server().is_running(),
            "Server is not running, make sure it has been started with IggyCmdTest::setup()"
        );

        let client = self.harness.tcp_root_client().await.unwrap();

        test_case.prepare_server_state(&client).await;

        #[allow(deprecated)]
        let mut command = Command::cargo_bin("iggy").unwrap();

        let command_args = test_case.get_command();
        command.envs(command_args.get_env());
        command.env("COLUMNS", "500");
        command.args(test_case.protocol(self.harness.server()));
        command.env("COLUMNS", "200");

        println!(
            "Running: {} {} {}",
            command
                .get_envs()
                .map(|k| format!(
                    "{}={}",
                    k.0.to_str().unwrap(),
                    k.1.unwrap().to_str().unwrap()
                ))
                .collect::<Vec<String>>()
                .join(" "),
            command.get_program().to_str().unwrap(),
            command_args.get_opts_and_args().join(" ")
        );

        let command = command.args(command_args.get_opts_and_args());
        let assert = if let Some(stdin_input) = test_case.provide_stdin_input() {
            let mut child = command
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .expect("Failed to spawn child process");

            let mut stdin = child.stdin.take().expect("Failed to open stdin");
            std::thread::spawn(move || {
                stdin_input.into_iter().for_each(|line| {
                    stdin
                        .write_all(format!("{line}\n").as_bytes())
                        .unwrap_or_else(|_| panic!("Failed to write to stdin \"{line}\"",))
                });
            });

            child
                .wait_with_output()
                .expect("Failed to read stdout")
                .assert()
        } else {
            command.assert()
        };

        test_case.verify_command(assert);
        test_case.verify_server_state(&client).await;
    }

    pub(crate) async fn execute_test_for_help_command(&mut self, test_case: TestHelpCmd) {
        #[allow(deprecated)]
        let mut command = Command::cargo_bin("iggy").unwrap();

        let command_args = test_case.get_command();
        command.envs(command_args.get_env());
        command.env("COLUMNS", "200");

        println!(
            "Running: {} {} {}",
            command
                .get_envs()
                .map(|k| format!(
                    "{}={}",
                    k.0.to_str().unwrap(),
                    k.1.unwrap().to_str().unwrap()
                ))
                .collect::<Vec<String>>()
                .join(" "),
            command.get_program().to_str().unwrap(),
            command_args.get_opts_and_args().join(" ")
        );

        let assert = command.args(command_args.get_opts_and_args()).assert();
        test_case.verify_command(assert);
    }

    #[cfg(not(target_os = "macos"))]
    pub(crate) fn get_tcp_server_address(&self) -> Option<String> {
        self.harness.server().raw_tcp_addr()
    }
}

impl Default for IggyCmdTest {
    fn default() -> Self {
        Self::new(true)
    }
}
