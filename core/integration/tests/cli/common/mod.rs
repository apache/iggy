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
use iggy::clients::client::IggyClient;
use iggy::prelude::defaults::*;
use iggy::prelude::{Client, ClientWrapper, SystemClient, TcpClient, TcpClientConfig, UserClient};
use integration::test_server::TestServer;
use std::fmt::{Display, Formatter, Result};
use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Arc;

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
    fn protocol(&self, server: &TestServer) -> Vec<String> {
        vec![
            "--tcp-server-address".into(),
            server.get_raw_tcp_addr().unwrap(),
        ]
    }
}

pub(crate) struct IggyCmdTest {
    server: TestServer,
    client: IggyClient,
}

impl IggyCmdTest {
    pub(crate) fn new(start_server: bool) -> Self {
        let mut server = TestServer::default();
        if start_server {
            server.start();
        }
        let tcp_client_config = TcpClientConfig {
            server_address: server.get_raw_tcp_addr().unwrap(),
            ..TcpClientConfig::default()
        };
        let client = ClientWrapper::Tcp(TcpClient::create(Arc::new(tcp_client_config)).unwrap());
        let client = IggyClient::create(client, None, None);

        Self { server, client }
    }

    pub(crate) fn help_message() -> Self {
        Self::new(false)
    }

    pub(crate) async fn setup(&mut self) {
        self.client.connect().await.unwrap();

        let ping_result = self.client.ping().await;

        assert!(ping_result.is_ok());

        let identity_info = self
            .client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();

        assert_eq!(identity_info.user_id, 0);
    }

    pub(crate) async fn execute_test(&mut self, mut test_case: impl IggyCmdTestCase) {
        // Make sure server is started
        assert!(
            self.server.is_started(),
            "Server is not running, make sure it has been started with IggyCmdTest::setup()"
        );
        // Prepare iggy server state before test
        test_case.prepare_server_state(&self.client).await;
        // Get iggy tool
        #[allow(deprecated)]
        let mut command = Command::cargo_bin("iggy").unwrap();
        // Get command line arguments and environment variables from test case and execute command
        let command_args = test_case.get_command();
        // Set environment variables for the command
        command.envs(command_args.get_env());
        // Set a fixed terminal width for consistent help output formatting
        command.env("COLUMNS", "500");
        // Set server address for the command - it's randomized for each test
        command.args(test_case.protocol(&self.server));
        // Set COLUMNS environment variable to 200 to avoid truncation of output
        command.env("COLUMNS", "200");

        // Print used environment variables and command with all arguments.
        // By default, it will not be visible but once test is executed with
        // --nocapture flag, it will be visible.
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
            // Execute test command
            command.assert()
        };

        // Verify command output, exit code, etc in the test (if needed)
        test_case.verify_command(assert);
        // Verify iggy server state after the test
        test_case.verify_server_state(&self.client).await;
    }

    pub(crate) async fn execute_test_for_help_command(&mut self, test_case: TestHelpCmd) {
        // Get iggy tool
        #[allow(deprecated)]
        let mut command = Command::cargo_bin("iggy").unwrap();
        // Get command line arguments and environment variables from test case and execute command
        let command_args = test_case.get_command();
        // Set environment variables for the command
        command.envs(command_args.get_env());
        // Set COLUMNS environment variable to 200 to avoid truncation of output
        command.env("COLUMNS", "200");

        // Print used environment variables and command with all arguments.
        // By default, it will not be visible but once test is executed with
        // --nocapture flag, it will be visible.
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

        // Execute test command

        let assert = command.args(command_args.get_opts_and_args()).assert();
        // Verify command output, exit code, etc in the test (if needed)
        test_case.verify_command(assert);
    }

    #[cfg(not(target_os = "macos"))]
    pub(crate) fn get_tcp_server_address(&self) -> Option<String> {
        self.server.get_raw_tcp_addr()
    }
}

impl Default for IggyCmdTest {
    fn default() -> Self {
        Self::new(true)
    }
}
