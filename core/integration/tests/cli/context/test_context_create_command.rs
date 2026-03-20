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

use std::collections::HashMap;

use crate::cli::common::{
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use iggy_cli::commands::binary_context::common::ContextConfig;
use predicates::str::contains;
use serial_test::parallel;

use super::common::TestIggyContext;

struct TestContextCreateCmd {
    test_iggy_context: TestIggyContext,
    new_context_name: String,
    transport: Option<String>,
    tcp_server_address: Option<String>,
}

impl TestContextCreateCmd {
    fn new(
        test_iggy_context: TestIggyContext,
        new_context_name: String,
        transport: Option<String>,
        tcp_server_address: Option<String>,
    ) -> Self {
        Self {
            test_iggy_context,
            new_context_name,
            transport,
            tcp_server_address,
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestContextCreateCmd {
    async fn prepare_server_state(&mut self, _client: &dyn Client) {
        self.test_iggy_context.prepare().await;
    }

    fn get_command(&self) -> IggyCmdCommand {
        let mut cmd = IggyCmdCommand::new()
            .env(
                "IGGY_HOME",
                self.test_iggy_context.get_iggy_home().to_str().unwrap(),
            )
            .arg("context")
            .arg("create")
            .arg(self.new_context_name.clone())
            .with_env_credentials();

        if let Some(transport) = &self.transport {
            cmd = cmd.arg("--transport").arg(transport.clone());
        }

        if let Some(addr) = &self.tcp_server_address {
            cmd = cmd.arg("--tcp-server-address").arg(addr.clone());
        }

        cmd
    }

    fn verify_command(&self, command_state: Assert) {
        command_state.success().stdout(contains(format!(
            "context '{}' created successfully",
            self.new_context_name
        )));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {
        let saved_contexts = self.test_iggy_context.read_saved_contexts().await;
        assert!(saved_contexts.is_some());
        let contexts = saved_contexts.unwrap();
        assert!(contexts.contains_key(&self.new_context_name));
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;

    iggy_cmd_test
        .execute_test(TestContextCreateCmd::new(
            TestIggyContext::new(
                Some(HashMap::from([(
                    "default".to_string(),
                    ContextConfig::default(),
                )])),
                None,
            ),
            "production".to_string(),
            Some("tcp".to_string()),
            Some("10.0.0.1:8090".to_string()),
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_create_minimal_context() {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;

    iggy_cmd_test
        .execute_test(TestContextCreateCmd::new(
            TestIggyContext::new(
                Some(HashMap::from([(
                    "default".to_string(),
                    ContextConfig::default(),
                )])),
                None,
            ),
            "staging".to_string(),
            None,
            None,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["context", "create", "--help"],
            format!(
                r#"Create a new context

Creates a new named context in the contexts configuration file.
After creating a context, use 'iggy context use <name>' to activate it.

Examples
 iggy context create production --transport tcp --tcp-server-address 10.0.0.1:8090
 iggy context create dev --transport http --http-api-url http://localhost:3000
 iggy context create local --username iggy --password iggy

{USAGE_PREFIX} context create [OPTIONS] <CONTEXT_NAME>

Arguments:
  <CONTEXT_NAME>
{CLAP_INDENT}Name of the context to create

Options:
      --transport <TRANSPORT>
{CLAP_INDENT}Transport protocol (tcp, quic, http, ws)

      --tcp-server-address <TCP_SERVER_ADDRESS>
{CLAP_INDENT}TCP server address (e.g., 127.0.0.1:8090)

      --http-api-url <HTTP_API_URL>
{CLAP_INDENT}HTTP API URL (e.g., http://localhost:3000)

      --quic-server-address <QUIC_SERVER_ADDRESS>
{CLAP_INDENT}QUIC server address (e.g., 127.0.0.1:8080)

      --tcp-tls-enabled <TCP_TLS_ENABLED>
{CLAP_INDENT}Enable TLS for TCP transport
{CLAP_INDENT}
{CLAP_INDENT}[possible values: true, false]

      --username <USERNAME>
{CLAP_INDENT}Username for authentication

      --password <PASSWORD>
{CLAP_INDENT}Password for authentication

      --token <TOKEN>
{CLAP_INDENT}Personal access token

      --token-name <TOKEN_NAME>
{CLAP_INDENT}Personal access token name (for keyring lookup)

  -h, --help
{CLAP_INDENT}Print help (see a summary with '-h')
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
            vec!["context", "create", "-h"],
            format!(
                r#"Create a new context

{USAGE_PREFIX} context create [OPTIONS] <CONTEXT_NAME>

Arguments:
  <CONTEXT_NAME>  Name of the context to create

Options:
      --transport <TRANSPORT>                    Transport protocol (tcp, quic, http, ws)
      --tcp-server-address <TCP_SERVER_ADDRESS>  TCP server address (e.g., 127.0.0.1:8090)
      --http-api-url <HTTP_API_URL>              HTTP API URL (e.g., http://localhost:3000)
      --quic-server-address <QUIC_SERVER_ADDRESS>  QUIC server address (e.g., 127.0.0.1:8080)
      --tcp-tls-enabled <TCP_TLS_ENABLED>        Enable TLS for TCP transport [possible values: true, false]
      --username <USERNAME>                      Username for authentication
      --password <PASSWORD>                      Password for authentication
      --token <TOKEN>                            Personal access token
      --token-name <TOKEN_NAME>                  Personal access token name (for keyring lookup)
  -h, --help                                     Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
