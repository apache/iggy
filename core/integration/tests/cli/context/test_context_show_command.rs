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

use std::collections::BTreeMap;

use crate::cli::common::{
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use iggy_cli::commands::binary_context::common::ContextConfig;
use iggy_common::ArgsOptional;
use predicates::str::contains;
use serial_test::parallel;

use super::common::TestIggyContext;

struct TestContextShowCmd {
    test_iggy_context: TestIggyContext,
    context_to_show: String,
    expected_rows: Vec<String>,
}

impl TestContextShowCmd {
    fn new(
        test_iggy_context: TestIggyContext,
        context_to_show: String,
        expected_rows: Vec<String>,
    ) -> Self {
        Self {
            test_iggy_context,
            context_to_show,
            expected_rows,
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestContextShowCmd {
    async fn prepare_server_state(&mut self, _client: &dyn Client) {
        self.test_iggy_context.prepare().await;
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .env(
                "IGGY_HOME",
                self.test_iggy_context.get_iggy_home().to_str().unwrap(),
            )
            .arg("context")
            .arg("show")
            .arg(self.context_to_show.clone())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let mut command_state = command_state.success();

        for row in &self.expected_rows {
            command_state = command_state.stdout(contains(row.as_str()));
        }
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[parallel]
pub async fn should_show_context_with_all_fields() {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;

    let config = ContextConfig {
        username: Some("admin".to_string()),
        password: Some("secret".to_string()),
        token: None,
        token_name: None,
        iggy: ArgsOptional {
            transport: Some("tcp".to_string()),
            tcp_server_address: Some("10.0.0.1:8090".to_string()),
            tcp_tls_enabled: Some(true),
            ..Default::default()
        },
        extra: Default::default(),
    };

    iggy_cmd_test
        .execute_test(TestContextShowCmd::new(
            TestIggyContext::new(
                Some(BTreeMap::from([
                    ("default".to_string(), ContextConfig::default()),
                    ("production".to_string(), config),
                ])),
                None,
            ),
            "production".to_string(),
            vec![
                "production".to_string(),
                "tcp".to_string(),
                "10.0.0.1:8090".to_string(),
                "true".to_string(),
                "admin".to_string(),
                "********".to_string(),
            ],
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_show_default_context() {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;

    iggy_cmd_test
        .execute_test(TestContextShowCmd::new(
            TestIggyContext::new(None, None),
            "default".to_string(),
            vec!["default*".to_string()],
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_show_active_context_with_asterisk() {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;

    iggy_cmd_test
        .execute_test(TestContextShowCmd::new(
            TestIggyContext::new(
                Some(BTreeMap::from([
                    ("default".to_string(), ContextConfig::default()),
                    ("dev".to_string(), ContextConfig::default()),
                ])),
                Some("dev".to_string()),
            ),
            "dev".to_string(),
            vec!["dev*".to_string()],
        ))
        .await;
}

struct TestContextShowNotFoundCmd {
    test_iggy_context: TestIggyContext,
    context_to_show: String,
}

impl TestContextShowNotFoundCmd {
    fn new(test_iggy_context: TestIggyContext, context_to_show: String) -> Self {
        Self {
            test_iggy_context,
            context_to_show,
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestContextShowNotFoundCmd {
    async fn prepare_server_state(&mut self, _client: &dyn Client) {
        self.test_iggy_context.prepare().await;
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .env(
                "IGGY_HOME",
                self.test_iggy_context.get_iggy_home().to_str().unwrap(),
            )
            .arg("context")
            .arg("show")
            .arg(self.context_to_show.clone())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        command_state.failure().stderr(contains("not found"));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[parallel]
pub async fn should_fail_for_nonexistent_context() {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;

    iggy_cmd_test
        .execute_test(TestContextShowNotFoundCmd::new(
            TestIggyContext::new(None, None),
            "nonexistent".to_string(),
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["context", "show", "--help"],
            format!(
                r#"Show details of a specific context

Displays the full configuration of a named context including
transport, server addresses, TLS settings, and credentials.

Examples
 iggy context show default
 iggy context show production

{USAGE_PREFIX} context show <CONTEXT_NAME>

Arguments:
  <CONTEXT_NAME>
{CLAP_INDENT}Name of the context to show

Options:
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
            vec!["context", "show", "-h"],
            format!(
                r#"Show details of a specific context

{USAGE_PREFIX} context show <CONTEXT_NAME>

Arguments:
  <CONTEXT_NAME>  Name of the context to show

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
