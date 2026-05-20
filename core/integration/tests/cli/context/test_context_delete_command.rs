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
use predicates::str::contains;
use serial_test::parallel;

use super::common::TestIggyContext;

struct TestContextDeleteCmd {
    test_iggy_context: TestIggyContext,
    context_to_delete: String,
}

impl TestContextDeleteCmd {
    fn new(test_iggy_context: TestIggyContext, context_to_delete: String) -> Self {
        Self {
            test_iggy_context,
            context_to_delete,
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestContextDeleteCmd {
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
            .arg("delete")
            .arg(self.context_to_delete.clone())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        command_state.success().stdout(contains(format!(
            "context '{}' deleted successfully",
            self.context_to_delete
        )));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {
        let saved_contexts = self.test_iggy_context.read_saved_contexts().await;
        assert!(saved_contexts.is_some());
        let contexts = saved_contexts.unwrap();
        assert!(!contexts.contains_key(&self.context_to_delete));
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;

    iggy_cmd_test
        .execute_test(TestContextDeleteCmd::new(
            TestIggyContext::new(
                Some(BTreeMap::from([
                    ("default".to_string(), ContextConfig::default()),
                    ("production".to_string(), ContextConfig::default()),
                ])),
                None,
            ),
            "production".to_string(),
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_reset_active_context_on_delete() {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;

    let test_context = TestIggyContext::new(
        Some(BTreeMap::from([
            ("default".to_string(), ContextConfig::default()),
            ("staging".to_string(), ContextConfig::default()),
        ])),
        Some("staging".to_string()),
    );

    let test_cmd = TestContextDeleteActiveCmd::new(test_context, "staging".to_string());

    iggy_cmd_test.execute_test(test_cmd).await;
}

struct TestContextDeleteActiveCmd {
    test_iggy_context: TestIggyContext,
    context_to_delete: String,
}

impl TestContextDeleteActiveCmd {
    fn new(test_iggy_context: TestIggyContext, context_to_delete: String) -> Self {
        Self {
            test_iggy_context,
            context_to_delete,
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestContextDeleteActiveCmd {
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
            .arg("delete")
            .arg(self.context_to_delete.clone())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        command_state.success().stdout(contains(format!(
            "context '{}' deleted successfully",
            self.context_to_delete
        )));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {
        let saved_contexts = self.test_iggy_context.read_saved_contexts().await;
        assert!(saved_contexts.is_some());
        let contexts = saved_contexts.unwrap();
        assert!(!contexts.contains_key(&self.context_to_delete));

        let active_key = self.test_iggy_context.read_saved_context_key().await;
        assert_eq!(active_key, Some("default".to_string()));
    }
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["context", "delete", "--help"],
            format!(
                r#"Delete an existing context

Removes a named context from the contexts configuration file.
The 'default' context cannot be deleted. If the deleted context
was the active context, the active context resets to 'default'.

Examples
 iggy context delete production

{USAGE_PREFIX} context delete <CONTEXT_NAME>

Arguments:
  <CONTEXT_NAME>
{CLAP_INDENT}Name of the context to delete

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
            vec!["context", "delete", "-h"],
            format!(
                r#"Delete an existing context

{USAGE_PREFIX} context delete <CONTEXT_NAME>

Arguments:
  <CONTEXT_NAME>  Name of the context to delete

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
