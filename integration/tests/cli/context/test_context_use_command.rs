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
    IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, CLAP_INDENT, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::cli::context::common::ContextConfig;
use iggy::client::Client;
use predicates::str::contains;
use serial_test::parallel;

use super::common::TestIggyContext;
struct TestContextUseCmd {
    test_iggy_context: TestIggyContext,
    new_context_key: String,
}

impl TestContextUseCmd {
    fn new(test_iggy_context: TestIggyContext, new_context_key: String) -> Self {
        Self {
            test_iggy_context,
            new_context_key,
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestContextUseCmd {
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
            .arg("use")
            .arg(self.new_context_key.clone())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        command_state.success().stdout(contains(format!(
            "active context set to '{}'",
            self.new_context_key
        )));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {
        let saved_key = self
            .test_iggy_context
            .read_saved_context_key()
            .await
            .unwrap();
        assert_eq!(self.new_context_key, saved_key);
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;

    iggy_cmd_test
        .execute_test(TestContextUseCmd::new(
            TestIggyContext::new(
                Some(HashMap::from([
                    ("default".to_string(), ContextConfig::default()),
                    ("second".to_string(), ContextConfig::default()),
                ])),
                None,
            ),
            "second".to_string(),
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["context", "use", "--help"],
            format!(
                r#"Set the active context

Examples
 iggy context use dev
 iggy context use default

{USAGE_PREFIX} context use <CONTEXT_NAME>

Arguments:
  <CONTEXT_NAME>
{CLAP_INDENT}Name of the context to use

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
            vec!["context", "use", "-h"],
            format!(
                r#"Set the active context

{USAGE_PREFIX} context use <CONTEXT_NAME>

Arguments:
  <CONTEXT_NAME>  Name of the context to use

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
