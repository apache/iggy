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

use crate::cli::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, USAGE_PREFIX};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use predicates::str::{contains, starts_with};
use serial_test::parallel;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum TestClusterMetadataCmdOutput {
    Table,
    List,
}

struct TestClusterMetadataCmd {
    output_mode: TestClusterMetadataCmdOutput,
}

impl TestClusterMetadataCmd {
    fn new(output_mode: TestClusterMetadataCmdOutput) -> Self {
        Self { output_mode }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestClusterMetadataCmd {
    async fn prepare_server_state(&mut self, _client: &dyn Client) {}

    fn get_command(&self) -> IggyCmdCommand {
        let command = IggyCmdCommand::new().arg("cluster").arg("metadata");

        match self.output_mode {
            TestClusterMetadataCmdOutput::Table => command.with_env_credentials(),
            TestClusterMetadataCmdOutput::List => command
                .arg("--list-mode")
                .arg("list")
                .with_env_credentials(),
        }
    }

    fn verify_command(&self, command_state: Assert) {
        match self.output_mode {
            TestClusterMetadataCmdOutput::Table => {
                command_state
                    .success()
                    .stdout(starts_with(
                        "Executing get cluster metadata in table mode\n",
                    ))
                    .stdout(contains("Cluster name:"))
                    .stdout(contains("single-node"))
                    .stdout(contains("Name"))
                    .stdout(contains("IP"))
                    .stdout(contains("TCP"))
                    .stdout(contains("QUIC"))
                    .stdout(contains("HTTP"))
                    .stdout(contains("WebSocket"))
                    .stdout(contains("Role"))
                    .stdout(contains("Status"))
                    .stdout(contains("iggy-node"))
                    .stdout(contains("leader"))
                    .stdout(contains("healthy"));
            }
            TestClusterMetadataCmdOutput::List => {
                command_state
                    .success()
                    .stdout(starts_with("Executing get cluster metadata in list mode\n"))
                    .stdout(contains("Cluster name:"))
                    .stdout(contains("single-node"))
                    .stdout(contains("iggy-node"))
                    .stdout(contains("leader"))
                    .stdout(contains("healthy"));
            }
        }
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful_table_mode() {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestClusterMetadataCmd::new(
            TestClusterMetadataCmdOutput::Table,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful_list_mode() {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestClusterMetadataCmd::new(
            TestClusterMetadataCmdOutput::List,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();
    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["cluster", "metadata", "--help"],
            format!(
                r#"Get cluster metadata

{USAGE_PREFIX} cluster metadata [OPTIONS]

Options:
  -l, --list-mode <LIST_MODE>  [default: table] [possible values: table, list]
  -h, --help                   Print help
"#,
            ),
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_short_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();
    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["cluster", "metadata", "-h"],
            format!(
                r#"Get cluster metadata

{USAGE_PREFIX} cluster metadata [OPTIONS]

Options:
  -l, --list-mode <LIST_MODE>  [default: table] [possible values: table, list]
  -h, --help                   Print help
"#,
            ),
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_cluster_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();
    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["cluster", "--help"],
            format!(
                r#"cluster operations

{USAGE_PREFIX} cluster <COMMAND>

Commands:
  metadata  Get cluster metadata [aliases: m]
  help      Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
"#,
            ),
        ))
        .await;
}
