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

use crate::cli::common::list_runs::{ListRun, execute_list_runs};
use crate::cli::common::{
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, OutputFormat, TestHelpCmd,
    USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use futures::future::BoxFuture;
use iggy::prelude::Client;
use iggy::prelude::{Identifier, StreamCreateOptions};
use predicates::str::{contains, starts_with};
use serial_test::parallel;

/// The field of the stream name in a list mode row.
const NAME_COLUMN: usize = 2;

struct TestStreamListCmd {
    name: String,
    output: OutputFormat,
}

impl TestStreamListCmd {
    fn new(name: String, output: OutputFormat) -> Self {
        Self { name, output }
    }

    fn to_args(&self) -> Vec<&str> {
        self.output.to_args()
    }
}

#[async_trait]
impl IggyCmdTestCase for TestStreamListCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client
            .create_stream(&self.name, &StreamCreateOptions::default())
            .await;
        assert!(stream.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("stream")
            .arg("list")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        command_state
            .success()
            .stdout(starts_with(format!(
                "Executing list streams in {} mode",
                self.output
            )))
            .stdout(contains(self.name.clone()))
            .stdout(contains(match self.output {
                OutputFormat::List => format!("{}|0 B|0|0|none\n", self.name),
                OutputFormat::Default | OutputFormat::Table => "| Encryption |".to_owned(),
            }));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestStreamListCmd::new(
            String::from("prod"),
            OutputFormat::Default,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestStreamListCmd::new(
            String::from("testing"),
            OutputFormat::List,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestStreamListCmd::new(
            String::from("misc"),
            OutputFormat::Table,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_print_pages() {
    execute_list_runs(
        seed_streams,
        NAME_COLUMN,
        vec![
            ListRun::new(
                "stream list -l list",
                &["pages-s1", "pages-s4", "pages-s3"],
                false,
            ),
            ListRun::new(
                "stream list -l list --sort created",
                &["pages-s1", "pages-s3", "pages-s4"],
                false,
            ),
            ListRun::new(
                "stream list -l list --sort created --desc --limit 2",
                &["pages-s4", "pages-s3"],
                true,
            ),
            ListRun::new(
                "stream list -l list --sort created --desc --limit 2 --after {next}",
                &["pages-s1"],
                false,
            ),
            ListRun::new(
                "stream list -l list --limit 3",
                &["pages-s1", "pages-s4", "pages-s3"],
                false,
            ),
            ListRun::new("stream list -l list --name s3", &["pages-s3"], false),
            ListRun::new(
                "stream list -o json --sort created --desc --limit 2",
                &["pages-s4", "pages-s3"],
                true,
            ),
            ListRun::new(
                "stream list -o json --sort created --desc --limit 2 --after {next}",
                &["pages-s1"],
                false,
            ),
            ListRun::new(
                "stream list -o json --sort created",
                &["pages-s1", "pages-s3", "pages-s4"],
                false,
            ),
        ],
    )
    .await;
}

/// Stream `pages-s4` takes the ID of the deleted `pages-s2`, so the ID order
/// is s1 s4 s3 and the creation order is s1 s3 s4.
fn seed_streams(client: &dyn Client) -> BoxFuture<'_, ()> {
    Box::pin(async move {
        for name in ["pages-s1", "pages-s2", "pages-s3"] {
            client
                .create_stream(name, &StreamCreateOptions::default())
                .await
                .expect("create stream");
        }
        client
            .delete_stream(&Identifier::named("pages-s2").expect("a stream name"))
            .await
            .expect("delete stream");
        client
            .create_stream("pages-s4", &StreamCreateOptions::default())
            .await
            .expect("create stream");
    })
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["stream", "list", "--help"],
            format!(
                r#"List all streams

Examples:
 iggy stream list
 iggy stream list --list-mode table
 iggy stream list -l table
 iggy stream list --sort created --desc --limit 20
 iggy stream list --name prod --limit 50 --after pg1_AQcAAAAAAAAA

{USAGE_PREFIX} stream list [OPTIONS]

Options:
  -l, --list-mode <LIST_MODE>
          List mode (table or list)
{CLAP_INDENT}
          [default: table]
          [possible values: table, list]

  -o, --output <OUTPUT>
          Output format (table, list or json), cannot be combined with --list-mode
{CLAP_INDENT}
          [possible values: table, list, json]

      --sort <SORT>
          Order of the rows, by ID or by creation time
{CLAP_INDENT}
          [default: id]
          [possible values: id, created]

      --desc
          Print the rows in descending order

      --limit <N>
          Print at most N rows
{CLAP_INDENT}
          If more rows follow them, the last line is `next: <CURSOR>`. Pass that cursor to --after to print the rows that follow.

      --after <CURSOR>
          Print the rows after this cursor
{CLAP_INDENT}
          The cursor comes from the `next:` line of a list with --limit. Use the same other flags as that list.

      --name <TEXT>
          Print only the rows whose name holds this text (case-sensitive)

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
            vec!["stream", "list", "-h"],
            format!(
                r#"List all streams

{USAGE_PREFIX} stream list [OPTIONS]

Options:
  -l, --list-mode <LIST_MODE>  List mode (table or list) [default: table] [possible values: table, list]
  -o, --output <OUTPUT>        Output format (table, list or json), cannot be combined with --list-mode [possible values: table, list, json]
      --sort <SORT>            Order of the rows, by ID or by creation time [default: id] [possible values: id, created]
      --desc                   Print the rows in descending order
      --limit <N>              Print at most N rows
      --after <CURSOR>         Print the rows after this cursor
      --name <TEXT>            Print only the rows whose name holds this text (case-sensitive)
  -h, --help                   Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
