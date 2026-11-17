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
use iggy::prelude::UserCreateOptions;
use iggy::prelude::UserStatus;
use iggy::prelude::{DEFAULT_ROOT_USERNAME, Identifier};
use predicates::str::{contains, starts_with};
use serial_test::parallel;

/// The field of the username in a list mode row.
const NAME_COLUMN: usize = 3;

struct TestUserListCmd {
    username: String,
    status: UserStatus,
    output: OutputFormat,
}

impl TestUserListCmd {
    fn new(username: String, status: UserStatus, output: OutputFormat) -> Self {
        Self {
            username,
            status,
            output,
        }
    }

    fn to_args(&self) -> Vec<&str> {
        self.output.to_args()
    }
}

#[async_trait]
impl IggyCmdTestCase for TestUserListCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client
            .create_user(
                &self.username,
                "secret",
                self.status,
                None,
                &UserCreateOptions::default(),
            )
            .await;
        assert!(stream.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("user")
            .arg("list")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        command_state
            .success()
            .stdout(starts_with(format!(
                "Executing list users in {} mode",
                self.output
            )))
            .stdout(contains(self.username.clone()))
            .stdout(contains(self.status.to_string()));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestUserListCmd::new(
            String::from("producer"),
            UserStatus::Active,
            OutputFormat::Default,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserListCmd::new(
            String::from("testing"),
            UserStatus::Inactive,
            OutputFormat::List,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserListCmd::new(
            String::from("misc"),
            UserStatus::Active,
            OutputFormat::Table,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_print_pages() {
    execute_list_runs(
        seed_users,
        NAME_COLUMN,
        vec![
            ListRun::new(
                "user list -l list",
                &[DEFAULT_ROOT_USERNAME, "pages-u1", "pages-u4", "pages-u3"],
                false,
            ),
            ListRun::new(
                "user list -l list --sort created --limit 2",
                &[DEFAULT_ROOT_USERNAME, "pages-u1"],
                true,
            ),
            ListRun::new(
                "user list -l list --sort created --after {next}",
                &["pages-u3", "pages-u4"],
                false,
            ),
            ListRun::new(
                "user list -l list --name pages-u --desc",
                &["pages-u3", "pages-u4", "pages-u1"],
                false,
            ),
        ],
    )
    .await;
}

/// User `pages-u4` takes the ID of the deleted `pages-u2`, so the ID order is
/// u1 u4 u3 and the creation order is u1 u3 u4, both after the root user.
fn seed_users(client: &dyn Client) -> BoxFuture<'_, ()> {
    Box::pin(async move {
        let create_user = |name| async move {
            client
                .create_user(
                    name,
                    "secret",
                    UserStatus::Active,
                    None,
                    &UserCreateOptions::default(),
                )
                .await
                .expect("create user");
        };
        for name in ["pages-u1", "pages-u2", "pages-u3"] {
            create_user(name).await;
        }
        client
            .delete_user(&Identifier::named("pages-u2").expect("a username"))
            .await
            .expect("delete user");
        create_user("pages-u4").await;
    })
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["user", "list", "--help"],
            format!(
                r#"List all users

Examples:
 iggy user list
 iggy user list --list-mode table
 iggy user list -l table
 iggy user list --sort created --desc --limit 20

{USAGE_PREFIX} user list [OPTIONS]

Options:
  -l, --list-mode <LIST_MODE>
          List mode (table or list)
{CLAP_INDENT}
          [default: table]
          [possible values: table, list]

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
            vec!["user", "list", "-h"],
            format!(
                r#"List all users

{USAGE_PREFIX} user list [OPTIONS]

Options:
  -l, --list-mode <LIST_MODE>  List mode (table or list) [default: table] [possible values: table, list]
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
