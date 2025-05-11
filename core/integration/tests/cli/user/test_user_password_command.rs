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

use crate::cli::common::{
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, TestUserId,
    USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use iggy::prelude::UserId;
use iggy::prelude::UserStatus;
use predicates::str::diff;
use serial_test::parallel;

enum ProvidePasswords {
    BothAsArgs,
    CurrentAsArg,
    BothViaStdin,
}

struct TestUserPasswordCmd {
    username: String,
    password: String,
    new_password: String,
    using_identifier: TestUserId,
    password_input: ProvidePasswords,
    user_id: Option<UserId>,
}

impl TestUserPasswordCmd {
    fn new(
        username: String,
        password: String,
        new_password: String,
        using_identifier: TestUserId,
        password_input: ProvidePasswords,
    ) -> Self {
        Self {
            username,
            password,
            new_password,
            using_identifier,
            password_input,
            user_id: None,
        }
    }

    fn to_args(&self) -> Vec<String> {
        match self.using_identifier {
            TestUserId::Named => vec![self.username.clone()],
            TestUserId::Numeric => {
                vec![format!("{}", self.user_id.unwrap())]
            }
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestUserPasswordCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let create_user = client
            .create_user(&self.username, &self.password, UserStatus::Active, None)
            .await;
        assert!(create_user.is_ok());
        let user = client
            .get_user(&self.username.as_str().try_into().unwrap())
            .await;
        assert!(user.is_ok());
        let user = user.unwrap().expect("User not found");
        self.user_id = Some(user.id);
    }

    fn get_command(&self) -> IggyCmdCommand {
        let command = IggyCmdCommand::new()
            .arg("user")
            .arg("password")
            .with_env_credentials();

        match self.password_input {
            ProvidePasswords::BothAsArgs => command
                .args(self.to_args())
                .arg(self.password.clone())
                .arg(self.new_password.clone()),
            ProvidePasswords::CurrentAsArg => {
                command.args(self.to_args()).arg(self.password.clone())
            }
            ProvidePasswords::BothViaStdin => command.args(self.to_args()),
        }
    }

    fn provide_stdin_input(&self) -> Option<Vec<String>> {
        match self.password_input {
            ProvidePasswords::BothAsArgs => None,
            ProvidePasswords::CurrentAsArg => Some(vec![self.new_password.clone()]),
            ProvidePasswords::BothViaStdin => {
                Some(vec![self.password.clone(), self.new_password.clone()])
            }
        }
    }

    fn verify_command(&self, command_state: Assert) {
        let message = match self.using_identifier {
            TestUserId::Named => format!(
                "Executing change password for user with ID: {}\nPassword for user with ID: {} changed\n",
                self.username, self.username
            ),
            TestUserId::Numeric => format!(
                "Executing change password for user with ID: {}\nPassword for user with ID: {} changed\n",
                self.user_id.unwrap(),
                self.user_id.unwrap()
            ),
        };

        command_state.success().stdout(diff(message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let deleted = client
            .delete_user(&self.user_id.unwrap().try_into().unwrap())
            .await;
        assert!(deleted.is_ok());
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestUserPasswordCmd::new(
            String::from("user"),
            String::from("password"),
            String::from("new-password"),
            TestUserId::Numeric,
            ProvidePasswords::BothAsArgs,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserPasswordCmd::new(
            String::from("admin"),
            String::from("easy-pass"),
            String::from("P@$$w0r4"),
            TestUserId::Named,
            ProvidePasswords::CurrentAsArg,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserPasswordCmd::new(
            String::from("easy_one"),
            String::from("1234"),
            String::from("4321"),
            TestUserId::Named,
            ProvidePasswords::BothViaStdin,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserPasswordCmd::new(
            String::from("same_all_the_time"),
            String::from("password"),
            String::from("password"),
            TestUserId::Named,
            ProvidePasswords::CurrentAsArg,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["user", "password", "--help"],
            format!(
                r#"Change password for user with given ID

The user ID can be specified as either a username or an ID

Examples:
 iggy user password 2
 iggy user password client
 iggy user password 3 current_password new_password
 iggy user password testuser curpwd p@sswor4

{USAGE_PREFIX} user password <USER_ID> [CURRENT_PASSWORD] [NEW_PASSWORD]

Arguments:
  <USER_ID>
          User ID to update
{CLAP_INDENT}
          The user ID can be specified as either a username or an ID

  [CURRENT_PASSWORD]
          Current password
{CLAP_INDENT}
          Current password, must be between 3 and 100 characters long.
          An optional parameter to specify the current password for the given user.
          If not provided, the user will be prompted interactively to enter the
          password securely, and the quiet mode option will not have any effect.

  [NEW_PASSWORD]
          New password
{CLAP_INDENT}
          New password, must be between 3 and 100 characters long.
          An optional parameter to specify the new password for the given user.
          If not provided, the user will be prompted interactively to enter the
          password securely, and the quiet mode option will not have any effect.

Options:
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
            vec!["user", "password", "-h"],
            format!(
                r#"Change password for user with given ID

{USAGE_PREFIX} user password <USER_ID> [CURRENT_PASSWORD] [NEW_PASSWORD]

Arguments:
  <USER_ID>           User ID to update
  [CURRENT_PASSWORD]  Current password
  [NEW_PASSWORD]      New password

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
