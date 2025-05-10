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

struct TestUserNameCmd {
    username: String,
    new_username: String,
    using_identifier: TestUserId,
    user_id: Option<UserId>,
}

impl TestUserNameCmd {
    fn new(username: String, new_username: String, using_identifier: TestUserId) -> Self {
        Self {
            username,
            new_username,
            using_identifier,
            user_id: None,
        }
    }

    fn to_args(&self) -> Vec<String> {
        match self.using_identifier {
            TestUserId::Named => vec![self.username.clone(), self.new_username.clone()],
            TestUserId::Numeric => {
                vec![
                    format!("{}", self.user_id.unwrap()),
                    self.new_username.clone(),
                ]
            }
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestUserNameCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let create_user = client
            .create_user(&self.username, "secret", UserStatus::Active, None)
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
        IggyCmdCommand::new()
            .arg("user")
            .arg("name")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let numeric_id = self.user_id.unwrap().to_string();
        let identifier = match self.using_identifier {
            TestUserId::Named => &self.username,
            TestUserId::Numeric => &numeric_id,
        };

        let message = format!(
            "Executing update user with ID: {} with username: {}\nUser with ID: {} updated with username: {}\n",
            identifier, self.new_username, identifier, self.new_username
        );

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
        .execute_test(TestUserNameCmd::new(
            String::from("tester"),
            String::from("testing"),
            TestUserId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserNameCmd::new(
            String::from("user1"),
            String::from("producer"),
            TestUserId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserNameCmd::new(
            String::from("same_name"),
            String::from("same_name"),
            TestUserId::Named,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["user", "name", "--help"],
            format!(
                r#"Change username for user with given ID

The user ID can be specified as either a username or an ID

Examples:
 iggy user name 2 new_user_name
 iggy user name testuser test_user

{USAGE_PREFIX} user name <USER_ID> <USERNAME>

Arguments:
  <USER_ID>
          User ID to update
{CLAP_INDENT}
          The user ID can be specified as either a username or an ID

  <USERNAME>
          New username
{CLAP_INDENT}
          New and unique identifier for the user account on iggy server,
          must be between 3 and 50 characters long.

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
            vec!["user", "name", "-h"],
            format!(
                r#"Change username for user with given ID

{USAGE_PREFIX} user name <USER_ID> <USERNAME>

Arguments:
  <USER_ID>   User ID to update
  <USERNAME>  New username

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
