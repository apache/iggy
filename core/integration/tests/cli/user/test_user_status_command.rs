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

struct TestUserStatusCmd {
    username: String,
    status: UserStatus,
    new_status: UserStatus,
    using_identifier: TestUserId,
    user_id: Option<UserId>,
}

impl TestUserStatusCmd {
    fn new(
        username: String,
        status: UserStatus,
        new_status: UserStatus,
        using_identifier: TestUserId,
    ) -> Self {
        Self {
            username,
            status,
            new_status,
            using_identifier,
            user_id: None,
        }
    }

    fn to_args(&self) -> Vec<String> {
        match self.using_identifier {
            TestUserId::Named => vec![
                self.username.clone(),
                format!("{}", self.new_status.clone()),
            ],
            TestUserId::Numeric => {
                vec![
                    format!("{}", self.user_id.unwrap()),
                    format!("{}", self.new_status.clone()),
                ]
            }
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestUserStatusCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let create_user = client
            .create_user(&self.username, "secret", self.status, None)
            .await;
        assert!(create_user.is_ok());
        let user = client
            .get_user(&self.username.clone().try_into().unwrap())
            .await;
        assert!(user.is_ok());
        let user = user.unwrap().expect("User not found");
        self.user_id = Some(user.id);
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("user")
            .arg("status")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let message = match self.using_identifier {
            TestUserId::Named => format!(
                "Executing update user with ID: {} with status: {}\nUser with ID: {} updated with status: {}\n",
                self.username, self.new_status, self.username, self.new_status
            ),
            TestUserId::Numeric => format!(
                "Executing update user with ID: {} with status: {}\nUser with ID: {} updated with status: {}\n",
                self.user_id.unwrap(),
                self.new_status,
                self.user_id.unwrap(),
                self.new_status
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
        .execute_test(TestUserStatusCmd::new(
            String::from("user"),
            UserStatus::Active,
            UserStatus::Inactive,
            TestUserId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserStatusCmd::new(
            String::from("admin"),
            UserStatus::Inactive,
            UserStatus::Active,
            TestUserId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserStatusCmd::new(
            String::from("inactive_user"),
            UserStatus::Inactive,
            UserStatus::Inactive,
            TestUserId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserStatusCmd::new(
            String::from("active_user"),
            UserStatus::Active,
            UserStatus::Active,
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
            vec!["user", "status", "--help"],
            format!(
                r#"Change status for user with given ID

The user ID can be specified as either a username or an ID

Examples:
 iggy user status 2 active
 iggy user status testuser inactive

{USAGE_PREFIX} user status <USER_ID> <STATUS>

Arguments:
  <USER_ID>
          User ID to update
{CLAP_INDENT}
          The user ID can be specified as either a username or an ID

  <STATUS>
          New status
{CLAP_INDENT}
          [possible values: active, inactive]

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
            vec!["user", "status", "-h"],
            format!(
                r#"Change status for user with given ID

{USAGE_PREFIX} user status <USER_ID> <STATUS>

Arguments:
  <USER_ID>  User ID to update
  <STATUS>   New status [possible values: active, inactive]

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
