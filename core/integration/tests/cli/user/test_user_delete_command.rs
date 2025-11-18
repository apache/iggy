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
use iggy::prelude::UserStatus;
use predicates::str::diff;
use serial_test::parallel;

struct TestUserDeleteCmd {
    username: String,
    password: String,
    status: UserStatus,
    user_id: u32,
    using_identifier: TestUserId,
}

impl TestUserDeleteCmd {
    fn new(
        username: String,
        password: String,
        status: UserStatus,
        user_id: u32,
        using_identifier: TestUserId,
    ) -> Self {
        Self {
            username,
            password,
            status,
            user_id,
            using_identifier,
        }
    }

    fn to_arg(&self) -> String {
        match self.using_identifier {
            TestUserId::Named => self.username.clone(),
            TestUserId::Numeric => format!("{}", self.user_id),
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestUserDeleteCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let user = client
            .create_user(&self.username, &self.password, self.status, None)
            .await;
        assert!(user.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("user")
            .arg("delete")
            .arg(self.to_arg())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let message = match self.using_identifier {
            TestUserId::Named => format!(
                "Executing delete user with ID: {}\nUser with ID: {} deleted\n",
                self.username, self.username
            ),
            TestUserId::Numeric => format!(
                "Executing delete user with ID: {}\nUser with ID: {} deleted\n",
                self.user_id, self.user_id
            ),
        };

        command_state.success().stdout(diff(message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let users = client.get_users().await;
        assert!(users.is_ok());
        let users = users.unwrap();
        assert_eq!(users.len(), 1); // Only root
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestUserDeleteCmd::new(
            String::from("username"),
            String::from("password"),
            UserStatus::Active,
            1,
            TestUserId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserDeleteCmd::new(
            String::from("testuser"),
            String::from("testpass"),
            UserStatus::Inactive,
            1,
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
            vec!["user", "delete", "--help"],
            format!(
                r"Delete user with given ID

The user ID can be specified as either a username or an ID

Examples:
 iggy user delete 2
 iggy user delete testuser

{USAGE_PREFIX} user delete <USER_ID>

Arguments:
  <USER_ID>
          User ID to delete
{CLAP_INDENT}
          The user ID can be specified as either a username or an ID

Options:
  -h, --help
          Print help (see a summary with '-h')
",
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
            vec!["user", "delete", "-h"],
            format!(
                r#"Delete user with given ID

{USAGE_PREFIX} user delete <USER_ID>

Arguments:
  <USER_ID>  User ID to delete

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
