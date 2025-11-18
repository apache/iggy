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
use ahash::AHashMap;
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::{Client, Permissions, StreamPermissions, TopicPermissions, UserId, UserStatus};
use predicates::str::{is_match, starts_with};
use serial_test::parallel;

enum TestUser<'a> {
    Existing(&'a str),
    New(&'a str),
    NewWithGlobalPerms(&'a str),
    NewWithStreamPerms(&'a str, usize),
    NewWithStreamAndTopicPerms(&'a str, usize, usize),
}

struct TestUserGetCmd {
    username: String,
    status: UserStatus,
    user_id: Option<UserId>,
    using_user_id: TestUserId,
    create_user: bool,
    check_global_perms: bool,
    check_stream_perms: Option<usize>,
    check_topic_perms: Option<usize>,
}

impl TestUserGetCmd {
    fn new(test_user: TestUser, status: UserStatus, using_user_id: TestUserId) -> Self {
        let (username, create_user, check_global_perms, check_stream_perms, check_topic_perms) =
            match test_user {
                TestUser::Existing(username) => (username.into(), false, true, None, None),
                TestUser::New(username) => (username.into(), true, false, None, None),
                TestUser::NewWithGlobalPerms(username) => (username.into(), true, true, None, None),
                TestUser::NewWithStreamPerms(username, stream_id) => {
                    (username.into(), true, true, Some(stream_id), None)
                }
                TestUser::NewWithStreamAndTopicPerms(username, stream_id, topic_id) => {
                    (username.into(), true, true, Some(stream_id), Some(topic_id))
                }
            };

        Self {
            username,
            status,
            user_id: None,
            using_user_id,
            create_user,
            check_global_perms,
            check_stream_perms,
            check_topic_perms,
        }
    }

    fn get_user_id(&self) -> String {
        match self.using_user_id {
            TestUserId::Named => self.username.clone(),
            TestUserId::Numeric => format!("{}", self.user_id.unwrap()),
        }
    }

    fn get_permissions(&self) -> Option<Permissions> {
        if self.check_global_perms {
            let mut permissions = Permissions::default();

            if let Some(stream_id) = self.check_stream_perms {
                let mut stream_perms = StreamPermissions::default();

                if let Some(topic_id) = self.check_topic_perms {
                    stream_perms.topics =
                        Some(AHashMap::from([(topic_id, TopicPermissions::default())]));
                };

                permissions.streams = Some(AHashMap::from([(stream_id, stream_perms)]));
            }

            Some(permissions)
        } else {
            None
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestUserGetCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        if self.create_user {
            let create_user = client
                .create_user(
                    &self.username,
                    "secret",
                    self.status,
                    self.get_permissions(),
                )
                .await;
            assert!(create_user.is_ok());
        }
        let user = client
            .get_user(&self.username.as_str().try_into().unwrap())
            .await;
        assert!(user.is_ok());
        let user = user.unwrap().expect("Failed to get user");
        self.user_id = Some(user.id);
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("user")
            .arg("get")
            .arg(self.get_user_id())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let user_id = self.get_user_id();

        let assert = command_state
            .success()
            .stdout(starts_with(format!(
                "Executing get user with ID: {user_id}"
            )))
            .stdout(is_match(format!("| Username[ ]+| {}", self.username)).unwrap())
            .stdout(is_match(format!("| Status[ ]+| {}", self.status)).unwrap());

        // Check global permissions
        let assert = if self.check_global_perms {
            assert.stdout(is_match("Global[ ]+|[ ]+Permission[ ]+| Value").unwrap())
        } else {
            assert
        };

        // Check stream permissions
        let assert = if let Some(stream_id) = self.check_stream_perms {
            assert.stdout(
                is_match(format!("Stream: {stream_id}[ ]+|[ ]+Permission[ ]+| Value")).unwrap(),
            )
        } else {
            assert
        };

        // Check topic permissions
        if let Some(topic_id) = self.check_topic_perms {
            assert.stdout(
                is_match(format!("Topic: {topic_id}[ ]+|[ ]+Permission[ ]+| Value")).unwrap(),
            );
        }
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        if self.create_user {
            let deleted = client
                .delete_user(&self.username.as_str().try_into().unwrap())
                .await;
            assert!(deleted.is_ok());
        }
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestUserGetCmd::new(
            TestUser::New("producer"),
            UserStatus::Active,
            TestUserId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserGetCmd::new(
            TestUser::New("testing"),
            UserStatus::Inactive,
            TestUserId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserGetCmd::new(
            TestUser::NewWithGlobalPerms("misc"),
            UserStatus::Active,
            TestUserId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserGetCmd::new(
            TestUser::NewWithStreamPerms("tools", 3),
            UserStatus::Active,
            TestUserId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserGetCmd::new(
            TestUser::NewWithStreamAndTopicPerms("limited", 1, 3),
            UserStatus::Active,
            TestUserId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserGetCmd::new(
            TestUser::Existing("iggy"),
            UserStatus::Active,
            TestUserId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserGetCmd::new(
            TestUser::Existing("iggy"),
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
            vec!["user", "get", "--help"],
            format!(
                r#"Get details of a single user with given ID

The user ID can be specified as either a username or an ID

Examples:
 iggy user get 2
 iggy user get testuser

{USAGE_PREFIX} user get <USER_ID>

Arguments:
  <USER_ID>
          User ID to get
{CLAP_INDENT}
          The user ID can be specified as either a username or an ID

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
            vec!["user", "get", "-h"],
            format!(
                r#"Get details of a single user with given ID

{USAGE_PREFIX} user get <USER_ID>

Arguments:
  <USER_ID>  User ID to get

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
