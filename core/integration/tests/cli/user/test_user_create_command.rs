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
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, USAGE_PREFIX,
};
use crate::cli::user::common::PermissionsTestArgs;
use ahash::AHashMap;
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use iggy::prelude::{GlobalPermissions, StreamPermissions, TopicPermissions};
use iggy::prelude::{Permissions, UserStatus};
use predicates::str::diff;
use serial_test::parallel;

#[derive(Debug, Clone, Default)]
enum UserStatusTest {
    #[default]
    Default,
    Active,
    Inactive,
}

impl UserStatusTest {
    fn as_arg(&self) -> Vec<String> {
        match self {
            Self::Default => vec![],
            Self::Active => vec![String::from("--user-status"), String::from("active")],
            Self::Inactive => vec![String::from("--user-status"), String::from("inactive")],
        }
    }
}

impl From<UserStatusTest> for UserStatus {
    fn from(value: UserStatusTest) -> Self {
        match value {
            UserStatusTest::Default => Self::Active,
            UserStatusTest::Active => Self::Active,
            UserStatusTest::Inactive => Self::Inactive,
        }
    }
}

struct TestUserCreateCmd {
    username: String,
    password: String,
    status: UserStatusTest,
    permissions: PermissionsTestArgs,
}

impl TestUserCreateCmd {
    fn new(
        username: String,
        password: String,
        status: UserStatusTest,
        permissions: PermissionsTestArgs,
    ) -> Self {
        Self {
            username,
            password,
            status,
            permissions,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut args = vec![self.username.clone(), self.password.clone()];
        args.extend(self.status.as_arg());
        args.extend(self.permissions.as_arg());

        args
    }
}

#[async_trait]
impl IggyCmdTestCase for TestUserCreateCmd {
    async fn prepare_server_state(&mut self, _client: &dyn Client) {}

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("user")
            .arg("create")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        command_state
            .success()
            .stdout(diff(format!("Executing create user with username: {} and password: {}\nUser with username: {} and password: {} created\n",
                                            self.username, self.password, self.username, self.password)));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let user = client
            .get_user(&self.username.as_str().try_into().unwrap())
            .await;
        assert!(user.is_ok());
        let user = user.unwrap().expect("User not found");
        assert_eq!(user.username, self.username);
        assert_eq!(user.status, self.status.clone().into());
        assert_eq!(
            user.permissions,
            self.permissions.expected_permissions.clone()
        );
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestUserCreateCmd::new(
            String::from("username"),
            String::from("password"),
            UserStatusTest::Default,
            PermissionsTestArgs::default(),
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserCreateCmd::new(
            String::from("active_user"),
            String::from("password"),
            UserStatusTest::Active,
            PermissionsTestArgs::default(),
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserCreateCmd::new(
            String::from("inactive_user"),
            String::from("password"),
            UserStatusTest::Inactive,
            PermissionsTestArgs::default(),
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserCreateCmd::new(
            String::from("reader"),
            String::from("password"),
            UserStatusTest::Active,
            PermissionsTestArgs::new(
                Some(String::from("r_srv,r_usr,r_top")),
                vec![],
                Some(Permissions {
                    global: GlobalPermissions {
                        manage_servers: false,
                        read_servers: true,
                        manage_users: false,
                        read_users: true,
                        manage_streams: false,
                        read_streams: false,
                        manage_topics: false,
                        read_topics: true,
                        poll_messages: false,
                        send_messages: false,
                    },
                    streams: None,
                }),
            ),
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserCreateCmd::new(
            String::from("stream3"),
            String::from("password"),
            UserStatusTest::Active,
            PermissionsTestArgs::new(
                None,
                vec![String::from("3")],
                Some(Permissions {
                    global: GlobalPermissions::default(),
                    streams: Some(AHashMap::from([(3usize, StreamPermissions::default())])),
                }),
            ),
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserCreateCmd::new(
            String::from("stream1"),
            String::from("password"),
            UserStatusTest::Active,
            PermissionsTestArgs::new(
                None,
                vec![String::from("1#1:m_top,r_top,p_msg,s_msg")],
                Some(Permissions {
                    global: GlobalPermissions::default(),
                    streams: Some(AHashMap::from([(
                        1usize,
                        StreamPermissions {
                            topics: Some(AHashMap::from([(
                                1,
                                TopicPermissions {
                                    manage_topic: true,
                                    read_topic: true,
                                    poll_messages: true,
                                    send_messages: true,
                                },
                            )])),
                            ..Default::default()
                        },
                    )])),
                }),
            ),
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestUserCreateCmd::new(
            String::from("giant"),
            String::from("password"),
            UserStatusTest::Active,
            PermissionsTestArgs::new(
                Some(String::from("m_srv,r_srv,m_str,r_str")),
                vec![String::from("2#1:p_msg,s_msg")],
                Some(Permissions {
                    global: GlobalPermissions {
                        manage_servers: true,
                        read_servers: true,
                        manage_users: false,
                        read_users: false,
                        manage_streams: true,
                        read_streams: true,
                        manage_topics: false,
                        read_topics: false,
                        poll_messages: false,
                        send_messages: false,
                    },
                    streams: Some(AHashMap::from([(
                        2usize,
                        StreamPermissions {
                            topics: Some(AHashMap::from([(
                                1,
                                TopicPermissions {
                                    manage_topic: false,
                                    read_topic: false,
                                    poll_messages: true,
                                    send_messages: true,
                                },
                            )])),
                            ..Default::default()
                        },
                    )])),
                }),
            ),
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["user", "create", "--help"],
            format!(
                r#"Create user with given username and password

Examples
 iggy user create testuser pass#1%X!
 iggy user create guest guess --user-status inactive

{USAGE_PREFIX} user create [OPTIONS] <USERNAME> <PASSWORD>

Arguments:
  <USERNAME>
          Username
{CLAP_INDENT}
          Unique identifier for the user account on iggy server,
          must be between 3 and 50 characters long.

  <PASSWORD>
          Password
{CLAP_INDENT}
          Password of the user, must be between 3 and 100 characters long.

Options:
  -u, --user-status <USER_STATUS>
          User status
{CLAP_INDENT}
          [default: active]
          [possible values: active, inactive]

  -g, --global-permissions <GLOBAL_PERMISSIONS>
          Set global permissions for created user
{CLAP_INDENT}
          All global permissions by default are set to false and this command line option
          allows to set each permission individually. Permissions are separated
          by comma and each permission is identified by the same name as in the iggy
          SDK in iggy::models::permissions::GlobalPermissions struct. For each permission
          there's long variant (same as in SDK) and short variant.
{CLAP_INDENT}
          Available permissions (long and short versions):  manage_servers / m_srv,
          read_servers / r_srv, manage_users / m_usr, read_users / r_usr,
          manage_streams / m_str, read_streams / r_str, manage_topics / m_top,
          read_topics / r_top, poll_messages / p_msg, send_messages / s_msg
{CLAP_INDENT}
          Examples:
           iggy user create guest guess --global-permissions p_msg,s_msg
           iggy user create admin pass#1%X! -g m_srv,r_srv,m_usr,r_usr,m_str,r_str,m_top,r_top,p_msg,s_msg

  -s, --stream-permissions <STREAM_PERMISSIONS>
          Set stream permissions for created user
{CLAP_INDENT}
          Stream permissions are defined by each stream separately. Setting permission for stream
          allows to set each permission individually, by default, if no permission is provided
          (only stream ID is provided) all are set fo false. Stream permission format consists
          of stream ID followed by colon (:) and list of permissions separated by comma (,).
          For each stream permission there's long variant (same as in SDK in
          iggy::models::permissions::StreamPermissions) and short variant.
{CLAP_INDENT}
          Available stream permissions: manage_stream / m_str, read_stream / r_str, manage_topics / m_top,
          read_topics / r_top, poll_messages / p_msg, send_messages / s_msg.
{CLAP_INDENT}
          For each stream one can set permissions for each topic separately. Topic permissions
          are defined for each topic separately. Setting permission for topic allows to set each
          permission individually, by default, if no permission is provided (only topic ID is provided)
          all are set fo false. Topic permission format consists of topic ID followed by colon (:)
          and list of permissions separated by comma (,). For each topic permission there's long
          variant (same as in SDK in iggy::models::permissions::TopicPermissions) and short variant.
          Topic permissions are separated by hash (#) after stream permissions.
{CLAP_INDENT}
          Available topic permissions: manage_topic / m_top, read_topic / r_top, poll_messages / p_msg,
          send_messages / s_msg.
{CLAP_INDENT}
          Permissions format: STREAM_ID\[:STREAM_PERMISSIONS\]\[#TOPIC_ID\[:TOPIC_PERMISSIONS\]\]
{CLAP_INDENT}
          Examples:
           iggy user create guest guest -s 1:manage_topics,read_topics
           iggy user create admin p@Ss! --stream-permissions 2:m_str,r_str,m_top,r_top,p_msg,s_msg
           iggy user create sender s3n43r -s 3#1:s_msg#2:s_msg
           iggy user create user1 test12 -s 4:manage_stream,r_top#1:s_msg,p_msg#2:manage_topic

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
            vec!["user", "create", "-h"],
            format!(
                r#"Create user with given username and password

{USAGE_PREFIX} user create [OPTIONS] <USERNAME> <PASSWORD>

Arguments:
  <USERNAME>  Username
  <PASSWORD>  Password

Options:
  -u, --user-status <USER_STATUS>                User status [default: active] [possible values: active, inactive]
  -g, --global-permissions <GLOBAL_PERMISSIONS>  Set global permissions for created user
  -s, --stream-permissions <STREAM_PERMISSIONS>  Set stream permissions for created user
  -h, --help                                     Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
