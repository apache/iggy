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

use super::constants::{
    MANAGE_SERVERS_LONG, MANAGE_SERVERS_SHORT, MANAGE_STREAMS_LONG, MANAGE_STREAMS_SHORT,
    MANAGE_TOPICS_LONG, MANAGE_TOPICS_SHORT, MANAGE_USERS_LONG, MANAGE_USERS_SHORT,
    POLL_MESSAGES_LONG, POLL_MESSAGES_SHORT, READ_SERVERS_LONG, READ_SERVERS_SHORT,
    READ_STREAMS_LONG, READ_STREAMS_SHORT, READ_TOPICS_LONG, READ_TOPICS_SHORT, READ_USERS_LONG,
    READ_USERS_SHORT, SEND_MESSAGES_LONG, SEND_MESSAGES_SHORT,
};
use iggy::prelude::GlobalPermissions;
use std::str::FromStr;

#[derive(Debug, PartialEq)]
pub(super) enum GlobalPermission {
    ManageServers,
    ReadServers,
    ManageUsers,
    ReadUsers,
    ManageStreams,
    ReadStreams,
    ManageTopics,
    ReadTopics,
    PollMessages,
    SendMessages,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GlobalPermissionError(String);

impl FromStr for GlobalPermission {
    type Err = GlobalPermissionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            MANAGE_SERVERS_SHORT | MANAGE_SERVERS_LONG => Ok(GlobalPermission::ManageServers),
            READ_SERVERS_SHORT | READ_SERVERS_LONG => Ok(GlobalPermission::ReadServers),
            MANAGE_USERS_SHORT | MANAGE_USERS_LONG => Ok(GlobalPermission::ManageUsers),
            READ_USERS_SHORT | READ_USERS_LONG => Ok(GlobalPermission::ReadUsers),
            MANAGE_STREAMS_SHORT | MANAGE_STREAMS_LONG => Ok(GlobalPermission::ManageStreams),
            READ_STREAMS_SHORT | READ_STREAMS_LONG => Ok(GlobalPermission::ReadStreams),
            MANAGE_TOPICS_SHORT | MANAGE_TOPICS_LONG => Ok(GlobalPermission::ManageTopics),
            READ_TOPICS_SHORT | READ_TOPICS_LONG => Ok(GlobalPermission::ReadTopics),
            POLL_MESSAGES_SHORT | POLL_MESSAGES_LONG => Ok(GlobalPermission::PollMessages),
            SEND_MESSAGES_SHORT | SEND_MESSAGES_LONG => Ok(GlobalPermission::SendMessages),
            "" => Err(GlobalPermissionError("[empty]".to_owned())),
            _ => Err(GlobalPermissionError(s.to_owned())),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GlobalPermissionsArg {
    pub(crate) permissions: GlobalPermissions,
}

impl From<GlobalPermissionsArg> for GlobalPermissions {
    fn from(cmd: GlobalPermissionsArg) -> Self {
        cmd.permissions
    }
}

impl GlobalPermissionsArg {
    pub(super) fn new(permissions: Vec<GlobalPermission>) -> Self {
        let mut result = GlobalPermissionsArg {
            permissions: GlobalPermissions::default(),
        };

        for permission in permissions {
            result.set_permission(permission);
        }

        result
    }

    fn set_permission(&mut self, permission: GlobalPermission) {
        match permission {
            GlobalPermission::ManageServers => self.permissions.manage_servers = true,
            GlobalPermission::ReadServers => self.permissions.read_servers = true,
            GlobalPermission::ManageUsers => self.permissions.manage_users = true,
            GlobalPermission::ReadUsers => self.permissions.read_users = true,
            GlobalPermission::ManageStreams => self.permissions.manage_streams = true,
            GlobalPermission::ReadStreams => self.permissions.read_streams = true,
            GlobalPermission::ManageTopics => self.permissions.manage_topics = true,
            GlobalPermission::ReadTopics => self.permissions.read_topics = true,
            GlobalPermission::PollMessages => self.permissions.poll_messages = true,
            GlobalPermission::SendMessages => self.permissions.send_messages = true,
        }
    }
}

impl FromStr for GlobalPermissionsArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (values, errors): (Vec<_>, Vec<_>) = s
            .split(',')
            .map(|p| p.parse::<GlobalPermission>())
            .partition(Result::is_ok);

        if !errors.is_empty() {
            let errors = errors
                .into_iter()
                .map(|e| format!("\"{}\"", e.err().unwrap().0))
                .collect::<Vec<String>>();

            return Err(format!(
                "Unknown global permission{} {}",
                match errors.len() {
                    1 => "",
                    _ => "s",
                },
                errors.join(", "),
            ));
        }

        Ok(GlobalPermissionsArg::new(
            values.into_iter().map(|p| p.unwrap()).collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_deserialize_single_permission() {
        assert_eq!(
            GlobalPermission::from_str("manage_servers").unwrap(),
            GlobalPermission::ManageServers
        );
        assert_eq!(
            GlobalPermission::from_str("read_servers").unwrap(),
            GlobalPermission::ReadServers
        );
        assert_eq!(
            GlobalPermission::from_str("manage_users").unwrap(),
            GlobalPermission::ManageUsers
        );
        assert_eq!(
            GlobalPermission::from_str("read_users").unwrap(),
            GlobalPermission::ReadUsers
        );
        assert_eq!(
            GlobalPermission::from_str("manage_streams").unwrap(),
            GlobalPermission::ManageStreams
        );
        assert_eq!(
            GlobalPermission::from_str("read_streams").unwrap(),
            GlobalPermission::ReadStreams
        );
        assert_eq!(
            GlobalPermission::from_str("manage_topics").unwrap(),
            GlobalPermission::ManageTopics
        );
        assert_eq!(
            GlobalPermission::from_str("read_topics").unwrap(),
            GlobalPermission::ReadTopics
        );
        assert_eq!(
            GlobalPermission::from_str("poll_messages").unwrap(),
            GlobalPermission::PollMessages
        );
        assert_eq!(
            GlobalPermission::from_str("send_messages").unwrap(),
            GlobalPermission::SendMessages
        );
    }

    #[test]
    fn should_deserialize_single_short_permission() {
        assert_eq!(
            GlobalPermission::from_str("m_srv").unwrap(),
            GlobalPermission::ManageServers
        );
        assert_eq!(
            GlobalPermission::from_str("r_srv").unwrap(),
            GlobalPermission::ReadServers
        );
        assert_eq!(
            GlobalPermission::from_str("m_usr").unwrap(),
            GlobalPermission::ManageUsers
        );
        assert_eq!(
            GlobalPermission::from_str("r_usr").unwrap(),
            GlobalPermission::ReadUsers
        );
        assert_eq!(
            GlobalPermission::from_str("m_str").unwrap(),
            GlobalPermission::ManageStreams
        );
        assert_eq!(
            GlobalPermission::from_str("r_str").unwrap(),
            GlobalPermission::ReadStreams
        );
        assert_eq!(
            GlobalPermission::from_str("m_top").unwrap(),
            GlobalPermission::ManageTopics
        );
        assert_eq!(
            GlobalPermission::from_str("r_top").unwrap(),
            GlobalPermission::ReadTopics
        );
        assert_eq!(
            GlobalPermission::from_str("p_msg").unwrap(),
            GlobalPermission::PollMessages
        );
        assert_eq!(
            GlobalPermission::from_str("s_msg").unwrap(),
            GlobalPermission::SendMessages
        );
    }

    #[test]
    fn should_not_deserialize_single_permission() {
        let wrong_permission = GlobalPermission::from_str("rad_topic");
        assert!(wrong_permission.is_err());
        assert_eq!(
            wrong_permission.unwrap_err(),
            GlobalPermissionError("rad_topic".to_owned())
        );
        let empty_permission = GlobalPermission::from_str("");
        assert!(empty_permission.is_err());
        assert_eq!(
            empty_permission.unwrap_err(),
            GlobalPermissionError("[empty]".to_owned())
        );
    }

    #[test]
    fn should_not_deserialize_single_short_permission() {
        let wrong_permission = GlobalPermission::from_str("w_top");
        assert!(wrong_permission.is_err());
        assert_eq!(
            wrong_permission.unwrap_err(),
            GlobalPermissionError("w_top".to_owned())
        );
        let wrong_permission = GlobalPermission::from_str("p_top");
        assert!(wrong_permission.is_err());
        assert_eq!(
            wrong_permission.unwrap_err(),
            GlobalPermissionError("p_top".to_owned())
        );
    }

    #[test]
    fn should_deserialize_permissions() {
        assert_eq!(
            GlobalPermissionsArg::from_str("manage_servers,read_servers,manage_users,read_users,manage_streams,read_streams,manage_topics,read_topics,poll_messages,send_messages")
                .unwrap(),
            GlobalPermissionsArg {
                permissions: GlobalPermissions {
                    manage_servers: true,
                    read_servers: true,
                    manage_users: true,
                    read_users: true,
                    manage_streams: true,
                    read_streams: true,
                    manage_topics: true,
                    read_topics: true,
                    poll_messages: true,
                    send_messages: true,
                }
            }
        );
        assert_eq!(
            GlobalPermissionsArg::from_str("manage_topics,read_topics").unwrap(),
            GlobalPermissionsArg {
                permissions: GlobalPermissions {
                    manage_servers: false,
                    read_servers: false,
                    manage_users: false,
                    read_users: false,
                    manage_streams: false,
                    read_streams: false,
                    manage_topics: true,
                    read_topics: true,
                    poll_messages: false,
                    send_messages: false,
                }
            }
        );
        assert_eq!(
            GlobalPermissionsArg::from_str("send_messages,manage_servers").unwrap(),
            GlobalPermissionsArg {
                permissions: GlobalPermissions {
                    manage_servers: true,
                    read_servers: false,
                    manage_users: false,
                    read_users: false,
                    manage_streams: false,
                    read_streams: false,
                    manage_topics: false,
                    read_topics: false,
                    poll_messages: false,
                    send_messages: true,
                }
            }
        );
    }

    #[test]
    fn should_deserialize_short_permissions() {
        assert_eq!(
            GlobalPermissionsArg::from_str(
                "m_srv,r_srv,m_usr,r_usr,m_str,r_str,m_top,r_top,p_msg,s_msg"
            )
            .unwrap(),
            GlobalPermissionsArg {
                permissions: GlobalPermissions {
                    manage_servers: true,
                    read_servers: true,
                    manage_users: true,
                    read_users: true,
                    manage_streams: true,
                    read_streams: true,
                    manage_topics: true,
                    read_topics: true,
                    poll_messages: true,
                    send_messages: true,
                }
            }
        );
        assert_eq!(
            GlobalPermissionsArg::from_str("m_top,r_top").unwrap(),
            GlobalPermissionsArg {
                permissions: GlobalPermissions {
                    manage_servers: false,
                    read_servers: false,
                    manage_users: false,
                    read_users: false,
                    manage_streams: false,
                    read_streams: false,
                    manage_topics: true,
                    read_topics: true,
                    poll_messages: false,
                    send_messages: false,
                }
            }
        );
        assert_eq!(
            GlobalPermissionsArg::from_str("s_msg,m_srv").unwrap(),
            GlobalPermissionsArg {
                permissions: GlobalPermissions {
                    manage_servers: true,
                    read_servers: false,
                    manage_users: false,
                    read_users: false,
                    manage_streams: false,
                    read_streams: false,
                    manage_topics: false,
                    read_topics: false,
                    poll_messages: false,
                    send_messages: true,
                }
            }
        );
    }

    #[test]
    fn should_not_deserialize_permissions() {
        let wrong_id = GlobalPermissionsArg::from_str("4a");
        assert!(wrong_id.is_err());
        assert_eq!(wrong_id.unwrap_err(), "Unknown global permission \"4a\"");
        let wrong_permission = GlobalPermissionsArg::from_str("read_topic");
        assert!(wrong_permission.is_err());
        assert_eq!(
            wrong_permission.unwrap_err(),
            "Unknown global permission \"read_topic\""
        );
        let multiple_wrong = GlobalPermissionsArg::from_str("read_topic,sent_messages");
        assert!(multiple_wrong.is_err());
        assert_eq!(
            multiple_wrong.unwrap_err(),
            "Unknown global permissions \"read_topic\", \"sent_messages\""
        );
    }

    #[test]
    fn should_not_deserialize_short_permissions() {
        let wrong_permission = GlobalPermissionsArg::from_str("r_topic");
        assert!(wrong_permission.is_err());
        assert_eq!(
            wrong_permission.unwrap_err(),
            "Unknown global permission \"r_topic\""
        );
        let multiple_wrong = GlobalPermissionsArg::from_str("r_topic,sent_msg");
        assert!(multiple_wrong.is_err());
        assert_eq!(
            multiple_wrong.unwrap_err(),
            "Unknown global permissions \"r_topic\", \"sent_msg\""
        );
    }
}
