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

use iggy::prelude::TopicPermissions;
use std::str::FromStr;

use super::constants::{
    MANAGE_TOPIC_LONG, MANAGE_TOPIC_SHORT, POLL_MESSAGES_LONG, POLL_MESSAGES_SHORT,
    READ_TOPIC_LONG, READ_TOPIC_SHORT, SEND_MESSAGES_LONG, SEND_MESSAGES_SHORT,
};

#[derive(Clone, Debug, PartialEq)]
enum TopicPermission {
    ManageTopic,
    ReadTopic,
    PollMessages,
    SendMessages,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TopicPermissionError(String);

impl FromStr for TopicPermission {
    type Err = TopicPermissionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            MANAGE_TOPIC_SHORT | MANAGE_TOPIC_LONG => Ok(TopicPermission::ManageTopic),
            READ_TOPIC_SHORT | READ_TOPIC_LONG => Ok(TopicPermission::ReadTopic),
            POLL_MESSAGES_SHORT | POLL_MESSAGES_LONG => Ok(TopicPermission::PollMessages),
            SEND_MESSAGES_SHORT | SEND_MESSAGES_LONG => Ok(TopicPermission::SendMessages),
            "" => Err(TopicPermissionError("[empty]".to_owned())),
            _ => Err(TopicPermissionError(s.to_owned())),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TopicPermissionsArg {
    pub(crate) topic_id: usize,
    pub(crate) permissions: TopicPermissions,
}

impl From<TopicPermissionsArg> for TopicPermissions {
    fn from(cmd: TopicPermissionsArg) -> Self {
        cmd.permissions
    }
}

impl TopicPermissionsArg {
    fn new(topic_id: usize, topic_permissions: Vec<TopicPermission>) -> Self {
        let mut result = Self {
            topic_id,
            permissions: TopicPermissions::default(),
        };

        for permission in topic_permissions {
            result.set_permission(permission);
        }

        result
    }

    fn set_permission(&mut self, permission: TopicPermission) {
        match permission {
            TopicPermission::ManageTopic => self.permissions.manage_topic = true,
            TopicPermission::ReadTopic => self.permissions.read_topic = true,
            TopicPermission::PollMessages => self.permissions.poll_messages = true,
            TopicPermission::SendMessages => self.permissions.send_messages = true,
        }
    }
}

impl FromStr for TopicPermissionsArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(':');
        let topic_id = parts
            .next()
            .ok_or("Missing topic ID".to_string())
            .and_then(|id| {
                id.parse()
                    .map_err(|error| format!("Invalid topic ID - {error}"))
            })?;

        let permissions: Vec<TopicPermission> = match parts.next() {
            Some(permissions_str) => {
                let (values, errors): (Vec<_>, Vec<_>) = permissions_str
                    .split(',')
                    .map(|s| s.parse::<TopicPermission>())
                    .partition(Result::is_ok);

                if !errors.is_empty() {
                    let errors = errors
                        .into_iter()
                        .map(|e| format!("\"{}\"", e.err().unwrap().0))
                        .collect::<Vec<String>>();

                    return Err(format!(
                        "Unknown permission{} {} for topic ID: {}",
                        match errors.len() {
                            1 => "",
                            _ => "s",
                        },
                        errors.join(", "),
                        topic_id
                    ));
                }

                values.into_iter().map(|p| p.unwrap()).collect()
            }
            None => vec![],
        };

        Ok(TopicPermissionsArg::new(topic_id, permissions))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_deserialize_single_permission() {
        assert_eq!(
            TopicPermission::from_str("manage_topic").unwrap(),
            TopicPermission::ManageTopic
        );
        assert_eq!(
            TopicPermission::from_str("read_topic").unwrap(),
            TopicPermission::ReadTopic
        );
        assert_eq!(
            TopicPermission::from_str("poll_messages").unwrap(),
            TopicPermission::PollMessages
        );
        assert_eq!(
            TopicPermission::from_str("send_messages").unwrap(),
            TopicPermission::SendMessages
        );
    }

    #[test]
    fn should_deserialize_single_short_permission() {
        assert_eq!(
            TopicPermission::from_str("m_top").unwrap(),
            TopicPermission::ManageTopic
        );
        assert_eq!(
            TopicPermission::from_str("r_top").unwrap(),
            TopicPermission::ReadTopic
        );
        assert_eq!(
            TopicPermission::from_str("p_msg").unwrap(),
            TopicPermission::PollMessages
        );
        assert_eq!(
            TopicPermission::from_str("s_msg").unwrap(),
            TopicPermission::SendMessages
        );
    }

    #[test]
    fn should_not_deserialize_single_permission() {
        let wrong_permission = TopicPermission::from_str("rad_topic");
        assert!(wrong_permission.is_err());
        assert_eq!(
            wrong_permission.unwrap_err(),
            TopicPermissionError("rad_topic".to_owned())
        );
        let empty_permission = TopicPermission::from_str("");
        assert!(empty_permission.is_err());
        assert_eq!(
            empty_permission.unwrap_err(),
            TopicPermissionError("[empty]".to_owned())
        );
    }

    #[test]
    fn should_not_deserialize_single_short_permission() {
        let wrong_permission = TopicPermission::from_str("w_top");
        assert!(wrong_permission.is_err());
        assert_eq!(
            wrong_permission.unwrap_err(),
            TopicPermissionError("w_top".to_owned())
        );
        let wrong_permission = TopicPermission::from_str("p_top");
        assert!(wrong_permission.is_err());
        assert_eq!(
            wrong_permission.unwrap_err(),
            TopicPermissionError("p_top".to_owned())
        );
    }

    #[test]
    fn should_deserialize_permissions() {
        assert_eq!(
            TopicPermissionsArg::from_str("1:manage_topic,read_topic,poll_messages,send_messages")
                .unwrap(),
            TopicPermissionsArg {
                topic_id: 1,
                permissions: TopicPermissions {
                    manage_topic: true,
                    read_topic: true,
                    poll_messages: true,
                    send_messages: true,
                }
            }
        );
        assert_eq!(
            TopicPermissionsArg::from_str("1:manage_topic,read_topic").unwrap(),
            TopicPermissionsArg {
                topic_id: 1,
                permissions: TopicPermissions {
                    manage_topic: true,
                    read_topic: true,
                    poll_messages: false,
                    send_messages: false,
                }
            }
        );
        assert_eq!(
            TopicPermissionsArg::from_str("52:send_messages,read_topic").unwrap(),
            TopicPermissionsArg {
                topic_id: 52,
                permissions: TopicPermissions {
                    manage_topic: false,
                    read_topic: true,
                    poll_messages: false,
                    send_messages: true,
                }
            }
        );
        assert_eq!(
            TopicPermissionsArg::from_str("66").unwrap(),
            TopicPermissionsArg {
                topic_id: 66,
                permissions: TopicPermissions {
                    manage_topic: false,
                    read_topic: false,
                    poll_messages: false,
                    send_messages: false,
                }
            }
        );
        assert_eq!(
            TopicPermissionsArg::from_str("3:send_messages").unwrap(),
            TopicPermissionsArg {
                topic_id: 3,
                permissions: TopicPermissions {
                    manage_topic: false,
                    read_topic: false,
                    poll_messages: false,
                    send_messages: true,
                }
            }
        );
    }

    #[test]
    fn should_deserialize_short_permissions() {
        assert_eq!(
            TopicPermissionsArg::from_str("4:m_top,r_top,p_msg,s_msg").unwrap(),
            TopicPermissionsArg {
                topic_id: 4,
                permissions: TopicPermissions {
                    manage_topic: true,
                    read_topic: true,
                    poll_messages: true,
                    send_messages: true,
                }
            }
        );
        assert_eq!(
            TopicPermissionsArg::from_str("2:m_top,r_top").unwrap(),
            TopicPermissionsArg {
                topic_id: 2,
                permissions: TopicPermissions {
                    manage_topic: true,
                    read_topic: true,
                    poll_messages: false,
                    send_messages: false,
                }
            }
        );
        assert_eq!(
            TopicPermissionsArg::from_str("41:s_msg,r_top").unwrap(),
            TopicPermissionsArg {
                topic_id: 41,
                permissions: TopicPermissions {
                    manage_topic: false,
                    read_topic: true,
                    poll_messages: false,
                    send_messages: true,
                }
            }
        );
        assert_eq!(
            TopicPermissionsArg::from_str("99:s_msg").unwrap(),
            TopicPermissionsArg {
                topic_id: 99,
                permissions: TopicPermissions {
                    manage_topic: false,
                    read_topic: false,
                    poll_messages: false,
                    send_messages: true,
                }
            }
        );
    }

    #[test]
    fn should_not_deserialize_permissions() {
        let wrong_id = TopicPermissionsArg::from_str("4a");
        assert!(wrong_id.is_err());
        assert_eq!(
            wrong_id.unwrap_err(),
            "Invalid topic ID - invalid digit found in string"
        );
        let wrong_permission = TopicPermissionsArg::from_str("41:reed_topic");
        assert!(wrong_permission.is_err());
        assert_eq!(
            wrong_permission.unwrap_err(),
            "Unknown permission \"reed_topic\" for topic ID: 41"
        );
        let multiple_wrong = TopicPermissionsArg::from_str("56:reed_topic,sent_messages");
        assert!(multiple_wrong.is_err());
        assert_eq!(
            multiple_wrong.unwrap_err(),
            "Unknown permissions \"reed_topic\", \"sent_messages\" for topic ID: 56"
        );
    }

    #[test]
    fn should_not_deserialize_short_permissions() {
        let wrong_permission = TopicPermissionsArg::from_str("4:r_topic");
        assert!(wrong_permission.is_err());
        assert_eq!(
            wrong_permission.unwrap_err(),
            "Unknown permission \"r_topic\" for topic ID: 4"
        );
        let multiple_wrong = TopicPermissionsArg::from_str("55:r_topic,sent_msg");
        assert!(multiple_wrong.is_err());
        assert_eq!(
            multiple_wrong.unwrap_err(),
            "Unknown permissions \"r_topic\", \"sent_msg\" for topic ID: 55"
        );
    }
}
