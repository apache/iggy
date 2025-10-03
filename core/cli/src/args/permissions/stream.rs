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
    MANAGE_STREAM_LONG, MANAGE_STREAM_SHORT, MANAGE_TOPICS_LONG, MANAGE_TOPICS_SHORT,
    POLL_MESSAGES_LONG, POLL_MESSAGES_SHORT, READ_STREAM_LONG, READ_STREAM_SHORT, READ_TOPICS_LONG,
    READ_TOPICS_SHORT, SEND_MESSAGES_LONG, SEND_MESSAGES_SHORT,
};
use crate::args::permissions::topic::TopicPermissionsArg;
use ahash::AHashMap;
use iggy::prelude::StreamPermissions;
use std::str::FromStr;

#[derive(Debug, PartialEq)]
pub(super) enum StreamPermission {
    ManageStream,
    ReadStream,
    ManageTopics,
    ReadTopics,
    PollMessages,
    SendMessages,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StreamPermissionError(String);

impl FromStr for StreamPermission {
    type Err = StreamPermissionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            MANAGE_STREAM_SHORT | MANAGE_STREAM_LONG => Ok(StreamPermission::ManageStream),
            READ_STREAM_SHORT | READ_STREAM_LONG => Ok(StreamPermission::ReadStream),
            MANAGE_TOPICS_SHORT | MANAGE_TOPICS_LONG => Ok(StreamPermission::ManageTopics),
            READ_TOPICS_SHORT | READ_TOPICS_LONG => Ok(StreamPermission::ReadTopics),
            POLL_MESSAGES_SHORT | POLL_MESSAGES_LONG => Ok(StreamPermission::PollMessages),
            SEND_MESSAGES_SHORT | SEND_MESSAGES_LONG => Ok(StreamPermission::SendMessages),
            "" => Err(StreamPermissionError("[empty]".to_owned())),
            _ => Err(StreamPermissionError(s.to_owned())),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StreamPermissionsArg {
    pub(crate) stream_id: usize,
    pub(crate) permissions: StreamPermissions,
}

impl From<StreamPermissionsArg> for StreamPermissions {
    fn from(cmd: StreamPermissionsArg) -> Self {
        cmd.permissions
    }
}

impl StreamPermissionsArg {
    pub(super) fn new(
        stream_id: usize,
        stream_permissions: Vec<StreamPermission>,
        topic_permissions: Vec<TopicPermissionsArg>,
    ) -> Self {
        let mut result = Self {
            stream_id,
            permissions: StreamPermissions::default(),
        };

        for permission in stream_permissions {
            result.set_permission(permission);
        }

        if !topic_permissions.is_empty() {
            let mut permissions = AHashMap::new();
            for permission in topic_permissions {
                permissions.insert(permission.topic_id, permission.permissions);
            }

            result.permissions.topics = Some(permissions);
        }

        result
    }

    fn set_permission(&mut self, permission: StreamPermission) {
        match permission {
            StreamPermission::ManageStream => self.permissions.manage_stream = true,
            StreamPermission::ReadStream => self.permissions.read_stream = true,
            StreamPermission::ManageTopics => self.permissions.manage_topics = true,
            StreamPermission::ReadTopics => self.permissions.read_topics = true,
            StreamPermission::PollMessages => self.permissions.poll_messages = true,
            StreamPermission::SendMessages => self.permissions.send_messages = true,
        }
    }
}

impl FromStr for StreamPermissionsArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut p = s.split('#');

        let stream_part = match p.next() {
            Some(part) => part,
            None => return Err("Missing stream permissions part".to_string()),
        };

        let mut parts = stream_part.split(':');
        let stream_id = parts
            .next()
            .ok_or("Missing stream ID".to_string())
            .and_then(|id| {
                id.parse()
                    .map_err(|error| format!("Invalid stream ID - {error}"))
            })?;

        let (stream_permissions, stream_errors): (Vec<StreamPermission>, Option<String>) =
            match parts.next() {
                Some(permissions_str) => {
                    let (values, errors): (Vec<_>, Vec<_>) = permissions_str
                        .split(',')
                        .map(|s| s.parse::<StreamPermission>())
                        .partition(Result::is_ok);

                    let error = if !errors.is_empty() {
                        let errors = errors
                            .into_iter()
                            .map(|e| format!("\"{}\"", e.err().unwrap().0))
                            .collect::<Vec<String>>();

                        Some(format!(
                            "Unknown permission{} {} for stream ID: {}",
                            match errors.len() {
                                1 => "",
                                _ => "s",
                            },
                            errors.join(", "),
                            stream_id
                        ))
                    } else {
                        None
                    };

                    (values.into_iter().map(|p| p.unwrap()).collect(), error)
                }
                None => (vec![], None),
            };

        let (topic_permissions, topic_errors): (Vec<TopicPermissionsArg>, Option<String>) = {
            let (permissions, errors): (Vec<_>, Vec<_>) = p
                .map(|s| s.parse::<TopicPermissionsArg>())
                .partition(Result::is_ok);

            let errors = if !errors.is_empty() {
                Some(
                    errors
                        .into_iter()
                        .map(|e| e.err().unwrap())
                        .collect::<Vec<String>>()
                        .join("; "),
                )
            } else {
                None
            };

            (
                permissions.into_iter().map(|p| p.unwrap()).collect(),
                errors,
            )
        };

        match (stream_errors, topic_errors) {
            (Some(e), Some(f)) => return Err(format!("{e}; {f}")),
            (Some(e), None) => return Err(e),
            (None, Some(f)) => return Err(f),
            (None, None) => (),
        }

        Ok(StreamPermissionsArg::new(
            stream_id,
            stream_permissions,
            topic_permissions,
        ))
    }
}

#[cfg(test)]
mod tests {
    use iggy::prelude::TopicPermissions;

    use super::*;

    #[test]
    fn should_deserialize_single_permission() {
        assert_eq!(
            StreamPermission::from_str("manage_stream").unwrap(),
            StreamPermission::ManageStream
        );
        assert_eq!(
            StreamPermission::from_str("read_stream").unwrap(),
            StreamPermission::ReadStream
        );
        assert_eq!(
            StreamPermission::from_str("manage_topics").unwrap(),
            StreamPermission::ManageTopics
        );
        assert_eq!(
            StreamPermission::from_str("read_topics").unwrap(),
            StreamPermission::ReadTopics
        );
        assert_eq!(
            StreamPermission::from_str("poll_messages").unwrap(),
            StreamPermission::PollMessages
        );
        assert_eq!(
            StreamPermission::from_str("send_messages").unwrap(),
            StreamPermission::SendMessages
        );
    }

    #[test]
    fn should_deserialize_single_short_permission() {
        assert_eq!(
            StreamPermission::from_str("m_str").unwrap(),
            StreamPermission::ManageStream
        );
        assert_eq!(
            StreamPermission::from_str("r_str").unwrap(),
            StreamPermission::ReadStream
        );
        assert_eq!(
            StreamPermission::from_str("m_top").unwrap(),
            StreamPermission::ManageTopics
        );
        assert_eq!(
            StreamPermission::from_str("r_top").unwrap(),
            StreamPermission::ReadTopics
        );
        assert_eq!(
            StreamPermission::from_str("p_msg").unwrap(),
            StreamPermission::PollMessages
        );
        assert_eq!(
            StreamPermission::from_str("s_msg").unwrap(),
            StreamPermission::SendMessages
        );
    }

    #[test]
    fn should_not_deserialize_single_permission() {
        let wrong_permission = StreamPermission::from_str("read_topic");
        assert!(wrong_permission.is_err());
        assert_eq!(
            wrong_permission.unwrap_err(),
            StreamPermissionError("read_topic".to_owned())
        );
        let empty_permission = StreamPermission::from_str("");
        assert!(empty_permission.is_err());
        assert_eq!(
            empty_permission.unwrap_err(),
            StreamPermissionError("[empty]".to_owned())
        );
    }

    #[test]
    fn should_not_deserialize_single_short_permission() {
        let wrong_permission = StreamPermission::from_str("w_top");
        assert!(wrong_permission.is_err());
        assert_eq!(
            wrong_permission.unwrap_err(),
            StreamPermissionError("w_top".to_owned())
        );
        let wrong_permission = StreamPermission::from_str("m_msg");
        assert!(wrong_permission.is_err());
        assert_eq!(
            wrong_permission.unwrap_err(),
            StreamPermissionError("m_msg".to_owned())
        );
    }

    #[test]
    fn should_deserialize_permissions() {
        assert_eq!(
            StreamPermissionsArg::from_str(
                "1:manage_stream,read_stream,manage_topics,read_topics,poll_messages,send_messages"
            )
            .unwrap(),
            StreamPermissionsArg {
                stream_id: 1,
                permissions: StreamPermissions {
                    manage_stream: true,
                    read_stream: true,
                    manage_topics: true,
                    read_topics: true,
                    poll_messages: true,
                    send_messages: true,
                    topics: None,
                }
            }
        );
        assert_eq!(
            StreamPermissionsArg::from_str("1:manage_topics,read_topics").unwrap(),
            StreamPermissionsArg {
                stream_id: 1,
                permissions: StreamPermissions {
                    manage_stream: false,
                    read_stream: false,
                    manage_topics: true,
                    read_topics: true,
                    poll_messages: false,
                    send_messages: false,
                    topics: None,
                }
            }
        );
        assert_eq!(
            StreamPermissionsArg::from_str("39:send_messages,read_topics,read_stream").unwrap(),
            StreamPermissionsArg {
                stream_id: 39,
                permissions: StreamPermissions {
                    manage_stream: false,
                    read_stream: true,
                    manage_topics: false,
                    read_topics: true,
                    poll_messages: false,
                    send_messages: true,
                    topics: None,
                }
            }
        );
        assert_eq!(
            StreamPermissionsArg::from_str("71").unwrap(),
            StreamPermissionsArg {
                stream_id: 71,
                permissions: StreamPermissions {
                    manage_stream: false,
                    read_stream: false,
                    manage_topics: false,
                    read_topics: false,
                    poll_messages: false,
                    send_messages: false,
                    topics: None,
                }
            }
        );
        assert_eq!(
            StreamPermissionsArg::from_str("9:send_messages").unwrap(),
            StreamPermissionsArg {
                stream_id: 9,
                permissions: StreamPermissions {
                    manage_stream: false,
                    read_stream: false,
                    manage_topics: false,
                    read_topics: false,
                    poll_messages: false,
                    send_messages: true,
                    topics: None,
                }
            }
        );
        assert_eq!(
            StreamPermissionsArg::from_str("9#1#2").unwrap(),
            StreamPermissionsArg {
                stream_id: 9,
                permissions: StreamPermissions {
                    manage_stream: false,
                    read_stream: false,
                    manage_topics: false,
                    read_topics: false,
                    poll_messages: false,
                    send_messages: false,
                    topics: Some(AHashMap::from([
                        (
                            2,
                            TopicPermissions {
                                manage_topic: false,
                                read_topic: false,
                                poll_messages: false,
                                send_messages: false,
                            }
                        ),
                        (
                            1,
                            TopicPermissions {
                                manage_topic: false,
                                read_topic: false,
                                poll_messages: false,
                                send_messages: false,
                            }
                        )
                    ])),
                }
            }
        );
        assert_eq!(
            StreamPermissionsArg::from_str("4:manage_topics#1:manage_topic#2:manage_topic")
                .unwrap(),
            StreamPermissionsArg {
                stream_id: 4,
                permissions: StreamPermissions {
                    manage_stream: false,
                    read_stream: false,
                    manage_topics: true,
                    read_topics: false,
                    poll_messages: false,
                    send_messages: false,
                    topics: Some(AHashMap::from([
                        (
                            2,
                            TopicPermissions {
                                manage_topic: true,
                                read_topic: false,
                                poll_messages: false,
                                send_messages: false,
                            }
                        ),
                        (
                            1,
                            TopicPermissions {
                                manage_topic: true,
                                read_topic: false,
                                poll_messages: false,
                                send_messages: false,
                            }
                        )
                    ])),
                }
            }
        );
    }

    #[test]
    fn should_deserialize_short_permissions() {
        assert_eq!(
            StreamPermissionsArg::from_str("111:m_str,r_str,m_top,r_top,p_msg,s_msg").unwrap(),
            StreamPermissionsArg {
                stream_id: 111,
                permissions: StreamPermissions {
                    manage_stream: true,
                    read_stream: true,
                    manage_topics: true,
                    read_topics: true,
                    poll_messages: true,
                    send_messages: true,
                    topics: None,
                }
            }
        );
        assert_eq!(
            StreamPermissionsArg::from_str("27:m_top,r_top").unwrap(),
            StreamPermissionsArg {
                stream_id: 27,
                permissions: StreamPermissions {
                    manage_stream: false,
                    read_stream: false,
                    manage_topics: true,
                    read_topics: true,
                    poll_messages: false,
                    send_messages: false,
                    topics: None,
                }
            }
        );
        assert_eq!(
            StreamPermissionsArg::from_str("9:s_msg,r_top,r_str").unwrap(),
            StreamPermissionsArg {
                stream_id: 9,
                permissions: StreamPermissions {
                    manage_stream: false,
                    read_stream: true,
                    manage_topics: false,
                    read_topics: true,
                    poll_messages: false,
                    send_messages: true,
                    topics: None,
                }
            }
        );
        assert_eq!(
            StreamPermissionsArg::from_str("4:s_msg").unwrap(),
            StreamPermissionsArg {
                stream_id: 4,
                permissions: StreamPermissions {
                    manage_stream: false,
                    read_stream: false,
                    manage_topics: false,
                    read_topics: false,
                    poll_messages: false,
                    send_messages: true,
                    topics: None,
                }
            }
        );
        assert_eq!(
            StreamPermissionsArg::from_str("6:m_top#1:m_top#2:m_top").unwrap(),
            StreamPermissionsArg {
                stream_id: 6,
                permissions: StreamPermissions {
                    manage_stream: false,
                    read_stream: false,
                    manage_topics: true,
                    read_topics: false,
                    poll_messages: false,
                    send_messages: false,
                    topics: Some(AHashMap::from([
                        (
                            2,
                            TopicPermissions {
                                manage_topic: true,
                                read_topic: false,
                                poll_messages: false,
                                send_messages: false,
                            }
                        ),
                        (
                            1,
                            TopicPermissions {
                                manage_topic: true,
                                read_topic: false,
                                poll_messages: false,
                                send_messages: false,
                            }
                        )
                    ])),
                }
            }
        );
    }

    #[test]
    fn should_not_deserialize_permissions() {
        let wrong_id = StreamPermissionsArg::from_str("4a");
        assert!(wrong_id.is_err());
        assert_eq!(
            wrong_id.unwrap_err(),
            "Invalid stream ID - invalid digit found in string"
        );
        let wrong_permission = StreamPermissionsArg::from_str("4:read_topic");
        assert!(wrong_permission.is_err());
        assert_eq!(
            wrong_permission.unwrap_err(),
            "Unknown permission \"read_topic\" for stream ID: 4"
        );
        let multiple_wrong = StreamPermissionsArg::from_str("55:read_topic,sent_messages");
        assert!(multiple_wrong.is_err());
        assert_eq!(
            multiple_wrong.unwrap_err(),
            "Unknown permissions \"read_topic\", \"sent_messages\" for stream ID: 55"
        );
        let multiple_wrong_with_topic =
            StreamPermissionsArg::from_str("55:read_topic,sent_messages#4:reed_topic");
        assert!(multiple_wrong_with_topic.is_err());
        assert_eq!(
            multiple_wrong_with_topic.unwrap_err(),
            "Unknown permissions \"read_topic\", \"sent_messages\" for stream ID: 55; Unknown permission \"reed_topic\" for topic ID: 4"
        );
    }

    #[test]
    fn should_not_deserialize_short_permissions() {
        let wrong_id = StreamPermissionsArg::from_str("1x");
        assert!(wrong_id.is_err());
        assert_eq!(
            wrong_id.unwrap_err(),
            "Invalid stream ID - invalid digit found in string"
        );
        let wrong_permission = StreamPermissionsArg::from_str("7:x_topic");
        assert!(wrong_permission.is_err());
        assert_eq!(
            wrong_permission.unwrap_err(),
            "Unknown permission \"x_topic\" for stream ID: 7"
        );
        let multiple_wrong = StreamPermissionsArg::from_str("37:w_top,s_msgs");
        assert!(multiple_wrong.is_err());
        assert_eq!(
            multiple_wrong.unwrap_err(),
            "Unknown permissions \"w_top\", \"s_msgs\" for stream ID: 37"
        );
        let multiple_wrong_with_topic = StreamPermissionsArg::from_str("11:r_to,s_msg#4:r_to");
        assert!(multiple_wrong_with_topic.is_err());
        assert_eq!(
            multiple_wrong_with_topic.unwrap_err(),
            "Unknown permission \"r_to\" for stream ID: 11; Unknown permission \"r_to\" for topic ID: 4"
        );
        let multiple_wrong_topics =
            StreamPermissionsArg::from_str("11:r_top,s_msg#7:r_tox#5:w_top,s_top");
        assert!(multiple_wrong_topics.is_err());
        assert_eq!(
            multiple_wrong_topics.unwrap_err(),
            "Unknown permission \"r_tox\" for topic ID: 7; Unknown permissions \"w_top\", \"s_top\" for topic ID: 5"
        );
    }
}
