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

use crate::BytesSerializable;
use crate::error::IggyError;
use ahash::AHashMap;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use comfy_table::Table;
use comfy_table::presets::ASCII_NO_BORDERS;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// `Permissions` is used to define the permissions of a user.
/// It consists of global permissions and stream permissions.
/// Global permissions are applied to all streams.
/// Stream permissions are applied to a specific stream.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Default)]
pub struct Permissions {
    /// Global permissions are applied to all streams.
    pub global: GlobalPermissions,

    /// Stream permissions are applied to a specific stream.
    pub streams: Option<AHashMap<usize, StreamPermissions>>,
}

/// `GlobalPermissions` are applied to all streams without a need to specify them one by one in the `streams` field.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Default)]
pub struct GlobalPermissions {
    /// `manage_servers` permission allows to manage the servers and includes all the permissions of `read_servers`.
    pub manage_servers: bool,

    /// `read_servers` permission allows to invoke the following methods:
    /// - get_stats
    /// - get_clients
    /// - get_client
    pub read_servers: bool,

    /// `manage_users` permission allows to manage the users and includes all the permissions of `read_users`.
    /// Additionally, the following methods can be invoked:
    /// - create_user
    /// - update_user
    /// - delete_user
    /// - update_permissions
    /// - change_password
    pub manage_users: bool,

    /// `read_users` permission allows to invoke the following methods:
    /// - get_user
    /// - get_users
    pub read_users: bool,

    /// `manage_streams` permission allows to manage the streams and includes all the permissions of `read_streams`.
    /// Also, it allows to manage all the topics of a stream, thus it has all the permissions of `manage_topics`.
    /// Additionally, the following methods can be invoked:
    /// - create_stream
    /// - update_stream
    /// - delete_stream
    pub manage_streams: bool,

    /// `read_streams` permission allows to read the streams and includes all the permissions of `read_topics`.
    /// Additionally, the following methods can be invoked:
    /// - get_stream
    /// - get_streams
    pub read_streams: bool,

    /// `manage_topics` permission allows to manage the topics and includes all the permissions of `read_topics`.
    /// Also, it allows to manage all the partitions of a topic, thus it has all the permissions of `manage_topic`.
    /// Additionally, the following methods can be invoked:
    /// - create_topic
    /// - update_topic
    /// - delete_topic
    pub manage_topics: bool,

    /// `read_topics` permission allows to read the topics, manage consumer groups, and includes all the permissions of `poll_messages`.
    /// Additionally, the following methods can be invoked:
    /// - get_topic
    /// - get_topics
    /// - get_consumer_group
    /// - get_consumer_groups
    /// - join_consumer_group
    /// - leave_consumer_group
    /// - create_consumer_group
    /// - delete_consumer_group
    pub read_topics: bool,

    /// `poll_messages` permission allows to poll messages from all the streams and theirs topics.
    pub poll_messages: bool,

    /// `send_messages` permission allows to send messages to all the streams and theirs topics.
    pub send_messages: bool,
}

/// `StreamPermissions` are applied to a specific stream and its all topics. If you want to define granular permissions for each topic, use the `topics` field.
/// These permissions do not override the global permissions, but extend them, and allow more granular control over the streams and the users that can access them.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Default)]
pub struct StreamPermissions {
    /// `manage_stream` permission allows to manage the stream and includes all the permissions of `read_stream`.
    /// Also, it allows to manage all the topics of a stream, thus it has all the permissions of `manage_topics`.
    /// Additionally, the following methods can be invoked:
    /// - create_stream
    /// - update_stream
    /// - delete_stream
    pub manage_stream: bool,

    /// `read_stream` permission allows to read the stream and includes all the permissions of `read_topics`.
    /// Also, it allows to read all the messages of a topic, thus it has all the permissions of `poll_messages`.
    /// Additionally, the following methods can be invoked:
    /// - get_stream
    /// - get_streams
    pub read_stream: bool,

    /// `manage_topics` permission allows to manage the topics and includes all the permissions of `read_topics`.
    /// Also, it allows to manage all the partitions of a topic, thus it has all the permissions of `manage_topic`.
    /// Additionally, the following methods can be invoked:
    /// - create_topic
    /// - update_topic
    /// - delete_topic
    pub manage_topics: bool,

    /// `read_topics` permission allows to read the topics, manage consumer groups, and includes all the permissions of `poll_messages`.
    pub read_topics: bool,

    /// `poll_messages` permission allows to poll messages from the stream and its topics.
    pub poll_messages: bool,

    /// `send_messages` permission allows to send messages to the stream and its topics.
    pub send_messages: bool,

    /// The `topics` field allows to define the granular permissions for each topic of a stream.
    pub topics: Option<AHashMap<usize, TopicPermissions>>,
}

/// `TopicPermissions` are applied to a specific topic of a stream. This is the lowest level of permissions.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Default)]
pub struct TopicPermissions {
    /// `manage_topic` permission allows to manage the topic and includes all the permissions of `read_topic`.
    pub manage_topic: bool,

    /// `read_topic` permission allows to read the topic, manage consumer groups, and includes all the permissions of `poll_messages`.
    pub read_topic: bool,

    /// `poll_messages` permission allows to poll messages from the topic.
    pub poll_messages: bool,

    /// `send_messages` permission allows to send messages to the topic.
    pub send_messages: bool,
}

impl Permissions {
    pub fn root() -> Self {
        Self {
            global: GlobalPermissions {
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
            },
            streams: None,
        }
    }
}

impl Display for Permissions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut result = String::new();
        result.push_str(&format!("manage_servers: {}\n", self.global.manage_servers));
        result.push_str(&format!("read_servers: {}\n", self.global.read_servers));
        result.push_str(&format!("manage_users: {}\n", self.global.manage_users));
        result.push_str(&format!("read_users: {}\n", self.global.read_users));
        result.push_str(&format!("manage_streams: {}\n", self.global.manage_streams));
        result.push_str(&format!("read_streams: {}\n", self.global.read_streams));
        result.push_str(&format!("manage_topics: {}\n", self.global.manage_topics));
        result.push_str(&format!("read_topics: {}\n", self.global.read_topics));
        result.push_str(&format!("poll_messages: {}\n", self.global.poll_messages));
        result.push_str(&format!("send_messages: {}\n", self.global.send_messages));
        if let Some(streams) = &self.streams {
            for (stream_id, stream) in streams {
                result.push_str(&format!("stream_id: {stream_id}\n"));
                result.push_str(&format!("manage_stream: {}\n", stream.manage_stream));
                result.push_str(&format!("read_stream: {}\n", stream.read_stream));
                result.push_str(&format!("manage_topics: {}\n", stream.manage_topics));
                result.push_str(&format!("read_topics: {}\n", stream.read_topics));
                result.push_str(&format!("poll_messages: {}\n", stream.poll_messages));
                result.push_str(&format!("send_messages: {}\n", stream.send_messages));
                if let Some(topics) = &stream.topics {
                    for (topic_id, topic) in topics {
                        result.push_str(&format!("topic_id: {topic_id}\n"));
                        result.push_str(&format!("manage_topic: {}\n", topic.manage_topic));
                        result.push_str(&format!("read_topic: {}\n", topic.read_topic));
                        result.push_str(&format!("poll_messages: {}\n", topic.poll_messages));
                        result.push_str(&format!("send_messages: {}\n", topic.send_messages));
                    }
                }
            }
        }

        write!(f, "{result}")
    }
}

impl BytesSerializable for Permissions {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_u8(if self.global.manage_servers { 1 } else { 0 });
        bytes.put_u8(if self.global.read_servers { 1 } else { 0 });
        bytes.put_u8(if self.global.manage_users { 1 } else { 0 });
        bytes.put_u8(if self.global.read_users { 1 } else { 0 });
        bytes.put_u8(if self.global.manage_streams { 1 } else { 0 });
        bytes.put_u8(if self.global.read_streams { 1 } else { 0 });
        bytes.put_u8(if self.global.manage_topics { 1 } else { 0 });
        bytes.put_u8(if self.global.read_topics { 1 } else { 0 });
        bytes.put_u8(if self.global.poll_messages { 1 } else { 0 });
        bytes.put_u8(if self.global.send_messages { 1 } else { 0 });
        if let Some(streams) = &self.streams {
            bytes.put_u8(1);
            let streams_count = streams.len();
            let mut current_stream = 1;
            for (stream_id, stream) in streams {
                bytes.put_u32_le(*stream_id as u32);
                bytes.put_u8(if stream.manage_stream { 1 } else { 0 });
                bytes.put_u8(if stream.read_stream { 1 } else { 0 });
                bytes.put_u8(if stream.manage_topics { 1 } else { 0 });
                bytes.put_u8(if stream.read_topics { 1 } else { 0 });
                bytes.put_u8(if stream.poll_messages { 1 } else { 0 });
                bytes.put_u8(if stream.send_messages { 1 } else { 0 });
                if let Some(topics) = &stream.topics {
                    bytes.put_u8(1);
                    let topics_count = topics.len();
                    let mut current_topic = 1;
                    for (topic_id, topic) in topics {
                        bytes.put_u32_le(*topic_id as u32);
                        bytes.put_u8(if topic.manage_topic { 1 } else { 0 });
                        bytes.put_u8(if topic.read_topic { 1 } else { 0 });
                        bytes.put_u8(if topic.poll_messages { 1 } else { 0 });
                        bytes.put_u8(if topic.send_messages { 1 } else { 0 });
                        if current_topic < topics_count {
                            current_topic += 1;
                            bytes.put_u8(1);
                        } else {
                            bytes.put_u8(0);
                        }
                    }
                } else {
                    bytes.put_u8(0);
                }
                if current_stream < streams_count {
                    current_stream += 1;
                    bytes.put_u8(1);
                } else {
                    bytes.put_u8(0);
                }
            }
        } else {
            bytes.put_u8(0);
        }
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        let mut bytes = bytes;
        let manage_servers = bytes.get_u8() == 1;
        let read_servers = bytes.get_u8() == 1;
        let manage_users = bytes.get_u8() == 1;
        let read_users = bytes.get_u8() == 1;
        let manage_streams = bytes.get_u8() == 1;
        let read_streams = bytes.get_u8() == 1;
        let manage_topics = bytes.get_u8() == 1;
        let read_topics = bytes.get_u8() == 1;
        let poll_messages = bytes.get_u8() == 1;
        let send_messages = bytes.get_u8() == 1;
        let mut streams = None;
        if bytes.get_u8() == 1 {
            let mut streams_map = AHashMap::new();
            loop {
                let stream_id = bytes.get_u32_le() as usize;
                let manage_stream = bytes.get_u8() == 1;
                let read_stream = bytes.get_u8() == 1;
                let manage_topics = bytes.get_u8() == 1;
                let read_topics = bytes.get_u8() == 1;
                let poll_messages = bytes.get_u8() == 1;
                let send_messages = bytes.get_u8() == 1;
                let mut topics = None;
                if bytes.get_u8() == 1 {
                    let mut topics_map = AHashMap::new();
                    loop {
                        let topic_id = bytes.get_u32_le() as usize;
                        let manage_topic = bytes.get_u8() == 1;
                        let read_topic = bytes.get_u8() == 1;
                        let poll_messages = bytes.get_u8() == 1;
                        let send_messages = bytes.get_u8() == 1;
                        topics_map.insert(
                            topic_id,
                            TopicPermissions {
                                manage_topic,
                                read_topic,
                                poll_messages,
                                send_messages,
                            },
                        );
                        if bytes.get_u8() == 0 {
                            break;
                        }
                    }
                    topics = Some(topics_map);
                }
                streams_map.insert(
                    stream_id,
                    StreamPermissions {
                        manage_stream,
                        read_stream,
                        manage_topics,
                        read_topics,
                        poll_messages,
                        send_messages,
                        topics,
                    },
                );
                if bytes.get_u8() == 0 {
                    break;
                }
            }
            streams = Some(streams_map);
        }
        Ok(Self {
            global: GlobalPermissions {
                manage_servers,
                read_servers,
                manage_users,
                read_users,
                manage_streams,
                read_streams,
                manage_topics,
                read_topics,
                poll_messages,
                send_messages,
            },
            streams,
        })
    }
}

impl From<GlobalPermissions> for Table {
    fn from(value: GlobalPermissions) -> Self {
        let mut table = Self::new();

        table.load_preset(ASCII_NO_BORDERS);
        table.set_header(vec!["Permission", "Value"]);
        table.add_row(vec![
            "Manage Servers",
            value.manage_servers.to_string().as_str(),
        ]);
        table.add_row(vec![
            "Read Servers",
            value.read_servers.to_string().as_str(),
        ]);
        table.add_row(vec![
            "Manage Users",
            value.manage_users.to_string().as_str(),
        ]);
        table.add_row(vec!["Read Users", value.read_users.to_string().as_str()]);
        table.add_row(vec![
            "Manage Streams",
            value.manage_streams.to_string().as_str(),
        ]);
        table.add_row(vec![
            "Read Streams",
            value.read_streams.to_string().as_str(),
        ]);
        table.add_row(vec![
            "Manage Topics",
            value.manage_topics.to_string().as_str(),
        ]);
        table.add_row(vec!["Read Topics", value.read_topics.to_string().as_str()]);
        table.add_row(vec![
            "Poll Messages",
            value.poll_messages.to_string().as_str(),
        ]);
        table.add_row(vec![
            "Send Messages",
            value.send_messages.to_string().as_str(),
        ]);

        table
    }
}

impl From<&TopicPermissions> for Table {
    fn from(value: &TopicPermissions) -> Self {
        let mut table = Self::new();

        table.load_preset(ASCII_NO_BORDERS);
        table.set_header(vec!["Permission", "Value"]);
        table.add_row(vec![
            "Manage Topic",
            value.manage_topic.to_string().as_str(),
        ]);
        table.add_row(vec!["Read Topic", value.read_topic.to_string().as_str()]);
        table.add_row(vec![
            "Poll Messages",
            value.poll_messages.to_string().as_str(),
        ]);
        table.add_row(vec![
            "Send Messages",
            value.send_messages.to_string().as_str(),
        ]);

        table
    }
}

impl From<&StreamPermissions> for Table {
    fn from(value: &StreamPermissions) -> Self {
        let mut table = Self::new();

        table.load_preset(ASCII_NO_BORDERS);
        table.set_header(vec!["Permission", "Value"]);
        table.add_row(vec![
            "Manage Stream",
            value.manage_stream.to_string().as_str(),
        ]);
        table.add_row(vec!["Read Stream", value.read_stream.to_string().as_str()]);
        table.add_row(vec![
            "Manage Topics",
            value.manage_topics.to_string().as_str(),
        ]);
        table.add_row(vec!["Read Topics", value.read_topics.to_string().as_str()]);
        table.add_row(vec![
            "Poll Messages",
            value.poll_messages.to_string().as_str(),
        ]);
        table.add_row(vec![
            "Send Messages",
            value.send_messages.to_string().as_str(),
        ]);

        if let Some(topics) = &value.topics {
            topics.iter().for_each(|(topic_id, topic_permissions)| {
                let topic_table: Table = topic_permissions.into();
                table.add_row(vec![
                    format!("Topic: {topic_id}").as_str(),
                    format!("{topic_table}").as_str(),
                ]);
            });
        }

        table
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_and_deserialized_from_bytes() {
        let permissions = Permissions {
            global: GlobalPermissions {
                manage_servers: true,
                read_servers: true,
                manage_users: true,
                read_users: true,
                manage_streams: false,
                read_streams: true,
                manage_topics: false,
                read_topics: true,
                poll_messages: true,
                send_messages: true,
            },
            streams: Some(AHashMap::from([
                (
                    1,
                    StreamPermissions {
                        manage_stream: true,
                        read_stream: true,
                        manage_topics: true,
                        read_topics: true,
                        poll_messages: true,
                        send_messages: true,
                        topics: Some(AHashMap::from([
                            (
                                1,
                                TopicPermissions {
                                    manage_topic: true,
                                    read_topic: true,
                                    poll_messages: true,
                                    send_messages: true,
                                },
                            ),
                            (
                                2,
                                TopicPermissions {
                                    manage_topic: true,
                                    read_topic: false,
                                    poll_messages: true,
                                    send_messages: false,
                                },
                            ),
                        ])),
                    },
                ),
                (
                    2,
                    StreamPermissions {
                        manage_stream: false,
                        read_stream: true,
                        manage_topics: false,
                        read_topics: true,
                        poll_messages: true,
                        send_messages: true,
                        topics: None,
                    },
                ),
            ])),
        };

        let bytes = permissions.to_bytes();
        let deserialized_permissions = Permissions::from_bytes(bytes).unwrap();

        assert_eq!(permissions, deserialized_permissions);
    }
}
