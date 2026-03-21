// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::WireError;
use crate::codec::{WireDecode, WireEncode, read_u8, read_u32_le};
use bytes::{BufMut, BytesMut};

const HAS_NEXT: u8 = 1;
const NO_NEXT: u8 = 0;

fn bool_to_u8(b: bool) -> u8 {
    u8::from(b)
}

fn read_bool(buf: &[u8], offset: usize) -> Result<bool, WireError> {
    Ok(read_u8(buf, offset)? != 0)
}

/// Full permission set for a user.
///
/// Wire format:
/// ```text
/// [10 x bool as u8: global permissions]
/// [has_streams:1]
///   loop streams:
///     [stream_id:u32_le][6 x bool: stream perms][has_topics:1]
///     loop topics:
///       [topic_id:u32_le][4 x bool: topic perms][has_next_topic:1]
///     [has_next_stream:1]
/// ```
///
/// Streams and topics are stored as `Vec` sorted by ID for deterministic encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WirePermissions {
    pub global: WireGlobalPermissions,
    pub streams: Vec<WireStreamPermissions>,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireGlobalPermissions {
    pub manage_servers: bool,
    pub read_servers: bool,
    pub manage_users: bool,
    pub read_users: bool,
    pub manage_streams: bool,
    pub read_streams: bool,
    pub manage_topics: bool,
    pub read_topics: bool,
    pub poll_messages: bool,
    pub send_messages: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireStreamPermissions {
    pub stream_id: u32,
    pub manage_stream: bool,
    pub read_stream: bool,
    pub manage_topics: bool,
    pub read_topics: bool,
    pub poll_messages: bool,
    pub send_messages: bool,
    pub topics: Vec<WireTopicPermissions>,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireTopicPermissions {
    pub topic_id: u32,
    pub manage_topic: bool,
    pub read_topic: bool,
    pub poll_messages: bool,
    pub send_messages: bool,
}

impl WireGlobalPermissions {
    fn encode_into(&self, buf: &mut BytesMut) {
        buf.put_u8(bool_to_u8(self.manage_servers));
        buf.put_u8(bool_to_u8(self.read_servers));
        buf.put_u8(bool_to_u8(self.manage_users));
        buf.put_u8(bool_to_u8(self.read_users));
        buf.put_u8(bool_to_u8(self.manage_streams));
        buf.put_u8(bool_to_u8(self.read_streams));
        buf.put_u8(bool_to_u8(self.manage_topics));
        buf.put_u8(bool_to_u8(self.read_topics));
        buf.put_u8(bool_to_u8(self.poll_messages));
        buf.put_u8(bool_to_u8(self.send_messages));
    }

    fn decode_at(buf: &[u8], pos: usize) -> Result<(Self, usize), WireError> {
        let mut p = pos;
        let manage_servers = read_bool(buf, p)?;
        p += 1;
        let read_servers = read_bool(buf, p)?;
        p += 1;
        let manage_users = read_bool(buf, p)?;
        p += 1;
        let read_users = read_bool(buf, p)?;
        p += 1;
        let manage_streams = read_bool(buf, p)?;
        p += 1;
        let read_streams = read_bool(buf, p)?;
        p += 1;
        let manage_topics = read_bool(buf, p)?;
        p += 1;
        let read_topics = read_bool(buf, p)?;
        p += 1;
        let poll_messages = read_bool(buf, p)?;
        p += 1;
        let send_messages = read_bool(buf, p)?;
        p += 1;

        Ok((
            Self {
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
            p,
        ))
    }
}

impl WireTopicPermissions {
    const ENCODED_SIZE: usize = 4 + 4 + 1; // topic_id + 4 bools + has_next_topic

    fn encode_into(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.topic_id);
        buf.put_u8(bool_to_u8(self.manage_topic));
        buf.put_u8(bool_to_u8(self.read_topic));
        buf.put_u8(bool_to_u8(self.poll_messages));
        buf.put_u8(bool_to_u8(self.send_messages));
    }

    fn decode_at(buf: &[u8], pos: usize) -> Result<(Self, usize), WireError> {
        let mut p = pos;
        let topic_id = read_u32_le(buf, p)?;
        p += 4;
        let manage_topic = read_bool(buf, p)?;
        p += 1;
        let read_topic = read_bool(buf, p)?;
        p += 1;
        let poll_messages = read_bool(buf, p)?;
        p += 1;
        let send_messages = read_bool(buf, p)?;
        p += 1;

        Ok((
            Self {
                topic_id,
                manage_topic,
                read_topic,
                poll_messages,
                send_messages,
            },
            p,
        ))
    }
}

impl WireStreamPermissions {
    #[allow(clippy::missing_const_for_fn)]
    fn encoded_size(&self) -> usize {
        // stream_id(4) + 6 bools(6) + has_topics(1) + topics + has_next_stream(1)
        let topics_size: usize = if self.topics.is_empty() {
            0
        } else {
            self.topics.len() * WireTopicPermissions::ENCODED_SIZE
        };
        4 + 6 + 1 + topics_size + 1
    }

    fn encode_into(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.stream_id);
        buf.put_u8(bool_to_u8(self.manage_stream));
        buf.put_u8(bool_to_u8(self.read_stream));
        buf.put_u8(bool_to_u8(self.manage_topics));
        buf.put_u8(bool_to_u8(self.read_topics));
        buf.put_u8(bool_to_u8(self.poll_messages));
        buf.put_u8(bool_to_u8(self.send_messages));

        if self.topics.is_empty() {
            buf.put_u8(NO_NEXT);
        } else {
            buf.put_u8(HAS_NEXT);
            for (i, topic) in self.topics.iter().enumerate() {
                topic.encode_into(buf);
                let is_last = i == self.topics.len() - 1;
                buf.put_u8(if is_last { NO_NEXT } else { HAS_NEXT });
            }
        }
    }

    fn decode_at(buf: &[u8], pos: usize) -> Result<(Self, usize), WireError> {
        let mut p = pos;
        let stream_id = read_u32_le(buf, p)?;
        p += 4;
        let manage_stream = read_bool(buf, p)?;
        p += 1;
        let read_stream = read_bool(buf, p)?;
        p += 1;
        let manage_topics = read_bool(buf, p)?;
        p += 1;
        let read_topics = read_bool(buf, p)?;
        p += 1;
        let poll_messages = read_bool(buf, p)?;
        p += 1;
        let send_messages = read_bool(buf, p)?;
        p += 1;

        let has_topics = read_bool(buf, p)?;
        p += 1;

        let mut topics = Vec::new();
        if has_topics {
            loop {
                let (topic, next_p) = WireTopicPermissions::decode_at(buf, p)?;
                p = next_p;
                topics.push(topic);
                let has_next = read_bool(buf, p)?;
                p += 1;
                if !has_next {
                    break;
                }
            }
        }

        Ok((
            Self {
                stream_id,
                manage_stream,
                read_stream,
                manage_topics,
                read_topics,
                poll_messages,
                send_messages,
                topics,
            },
            p,
        ))
    }
}

impl WireEncode for WirePermissions {
    fn encoded_size(&self) -> usize {
        // global(10) + has_streams(1) + streams
        let streams_size: usize = if self.streams.is_empty() {
            0
        } else {
            self.streams
                .iter()
                .map(WireStreamPermissions::encoded_size)
                .sum()
        };
        10 + 1 + streams_size
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.global.encode_into(buf);

        if self.streams.is_empty() {
            buf.put_u8(NO_NEXT);
        } else {
            buf.put_u8(HAS_NEXT);
            for (i, stream) in self.streams.iter().enumerate() {
                stream.encode_into(buf);
                let is_last = i == self.streams.len() - 1;
                buf.put_u8(if is_last { NO_NEXT } else { HAS_NEXT });
            }
        }
    }
}

impl WireDecode for WirePermissions {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (global, mut p) = WireGlobalPermissions::decode_at(buf, 0)?;

        let has_streams = read_bool(buf, p)?;
        p += 1;

        let mut streams = Vec::new();
        if has_streams {
            loop {
                let (stream, next_p) = WireStreamPermissions::decode_at(buf, p)?;
                p = next_p;
                streams.push(stream);
                let has_next = read_bool(buf, p)?;
                p += 1;
                if !has_next {
                    break;
                }
            }
        }

        Ok((Self { global, streams }, p))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_global(all: bool) -> WireGlobalPermissions {
        WireGlobalPermissions {
            manage_servers: all,
            read_servers: all,
            manage_users: all,
            read_users: all,
            manage_streams: all,
            read_streams: all,
            manage_topics: all,
            read_topics: all,
            poll_messages: all,
            send_messages: all,
        }
    }

    fn make_topic(id: u32) -> WireTopicPermissions {
        WireTopicPermissions {
            topic_id: id,
            manage_topic: true,
            read_topic: true,
            poll_messages: false,
            send_messages: true,
        }
    }

    fn make_stream(id: u32, topics: Vec<WireTopicPermissions>) -> WireStreamPermissions {
        WireStreamPermissions {
            stream_id: id,
            manage_stream: true,
            read_stream: true,
            manage_topics: false,
            read_topics: true,
            poll_messages: true,
            send_messages: false,
            topics,
        }
    }

    #[test]
    fn roundtrip_global_only() {
        let perms = WirePermissions {
            global: make_global(true),
            streams: vec![],
        };
        let bytes = perms.to_bytes();
        assert_eq!(bytes.len(), 11); // 10 global + 1 has_streams=0
        let (decoded, consumed) = WirePermissions::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, perms);
    }

    #[test]
    fn roundtrip_with_streams_no_topics() {
        let perms = WirePermissions {
            global: make_global(false),
            streams: vec![make_stream(1, vec![]), make_stream(2, vec![])],
        };
        let bytes = perms.to_bytes();
        let (decoded, consumed) = WirePermissions::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, perms);
    }

    #[test]
    fn roundtrip_with_streams_and_topics() {
        let perms = WirePermissions {
            global: make_global(true),
            streams: vec![
                make_stream(1, vec![make_topic(10), make_topic(20)]),
                make_stream(2, vec![make_topic(30)]),
            ],
        };
        let bytes = perms.to_bytes();
        let (decoded, consumed) = WirePermissions::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, perms);
    }

    #[test]
    fn roundtrip_single_stream_single_topic() {
        let perms = WirePermissions {
            global: make_global(false),
            streams: vec![make_stream(42, vec![make_topic(7)])],
        };
        let bytes = perms.to_bytes();
        let (decoded, consumed) = WirePermissions::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, perms);
    }

    #[test]
    fn global_permissions_all_false() {
        let perms = WirePermissions {
            global: make_global(false),
            streams: vec![],
        };
        let bytes = perms.to_bytes();
        // First 10 bytes should all be 0
        for byte in &bytes[..10] {
            assert_eq!(*byte, 0);
        }
        let (decoded, _) = WirePermissions::decode(&bytes).unwrap();
        assert_eq!(decoded, perms);
    }

    #[test]
    fn global_permissions_all_true() {
        let perms = WirePermissions {
            global: make_global(true),
            streams: vec![],
        };
        let bytes = perms.to_bytes();
        for byte in &bytes[..10] {
            assert_eq!(*byte, 1);
        }
    }

    #[test]
    fn truncated_buffer_global_only() {
        let perms = WirePermissions {
            global: make_global(true),
            streams: vec![],
        };
        let bytes = perms.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                WirePermissions::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn truncated_buffer_with_streams() {
        let perms = WirePermissions {
            global: make_global(true),
            streams: vec![make_stream(1, vec![make_topic(10)])],
        };
        let bytes = perms.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                WirePermissions::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
