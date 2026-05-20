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

use bytes::{BufMut, BytesMut};
use iggy_binary_protocol::requests::{
    consumer_groups::CreateConsumerGroupRequest,
    personal_access_tokens::CreatePersonalAccessTokenRequest, streams::CreateStreamRequest,
    topics::CreateTopicRequest, users::CreateUserRequest,
};
use iggy_binary_protocol::{WireDecode, WireEncode};
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateStreamWithId {
    pub stream_id: u32,
    pub command: CreateStreamRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicWithId {
    pub topic_id: u32,
    pub command: CreateTopicRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateConsumerGroupWithId {
    pub group_id: u32,
    pub command: CreateConsumerGroupRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateUserWithId {
    pub user_id: u32,
    pub command: CreateUserRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePersonalAccessTokenWithHash {
    pub hash: String,
    pub command: CreatePersonalAccessTokenRequest,
}

impl Display for CreateStreamWithId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "CreateStreamWithId {{ name: {}, stream_id: {} }}",
            self.command.name, self.stream_id
        )
    }
}

impl Display for CreateTopicWithId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "CreateTopicWithId {{ name: {}, topic_id: {} }}",
            self.command.name, self.topic_id
        )
    }
}

impl Display for CreateConsumerGroupWithId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "CreateConsumerGroupWithId {{ name: {}, group_id: {} }}",
            self.command.name, self.group_id
        )
    }
}

impl Display for CreateUserWithId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "CreateUserWithId {{ username: {}, user_id: {} }}",
            self.command.username, self.user_id
        )
    }
}

impl Display for CreatePersonalAccessTokenWithHash {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "CreatePersonalAccessTokenWithHash {{ name: {}, hash: [REDACTED] }}",
            self.command.name,
        )
    }
}

// Wire format for WithId wrappers: id:u32_le | inner_length:u32_le | inner_bytes

impl WireEncode for CreateStreamWithId {
    fn encoded_size(&self) -> usize {
        4 + 4 + self.command.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.stream_id);
        buf.put_u32_le(self.command.encoded_size() as u32);
        self.command.encode(buf);
    }
}

impl WireDecode for CreateStreamWithId {
    fn decode(buf: &[u8]) -> Result<(Self, usize), iggy_binary_protocol::WireError> {
        if buf.len() < 8 {
            return Err(iggy_binary_protocol::WireError::UnexpectedEof {
                offset: 0,
                need: 8,
                have: buf.len(),
            });
        }
        let stream_id = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let command_length = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
        let total = 8usize.checked_add(command_length).ok_or(
            iggy_binary_protocol::WireError::UnexpectedEof {
                offset: 4,
                need: command_length,
                have: buf.len() - 8,
            },
        )?;
        if buf.len() < total {
            return Err(iggy_binary_protocol::WireError::UnexpectedEof {
                offset: 8,
                need: command_length,
                have: buf.len() - 8,
            });
        }
        let (command, _) = CreateStreamRequest::decode(&buf[8..total])?;
        Ok((Self { stream_id, command }, total))
    }
}

impl WireEncode for CreateTopicWithId {
    fn encoded_size(&self) -> usize {
        4 + 4 + self.command.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.topic_id);
        buf.put_u32_le(self.command.encoded_size() as u32);
        self.command.encode(buf);
    }
}

impl WireDecode for CreateTopicWithId {
    fn decode(buf: &[u8]) -> Result<(Self, usize), iggy_binary_protocol::WireError> {
        if buf.len() < 8 {
            return Err(iggy_binary_protocol::WireError::UnexpectedEof {
                offset: 0,
                need: 8,
                have: buf.len(),
            });
        }
        let topic_id = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let command_length = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
        let total = 8usize.checked_add(command_length).ok_or(
            iggy_binary_protocol::WireError::UnexpectedEof {
                offset: 4,
                need: command_length,
                have: buf.len() - 8,
            },
        )?;
        if buf.len() < total {
            return Err(iggy_binary_protocol::WireError::UnexpectedEof {
                offset: 8,
                need: command_length,
                have: buf.len() - 8,
            });
        }
        let (command, _) = CreateTopicRequest::decode(&buf[8..total])?;
        Ok((Self { topic_id, command }, total))
    }
}

impl WireEncode for CreateConsumerGroupWithId {
    fn encoded_size(&self) -> usize {
        4 + 4 + self.command.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.group_id);
        buf.put_u32_le(self.command.encoded_size() as u32);
        self.command.encode(buf);
    }
}

impl WireDecode for CreateConsumerGroupWithId {
    fn decode(buf: &[u8]) -> Result<(Self, usize), iggy_binary_protocol::WireError> {
        if buf.len() < 8 {
            return Err(iggy_binary_protocol::WireError::UnexpectedEof {
                offset: 0,
                need: 8,
                have: buf.len(),
            });
        }
        let group_id = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let command_length = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
        let total = 8usize.checked_add(command_length).ok_or(
            iggy_binary_protocol::WireError::UnexpectedEof {
                offset: 4,
                need: command_length,
                have: buf.len() - 8,
            },
        )?;
        if buf.len() < total {
            return Err(iggy_binary_protocol::WireError::UnexpectedEof {
                offset: 8,
                need: command_length,
                have: buf.len() - 8,
            });
        }
        let (command, _) = CreateConsumerGroupRequest::decode(&buf[8..total])?;
        Ok((Self { group_id, command }, total))
    }
}

impl WireEncode for CreateUserWithId {
    fn encoded_size(&self) -> usize {
        4 + 4 + self.command.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.user_id);
        buf.put_u32_le(self.command.encoded_size() as u32);
        self.command.encode(buf);
    }
}

impl WireDecode for CreateUserWithId {
    fn decode(buf: &[u8]) -> Result<(Self, usize), iggy_binary_protocol::WireError> {
        if buf.len() < 8 {
            return Err(iggy_binary_protocol::WireError::UnexpectedEof {
                offset: 0,
                need: 8,
                have: buf.len(),
            });
        }
        let user_id = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let command_length = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
        let total = 8usize.checked_add(command_length).ok_or(
            iggy_binary_protocol::WireError::UnexpectedEof {
                offset: 4,
                need: command_length,
                have: buf.len() - 8,
            },
        )?;
        if buf.len() < total {
            return Err(iggy_binary_protocol::WireError::UnexpectedEof {
                offset: 8,
                need: command_length,
                have: buf.len() - 8,
            });
        }
        let (command, _) = CreateUserRequest::decode(&buf[8..total])?;
        Ok((Self { user_id, command }, total))
    }
}

impl WireEncode for CreatePersonalAccessTokenWithHash {
    fn encoded_size(&self) -> usize {
        4 + self.hash.len() + 4 + self.command.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.hash.len() as u32);
        buf.put_slice(self.hash.as_bytes());
        buf.put_u32_le(self.command.encoded_size() as u32);
        self.command.encode(buf);
    }
}

impl WireDecode for CreatePersonalAccessTokenWithHash {
    fn decode(buf: &[u8]) -> Result<(Self, usize), iggy_binary_protocol::WireError> {
        if buf.len() < 4 {
            return Err(iggy_binary_protocol::WireError::UnexpectedEof {
                offset: 0,
                need: 4,
                have: buf.len(),
            });
        }
        let hash_length = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        let mut pos = 4;
        if buf.len() < pos + hash_length {
            return Err(iggy_binary_protocol::WireError::UnexpectedEof {
                offset: pos,
                need: hash_length,
                have: buf.len() - pos,
            });
        }
        let hash = std::str::from_utf8(&buf[pos..pos + hash_length])
            .map_err(|_| iggy_binary_protocol::WireError::InvalidUtf8 { offset: pos })?
            .to_string();
        pos += hash_length;
        if buf.len() < pos + 4 {
            return Err(iggy_binary_protocol::WireError::UnexpectedEof {
                offset: pos,
                need: 4,
                have: buf.len() - pos,
            });
        }
        let command_length = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if buf.len() < pos + command_length {
            return Err(iggy_binary_protocol::WireError::UnexpectedEof {
                offset: pos,
                need: command_length,
                have: buf.len() - pos,
            });
        }
        let (command, _) =
            CreatePersonalAccessTokenRequest::decode(&buf[pos..pos + command_length])?;
        pos += command_length;
        Ok((Self { hash, command }, pos))
    }
}
