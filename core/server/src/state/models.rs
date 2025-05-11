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

use crate::state::COMPONENT;
use bytes::{BufMut, Bytes, BytesMut};
use error_set::ErrContext;
use iggy_common::BytesSerializable;
use iggy_common::Command;
use iggy_common::IggyError;
use iggy_common::Validatable;
use iggy_common::create_consumer_group::CreateConsumerGroup;
use iggy_common::create_personal_access_token::CreatePersonalAccessToken;
use iggy_common::create_stream::CreateStream;
use iggy_common::create_topic::CreateTopic;
use iggy_common::create_user::CreateUser;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::str::from_utf8;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateStreamWithId {
    pub stream_id: u32,
    pub command: CreateStream,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateTopicWithId {
    pub topic_id: u32,
    pub command: CreateTopic,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateConsumerGroupWithId {
    pub group_id: u32,
    pub command: CreateConsumerGroup,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateUserWithId {
    pub user_id: u32,
    pub command: CreateUser,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct CreatePersonalAccessTokenWithHash {
    pub hash: String,
    pub command: CreatePersonalAccessToken,
}

impl Validatable<IggyError> for CreateStreamWithId {
    fn validate(&self) -> Result<(), IggyError> {
        self.command.validate()
    }
}

impl Command for CreateStreamWithId {
    fn code(&self) -> u32 {
        self.command.code()
    }
}

impl Validatable<IggyError> for CreateTopicWithId {
    fn validate(&self) -> Result<(), IggyError> {
        self.command.validate()
    }
}

impl Command for CreateTopicWithId {
    fn code(&self) -> u32 {
        self.command.code()
    }
}

impl Validatable<IggyError> for CreateConsumerGroupWithId {
    fn validate(&self) -> Result<(), IggyError> {
        self.command.validate()
    }
}

impl Command for CreateConsumerGroupWithId {
    fn code(&self) -> u32 {
        self.command.code()
    }
}

impl Validatable<IggyError> for CreateUserWithId {
    fn validate(&self) -> Result<(), IggyError> {
        self.command.validate()
    }
}

impl Command for CreateUserWithId {
    fn code(&self) -> u32 {
        self.command.code()
    }
}

impl Validatable<IggyError> for CreatePersonalAccessTokenWithHash {
    fn validate(&self) -> Result<(), IggyError> {
        self.command.validate()
    }
}

impl Command for CreatePersonalAccessTokenWithHash {
    fn code(&self) -> u32 {
        self.command.code()
    }
}

impl Display for CreateStreamWithId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "CreateStreamWithId {{ command: {}, stream ID: {} }}",
            self.command, self.stream_id
        )
    }
}

impl Display for CreateTopicWithId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "CreateTopicWithId {{ command: {}, topic ID: {} }}",
            self.command, self.topic_id
        )
    }
}

impl Display for CreateConsumerGroupWithId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "CreateConsumerGroupWithId {{ command: {}, group_id: {} }}",
            self.command, self.group_id
        )
    }
}

impl Display for CreateUserWithId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "CreateUserWithId {{ command: {}, user_id: {} }}",
            self.command, self.user_id
        )
    }
}

impl Display for CreatePersonalAccessTokenWithHash {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "CreatePersonalAccessTokenWithHash {{ command: {}, hash: {} }}",
            self.command, self.hash
        )
    }
}

impl BytesSerializable for CreateStreamWithId {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_u32_le(self.stream_id);
        let command_bytes = self.command.to_bytes();
        bytes.put_u32_le(command_bytes.len() as u32);
        bytes.put_slice(&command_bytes);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        let mut position = 0;
        let stream_id = u32::from_le_bytes(
            bytes[position..4]
                .try_into()
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to parse stream ID")
                })
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        position += 4;
        let command_length = u32::from_le_bytes(
            bytes[position..position + 4]
                .try_into()
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to parse stream command length")
                })
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        position += 4;
        let command_bytes = bytes.slice(position..position + command_length as usize);
        let command = CreateStream::from_bytes(command_bytes).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to parse stream command")
        })?;
        Ok(Self { stream_id, command })
    }
}

impl BytesSerializable for CreateTopicWithId {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_u32_le(self.topic_id);
        let command_bytes = self.command.to_bytes();
        bytes.put_u32_le(command_bytes.len() as u32);
        bytes.put_slice(&command_bytes);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        let mut position = 0;
        let topic_id = u32::from_le_bytes(
            bytes[position..4]
                .try_into()
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to parse topic ID")
                })
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        position += 4;
        let command_length = u32::from_le_bytes(
            bytes[position..position + 4]
                .try_into()
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to parse topic command length")
                })
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        position += 4;
        let command_bytes = bytes.slice(position..position + command_length as usize);
        let command = CreateTopic::from_bytes(command_bytes).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to parse topic command")
        })?;
        Ok(Self { topic_id, command })
    }
}

impl BytesSerializable for CreateConsumerGroupWithId {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_u32_le(self.group_id);
        let command_bytes = self.command.to_bytes();
        bytes.put_u32_le(command_bytes.len() as u32);
        bytes.put_slice(&command_bytes);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        let mut position = 0;
        let group_id = u32::from_le_bytes(
            bytes[position..4]
                .try_into()
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to parse consumer group ID")
                })
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        position += 4;
        let command_length = u32::from_le_bytes(
            bytes[position..position + 4]
                .try_into()
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to parse consumer group command length")
                })
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        position += 4;
        let command_bytes = bytes.slice(position..position + command_length as usize);
        let command =
            CreateConsumerGroup::from_bytes(command_bytes).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to parse consumer group command")
            })?;
        Ok(Self { group_id, command })
    }
}

impl BytesSerializable for CreateUserWithId {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_u32_le(self.user_id);
        let command_bytes = self.command.to_bytes();
        bytes.put_u32_le(command_bytes.len() as u32);
        bytes.put_slice(&command_bytes);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        let mut position = 0;
        let user_id = u32::from_le_bytes(
            bytes[position..4]
                .try_into()
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to parse user ID")
                })
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        position += 4;
        let command_length = u32::from_le_bytes(
            bytes[position..position + 4]
                .try_into()
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to parse user command length")
                })
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        position += 4;
        let command_bytes = bytes.slice(position..position + command_length as usize);
        let command = CreateUser::from_bytes(command_bytes).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to parse user command")
        })?;
        Ok(Self { user_id, command })
    }
}

impl BytesSerializable for CreatePersonalAccessTokenWithHash {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_u32_le(self.hash.len() as u32);
        bytes.put_slice(self.hash.as_bytes());
        let command_bytes = self.command.to_bytes();
        bytes.put_u32_le(command_bytes.len() as u32);
        bytes.put_slice(&command_bytes);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        let mut position = 0;
        let hash_length = u32::from_le_bytes(
            bytes[position..4]
                .try_into()
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to parse hash length")
                })
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        position += 4;
        let hash = from_utf8(&bytes[position..position + hash_length as usize])
            .map_err(|_| IggyError::InvalidUtf8)?
            .to_string();

        position += hash_length as usize;
        let command_length = u32::from_le_bytes(
            bytes[position..position + 4]
                .try_into()
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to parse personal access token command length")
                })
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        position += 4;
        let command_bytes = bytes.slice(position..position + command_length as usize);
        let command =
            CreatePersonalAccessToken::from_bytes(command_bytes).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to parse personal access token command"
                )
            })?;
        Ok(Self { hash, command })
    }
}
