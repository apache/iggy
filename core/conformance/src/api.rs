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

//! The methods that test files call, with their arguments and result forms.
//!
//! A method name is the Rust SDK method name. The argument types reject
//! unknown fields, and the orchestrator checks the arguments of each call
//! before it sends the call, so a mistake in a test file never reaches an
//! agent. The result check decodes a result into its canonical type.

use crate::canonical::{
    ConsumerGroupResult, IdentityResult, PolledMessagesResult, StreamDetailsResult, StreamResult,
    TopicDetailsResult, TopicResult,
};
use crate::protocol::EntityKind;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use strum::{EnumIter, EnumString, IntoStaticStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumString, IntoStaticStr, EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum Method {
    Connect,
    Disconnect,
    Ping,
    LoginUser,
    LogoutUser,
    GetStream,
    GetStreams,
    CreateStream,
    UpdateStream,
    DeleteStream,
    GetTopic,
    GetTopics,
    CreateTopic,
    UpdateTopic,
    DeleteTopic,
    SendMessages,
    PollMessages,
    CreateConsumerGroup,
    GetConsumerGroup,
    DeleteConsumerGroup,
    BuildReader,
    ResolveOperation,
    AbandonOperation,
    /// The next batch of a reader.
    Next,
}

impl Method {
    #[must_use]
    pub fn name(self) -> &'static str {
        self.into()
    }

    /// The entity kind that owns the method.
    #[must_use]
    pub fn target(self) -> EntityKind {
        match self {
            Self::Next => EntityKind::Reader,
            _ => EntityKind::Client,
        }
    }

    /// The kind of entity that the call creates, kept under `save_result_as`.
    #[must_use]
    pub fn creates(self) -> Option<EntityKind> {
        match self {
            Self::BuildReader => Some(EntityKind::Reader),
            _ => None,
        }
    }

    /// The agent capability that a call of the method needs.
    #[must_use]
    pub fn capability(self) -> Option<&'static str> {
        match self {
            Self::BuildReader | Self::Next => Some("reader"),
            Self::ResolveOperation => Some("resolve_operation"),
            Self::AbandonOperation => Some("abandon_operation"),
            _ => None,
        }
    }

    /// Decodes the arguments into the method's argument type.
    ///
    /// # Errors
    ///
    /// Returns the decode error, which names the field.
    pub fn check_args(self, args: &Value) -> Result<(), String> {
        match self {
            Self::Connect | Self::Disconnect | Self::Ping | Self::LogoutUser | Self::GetStreams => {
                decode::<NoArgs>(args)
            }
            Self::Next => decode::<NextArgs>(args),
            Self::LoginUser => decode::<LoginUserArgs>(args),
            Self::GetStream | Self::DeleteStream | Self::GetTopics => decode::<StreamArgs>(args),
            Self::CreateStream => decode::<CreateStreamArgs>(args),
            Self::UpdateStream => decode::<UpdateStreamArgs>(args),
            Self::GetTopic | Self::DeleteTopic => decode::<TopicArgs>(args),
            Self::CreateTopic => decode::<CreateTopicArgs>(args),
            Self::UpdateTopic => decode::<UpdateTopicArgs>(args),
            Self::SendMessages => decode::<SendMessagesArgs>(args),
            Self::PollMessages => decode::<PollMessagesArgs>(args),
            Self::CreateConsumerGroup => decode::<CreateConsumerGroupArgs>(args),
            Self::GetConsumerGroup | Self::DeleteConsumerGroup => decode::<ConsumerGroupArgs>(args),
            Self::BuildReader => decode::<BuildReaderArgs>(args),
            Self::ResolveOperation => decode::<ResolveOperationArgs>(args),
            Self::AbandonOperation => decode::<AbandonOperationArgs>(args),
        }
    }

    /// Decodes a result into its canonical type.
    ///
    /// # Errors
    ///
    /// Returns the decode error, which names a missing or unknown field.
    pub fn check_result(self, result: &Value) -> Result<(), String> {
        match self {
            Self::LoginUser => decode::<IdentityResult>(result),
            Self::GetStream => decode::<Option<StreamDetailsResult>>(result),
            Self::GetStreams => decode::<Vec<StreamResult>>(result),
            Self::CreateStream => decode::<StreamDetailsResult>(result),
            Self::GetTopic => decode::<Option<TopicDetailsResult>>(result),
            Self::GetTopics => decode::<Vec<TopicResult>>(result),
            Self::CreateTopic => decode::<TopicDetailsResult>(result),
            Self::PollMessages | Self::Next => decode::<PolledMessagesResult>(result),
            Self::CreateConsumerGroup => decode::<ConsumerGroupResult>(result),
            Self::GetConsumerGroup => decode::<Option<ConsumerGroupResult>>(result),
            Self::Connect
            | Self::Disconnect
            | Self::Ping
            | Self::LogoutUser
            | Self::UpdateStream
            | Self::DeleteStream
            | Self::UpdateTopic
            | Self::DeleteTopic
            | Self::SendMessages
            | Self::DeleteConsumerGroup
            | Self::BuildReader
            | Self::ResolveOperation
            | Self::AbandonOperation => decode::<()>(result),
        }
    }
}

fn decode<T: DeserializeOwned>(value: &Value) -> Result<(), String> {
    T::deserialize(value)
        .map(drop)
        .map_err(|error| error.to_string())
}

/// A stream, topic, group or consumer: a number is an id, text is a name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Id {
    Numeric(u32),
    Named(String),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoArgs {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NextArgs {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoginUserArgs {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamArgs {
    pub stream_id: Id,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateStreamArgs {
    pub name: String,
    /// A stream key of 32 bytes. Without it the server generates one.
    pub encryption_key_hex: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateStreamArgs {
    pub stream_id: Id,
    pub name: Option<String>,
    /// Option keys to reset to their server defaults.
    #[serde(default)]
    pub reset: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicArgs {
    pub stream_id: Id,
    pub topic_id: Id,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTopicArgs {
    pub stream_id: Id,
    pub name: String,
    pub partitions_count: Option<u32>,
    pub message_expiry: Option<Expiry>,
    pub max_topic_size: Option<TopicSize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateTopicArgs {
    pub stream_id: Id,
    pub topic_id: Id,
    pub name: Option<String>,
    pub message_expiry: Option<Expiry>,
    pub max_topic_size: Option<TopicSize>,
    #[serde(default)]
    pub reset: Vec<String>,
}

/// `never`, `server_default`, or microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum Expiry {
    Keyword(ExpiryKeyword),
    Microseconds(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpiryKeyword {
    Never,
    ServerDefault,
}

/// `unlimited`, `server_default`, or bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum TopicSize {
    Keyword(TopicSizeKeyword),
    Bytes(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopicSizeKeyword {
    Unlimited,
    ServerDefault,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SendMessagesArgs {
    pub stream_id: Id,
    pub topic_id: Id,
    pub partitioning: Partitioning,
    pub messages: Vec<MessageArg>,
}

/// `balanced`, `{partition_id: N}` or `{messages_key: text}`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Partitioning {
    Balanced,
    PartitionId(u32),
    MessagesKey(String),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageArg {
    /// The u128 id as decimal text. Without it the SDK picks the id.
    pub id: Option<String>,
    pub payload: Option<String>,
    pub payload_hex: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, HeaderArg>,
}

impl MessageArg {
    /// The payload bytes from exactly one of `payload` and `payload_hex`.
    ///
    /// # Errors
    ///
    /// Fails without a payload, with both, or with invalid hex.
    pub fn payload_bytes(&self) -> Result<Vec<u8>, String> {
        match (&self.payload, &self.payload_hex) {
            (Some(text), None) => Ok(text.as_bytes().to_vec()),
            (None, Some(hex_text)) => hex::decode(hex_text).map_err(|error| error.to_string()),
            _ => Err("a message needs exactly one of payload and payload_hex".to_owned()),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeaderArg {
    pub kind: String,
    pub value: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PollMessagesArgs {
    pub stream_id: Id,
    pub topic_id: Id,
    pub partition_id: u32,
    pub consumer: Option<ConsumerArg>,
    pub strategy: Strategy,
    pub count: u32,
    #[serde(default)]
    pub commit: Commit,
    pub max_wait_ms: Option<u64>,
    pub min_count: Option<u32>,
}

/// `{consumer: id}` or `{group: id}`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerArg {
    Consumer(Id),
    Group(Id),
}

/// `first`, `last`, `next`, `{offset: N}` or `{timestamp: microseconds}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Strategy {
    First,
    Last,
    Next,
    Offset(u64),
    Timestamp(u64),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Commit {
    #[default]
    Manual,
    OnRead,
    /// The client commits each message after the caller takes the next one.
    AfterProcessing,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateConsumerGroupArgs {
    pub stream_id: Id,
    pub topic_id: Id,
    pub name: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerGroupArgs {
    pub stream_id: Id,
    pub topic_id: Id,
    pub group_id: Id,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildReaderArgs {
    pub stream_id: Id,
    pub topic_id: Id,
    pub consumer: Option<ConsumerArg>,
    /// The partitions to read. Without it a group reader reads its assignment
    /// and a single consumer reads every partition.
    pub partitions: Option<Vec<u32>>,
    pub start: Option<Strategy>,
    pub max_wait_ms: Option<u64>,
    #[serde(default)]
    pub commit: Commit,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveOperationArgs {
    pub operation_id: u64,
    /// The budget of the resolution. Without it the SDK default applies.
    pub deadline_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AbandonOperationArgs {
    pub operation_id: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::str::FromStr;

    #[test]
    fn given_rust_method_names_when_parsing_should_find_methods() {
        assert_eq!(Method::from_str("get_stream"), Ok(Method::GetStream));
        assert_eq!(Method::BuildReader.name(), "build_reader");
        assert!(Method::from_str("GetStream").is_err());
    }

    #[test]
    fn given_an_unknown_argument_when_checking_should_name_it() {
        let error = Method::GetStream
            .check_args(&json!({"stream_id": 1, "streamId": 1}))
            .unwrap_err();
        assert!(error.contains("streamId"), "{error}");
    }

    #[test]
    fn given_ids_and_names_when_checking_should_accept_both() {
        assert!(
            Method::GetStream
                .check_args(&json!({"stream_id": 1}))
                .is_ok()
        );
        assert!(
            Method::GetStream
                .check_args(&json!({"stream_id": "orders"}))
                .is_ok()
        );
    }

    #[test]
    fn given_a_result_with_a_misnamed_field_when_checking_should_fail() {
        let result = json!({"user_id": 1, "userId": 1});
        assert!(Method::LoginUser.check_result(&result).is_err());
        assert!(
            Method::LoginUser
                .check_result(&json!({"user_id": 1}))
                .is_ok()
        );
    }

    #[test]
    fn given_a_unit_method_when_checking_a_value_should_fail() {
        assert!(Method::DeleteStream.check_result(&Value::Null).is_ok());
        assert!(Method::DeleteStream.check_result(&json!({})).is_err());
    }

    #[test]
    fn given_strategies_and_partitionings_when_decoding_should_accept_each_form() {
        let args = json!({
            "stream_id": 0, "topic_id": 0, "partition_id": 0,
            "consumer": {"group": "readers"}, "strategy": {"offset": 3}, "count": 10,
            "commit": "on_read", "max_wait_ms": 1000
        });
        assert!(Method::PollMessages.check_args(&args).is_ok());
        let send = json!({
            "stream_id": 0, "topic_id": 0, "partitioning": "balanced",
            "messages": [{"payload": "a"}, {"id": "7", "payload_hex": "ff"}]
        });
        assert!(Method::SendMessages.check_args(&send).is_ok());
    }
}
