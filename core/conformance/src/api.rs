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
    ConnectionInfoResult, ConsumerGroupResult, IdentityResult, OptionSpecResult,
    PolledMessagesResult, RawResult, ReceivedMessageResult, StreamDetailsResult, StreamResult,
    TopicDetailsResult, TopicResult, UserDetailsResult,
};
use crate::matcher;
use crate::protocol::EntityKind;
use iggy_common::MetadataChanges;
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
    BuildConsumer,
    ResolveOperation,
    AbandonOperation,
    SendBinaryRequest,
    SendBinaryRequestToReplica,
    DescribeOptions,
    CreateUser,
    GetUser,
    UpdateUser,
    DeleteUser,
    GetMetadataChanges,
    GetConnectionInfo,
    /// The next batch of a reader, or the next message of a consumer.
    Next,
}

impl Method {
    #[must_use]
    pub fn name(self) -> &'static str {
        self.into()
    }

    /// The entity kinds that have the method.
    #[must_use]
    pub fn targets(self) -> &'static [EntityKind] {
        match self {
            Self::Next => &[EntityKind::Reader, EntityKind::Consumer],
            _ => &[EntityKind::Client],
        }
    }

    /// The kind of entity that the call creates, kept under `save_result_as`.
    #[must_use]
    pub fn creates(self) -> Option<EntityKind> {
        match self {
            Self::BuildReader => Some(EntityKind::Reader),
            Self::BuildConsumer => Some(EntityKind::Consumer),
            _ => None,
        }
    }

    /// The agent capability that a call of the method on `kind` needs.
    #[must_use]
    pub fn capability(self, kind: EntityKind) -> Option<&'static str> {
        match self {
            Self::BuildReader => Some("reader"),
            Self::BuildConsumer => Some("consumer"),
            Self::Next if kind == EntityKind::Consumer => Some("consumer"),
            Self::Next => Some("reader"),
            Self::ResolveOperation => Some("resolve_operation"),
            Self::AbandonOperation => Some("abandon_operation"),
            Self::SendBinaryRequest => Some("raw_command"),
            Self::SendBinaryRequestToReplica => Some("raw_command_replica"),
            Self::DescribeOptions => Some("describe_options"),
            Self::GetMetadataChanges => Some("metadata_changes"),
            Self::GetConnectionInfo => Some("connection_info"),
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
            Self::Connect
            | Self::Disconnect
            | Self::Ping
            | Self::LogoutUser
            | Self::GetStreams
            | Self::GetConnectionInfo => decode::<NoArgs>(args),
            Self::BuildConsumer => decode::<BuildConsumerArgs>(args),
            Self::SendBinaryRequest => decode::<BinaryRequestArgs>(args),
            Self::SendBinaryRequestToReplica => decode::<BinaryRequestToReplicaArgs>(args),
            Self::DescribeOptions => decode::<DescribeOptionsArgs>(args),
            Self::CreateUser => decode::<CreateUserArgs>(args),
            Self::GetUser | Self::DeleteUser => decode::<UserArgs>(args),
            Self::UpdateUser => decode::<UpdateUserArgs>(args),
            Self::GetMetadataChanges => decode::<MetadataChangesArgs>(args),
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

    /// Decodes a result of a call on `kind` into its canonical type.
    ///
    /// # Errors
    ///
    /// Returns the decode error, which names a missing or unknown field.
    pub fn check_result(self, kind: EntityKind, result: &Value) -> Result<(), String> {
        match self {
            Self::Next if kind == EntityKind::Consumer => decode::<ReceivedMessageResult>(result),
            Self::SendBinaryRequest | Self::SendBinaryRequestToReplica => {
                decode::<RawResult>(result)
            }
            Self::DescribeOptions => decode::<Vec<OptionSpecResult>>(result),
            Self::CreateUser => decode::<UserDetailsResult>(result),
            Self::GetUser => decode::<Option<UserDetailsResult>>(result),
            Self::GetMetadataChanges => round_trip::<MetadataChanges>(result),
            Self::GetConnectionInfo => decode::<ConnectionInfoResult>(result),
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
            | Self::BuildConsumer
            | Self::ResolveOperation
            | Self::AbandonOperation
            | Self::UpdateUser
            | Self::DeleteUser => decode::<()>(result),
        }
    }
}

fn decode<T: DeserializeOwned>(value: &Value) -> Result<(), String> {
    T::deserialize(value)
        .map(drop)
        .map_err(|error| error.to_string())
}

/// For a form that the SDK types define, such as the HTTP JSON of metadata
/// changes: the value must decode into the type and encode back to itself, so
/// a missing, misnamed or extra field fails.
fn round_trip<T: DeserializeOwned + Serialize>(value: &Value) -> Result<(), String> {
    let decoded = T::deserialize(value).map_err(|error| error.to_string())?;
    let encoded = serde_json::to_value(&decoded).map_err(|error| error.to_string())?;
    if matcher::values_equal(&encoded, value) {
        Ok(())
    } else {
        Err(format!("it encodes back as {}", matcher::compact(&encoded)))
    }
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
    /// The partitions to read. A group reader reads its assignment without it,
    /// and a single consumer needs it.
    pub partitions: Option<Vec<u32>>,
    pub start: Option<Strategy>,
    pub max_wait_ms: Option<u64>,
    #[serde(default)]
    pub commit: Commit,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildConsumerArgs {
    pub stream_id: Id,
    pub topic_id: Id,
    /// The consumer group to join, which must exist.
    pub group: String,
    pub max_wait_ms: Option<u64>,
    /// The most messages that one poll of the consumer reads.
    pub count: Option<u32>,
    #[serde(default)]
    pub commit: Commit,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BinaryRequestArgs {
    pub code: u32,
    #[serde(default)]
    pub payload_hex: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BinaryRequestToReplicaArgs {
    pub code: u32,
    #[serde(default)]
    pub payload_hex: String,
    pub replica: u8,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeOptionsArgs {
    pub scope: OptionsScope,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionsScope {
    Stream,
    Topic,
    User,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateUserArgs {
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub status: UserStatus,
    /// Without it the user has no permissions.
    pub permissions: Option<PermissionsArg>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserStatus {
    #[default]
    Active,
    Inactive,
}

/// Global permissions only. Stream permissions are not in format 1.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PermissionsArg {
    #[serde(default)]
    pub global: GlobalPermissionsArg,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct GlobalPermissionsArg {
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserArgs {
    pub user_id: Id,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateUserArgs {
    pub user_id: Id,
    pub username: Option<String>,
    pub status: Option<UserStatus>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataChangesArgs {
    pub start: MetadataStart,
    /// Group names such as `streams`. Empty means every group.
    #[serde(default)]
    pub groups: Vec<String>,
    pub limit: Option<u32>,
}

/// `earliest`, `latest`, `{after: cursor}` or `{current: page token or null}`,
/// with the cursor and the token in their text forms.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataStart {
    Earliest,
    Latest,
    After(String),
    Current(Option<String>),
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
        let client = EntityKind::Client;
        let result = json!({"user_id": 1, "userId": 1});
        assert!(Method::LoginUser.check_result(client, &result).is_err());
        assert!(
            Method::LoginUser
                .check_result(client, &json!({"user_id": 1}))
                .is_ok()
        );
    }

    #[test]
    fn given_a_unit_method_when_checking_a_value_should_fail() {
        let client = EntityKind::Client;
        assert!(
            Method::DeleteStream
                .check_result(client, &Value::Null)
                .is_ok()
        );
        assert!(
            Method::DeleteStream
                .check_result(client, &json!({}))
                .is_err()
        );
    }

    #[test]
    fn given_metadata_starts_when_decoding_should_accept_each_form() {
        for start in [
            json!("latest"),
            json!({"after": "mc1_x"}),
            json!({"current": null}),
        ] {
            let args = json!({"start": start, "groups": ["streams"]});
            assert!(
                Method::GetMetadataChanges.check_args(&args).is_ok(),
                "{args}"
            );
        }
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
