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

use iggy_binary_protocol::codes::{
    CREATE_STREAM_CODE, CREATE_TOPIC_CODE, CREATE_USER_CODE, DESCRIBE_OPTIONS_CODE,
    GET_STREAM_CODE, GET_STREAMS_CODE, GET_TOPIC_CODE, GET_TOPICS_CODE, GET_USER_CODE,
    GET_USERS_CODE, UPDATE_RESOURCE_CODE,
};
use iggy_binary_protocol::requests::streams::CreateStreamRequest;
use iggy_binary_protocol::requests::system::{ResourceTarget, UpdateResourceRequest};
use iggy_binary_protocol::requests::topics::CreateTopicRequest;
use iggy_binary_protocol::requests::users::CreateUserRequest;
use iggy_binary_protocol::responses::system::OptionDescriptor as WireOptionDescriptor;
use iggy_binary_protocol::{
    IGGY_PROTOCOL_VERSION, IGGY_PROTOCOL_VERSION_MIN, Operation, ProtocolVersion, WireDecode,
    WireEncode, WireIdentifier, WireName, WireOptions, lookup_command,
};
use iggy_common::create_stream::CreateStream;
use iggy_common::create_topic::CreateTopic;
use iggy_common::create_user::CreateUser;
use iggy_common::resource_options_json::encode_value;
pub use iggy_common::wire_conversions::describe_options_response;
use iggy_common::wire_conversions::identifier_to_wire;
use iggy_common::{
    CATALOG, CompressionAlgorithm, DEFAULT_PARTITIONS_COUNT, Durability, HeaderKey, HeaderKind,
    HeaderValue, Identifier, IggyByteSize, IggyError, IggyExpiry, MaxTopicSize,
    OPTION_FLAG_CRITICAL, OPTION_FLAG_READ_ONLY, OPTION_FLAG_RESETTABLE, OPTION_FLAG_SECRET,
    OPTION_FLAG_UPDATABLE, OptionAdmissionContext, OptionColumns, OptionDescriptor, OptionInputMap,
    OptionKey, OptionMutability, OptionOperation, OptionStorage, OptionValue, OptionVisibility,
    OptionsPatch, OptionsScope, OptionsUpdate, ResourceOptions, STREAM_KEY_LENGTH,
    ServerKeyContext, StreamCreateOptions, StreamDetails, StreamEncryptionKey,
    StreamEncryptionSource, StreamKeyOrigin, StreamUpdateOptions, TopicCreateOptions, TopicDetails,
    TopicUpdateOptions, UserCreateOptions, UserInfoDetails, UserStatus, UserUpdateOptions, WrapSet,
    admit, critical_keys_marker,
};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::str::FromStr;

pub const SCHEMA_VERSION: u32 = 1;
pub const GENERATOR: &str = "core/common/examples/resource_options_fixtures.rs";
/// Relative to the `iggy_common` manifest directory.
pub const CANONICAL_PATH: &str = "fixtures/resource_options.json";
/// Byte-identical copies each foreign SDK loads from its own test resources.
pub const LANGUAGE_COPIES: [&str; 8] = [
    "../../foreign/python/tests/fixtures/resource_options.json",
    "../../foreign/cpp/tests/fixtures/resource_options.json",
    "../../foreign/php/tests/fixtures/resource_options.json",
    "../../foreign/node/src/wire/fixtures/resource_options.json",
    "../../foreign/go/contracts/testdata/resource_options.json",
    "../../foreign/java/java-sdk/src/test/resources/resource_options.json",
    "../../foreign/csharp/Iggy_SDK_Tests/Fixtures/resource_options.json",
    "../../foreign/swift/Tests/IggyTests/Fixtures/resource_options.json",
];
pub const SCOPES: [OptionsScope; 3] = [
    OptionsScope::Topic,
    OptionsScope::Stream,
    OptionsScope::User,
];

const COMMANDS: [(&str, u32); 11] = [
    ("create_stream", CREATE_STREAM_CODE),
    ("create_topic", CREATE_TOPIC_CODE),
    ("create_user", CREATE_USER_CODE),
    ("describe_options", DESCRIBE_OPTIONS_CODE),
    ("get_stream", GET_STREAM_CODE),
    ("get_streams", GET_STREAMS_CODE),
    ("get_topic", GET_TOPIC_CODE),
    ("get_topics", GET_TOPICS_CODE),
    ("get_user", GET_USER_CODE),
    ("get_users", GET_USERS_CODE),
    ("update_resource", UPDATE_RESOURCE_CODE),
];
const RETIRED_COMMANDS: [(&str, u32); 3] = [
    ("update_stream", 204),
    ("update_topic", 304),
    ("update_user", 35),
];
const OPERATIONS: [(&str, Operation); 5] = [
    ("rewrap_stream_keys", Operation::RewrapStreamKeys),
    ("update_resource", Operation::UpdateResource),
    ("update_stream_resolved", Operation::UpdateStreamResolved),
    ("update_topic_resolved", Operation::UpdateTopicResolved),
    ("update_user_resolved", Operation::UpdateUserResolved),
];
const RETIRED_OPERATIONS: [(&str, u8); 3] = [
    ("update_stream", 129),
    ("update_topic", 133),
    ("update_user", 142),
];
const HEADER_KINDS: [HeaderKind; 5] = [
    HeaderKind::Raw,
    HeaderKind::String,
    HeaderKind::Bool,
    HeaderKind::Uint32,
    HeaderKind::Uint64,
];
const OPTION_FLAGS: [(&str, u8); 5] = [
    ("critical", OPTION_FLAG_CRITICAL),
    ("read_only", OPTION_FLAG_READ_ONLY),
    ("resettable", OPTION_FLAG_RESETTABLE),
    ("secret", OPTION_FLAG_SECRET),
    ("updatable", OPTION_FLAG_UPDATABLE),
];
const USER_STATUSES: [UserStatus; 2] = [UserStatus::Active, UserStatus::Inactive];

#[derive(Debug, Serialize, Deserialize)]
pub struct ResourceOptionsFixture {
    pub schema_version: u32,
    pub generated_by: String,
    pub conventions: BTreeMap<String, String>,
    pub constants: WireConstants,
    pub catalog: Vec<CatalogRow>,
    pub describe_options: Vec<DescribeOptionsVector>,
    pub stream_key: StreamKeyVectors,
    pub update_resource: Vec<UpdateResourceVector>,
    pub update_resource_errors: Vec<UpdateResourceErrorVector>,
    pub create_stream: Vec<CreateStreamVector>,
    pub create_topic: Vec<CreateTopicVector>,
    pub create_user: Vec<CreateUserVector>,
    pub http_update: Vec<HttpUpdateVector>,
    pub stream_responses: Vec<StreamResponseVector>,
    pub topic_responses: Vec<TopicResponseVector>,
    pub user_responses: Vec<UserResponseVector>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct WireConstants {
    pub protocol_version: ProtocolVersionValue,
    pub protocol_version_min: ProtocolVersionValue,
    pub command_codes: BTreeMap<String, CommandCode>,
    pub error_codes: BTreeMap<String, u32>,
    pub retired_command_codes: BTreeMap<String, u32>,
    pub operations: BTreeMap<String, OperationValue>,
    pub retired_operations: BTreeMap<String, u8>,
    pub scope_codes: BTreeMap<String, u8>,
    pub header_kind_codes: BTreeMap<HeaderKind, u8>,
    pub option_flags: BTreeMap<String, u8>,
    pub user_status_codes: BTreeMap<String, u8>,
    pub stream_key_length: usize,
    pub never_returned_keys: Vec<OptionKey>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ProtocolVersionValue {
    pub packed: u32,
    pub text: String,
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct CommandCode {
    pub code: u32,
    pub operation: Option<u8>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct OperationValue {
    pub value: u8,
    pub internal: bool,
    pub request_operation: u8,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct CatalogRow {
    pub key: OptionKey,
    pub scopes: Vec<OptionsScope>,
    pub kind: HeaderKind,
    pub kind_code: u8,
    pub storage: String,
    pub mutability: String,
    pub visibility: String,
    pub critical: bool,
    pub flags: u8,
    pub described: bool,
    pub default: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DescribeOptionsVector {
    pub scope: OptionsScope,
    pub request_hex: String,
    pub response_hex: String,
    pub entries: Vec<DescribedOption>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct DescribedOption {
    pub key: String,
    pub kind: HeaderKind,
    pub flags: u8,
    pub default: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StreamKeyVectors {
    pub key_hex: String,
    pub key_base64: String,
    pub fingerprint_label: String,
    pub fingerprint: String,
    pub invalid_keys: Vec<InvalidKeyVector>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyInput {
    RawHex(String),
    Base64(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InvalidKeyVector {
    pub case: String,
    pub input: KeyInput,
    pub error: ErrorExpectation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorExpectation {
    pub name: String,
    pub code: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IdentifierInput {
    Numeric(u32),
    Named(String),
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TargetInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<IdentifierInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<IdentifierInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<IdentifierInput>,
}

/// Typed update input: absent fields keep their value, u64 values are decimal strings.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PatchInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_expiry: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_topic_size: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<UserStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reset: Vec<OptionKey>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateResourceVector {
    pub case: String,
    pub scope: OptionsScope,
    pub target: TargetInput,
    pub input: PatchInput,
    #[serde(with = "iggy_common::resource_options_json")]
    pub wire_set: ResourceOptions,
    pub wire_reset: Vec<String>,
    pub body_hex: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateResourceErrorVector {
    pub case: String,
    pub scope: OptionsScope,
    pub target: TargetInput,
    pub input: PatchInput,
    pub error: ErrorExpectation,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateStreamInput {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption_key: Option<KeyInput>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub raw: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateStreamVector {
    pub case: String,
    pub input: CreateStreamInput,
    #[serde(with = "iggy_common::resource_options_json")]
    pub wire_options: ResourceOptions,
    pub body_hex: String,
    /// The JSON body the SDK's HTTP create sends for the same input.
    pub http_body: String,
    pub admission_error: Option<ErrorExpectation>,
}

/// Typed topic create input. u64 values are decimal strings.
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTopicInput {
    pub stream_id: IdentifierInput,
    pub name: String,
    pub partitions_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression_algorithm: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_expiry: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_topic_size: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub segment_size: Option<String>,
    pub durability: Durability,
    pub consumer_offset_durability: Durability,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTopicVector {
    pub case: String,
    pub input: CreateTopicInput,
    #[serde(with = "iggy_common::resource_options_json")]
    pub wire_options: ResourceOptions,
    pub body_hex: String,
    /// The JSON body the SDK's HTTP create sends for the same input.
    pub http_body: String,
    pub admission_error: Option<ErrorExpectation>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateUserInput {
    pub username: String,
    pub password: String,
    pub status: UserStatus,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateUserVector {
    pub case: String,
    pub input: CreateUserInput,
    #[serde(with = "iggy_common::resource_options_json")]
    pub wire_options: ResourceOptions,
    pub body_hex: String,
    /// The JSON body the SDK's HTTP create sends for the same input.
    pub http_body: String,
    pub admission_error: Option<ErrorExpectation>,
}

/// Where the server stops an HTTP update body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HttpOutcome {
    Accepted,
    JsonRefused,
    DecodeRefused,
    AdmissionRefused,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HttpUpdateVector {
    pub case: String,
    pub scope: OptionsScope,
    pub target: TargetInput,
    pub body: String,
    pub update_resource_hex: Option<String>,
    pub outcome: HttpOutcome,
    pub error: Option<ErrorExpectation>,
}

#[derive(Debug, PartialEq)]
pub struct HttpEvaluation {
    pub update_resource: Option<Vec<u8>>,
    pub outcome: HttpOutcome,
    pub error: Option<ErrorExpectation>,
}

/// Stored state behind a stream response. Wrap nonces are random, so only projections are pinned.
#[derive(Debug, Serialize, Deserialize)]
pub struct StreamKeyState {
    pub encryption_key_source: StreamEncryptionSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_key: Option<KeyInput>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub kek_config_keys: Vec<String>,
    #[serde(with = "iggy_common::resource_options_json")]
    pub stored_extra: ResourceOptions,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StreamResponseVector {
    pub case: String,
    pub state: StreamKeyState,
    pub response_hex: String,
    pub expected: StreamProjection,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct StreamProjection {
    pub id: u32,
    pub created_at: u64,
    pub name: String,
    pub topics_count: u32,
    pub size_bytes: u64,
    pub messages_count: u64,
    #[serde(with = "iggy_common::resource_options_json")]
    pub options: ResourceOptions,
    pub encryption_key_source: Option<StreamEncryptionSource>,
    pub encryption_key_fingerprint: Option<String>,
    pub encryption_kek_fingerprints: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicResponseVector {
    pub case: String,
    pub response_hex: String,
    pub expected: TopicProjection,
}

/// A topic read plus the three settings the header no longer carries, resolved from options.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct TopicProjection {
    pub id: u32,
    pub created_at: u64,
    pub name: String,
    pub partitions_count: u32,
    pub size_bytes: u64,
    pub messages_count: u64,
    #[serde(with = "iggy_common::resource_options_json")]
    pub options: ResourceOptions,
    pub compression_algorithm: String,
    pub message_expiry: String,
    pub max_topic_size: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UserResponseVector {
    pub case: String,
    pub response_hex: String,
    pub expected: UserProjection,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct UserProjection {
    pub id: u32,
    pub created_at: u64,
    pub status: UserStatus,
    pub username: String,
    #[serde(with = "iggy_common::resource_options_json")]
    pub options: ResourceOptions,
}

impl WireConstants {
    /// The values the protocol crates define in this build.
    pub fn live() -> Result<Self, IggyError> {
        Ok(Self {
            protocol_version: ProtocolVersionValue::try_from(IGGY_PROTOCOL_VERSION)?,
            protocol_version_min: ProtocolVersionValue::try_from(IGGY_PROTOCOL_VERSION_MIN)?,
            command_codes: COMMANDS
                .iter()
                .map(|(name, code)| {
                    let meta = lookup_command(*code).ok_or(IggyError::InvalidCommand)?;
                    Ok((
                        (*name).to_owned(),
                        CommandCode {
                            code: *code,
                            operation: meta.operation.map(|operation| operation as u8),
                        },
                    ))
                })
                .collect::<Result<_, IggyError>>()?,
            error_codes: BTreeMap::from([(
                "cannot_modify_root_user".to_owned(),
                IggyError::CannotModifyRootUser.as_code(),
            )]),
            retired_command_codes: RETIRED_COMMANDS
                .iter()
                .map(|(name, code)| ((*name).to_owned(), *code))
                .collect(),
            operations: OPERATIONS
                .iter()
                .map(|(name, operation)| {
                    (
                        (*name).to_owned(),
                        OperationValue {
                            value: *operation as u8,
                            internal: operation.is_internal(),
                            request_operation: operation.request_operation() as u8,
                        },
                    )
                })
                .collect(),
            retired_operations: RETIRED_OPERATIONS
                .iter()
                .map(|(name, value)| ((*name).to_owned(), *value))
                .collect(),
            scope_codes: SCOPES
                .iter()
                .map(|scope| (scope.to_string(), scope.as_code()))
                .collect(),
            header_kind_codes: HEADER_KINDS
                .iter()
                .map(|kind| (*kind, kind.as_code()))
                .collect(),
            option_flags: OPTION_FLAGS
                .iter()
                .map(|(name, flag)| ((*name).to_owned(), *flag))
                .collect(),
            user_status_codes: USER_STATUSES
                .iter()
                .map(|status| (status.to_string(), status.as_code()))
                .collect(),
            stream_key_length: STREAM_KEY_LENGTH,
            never_returned_keys: CATALOG
                .iter()
                .filter(|descriptor| {
                    matches!(
                        descriptor.visibility,
                        OptionVisibility::Secret | OptionVisibility::Internal
                    )
                })
                .map(|descriptor| descriptor.key)
                .collect(),
        })
    }
}

impl KeyInput {
    /// The key this input names, parsed the way SDK callers parse it.
    pub fn parse(&self) -> Result<StreamEncryptionKey, IggyError> {
        match self {
            Self::RawHex(text) => {
                let bytes = hex::decode(text).map_err(|_| IggyError::InvalidEncryptionKey)?;
                StreamEncryptionKey::try_from(bytes.as_slice())
            }
            Self::Base64(text) => StreamEncryptionKey::from_str(text),
        }
    }
}

impl TargetInput {
    /// The wire target for `scope`, built the way the SDK clients build it.
    pub fn wire(&self, scope: OptionsScope) -> Result<ResourceTarget, IggyError> {
        let wire = |input: &Option<IdentifierInput>| -> Result<WireIdentifier, IggyError> {
            let input = input.as_ref().ok_or(IggyError::InvalidIdentifier)?;
            identifier_to_wire(&Identifier::try_from(input)?)
        };
        Ok(match scope {
            OptionsScope::Topic => ResourceTarget::Topic {
                stream_id: wire(&self.stream_id)?,
                topic_id: wire(&self.topic_id)?,
            },
            OptionsScope::Stream => ResourceTarget::Stream {
                stream_id: wire(&self.stream_id)?,
            },
            OptionsScope::User => ResourceTarget::User {
                user_id: wire(&self.user_id)?,
            },
        })
    }
}

impl PatchInput {
    /// Encode through the scope's typed update options, the public SDK API.
    pub fn patch(&self, scope: OptionsScope) -> Result<OptionsPatch, IggyError> {
        let reset = self.reset.iter().copied().collect();
        match scope {
            OptionsScope::Topic => TopicUpdateOptions {
                name: self.name.clone(),
                message_expiry: decimal(self.message_expiry.as_deref())?.map(IggyExpiry::from),
                max_topic_size: decimal(self.max_topic_size.as_deref())?.map(MaxTopicSize::from),
                reset,
                ..TopicUpdateOptions::default()
            }
            .to_patch(),
            OptionsScope::Stream => StreamUpdateOptions {
                name: self.name.clone(),
                reset,
                ..StreamUpdateOptions::default()
            }
            .to_patch(),
            OptionsScope::User => UserUpdateOptions {
                username: self.username.clone(),
                status: self.status,
                reset,
                ..UserUpdateOptions::default()
            }
            .to_patch(),
        }
    }
}

impl CreateStreamInput {
    /// The request the SDK's `create_stream` sends.
    pub fn request(&self) -> Result<CreateStreamRequest, IggyError> {
        Ok(CreateStreamRequest {
            name: WireName::new(self.name.as_str()).map_err(|_| IggyError::InvalidFormat)?,
            options: self.options()?.to_wire()?,
        })
    }

    /// The body the SDK's HTTP `create_stream` posts.
    pub fn http_body(&self) -> Result<String, IggyError> {
        serde_json::to_string(&CreateStream {
            name: self.name.clone(),
            options: OptionInputMap(self.options()?.to_input_map()?),
        })
        .map_err(|_| IggyError::InvalidFormat)
    }

    fn options(&self) -> Result<StreamCreateOptions, IggyError> {
        Ok(StreamCreateOptions {
            encryption_key: self
                .encryption_key
                .as_ref()
                .map(KeyInput::parse)
                .transpose()?,
            raw: self.raw.clone(),
        })
    }
}

impl CreateTopicInput {
    /// The request the SDK's `create_topic` sends.
    pub fn request(&self) -> Result<CreateTopicRequest, IggyError> {
        let options = self.options()?;
        Ok(CreateTopicRequest {
            stream_id: identifier_to_wire(&Identifier::try_from(&self.stream_id)?)?,
            partitions_count: options.partitions_count.unwrap_or(DEFAULT_PARTITIONS_COUNT),
            name: WireName::new(self.name.as_str()).map_err(|_| IggyError::InvalidFormat)?,
            options: options.to_wire()?,
        })
    }

    /// The body the SDK's HTTP `create_topic` posts.
    pub fn http_body(&self) -> Result<String, IggyError> {
        let options = self.options()?;
        serde_json::to_string(&CreateTopic {
            stream_id: Identifier::try_from(&self.stream_id)?,
            partitions_count: options.partitions_count.unwrap_or(DEFAULT_PARTITIONS_COUNT),
            name: self.name.clone(),
            options: OptionInputMap(options.to_input_map()?),
        })
        .map_err(|_| IggyError::InvalidFormat)
    }

    fn options(&self) -> Result<TopicCreateOptions, IggyError> {
        Ok(TopicCreateOptions {
            partitions_count: Some(self.partitions_count),
            compression_algorithm: self
                .compression_algorithm
                .as_deref()
                .map(CompressionAlgorithm::from_str)
                .transpose()
                .map_err(|_| IggyError::InvalidFormat)?,
            message_expiry: decimal(self.message_expiry.as_deref())?.map(IggyExpiry::from),
            max_topic_size: decimal(self.max_topic_size.as_deref())?.map(MaxTopicSize::from),
            segment_size: decimal(self.segment_size.as_deref())?.map(IggyByteSize::from),
            durability: self.durability,
            consumer_offset_durability: self.consumer_offset_durability,
            ..TopicCreateOptions::default()
        })
    }
}

impl CreateUserInput {
    /// The request the SDK's `create_user` sends, without permissions.
    pub fn request(&self) -> Result<CreateUserRequest, IggyError> {
        Ok(CreateUserRequest {
            username: WireName::new(self.username.as_str())
                .map_err(|_| IggyError::InvalidFormat)?,
            password: self.password.clone(),
            status: self.status.as_code(),
            permissions: None,
            options: UserCreateOptions::default().to_wire()?,
        })
    }

    /// The body the SDK's HTTP `create_user` posts, without permissions.
    pub fn http_body(&self) -> Result<String, IggyError> {
        serde_json::to_string(&CreateUser {
            username: self.username.clone(),
            password: SecretString::from(self.password.as_str()),
            status: self.status,
            permissions: None,
            options: OptionInputMap(UserCreateOptions::default().to_input_map()?),
        })
        .map_err(|_| IggyError::InvalidFormat)
    }
}

impl StreamKeyState {
    /// The options map the server stores: the extras plus the wrap set and its marker.
    pub fn stored_options(&self) -> Result<ResourceOptions, IggyError> {
        let mut stored = self.stored_extra.clone();
        let origin = match self.encryption_key_source {
            StreamEncryptionSource::Stream => StreamKeyOrigin::Client,
            StreamEncryptionSource::Server => StreamKeyOrigin::Server,
            StreamEncryptionSource::None => return Ok(stored),
        };
        let key = self
            .data_key
            .as_ref()
            .ok_or(IggyError::InvalidEncryptionKey)?
            .parse()?;
        let envelopes = self
            .kek_config_keys
            .iter()
            .map(|config_key| {
                ServerKeyContext::from_config(config_key, &[], true)
                    .and_then(|context| context.wrap(&key, origin))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let set = WrapSet::new(envelopes)?;
        stored.insert(
            HeaderKey::from(OptionKey::EncryptionKeyWrapped),
            OptionValue::explicit(HeaderValue::try_from(set.as_ref())?),
        );
        stored.insert(
            HeaderKey::from(OptionKey::CriticalKeys),
            OptionValue::explicit(critical_keys_marker(&[OptionKey::EncryptionKeyWrapped])?),
        );
        Ok(stored)
    }
}

impl TryFrom<u32> for ProtocolVersionValue {
    type Error = IggyError;

    fn try_from(packed: u32) -> Result<Self, Self::Error> {
        let text = ProtocolVersion(packed).to_string();
        let components = text
            .split('.')
            .map(u32::from_str)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| IggyError::InvalidVersion(text.clone()))?;
        let [major, minor, patch] = components[..] else {
            return Err(IggyError::InvalidVersion(text));
        };
        Ok(Self {
            packed,
            text,
            major,
            minor,
            patch,
        })
    }
}

impl TryFrom<&OptionDescriptor> for CatalogRow {
    type Error = IggyError;

    fn try_from(descriptor: &OptionDescriptor) -> Result<Self, Self::Error> {
        let storage = match descriptor.storage {
            OptionStorage::Map => "map",
            OptionStorage::Column => "column",
            OptionStorage::Computed => "computed",
        };
        let mutability = match descriptor.mutability {
            OptionMutability::CreateOnly => "create_only",
            OptionMutability::Updatable => "updatable",
            OptionMutability::ReadOnly => "read_only",
        };
        let visibility = match descriptor.visibility {
            OptionVisibility::Public => "public",
            OptionVisibility::Secret => "secret",
            OptionVisibility::Internal => "internal",
            OptionVisibility::ReadOnly => "read_only",
        };
        Ok(Self {
            key: descriptor.key,
            scopes: descriptor.scopes.to_vec(),
            kind: descriptor.kind,
            kind_code: descriptor.kind.as_code(),
            storage: storage.to_owned(),
            mutability: mutability.to_owned(),
            visibility: visibility.to_owned(),
            critical: descriptor.critical,
            flags: descriptor.flags(),
            described: descriptor.is_described(),
            default: descriptor
                .default_value()?
                .map(|value| encode_value(&value))
                .transpose()?,
        })
    }
}

impl TryFrom<&WireOptionDescriptor> for DescribedOption {
    type Error = IggyError;

    fn try_from(entry: &WireOptionDescriptor) -> Result<Self, Self::Error> {
        let kind = HeaderKind::from_code(entry.kind)?;
        let default = (!entry.default_value.is_empty())
            .then(|| {
                HeaderValue::from_raw(kind, &entry.default_value)
                    .and_then(|value| encode_value(&value))
            })
            .transpose()?;
        Ok(Self {
            key: entry.key.to_string(),
            kind,
            flags: entry.flags,
            default,
        })
    }
}

impl TryFrom<&IdentifierInput> for Identifier {
    type Error = IggyError;

    fn try_from(input: &IdentifierInput) -> Result<Self, Self::Error> {
        match input {
            IdentifierInput::Numeric(value) => Self::numeric(*value),
            IdentifierInput::Named(name) => Self::named(name),
        }
    }
}

impl From<&IggyError> for ErrorExpectation {
    fn from(error: &IggyError) -> Self {
        Self {
            name: error.as_string().to_owned(),
            code: error.as_code(),
        }
    }
}

impl From<&StreamDetails> for StreamProjection {
    fn from(details: &StreamDetails) -> Self {
        Self {
            id: details.id,
            created_at: details.created_at.as_micros(),
            name: details.name.clone(),
            topics_count: details.topics_count,
            size_bytes: details.size.as_bytes_u64(),
            messages_count: details.messages_count,
            options: details.options.clone(),
            encryption_key_source: details.encryption_key_source(),
            encryption_key_fingerprint: details.encryption_key_fingerprint(),
            encryption_kek_fingerprints: details.encryption_kek_fingerprints(),
        }
    }
}

impl From<&TopicDetails> for TopicProjection {
    fn from(details: &TopicDetails) -> Self {
        let resolved = details.resolved_options();
        Self {
            id: details.id,
            created_at: details.created_at.as_micros(),
            name: details.name.clone(),
            partitions_count: details.partitions_count,
            size_bytes: details.size.as_bytes_u64(),
            messages_count: details.messages_count,
            options: details.options.clone(),
            compression_algorithm: resolved.compression_algorithm.to_string(),
            message_expiry: u64::from(resolved.message_expiry).to_string(),
            max_topic_size: u64::from(resolved.max_topic_size).to_string(),
        }
    }
}

impl From<&UserInfoDetails> for UserProjection {
    fn from(details: &UserInfoDetails) -> Self {
        Self {
            id: details.id,
            created_at: details.created_at.as_micros(),
            status: details.status,
            username: details.username.clone(),
            options: details.options.clone(),
        }
    }
}

/// The `UpdateResource` an SDK client sends for this typed input.
pub fn update_resource_request(
    scope: OptionsScope,
    target: &TargetInput,
    input: &PatchInput,
) -> Result<UpdateResourceRequest, IggyError> {
    let patch = input.patch(scope)?;
    Ok(UpdateResourceRequest {
        target: target.wire(scope)?,
        reset: patch.reset,
        options: patch.set,
    })
}

/// The server's catalog admission of a decoded `UpdateResource`.
pub fn admit_update(request: &UpdateResourceRequest) -> Result<(), IggyError> {
    let reset: Vec<String> = request
        .reset
        .iter()
        .map(|name| name.as_str().to_owned())
        .collect();
    admit(
        OptionsScope::from_code(request.target.scope_code())?,
        OptionOperation::Update,
        &request.options,
        &reset,
        &OptionAdmissionContext::default(),
    )
    .map(|_| ())
}

/// The server's catalog admission of a create options block.
pub fn admit_create(
    scope: OptionsScope,
    create_columns: OptionColumns,
    options: &WireOptions,
) -> Result<(), IggyError> {
    admit(
        scope,
        OptionOperation::Create,
        options,
        &[],
        &OptionAdmissionContext {
            current: None,
            create_columns,
        },
    )
    .map(|_| ())
}

/// An HTTP update body followed through JSON parsing, the handler, the decoder and admission.
pub fn evaluate_http_update(
    scope: OptionsScope,
    target: &TargetInput,
    body: &str,
) -> Result<HttpEvaluation, IggyError> {
    let Ok(update) = serde_json::from_str::<OptionsUpdate>(body) else {
        return Ok(HttpEvaluation {
            update_resource: None,
            outcome: HttpOutcome::JsonRefused,
            error: None,
        });
    };
    let patch = update.to_patch()?;
    let bytes = UpdateResourceRequest {
        target: target.wire(scope)?,
        reset: patch.reset,
        options: patch.set,
    }
    .to_bytes()
    .to_vec();
    let verdict = UpdateResourceRequest::decode_from(&bytes)
        .map_err(|_| (HttpOutcome::DecodeRefused, IggyError::InvalidCommand))
        .and_then(|request| {
            admit_update(&request).map_err(|error| (HttpOutcome::AdmissionRefused, error))
        });
    let (outcome, error) = match verdict {
        Ok(()) => (HttpOutcome::Accepted, None),
        Err((outcome, error)) => (outcome, Some(ErrorExpectation::from(&error))),
    };
    Ok(HttpEvaluation {
        update_resource: Some(bytes),
        outcome,
        error,
    })
}

pub fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

fn decimal(text: Option<&str>) -> Result<Option<u64>, IggyError> {
    text.map(u64::from_str)
        .transpose()
        .map_err(|_| IggyError::InvalidFormat)
}
