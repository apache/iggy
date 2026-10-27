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

//! Shape and builder of the metadata change log golden vectors.
//!
//! Shared by the generator example and the drift test, so the test rebuilds
//! the fixture in process and compares it byte for byte with the canonical
//! file and every SDK copy.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::Bytes;
use iggy_binary_protocol::codes::GET_METADATA_CHANGES_CODE;
use iggy_binary_protocol::lookup_command;
use iggy_binary_protocol::primitives::metadata_changes::{
    MAX_PAGE_TOKEN_KEY_SIZE, MAX_PAGE_TOKEN_SIZE, METADATA_CHANGE_CURSOR_SIZE, WireChangeOptions,
    WireConsumerGroupCreatedBody, WireConsumerGroupDeletedBody, WireConsumerGroupItemBody,
    WireMemberItemBody, WireMemberJoinedBody, WireMemberLeftBody, WireMemberLeftReason,
    WireMetadataChange, WireMetadataChangeCursor, WireMetadataChangeGroup, WireMetadataChangeKind,
    WireMetadataChangePhase, WireMetadataChangeStartMode, WireMetadataChangeStatus,
    WireMetadataCurrentItem, WireMetadataCurrentPageToken, WirePartitionHandedOverBody,
    WirePartitionsChangedBody, WirePartitionsItemBody, WirePermissionsUpdatedReason,
    WireSessionBody, WireSessionClosedBody, WireSessionClosedReason, WireStreamBody,
    WireStreamDeletedBody, WireStreamKey, WireTokenBody, WireTokenDeletedBody,
    WireTokenDeletedReason, WireTopicBody, WireTopicDeletedBody, WireTopicItemBody, WireTopicKey,
    WireUserBody, WireUserIdentityBody, WireUserKey, WireUserPermissionsBody,
};
use iggy_binary_protocol::requests::system::GetMetadataChangesRequest;
use iggy_binary_protocol::responses::system::{
    GetMetadataChangesResponse, WireMetadataChangeRecord, WireMetadataItem,
};
use iggy_binary_protocol::{WireEncode, WireName, WireOptions};
use iggy_common::wire_conversions::{permissions_to_wire, resource_options_to_wire_split};
use iggy_common::{
    DEFAULT_METADATA_CHANGES_LIMIT, GlobalPermissions, HeaderKey, HeaderValue, IggyError,
    IggyTimestamp, MAX_METADATA_CHANGES_LIMIT, METADATA_CHANGE_CURSOR_PREFIX,
    METADATA_CURRENT_PAGE_TOKEN_PREFIX, MetadataChangeClass, MetadataChangeCursor,
    MetadataChangeGroup, MetadataChangeKind, MetadataChangePhase, MetadataChangeRecord,
    MetadataChangeStart, MetadataChangeStatus, MetadataChanges, MetadataChangesQuery,
    MetadataCurrentItem, MetadataCurrentPageToken, MetadataMemberLeftReason,
    MetadataPermissionsUpdatedReason, MetadataSessionClosedReason, MetadataTokenDeletedReason,
    OptionKey, OptionValue, Permissions, ResourceOptions, StreamPermissions, TopicPermissions,
};
use serde::Serialize;
use std::collections::BTreeMap;
use std::error::Error;
use std::str::FromStr;
use strum::IntoEnumIterator;

pub const SCHEMA_VERSION: u32 = 1;
pub const GENERATOR: &str = "core/common/examples/metadata_changes_fixtures.rs";
/// Relative to the `iggy_common` manifest directory.
pub const CANONICAL_PATH: &str = "fixtures/metadata_changes.json";
/// Byte-identical copies each foreign SDK loads from its own test resources.
pub const LANGUAGE_COPIES: [&str; 8] = [
    "../../foreign/python/tests/fixtures/metadata_changes.json",
    "../../foreign/cpp/tests/fixtures/metadata_changes.json",
    "../../foreign/php/tests/fixtures/metadata_changes.json",
    "../../foreign/node/src/wire/fixtures/metadata_changes.json",
    "../../foreign/go/contracts/testdata/metadata_changes.json",
    "../../foreign/java/java-sdk/src/test/resources/metadata_changes.json",
    "../../foreign/csharp/Iggy_SDK_Tests/Fixtures/metadata_changes.json",
    "../../foreign/swift/Tests/IggyTests/Fixtures/metadata_changes.json",
];

const LOG_ID: u128 = 0x0d4b_8f2a_6e1c_93d7;
const ABOVE_2_POW_53: u64 = (1 << 53) + 1;

pub type FixtureResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug, Serialize)]
pub struct MetadataChangesFixture {
    pub schema_version: u32,
    pub generated_by: String,
    pub conventions: BTreeMap<String, String>,
    pub constants: Constants,
    pub records: Vec<RecordVector>,
    pub items: Vec<ItemVector>,
    pub cursors: Vec<CursorVector>,
    pub page_tokens: Vec<PageTokenVector>,
    pub requests: Vec<RequestVector>,
    pub responses: Vec<ResponseVector>,
    pub large_values: LargeValuesVector,
    pub refused: RefusedVectors,
}

#[derive(Debug, Serialize)]
pub struct Constants {
    pub command_code: u32,
    pub command_name: String,
    pub cursor_prefix: String,
    pub page_token_prefix: String,
    pub cursor_size: usize,
    pub cursor_text_length: usize,
    pub max_page_token_size: usize,
    pub max_page_token_key_size: usize,
    pub default_limit: u32,
    pub max_limit: u32,
    pub groups: BTreeMap<String, GroupConstant>,
    pub kinds: BTreeMap<String, KindConstant>,
    pub statuses: BTreeMap<String, u8>,
    pub phases: BTreeMap<String, u8>,
    pub start_modes: BTreeMap<String, u8>,
    pub member_left_reasons: BTreeMap<String, u8>,
    pub token_deleted_reasons: BTreeMap<String, u8>,
    pub session_closed_reasons: BTreeMap<String, u8>,
    pub permissions_updated_reasons: BTreeMap<String, u8>,
}

#[derive(Debug, Serialize)]
pub struct GroupConstant {
    pub code: u8,
    pub class: String,
    pub parents: Vec<String>,
    pub page_token_key: String,
}

#[derive(Debug, Serialize)]
pub struct KindConstant {
    pub code: u8,
    pub group: String,
}

/// A record's binary forms and the JSON the HTTP route renders for it.
#[derive(Debug, Serialize)]
pub struct RecordVector {
    pub case: String,
    pub group: u8,
    pub kind: u8,
    pub body_hex: String,
    pub record_hex: String,
    pub expected: MetadataChangeRecord,
}

#[derive(Debug, Serialize)]
pub struct ItemVector {
    pub case: String,
    pub group: u8,
    pub body_hex: String,
    pub item_hex: String,
    pub expected: MetadataCurrentItem,
}

#[derive(Debug, Serialize)]
pub struct CursorVector {
    pub case: String,
    pub text: String,
    pub hex: String,
    pub log_id: String,
    pub op: u64,
    pub checksum: String,
    pub ordinal: u32,
}

#[derive(Debug, Serialize)]
pub struct PageTokenVector {
    pub case: String,
    pub text: String,
    pub hex: String,
    pub anchor: String,
    pub required_head: u64,
    pub groups_mask: u8,
    pub group: String,
    pub last_key_hex: String,
}

/// A query in the shape every SDK can build: start mode, positions as text.
#[derive(Debug, Clone, Serialize)]
pub struct QueryInput {
    pub start: String,
    pub cursor: Option<String>,
    pub page_token: Option<String>,
    pub groups: Vec<String>,
    pub since: Option<u64>,
    pub until: Option<u64>,
    pub limit: u32,
}

#[derive(Debug, Serialize)]
pub struct RequestVector {
    pub case: String,
    pub input: QueryInput,
    pub request_hex: String,
    pub http_query: String,
}

#[derive(Debug, Serialize)]
pub struct ResponseVector {
    pub case: String,
    pub response_hex: String,
    pub expected: MetadataChanges,
}

/// Unsigned values above 2^53 as decimal strings, so SDKs can prove they
/// decode them losslessly from the binary form.
#[derive(Debug, Serialize)]
pub struct LargeValuesVector {
    pub record_hex: String,
    pub op: String,
    pub ordinal: u32,
    pub timestamp: String,
    pub checksum: String,
    pub user_created_at: String,
    pub session: String,
    pub client_id: String,
}

#[derive(Debug, Serialize)]
pub struct RefusedVectors {
    pub cursors: Vec<RefusedText>,
    pub page_tokens: Vec<RefusedText>,
    pub record_bodies: Vec<RefusedBody>,
    pub requests: Vec<RefusedHex>,
    pub responses: Vec<RefusedHex>,
    pub queries: Vec<RefusedQuery>,
    pub http_queries: Vec<RefusedHttpQuery>,
}

#[derive(Debug, Serialize)]
pub struct RefusedText {
    pub case: String,
    pub text: String,
}

#[derive(Debug, Serialize)]
pub struct RefusedBody {
    pub case: String,
    pub group: u8,
    pub kind: u8,
    pub body_hex: String,
}

#[derive(Debug, Serialize)]
pub struct RefusedHex {
    pub case: String,
    pub hex: String,
}

#[derive(Debug, Serialize)]
pub struct RefusedQuery {
    pub case: String,
    pub input: QueryInput,
}

#[derive(Debug, Serialize)]
pub struct RefusedHttpQuery {
    pub case: String,
    pub query: String,
}

impl QueryInput {
    fn new(start: &str) -> Self {
        Self {
            start: start.to_owned(),
            cursor: None,
            page_token: None,
            groups: Vec::new(),
            since: None,
            until: None,
            limit: DEFAULT_METADATA_CHANGES_LIMIT,
        }
    }

    /// The domain query this input describes.
    ///
    /// # Errors
    ///
    /// `InvalidCommand` for an unknown start mode, group or position text.
    pub fn query(&self) -> Result<MetadataChangesQuery, IggyError> {
        let start = match (self.start.as_str(), &self.cursor, &self.page_token) {
            ("earliest", None, None) => MetadataChangeStart::Earliest,
            ("latest", None, None) => MetadataChangeStart::Latest,
            ("after", Some(cursor), None) => {
                MetadataChangeStart::After(MetadataChangeCursor::from_str(cursor)?)
            }
            ("current", None, token) => MetadataChangeStart::Current(
                token
                    .as_deref()
                    .map(MetadataCurrentPageToken::from_str)
                    .transpose()?,
            ),
            _ => return Err(IggyError::InvalidCommand),
        };
        let groups = self
            .groups
            .iter()
            .map(|name| MetadataChangeGroup::from_str(name).map_err(|_| IggyError::InvalidCommand))
            .collect::<Result<_, _>>()?;
        Ok(MetadataChangesQuery {
            start,
            groups,
            since: self.since.map(IggyTimestamp::from),
            until: self.until.map(IggyTimestamp::from),
            limit: self.limit,
        })
    }
}

/// Build the whole fixture from the live codecs.
///
/// # Errors
///
/// Propagates any codec or conversion failure, which means the fixture cannot
/// describe the current code.
pub fn build() -> FixtureResult<MetadataChangesFixture> {
    let records = record_vectors()?;
    let items = item_vectors()?;
    let cursors = cursor_vectors();
    let page_tokens = page_token_vectors();
    let requests = request_vectors()?;
    let responses = response_vectors(&records, &items)?;
    Ok(MetadataChangesFixture {
        schema_version: SCHEMA_VERSION,
        generated_by: GENERATOR.to_owned(),
        conventions: conventions(),
        constants: constants()?,
        records,
        items,
        cursors,
        page_tokens,
        requests,
        responses,
        large_values: large_values_vector()?,
        refused: refused_vectors()?,
    })
}

/// The fixture as the canonical file text.
///
/// # Errors
///
/// Propagates build and serialization failures.
pub fn render() -> FixtureResult<String> {
    let mut json = serde_json::to_string_pretty(&build()?)?;
    json.push('\n');
    Ok(json)
}

fn conventions() -> BTreeMap<String, String> {
    [
        (
            "byte_strings",
            "Every *_hex field is lowercase hex without separators.",
        ),
        (
            "u128_values",
            "128-bit values (log ids, checksums, client ids) are 32 lowercase hex characters in JSON.",
        ),
        (
            "u64_values",
            "Ops and timestamps are JSON numbers below 2^53 in every vector except large_values, which carries them as decimal strings.",
        ),
        (
            "record_hex",
            "record_hex is one record envelope as a response carries it. body_hex is the body alone.",
        ),
        (
            "expected",
            "expected is the JSON the HTTP route renders. Binary decoders must reach the same values.",
        ),
        (
            "request_hex",
            "request_hex is the command payload without framing. http_query is the query string of GET /metadata/changes.",
        ),
        (
            "refused",
            "Every refused vector must fail to decode or validate. Query and HTTP refusals are invalid_command.",
        ),
        (
            "unknown_kinds",
            "A record of an unknown group or kind is kept with its codes and raw body, never refused.",
        ),
    ]
    .into_iter()
    .map(|(name, text)| (name.to_owned(), text.to_owned()))
    .collect()
}

fn constants() -> FixtureResult<Constants> {
    let command = lookup_command(GET_METADATA_CHANGES_CODE).ok_or("code 17 is not registered")?;
    let groups = MetadataChangeGroup::iter()
        .map(|group| {
            let class = match group.class() {
                MetadataChangeClass::Resource => "resource",
                MetadataChangeClass::Activity => "activity",
            };
            let key = match group {
                MetadataChangeGroup::Streams => "[stream_id:u32]",
                MetadataChangeGroup::Topics | MetadataChangeGroup::Partitions => {
                    "[stream_id:u32][topic_id:u32]"
                }
                MetadataChangeGroup::ConsumerGroups => {
                    "[stream_id:u32][topic_id:u32][group_id:u32]"
                }
                MetadataChangeGroup::ConsumerGroupMembers => {
                    "[stream_id:u32][topic_id:u32][group_id:u32][member_id:u32]"
                }
                MetadataChangeGroup::Users => "[user_id:u32]",
                MetadataChangeGroup::PersonalAccessTokens => "[user_id:u32][name_len:u8][name]",
                MetadataChangeGroup::Sessions => "[epoch:u64]",
            };
            (
                group.to_string(),
                GroupConstant {
                    code: WireMetadataChangeGroup::from(group).as_u8(),
                    class: class.to_owned(),
                    parents: group.parents().iter().map(ToString::to_string).collect(),
                    page_token_key: key.to_owned(),
                },
            )
        })
        .collect();
    let kinds = (0..=u8::MAX)
        .filter_map(|code| WireMetadataChangeKind::from_code(code).ok())
        .map(|kind| {
            let domain = MetadataChangeKind::from(kind);
            (
                domain.to_string(),
                KindConstant {
                    code: kind.as_u8(),
                    group: domain.group().to_string(),
                },
            )
        })
        .collect();
    Ok(Constants {
        command_code: GET_METADATA_CHANGES_CODE,
        command_name: command.name.to_owned(),
        cursor_prefix: METADATA_CHANGE_CURSOR_PREFIX.to_owned(),
        page_token_prefix: METADATA_CURRENT_PAGE_TOKEN_PREFIX.to_owned(),
        cursor_size: METADATA_CHANGE_CURSOR_SIZE,
        cursor_text_length: MetadataChangeCursor::default().to_string().len(),
        max_page_token_size: MAX_PAGE_TOKEN_SIZE,
        max_page_token_key_size: MAX_PAGE_TOKEN_KEY_SIZE,
        default_limit: DEFAULT_METADATA_CHANGES_LIMIT,
        max_limit: MAX_METADATA_CHANGES_LIMIT,
        groups,
        kinds,
        statuses: codes(
            (0..=u8::MAX).filter_map(|code| WireMetadataChangeStatus::from_code(code).ok()),
            |status| MetadataChangeStatus::from(status).to_string(),
            WireMetadataChangeStatus::as_u8,
        ),
        phases: codes(
            (0..=u8::MAX).filter_map(|code| WireMetadataChangePhase::from_code(code).ok()),
            |phase| MetadataChangePhase::from(phase).to_string(),
            WireMetadataChangePhase::as_u8,
        ),
        start_modes: codes(
            (0..=u8::MAX).filter_map(|code| WireMetadataChangeStartMode::from_code(code).ok()),
            |mode| format!("{mode:?}").to_lowercase(),
            WireMetadataChangeStartMode::as_u8,
        ),
        member_left_reasons: codes(
            (0..=u8::MAX).filter_map(|code| WireMemberLeftReason::from_code(code).ok()),
            |reason| MetadataMemberLeftReason::from(reason).to_string(),
            WireMemberLeftReason::as_u8,
        ),
        token_deleted_reasons: codes(
            (0..=u8::MAX).filter_map(|code| WireTokenDeletedReason::from_code(code).ok()),
            |reason| MetadataTokenDeletedReason::from(reason).to_string(),
            WireTokenDeletedReason::as_u8,
        ),
        session_closed_reasons: codes(
            (0..=u8::MAX).filter_map(|code| WireSessionClosedReason::from_code(code).ok()),
            |reason| MetadataSessionClosedReason::from(reason).to_string(),
            WireSessionClosedReason::as_u8,
        ),
        permissions_updated_reasons: codes(
            (0..=u8::MAX).filter_map(|code| WirePermissionsUpdatedReason::from_code(code).ok()),
            |reason| MetadataPermissionsUpdatedReason::from(reason).to_string(),
            WirePermissionsUpdatedReason::as_u8,
        ),
    })
}

fn codes<T: Copy>(
    values: impl Iterator<Item = T>,
    name: impl Fn(T) -> String,
    code: impl Fn(T) -> u8,
) -> BTreeMap<String, u8> {
    values.map(|value| (name(value), code(value))).collect()
}

fn stream_key() -> WireStreamKey {
    WireStreamKey {
        stream_id: 3,
        stream_created_op: 4121,
    }
}

fn topic_key() -> WireTopicKey {
    WireTopicKey {
        stream: stream_key(),
        topic_id: 0,
        topic_created_op: 4125,
    }
}

fn user_key() -> WireUserKey {
    WireUserKey {
        user_id: 7,
        user_created_at: 1_760_300_000_000_000,
    }
}

fn name(text: &str) -> FixtureResult<WireName> {
    Ok(WireName::new(text)?)
}

fn readable(options: &ResourceOptions) -> FixtureResult<WireChangeOptions> {
    let (explicit, derived) = resource_options_to_wire_split(options)?;
    Ok(WireChangeOptions::Readable { explicit, derived })
}

fn stream_options() -> FixtureResult<WireChangeOptions> {
    readable(&ResourceOptions::from([
        (
            HeaderKey::from(OptionKey::EncryptionKeySource),
            OptionValue::derived(HeaderValue::try_from("generated")?),
        ),
        (
            HeaderKey::from(OptionKey::EncryptionKeyFingerprint),
            OptionValue::derived(HeaderValue::try_from("9f2c1a7e44b0d816")?),
        ),
        (
            HeaderKey::from(OptionKey::EncryptionKekFingerprint),
            OptionValue::derived(HeaderValue::try_from("51d0e3a9c27b6f48")?),
        ),
    ]))
}

fn topic_options() -> FixtureResult<WireChangeOptions> {
    readable(&ResourceOptions::from([
        (
            HeaderKey::from(OptionKey::MessageExpiry),
            OptionValue::explicit(HeaderValue::from(604_800_000_000u64)),
        ),
        (
            HeaderKey::from(OptionKey::SegmentSize),
            OptionValue::derived(HeaderValue::from(1_073_741_824u64)),
        ),
    ]))
}

fn empty_options() -> WireChangeOptions {
    WireChangeOptions::Readable {
        explicit: WireOptions::empty(),
        derived: WireOptions::empty(),
    }
}

fn permissions() -> Permissions {
    Permissions {
        global: GlobalPermissions {
            read_streams: true,
            ..GlobalPermissions::default()
        },
        streams: Some(BTreeMap::from([(
            3,
            StreamPermissions {
                read_stream: true,
                topics: Some(BTreeMap::from([(
                    0,
                    TopicPermissions {
                        poll_messages: true,
                        ..TopicPermissions::default()
                    },
                )])),
                ..StreamPermissions::default()
            },
        )])),
    }
}

fn record_hex(record: &WireMetadataChangeRecord) -> String {
    hex::encode(record.to_bytes())
}

fn wire_record(
    change: &WireMetadataChange,
    op: u64,
    ordinal: u32,
    actor_user_id: Option<u32>,
) -> WireMetadataChangeRecord {
    WireMetadataChangeRecord {
        op,
        ordinal,
        checksum: 0x0000_0000_0000_0000_a5c1_f703_8b9e_2d4c + u128::from(op),
        timestamp: 1_760_356_800_000_000 + op,
        actor_user_id,
        group: change.group().as_u8(),
        kind: change.kind().as_u8(),
        body: change.body_bytes(),
    }
}

fn change_cases() -> FixtureResult<Vec<(&'static str, WireMetadataChange, Option<u32>)>> {
    let token_name = || name("ci-token");
    Ok(vec![
        (
            "stream_created",
            WireMetadataChange::StreamCreated(WireStreamBody {
                key: stream_key(),
                created_at: 1_760_356_800_123_456,
                name: name("telemetry")?,
                options: stream_options()?,
            }),
            Some(4),
        ),
        (
            "stream_updated_unreadable_options",
            WireMetadataChange::StreamUpdated(WireStreamBody {
                key: stream_key(),
                created_at: 1_760_356_800_123_456,
                name: name("telemetry-v2")?,
                options: WireChangeOptions::Unreadable,
            }),
            None,
        ),
        (
            "stream_deleted_deferred",
            WireMetadataChange::StreamDeleted(WireStreamDeletedBody {
                key: stream_key(),
                created_at: 1_760_356_800_123_456,
                name: name("telemetry")?,
                requested_op: Some(4140),
            }),
            Some(4),
        ),
        (
            "topic_created",
            WireMetadataChange::TopicCreated(WireTopicBody {
                key: topic_key(),
                created_at: 1_760_356_801_000_000,
                name: name("node-metrics")?,
                partitions_count: 8,
                options: topic_options()?,
            }),
            Some(4),
        ),
        (
            "topic_updated",
            WireMetadataChange::TopicUpdated(WireTopicBody {
                key: topic_key(),
                created_at: 1_760_356_801_000_000,
                name: name("node-metrics-v2")?,
                partitions_count: 8,
                options: empty_options(),
            }),
            Some(4),
        ),
        (
            "topic_deleted_immediate",
            WireMetadataChange::TopicDeleted(WireTopicDeletedBody {
                key: topic_key(),
                created_at: 1_760_356_801_000_000,
                name: name("node-metrics")?,
                requested_op: None,
            }),
            Some(4),
        ),
        (
            "partitions_changed_deleted_tail",
            WireMetadataChange::PartitionsChanged(WirePartitionsChangedBody {
                topic: topic_key(),
                partitions_count: 5,
                removed_partition_ids: vec![5, 6, 7],
                requested_op: Some(4200),
            }),
            Some(4),
        ),
        (
            "partitions_changed_created",
            WireMetadataChange::PartitionsChanged(WirePartitionsChangedBody {
                topic: topic_key(),
                partitions_count: 10,
                removed_partition_ids: vec![],
                requested_op: None,
            }),
            Some(4),
        ),
        (
            "consumer_group_created",
            WireMetadataChange::ConsumerGroupCreated(WireConsumerGroupCreatedBody {
                topic: topic_key(),
                group_id: 0,
                name: name("readers")?,
                consumer_groups_count: 1,
            }),
            Some(4),
        ),
        (
            "consumer_group_deleted",
            WireMetadataChange::ConsumerGroupDeleted(WireConsumerGroupDeletedBody {
                topic: topic_key(),
                group_id: 0,
                name: name("readers")?,
                consumer_groups_count: 0,
                requested_op: Some(4300),
            }),
            Some(4),
        ),
        (
            "consumer_group_member_joined",
            WireMetadataChange::ConsumerGroupMemberJoined(WireMemberJoinedBody {
                topic: topic_key(),
                group_id: 0,
                member_id: 1,
                client_id: 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10,
                members_count: 2,
                partition_ids: vec![],
            }),
            Some(9),
        ),
        (
            "consumer_group_member_left_session_ended",
            WireMetadataChange::ConsumerGroupMemberLeft(WireMemberLeftBody {
                topic: topic_key(),
                group_id: 0,
                member_id: 1,
                client_id: 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10,
                members_count: 1,
                reason: WireMemberLeftReason::SessionEnded,
            }),
            Some(9),
        ),
        (
            "consumer_group_partition_handed_over",
            WireMetadataChange::ConsumerGroupPartitionHandedOver(WirePartitionHandedOverBody {
                topic: topic_key(),
                group_id: 0,
                partition_id: 2,
                member_id: Some(1),
            }),
            None,
        ),
        (
            "consumer_group_partition_released",
            WireMetadataChange::ConsumerGroupPartitionHandedOver(WirePartitionHandedOverBody {
                topic: topic_key(),
                group_id: 0,
                partition_id: 3,
                member_id: None,
            }),
            None,
        ),
        (
            "user_created_with_permissions",
            WireMetadataChange::UserCreated(WireUserBody {
                key: user_key(),
                username: name("telemetry-writer")?,
                status: 1,
                permissions: Some(permissions_to_wire(&permissions())),
                options: empty_options(),
            }),
            Some(0),
        ),
        (
            "user_updated_inactive",
            WireMetadataChange::UserUpdated(WireUserBody {
                key: user_key(),
                username: name("telemetry-writer")?,
                status: 2,
                permissions: None,
                options: empty_options(),
            }),
            Some(0),
        ),
        (
            "user_deleted",
            WireMetadataChange::UserDeleted(WireUserIdentityBody {
                key: user_key(),
                username: name("telemetry-writer")?,
            }),
            Some(0),
        ),
        (
            "user_password_changed",
            WireMetadataChange::UserPasswordChanged(WireUserIdentityBody {
                key: user_key(),
                username: name("telemetry-writer")?,
            }),
            Some(7),
        ),
        (
            "user_permissions_pruned",
            WireMetadataChange::UserPermissionsUpdated(WireUserPermissionsBody {
                key: user_key(),
                username: name("telemetry-writer")?,
                permissions: Some(permissions_to_wire(&Permissions::default())),
                reason: WirePermissionsUpdatedReason::ResourceDeleted,
            }),
            Some(4),
        ),
        (
            "personal_access_token_created",
            WireMetadataChange::PersonalAccessTokenCreated(WireTokenBody {
                user: user_key(),
                name: token_name()?,
                expiry: 1_760_400_000_000_000,
            }),
            Some(7),
        ),
        (
            "personal_access_token_deleted_expired",
            WireMetadataChange::PersonalAccessTokenDeleted(WireTokenDeletedBody {
                user: user_key(),
                name: token_name()?,
                reason: WireTokenDeletedReason::Expired,
            }),
            None,
        ),
        (
            "session_registered",
            WireMetadataChange::SessionRegistered(WireSessionBody {
                user: user_key(),
                client_id: 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10,
                session: 4500,
            }),
            Some(7),
        ),
        (
            "session_closed_expired",
            WireMetadataChange::SessionClosed(WireSessionClosedBody {
                user: user_key(),
                client_id: 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10,
                session: 4500,
                reason: WireSessionClosedReason::Expired,
            }),
            None,
        ),
        (
            "session_closed_revoked",
            WireMetadataChange::SessionClosed(WireSessionClosedBody {
                user: user_key(),
                client_id: 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10,
                session: 4500,
                reason: WireSessionClosedReason::Revoked,
            }),
            Some(7),
        ),
    ])
}

fn record_vectors() -> FixtureResult<Vec<RecordVector>> {
    change_cases()?
        .into_iter()
        .enumerate()
        .map(|(index, (case, change, actor))| {
            let op = 4121 + u64::try_from(index)?;
            let record = wire_record(&change, op, 0, actor);
            Ok(RecordVector {
                case: case.to_owned(),
                group: record.group,
                kind: record.kind,
                body_hex: hex::encode(&record.body),
                record_hex: record_hex(&record),
                expected: MetadataChangeRecord::try_from(record)?,
            })
        })
        .collect()
}

fn item_vectors() -> FixtureResult<Vec<ItemVector>> {
    let items = vec![
        (
            "stream",
            WireMetadataCurrentItem::Stream(WireStreamBody {
                key: stream_key(),
                created_at: 1_760_356_800_123_456,
                name: name("telemetry")?,
                options: stream_options()?,
            }),
        ),
        (
            "topic",
            WireMetadataCurrentItem::Topic(WireTopicItemBody {
                key: topic_key(),
                created_at: 1_760_356_801_000_000,
                name: name("node-metrics")?,
                options: topic_options()?,
            }),
        ),
        (
            "partitions",
            WireMetadataCurrentItem::Partitions(WirePartitionsItemBody {
                topic: topic_key(),
                partitions_count: 8,
            }),
        ),
        (
            "consumer_group",
            WireMetadataCurrentItem::ConsumerGroup(WireConsumerGroupItemBody {
                topic: topic_key(),
                group_id: 0,
                name: name("readers")?,
            }),
        ),
        (
            "consumer_group_member",
            WireMetadataCurrentItem::ConsumerGroupMember(WireMemberItemBody {
                topic: topic_key(),
                group_id: 0,
                member_id: 1,
                client_id: 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10,
                partition_ids: vec![0, 2, 4],
            }),
        ),
        (
            "user",
            WireMetadataCurrentItem::User(WireUserBody {
                key: user_key(),
                username: name("telemetry-writer")?,
                status: 1,
                permissions: Some(permissions_to_wire(&permissions())),
                options: empty_options(),
            }),
        ),
        (
            "personal_access_token_without_expiry",
            WireMetadataCurrentItem::PersonalAccessToken(WireTokenBody {
                user: user_key(),
                name: name("ci-token")?,
                expiry: 0,
            }),
        ),
        (
            "session",
            WireMetadataCurrentItem::Session(WireSessionBody {
                user: user_key(),
                client_id: 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10,
                session: 4500,
            }),
        ),
    ];
    items
        .into_iter()
        .map(|(case, item)| {
            let envelope = WireMetadataItem {
                group: item.group().as_u8(),
                body: item.body_bytes(),
            };
            Ok(ItemVector {
                case: case.to_owned(),
                group: envelope.group,
                body_hex: hex::encode(&envelope.body),
                item_hex: hex::encode(envelope.to_bytes()),
                expected: MetadataCurrentItem::try_from(envelope)?,
            })
        })
        .collect()
}

fn cursor_vector(case: &str, cursor: MetadataChangeCursor) -> CursorVector {
    CursorVector {
        case: case.to_owned(),
        text: cursor.to_string(),
        hex: hex::encode(WireMetadataChangeCursor::from(cursor).to_bytes()),
        log_id: format!("{:032x}", cursor.log_id),
        op: cursor.op,
        checksum: format!("{:032x}", cursor.checksum),
        ordinal: cursor.ordinal,
    }
}

fn cursor_vectors() -> Vec<CursorVector> {
    vec![
        cursor_vector("zero", MetadataChangeCursor::zero(LOG_ID)),
        cursor_vector(
            "first_record_of_op",
            MetadataChangeCursor {
                log_id: LOG_ID,
                op: 4120,
                checksum: 0x4c2d_9e8b_03f7_c1a5,
                ordinal: 0,
            },
        ),
        cursor_vector(
            "inside_op",
            MetadataChangeCursor {
                log_id: LOG_ID,
                op: 4187,
                checksum: 0xf2a4_c6e8_b0d1_f359,
                ordinal: 1,
            },
        ),
        cursor_vector(
            "every_field_at_maximum",
            MetadataChangeCursor {
                log_id: u128::MAX,
                op: u64::MAX,
                checksum: u128::MAX,
                ordinal: u32::MAX,
            },
        ),
    ]
}

fn page_token(
    group: WireMetadataChangeGroup,
    groups_mask: u8,
    last_key: Vec<u8>,
) -> MetadataCurrentPageToken {
    MetadataCurrentPageToken::from(WireMetadataCurrentPageToken {
        anchor: MetadataChangeCursor {
            log_id: LOG_ID,
            op: 4187,
            checksum: 0xf2a4_c6e8_b0d1_f359,
            ordinal: 1,
        }
        .into(),
        required_head: 4190,
        groups_mask,
        group,
        last_key: Bytes::from(last_key),
    })
}

fn token_name_key(user_id: u32, token_name: &str) -> Vec<u8> {
    let mut key = user_id.to_le_bytes().to_vec();
    key.extend(
        WireName::new(token_name)
            .map(|name| name.to_bytes().to_vec())
            .unwrap_or_default(),
    );
    key
}

fn page_token_vectors() -> Vec<PageTokenVector> {
    let all = u8::MAX;
    let ids = |values: &[u32]| {
        values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<u8>>()
    };
    let cases = vec![
        ("streams", WireMetadataChangeGroup::Streams, 0b1, ids(&[3])),
        (
            "topics",
            WireMetadataChangeGroup::Topics,
            0b11,
            ids(&[3, 0]),
        ),
        (
            "partitions",
            WireMetadataChangeGroup::Partitions,
            all,
            ids(&[3, 0]),
        ),
        (
            "consumer_groups",
            WireMetadataChangeGroup::ConsumerGroups,
            all,
            ids(&[3, 0, 2]),
        ),
        (
            "consumer_group_members",
            WireMetadataChangeGroup::ConsumerGroupMembers,
            all,
            ids(&[3, 0, 2, 1]),
        ),
        (
            "users",
            WireMetadataChangeGroup::Users,
            0b10_0000,
            ids(&[7]),
        ),
        (
            "personal_access_tokens",
            WireMetadataChangeGroup::PersonalAccessTokens,
            0b110_0000,
            token_name_key(7, "ci-token"),
        ),
        (
            "sessions",
            WireMetadataChangeGroup::Sessions,
            0b1010_0000,
            4500u64.to_le_bytes().to_vec(),
        ),
        (
            "longest_token_name",
            WireMetadataChangeGroup::PersonalAccessTokens,
            all,
            token_name_key(u32::MAX, &"n".repeat(255)),
        ),
    ];
    cases
        .into_iter()
        .map(|(case, group, mask, key)| {
            let token = page_token(group, mask, key);
            let wire: &WireMetadataCurrentPageToken = token.as_ref();
            PageTokenVector {
                case: case.to_owned(),
                text: token.to_string(),
                hex: hex::encode(wire.to_bytes()),
                anchor: token.anchor().to_string(),
                required_head: token.required_head(),
                groups_mask: token.groups().mask(),
                group: token.last_group().to_string(),
                last_key_hex: hex::encode(token.last_key()),
            }
        })
        .collect()
}

fn request_vectors() -> FixtureResult<Vec<RequestVector>> {
    let cursor = MetadataChangeCursor {
        log_id: LOG_ID,
        op: 4187,
        checksum: 0xf2a4_c6e8_b0d1_f359,
        ordinal: 1,
    }
    .to_string();
    let topics_token = page_token(
        WireMetadataChangeGroup::Topics,
        0b11,
        [3u32, 0]
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect(),
    )
    .to_string();
    let inputs = vec![
        ("earliest_every_group", QueryInput::new("earliest")),
        (
            "earliest_many_duplicate_groups",
            QueryInput {
                groups: std::iter::repeat_n("streams".to_owned(), 255)
                    .chain(std::iter::once("users".to_owned()))
                    .collect(),
                ..QueryInput::new("earliest")
            },
        ),
        (
            "earliest_since_users_and_tokens",
            QueryInput {
                groups: vec!["users".to_owned(), "personal_access_tokens".to_owned()],
                since: Some(1_760_353_200_000_000),
                ..QueryInput::new("earliest")
            },
        ),
        (
            "latest_streams",
            QueryInput {
                groups: vec!["streams".to_owned()],
                limit: 1,
                ..QueryInput::new("latest")
            },
        ),
        (
            "after_cursor_with_time_range",
            QueryInput {
                cursor: Some(cursor),
                groups: vec!["topics".to_owned(), "streams".to_owned()],
                since: Some(1_760_356_800_000_000),
                until: Some(1_760_360_400_000_000),
                limit: 500,
                ..QueryInput::new("after")
            },
        ),
        (
            "current_first_page",
            QueryInput {
                groups: vec!["streams".to_owned(), "topics".to_owned()],
                limit: 1000,
                ..QueryInput::new("current")
            },
        ),
        (
            "current_continuation",
            QueryInput {
                page_token: Some(topics_token),
                groups: vec!["streams".to_owned(), "topics".to_owned()],
                limit: 1000,
                ..QueryInput::new("current")
            },
        ),
    ];
    inputs
        .into_iter()
        .map(|(case, input)| {
            let query = input.query()?;
            query.validate()?;
            let request = GetMetadataChangesRequest::from(&query);
            let http_query = query
                .http_query_pairs()
                .into_iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join("&");
            Ok(RequestVector {
                case: case.to_owned(),
                input,
                request_hex: hex::encode(request.to_bytes()),
                http_query,
            })
        })
        .collect()
}

fn response_vector(
    case: &str,
    response: GetMetadataChangesResponse,
) -> FixtureResult<ResponseVector> {
    let response_hex = hex::encode(response.to_bytes());
    Ok(ResponseVector {
        case: case.to_owned(),
        response_hex,
        expected: MetadataChanges::try_from(response)?,
    })
}

fn wire_cursor(op: u64, ordinal: u32) -> WireMetadataChangeCursor {
    WireMetadataChangeCursor {
        log_id: LOG_ID,
        op,
        checksum: 0x0000_0000_0000_0000_a5c1_f703_8b9e_2d4c + u128::from(op),
        ordinal,
    }
}

fn response_vectors(
    records: &[RecordVector],
    items: &[ItemVector],
) -> FixtureResult<Vec<ResponseVector>> {
    let decode_record = |vector: &RecordVector| -> FixtureResult<WireMetadataChangeRecord> {
        let bytes = hex::decode(&vector.record_hex)?;
        Ok(iggy_binary_protocol::WireDecode::decode_from(&bytes)?)
    };
    let decode_item = |vector: &ItemVector| -> FixtureResult<WireMetadataItem> {
        let bytes = hex::decode(&vector.item_hex)?;
        Ok(iggy_binary_protocol::WireDecode::decode_from(&bytes)?)
    };
    let page_records = records
        .iter()
        .take(3)
        .map(decode_record)
        .collect::<FixtureResult<Vec<_>>>()?;
    let last = page_records.last().ok_or("no records")?;
    let changes = GetMetadataChangesResponse {
        status: WireMetadataChangeStatus::Ok,
        phase: WireMetadataChangePhase::Changes,
        log_id: Some(LOG_ID),
        oldest_op: 3871,
        retained_since: 1_760_352_000_000_000,
        head_op: 4190,
        next: Some(wire_cursor(last.op, last.ordinal)),
        has_more: true,
        records: page_records,
        items: vec![],
        current_page_token: None,
    };
    let mut unknown_record = decode_record(records.first().ok_or("no records")?)?;
    unknown_record.kind = 9;
    unknown_record.body = Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]);
    let mut future_group = unknown_record.clone();
    future_group.group = 200;
    future_group.kind = 1;
    future_group.ordinal = 1;
    let unknown = GetMetadataChangesResponse {
        next: Some(wire_cursor(future_group.op, future_group.ordinal)),
        has_more: false,
        records: vec![unknown_record, future_group],
        ..changes.clone()
    };
    let current_items = items
        .iter()
        .take(2)
        .map(decode_item)
        .collect::<FixtureResult<Vec<_>>>()?;
    let current_token = page_token(
        WireMetadataChangeGroup::Topics,
        0b11,
        [3u32, 0]
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect(),
    );
    let current = GetMetadataChangesResponse {
        status: WireMetadataChangeStatus::Ok,
        phase: WireMetadataChangePhase::Current,
        log_id: Some(LOG_ID),
        oldest_op: 1,
        retained_since: 0,
        head_op: 4190,
        next: Some(wire_cursor(4187, 1)),
        has_more: true,
        records: vec![],
        items: current_items,
        current_page_token: Some(current_token.into()),
    };
    let behind = GetMetadataChangesResponse {
        status: WireMetadataChangeStatus::Behind,
        phase: WireMetadataChangePhase::Changes,
        log_id: None,
        oldest_op: 0,
        retained_since: 0,
        head_op: 0,
        next: None,
        has_more: false,
        records: vec![],
        items: vec![],
        current_page_token: None,
    };
    let expired = GetMetadataChangesResponse {
        status: WireMetadataChangeStatus::Expired,
        log_id: Some(LOG_ID),
        oldest_op: 5000,
        retained_since: 1_760_360_000_000_000,
        head_op: 9000,
        ..behind.clone()
    };
    let reset = GetMetadataChangesResponse {
        status: WireMetadataChangeStatus::Reset,
        ..expired.clone()
    };
    Ok(vec![
        response_vector("changes_page", changes)?,
        response_vector("unknown_kind_and_group", unknown)?,
        response_vector("current_page", current)?,
        response_vector("behind_before_op_1", behind)?,
        response_vector("expired", expired)?,
        response_vector("reset", reset)?,
    ])
}

fn large_values_vector() -> FixtureResult<LargeValuesVector> {
    let client_id = u128::MAX - 1;
    let change = WireMetadataChange::SessionRegistered(WireSessionBody {
        user: WireUserKey {
            user_id: u32::MAX,
            user_created_at: ABOVE_2_POW_53,
        },
        client_id,
        session: ABOVE_2_POW_53 + 1,
    });
    let mut record = wire_record(&change, ABOVE_2_POW_53 + 2, u32::MAX, Some(u32::MAX));
    record.checksum = u128::MAX;
    record.timestamp = u64::MAX;
    Ok(LargeValuesVector {
        record_hex: record_hex(&record),
        op: record.op.to_string(),
        ordinal: record.ordinal,
        timestamp: record.timestamp.to_string(),
        checksum: format!("{:032x}", record.checksum),
        user_created_at: ABOVE_2_POW_53.to_string(),
        session: (ABOVE_2_POW_53 + 1).to_string(),
        client_id: format!("{client_id:032x}"),
    })
}

fn refused_vectors() -> FixtureResult<RefusedVectors> {
    let valid_cursor = MetadataChangeCursor {
        log_id: LOG_ID,
        op: 4187,
        checksum: 0xf2a4_c6e8_b0d1_f359,
        ordinal: 1,
    }
    .to_string();
    let encoded = &valid_cursor[4..];
    let non_canonical_cursor = WireMetadataChangeCursor {
        log_id: LOG_ID,
        op: 0,
        checksum: 1,
        ordinal: 0,
    };
    let cursors = vec![
        ("wrong_prefix", format!("mc2_{encoded}")),
        ("missing_prefix", encoded.to_owned()),
        ("too_long", format!("{valid_cursor}A")),
        ("too_short", valid_cursor[..62].to_owned()),
        ("padded", format!("mc1_{}=", &encoded[..58])),
        (
            "standard_alphabet_character",
            format!("mc1_+{}", &encoded[1..]),
        ),
        (
            "op_zero_with_checksum",
            format!(
                "mc1_{}",
                base64_url(&WireMetadataChangeCursor::to_bytes(&non_canonical_cursor))
            ),
        ),
    ];
    let valid_token = page_token(
        WireMetadataChangeGroup::Streams,
        0b1,
        3u32.to_le_bytes().to_vec(),
    );
    let token_text = valid_token.to_string();
    let mut wrong_width = WireMetadataCurrentPageToken::from(valid_token.clone());
    wrong_width.last_key = Bytes::from_static(&[3, 0, 0, 0, 0, 0, 0, 0]);
    let mut outside_mask = WireMetadataCurrentPageToken::from(valid_token.clone());
    outside_mask.groups_mask = 0b10;
    let mut head_below_anchor = WireMetadataCurrentPageToken::from(valid_token);
    head_below_anchor.required_head = 1;
    let page_tokens = vec![
        ("wrong_prefix", token_text.replacen("cs1_", "mc1_", 1)),
        (
            "trailing_byte",
            format!(
                "cs1_{}",
                base64_url(&[unvalidated_token_bytes(&token_text)?, vec![0]].concat())
            ),
        ),
        (
            "wrong_key_width",
            format!("cs1_{}", base64_url(&wrong_width.to_bytes())),
        ),
        (
            "group_outside_mask",
            format!("cs1_{}", base64_url(&outside_mask.to_bytes())),
        ),
        (
            "head_below_anchor",
            format!("cs1_{}", base64_url(&head_below_anchor.to_bytes())),
        ),
        ("empty", "cs1_".to_owned()),
    ];

    let records = change_cases()?;
    let body = |case: &str| -> FixtureResult<(WireMetadataChange, Vec<u8>)> {
        let (_, change, _) = records
            .iter()
            .find(|(name, _, _)| *name == case)
            .ok_or("missing case")?;
        Ok((change.clone(), change.body_bytes().to_vec()))
    };
    let (member_left, mut bad_reason) = body("consumer_group_member_left_session_ended")?;
    if let Some(last) = bad_reason.last_mut() {
        *last = 2;
    }
    let (stream_deleted, mut bad_presence) = body("stream_deleted_deferred")?;
    let presence_offset = bad_presence.len() - 9;
    bad_presence[presence_offset] = 2;
    let (partitions, mut unsorted_ids) = body("partitions_changed_deleted_tail")?;
    let first_id = 24 + 4 + 4;
    unsorted_ids[first_id..first_id + 4].copy_from_slice(&9u32.to_le_bytes());
    let (stream_created, stream_body) = body("stream_created")?;
    let (session, session_body) = body("session_registered")?;
    let record_bodies = vec![
        RefusedBody {
            case: "unknown_member_left_reason".to_owned(),
            group: member_left.group().as_u8(),
            kind: member_left.kind().as_u8(),
            body_hex: hex::encode(&bad_reason),
        },
        RefusedBody {
            case: "presence_byte_two".to_owned(),
            group: stream_deleted.group().as_u8(),
            kind: stream_deleted.kind().as_u8(),
            body_hex: hex::encode(&bad_presence),
        },
        RefusedBody {
            case: "unsorted_removed_partition_ids".to_owned(),
            group: partitions.group().as_u8(),
            kind: partitions.kind().as_u8(),
            body_hex: hex::encode(&unsorted_ids),
        },
        RefusedBody {
            case: "known_kind_under_wrong_group".to_owned(),
            group: WireMetadataChangeGroup::Topics.as_u8(),
            kind: stream_created.kind().as_u8(),
            body_hex: hex::encode(&stream_body),
        },
        RefusedBody {
            case: "trailing_byte".to_owned(),
            group: session.group().as_u8(),
            kind: session.kind().as_u8(),
            body_hex: hex::encode([session_body.as_slice(), &[0]].concat()),
        },
        RefusedBody {
            case: "truncated".to_owned(),
            group: session.group().as_u8(),
            kind: session.kind().as_u8(),
            body_hex: hex::encode(&session_body[..session_body.len() - 1]),
        },
    ];

    let earliest = GetMetadataChangesRequest::from(&MetadataChangesQuery::default())
        .to_bytes()
        .to_vec();
    let mut cursor_without_after = earliest.clone();
    cursor_without_after[5] = 1;
    let mut trailing = earliest.clone();
    trailing.push(0);
    let mut unknown_start = earliest;
    unknown_start[0] = 4;
    let mut non_canonical_after = vec![WireMetadataChangeStartMode::After.as_u8()];
    non_canonical_after.extend(WireMetadataChangeCursor::to_bytes(&non_canonical_cursor));
    non_canonical_after.extend([0, 0]);
    non_canonical_after.extend(DEFAULT_METADATA_CHANGES_LIMIT.to_le_bytes());
    non_canonical_after.extend([0, 0, 0]);
    let requests = vec![
        RefusedHex {
            case: "cursor_bytes_without_after".to_owned(),
            hex: hex::encode(&cursor_without_after),
        },
        RefusedHex {
            case: "trailing_byte".to_owned(),
            hex: hex::encode(&trailing),
        },
        RefusedHex {
            case: "unknown_start_mode".to_owned(),
            hex: hex::encode(&unknown_start),
        },
        RefusedHex {
            case: "non_canonical_cursor".to_owned(),
            hex: hex::encode(&non_canonical_after),
        },
    ];

    let items_in_changes_phase = GetMetadataChangesResponse {
        status: WireMetadataChangeStatus::Ok,
        phase: WireMetadataChangePhase::Changes,
        log_id: Some(LOG_ID),
        oldest_op: 1,
        retained_since: 0,
        head_op: 2,
        next: Some(wire_cursor(2, 0)),
        has_more: false,
        records: vec![],
        items: vec![WireMetadataItem {
            group: 1,
            body: item_body_for_refusal()?,
        }],
        current_page_token: None,
    }
    .to_bytes()
    .to_vec();
    let mut unknown_status = items_in_changes_phase.clone();
    unknown_status[0] = 4;
    let responses = vec![
        RefusedHex {
            case: "items_in_changes_phase".to_owned(),
            hex: hex::encode(&items_in_changes_phase),
        },
        RefusedHex {
            case: "unknown_status".to_owned(),
            hex: hex::encode(&unknown_status),
        },
    ];

    let queries = vec![
        (
            "limit_zero",
            QueryInput {
                limit: 0,
                ..QueryInput::new("earliest")
            },
        ),
        (
            "limit_above_maximum",
            QueryInput {
                limit: MAX_METADATA_CHANGES_LIMIT + 1,
                ..QueryInput::new("earliest")
            },
        ),
        (
            "since_above_until",
            QueryInput {
                since: Some(2),
                until: Some(1),
                ..QueryInput::new("earliest")
            },
        ),
        (
            "time_bound_with_latest",
            QueryInput {
                since: Some(1),
                ..QueryInput::new("latest")
            },
        ),
        (
            "time_bound_with_current",
            QueryInput {
                until: Some(1),
                ..QueryInput::new("current")
            },
        ),
        (
            "current_missing_parent_group",
            QueryInput {
                groups: vec!["topics".to_owned()],
                ..QueryInput::new("current")
            },
        ),
        (
            "page_token_for_other_groups",
            QueryInput {
                page_token: Some(
                    page_token(
                        WireMetadataChangeGroup::Streams,
                        0b1,
                        3u32.to_le_bytes().to_vec(),
                    )
                    .to_string(),
                ),
                groups: vec!["streams".to_owned(), "topics".to_owned()],
                ..QueryInput::new("current")
            },
        ),
    ];
    let http_queries = vec![
        ("no_start", "groups=streams"),
        ("from_and_after", "from=earliest&after=mc1_x"),
        ("unknown_from", "from=newest"),
        ("duplicate_key", "from=earliest&from=latest"),
        ("unknown_key", "from=earliest&verbose=1"),
        (
            "empty_group_element",
            "from=earliest&groups=streams,,topics",
        ),
        ("trailing_group_comma", "from=earliest&groups=streams,"),
        ("unknown_group", "from=earliest&groups=everything"),
        ("negative_since", "from=earliest&since=-1"),
        ("leading_zero_until", "from=earliest&until=01"),
        ("non_numeric_limit", "from=earliest&limit=ten"),
        (
            "token_without_current",
            "from=latest&current_after=cs1_AAAA",
        ),
    ];
    Ok(RefusedVectors {
        cursors: cursors
            .into_iter()
            .map(|(case, text)| RefusedText {
                case: case.to_owned(),
                text,
            })
            .collect(),
        page_tokens: page_tokens
            .into_iter()
            .map(|(case, text)| RefusedText {
                case: case.to_owned(),
                text,
            })
            .collect(),
        record_bodies,
        requests,
        responses,
        queries: queries
            .into_iter()
            .map(|(case, input)| RefusedQuery {
                case: case.to_owned(),
                input,
            })
            .collect(),
        http_queries: http_queries
            .into_iter()
            .map(|(case, query)| RefusedHttpQuery {
                case: case.to_owned(),
                query: query.to_owned(),
            })
            .collect(),
    })
}

fn item_body_for_refusal() -> FixtureResult<Bytes> {
    Ok(WireMetadataCurrentItem::Stream(WireStreamBody {
        key: stream_key(),
        created_at: 1,
        name: name("orders")?,
        options: empty_options(),
    })
    .body_bytes())
}

fn unvalidated_token_bytes(text: &str) -> FixtureResult<Vec<u8>> {
    let encoded = text.strip_prefix("cs1_").ok_or("missing prefix")?;
    Ok(URL_SAFE_NO_PAD.decode(encoded)?)
}

fn base64_url(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}
