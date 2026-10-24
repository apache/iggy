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

use std::str::FromStr;

use ext_php_rs::{
    binary::Binary,
    boxed::ZBox,
    convert::{IntoZval, StubLiteral},
    enum_::RegisteredEnum,
    error::Result as ConversionResult,
    exception::PhpResult,
    flags::DataType,
    php_class, php_enum, php_impl,
    types::{ZendHashTable, Zval},
};
use iggy::prelude::{
    IggyError, MetadataChange as RustMetadataChange,
    MetadataChangeCursor as RustMetadataChangeCursor,
    MetadataChangeGroup as RustMetadataChangeGroup, MetadataChangeKind as RustMetadataChangeKind,
    MetadataChangePhase as RustMetadataChangePhase,
    MetadataChangeRecord as RustMetadataChangeRecord,
    MetadataChangeStart as RustMetadataChangeStart,
    MetadataChangeStatus as RustMetadataChangeStatus, MetadataChanges as RustMetadataChanges,
    MetadataConsumerGroupCreated as RustMetadataConsumerGroupCreated,
    MetadataConsumerGroupDeleted as RustMetadataConsumerGroupDeleted,
    MetadataConsumerGroupItem as RustMetadataConsumerGroupItem,
    MetadataCurrentItem as RustMetadataCurrentItem,
    MetadataCurrentPageToken as RustMetadataCurrentPageToken,
    MetadataMemberItem as RustMetadataMemberItem, MetadataMemberJoined as RustMetadataMemberJoined,
    MetadataMemberLeft as RustMetadataMemberLeft,
    MetadataMemberLeftReason as RustMetadataMemberLeftReason, MetadataOptions,
    MetadataPartitionHandedOver as RustMetadataPartitionHandedOver,
    MetadataPartitionsChanged as RustMetadataPartitionsChanged,
    MetadataPartitionsItem as RustMetadataPartitionsItem,
    MetadataPermissionsUpdatedReason as RustMetadataPermissionsUpdatedReason,
    MetadataSession as RustMetadataSession, MetadataSessionClosed as RustMetadataSessionClosed,
    MetadataSessionClosedReason as RustMetadataSessionClosedReason,
    MetadataStreamChange as RustMetadataStreamChange,
    MetadataStreamDeleted as RustMetadataStreamDeleted,
    MetadataTokenChange as RustMetadataTokenChange,
    MetadataTokenDeleted as RustMetadataTokenDeleted,
    MetadataTokenDeletedReason as RustMetadataTokenDeletedReason,
    MetadataTopicChange as RustMetadataTopicChange,
    MetadataTopicDeleted as RustMetadataTopicDeleted, MetadataTopicItem as RustMetadataTopicItem,
    MetadataUserChange as RustMetadataUserChange, MetadataUserIdentity as RustMetadataUserIdentity,
    MetadataUserPermissionsUpdated as RustMetadataUserPermissionsUpdated, Permissions,
    StreamPermissions,
};
use iggy_binary_protocol::WireDecode;
use iggy_binary_protocol::responses::system::GetMetadataChangesResponse;

use crate::client::unsigned_argument;
use crate::error::to_php_exception;
use crate::options::options_array;

/// One page of the metadata change log, or of the current state, as
/// `Client::getMetadataChanges()` returns it.
///
/// Unsigned 64-bit values are ints, or decimal strings above `PHP_INT_MAX`. 128-bit values
/// (log ids, checksums, client ids) are decimal strings.
#[php_class]
#[php(name = "Iggy\\MetadataChanges")]
pub struct MetadataChanges {
    inner: RustMetadataChanges,
}

/// One change record: the committed operation that produced it, its position within that
/// operation and the typed body of what is true after it.
#[php_class]
#[php(name = "Iggy\\MetadataChangeRecord")]
pub struct MetadataChangeRecord {
    inner: RustMetadataChangeRecord,
}

/// One current state item of a `MetadataChangeStart::current()` page.
#[php_class]
#[php(name = "Iggy\\MetadataCurrentItem")]
pub struct MetadataCurrentItem {
    inner: RustMetadataCurrentItem,
}

/// Position of the last change record a reader consumed or scanned. Send it back in
/// `MetadataChangeStart::after()` to read the records after it.
///
/// Casting it to string gives its `mc1_` text, which `parse()` reads back.
#[php_class]
#[php(name = "Iggy\\MetadataChangeCursor")]
#[derive(Clone, Copy)]
pub struct MetadataChangeCursor {
    inner: RustMetadataChangeCursor,
}

/// Opaque continuation of a current state scan. Send it back unchanged in
/// `MetadataChangeStart::current()` with the same groups.
///
/// Casting it to string gives its `cs1_` text, which `parse()` reads back.
#[php_class]
#[php(name = "Iggy\\MetadataCurrentPageToken")]
#[derive(Clone)]
pub struct MetadataCurrentPageToken {
    inner: RustMetadataCurrentPageToken,
}

/// Where `Client::getMetadataChanges()` starts reading.
#[php_class]
#[php(name = "Iggy\\MetadataChangeStart")]
#[derive(Clone)]
pub struct MetadataChangeStart {
    pub(crate) inner: RustMetadataChangeStart,
}

/// Family of change records and the unit of filtering. Values are the HTTP API names.
#[php_enum]
#[php(name = "Iggy\\MetadataChangeGroup")]
#[derive(Clone, Copy)]
pub enum MetadataChangeGroup {
    #[php(value = "streams")]
    Streams,
    #[php(value = "topics")]
    Topics,
    #[php(value = "partitions")]
    Partitions,
    #[php(value = "consumer_groups")]
    ConsumerGroups,
    #[php(value = "consumer_group_members")]
    ConsumerGroupMembers,
    #[php(value = "users")]
    Users,
    #[php(value = "personal_access_tokens")]
    PersonalAccessTokens,
    #[php(value = "sessions")]
    Sessions,
}

/// Kind of a change record. Values are the HTTP API names.
#[php_enum]
#[php(name = "Iggy\\MetadataChangeKind")]
#[derive(Clone, Copy)]
pub enum MetadataChangeKind {
    #[php(value = "stream_created")]
    StreamCreated,
    #[php(value = "stream_updated")]
    StreamUpdated,
    #[php(value = "stream_deleted")]
    StreamDeleted,
    #[php(value = "topic_created")]
    TopicCreated,
    #[php(value = "topic_updated")]
    TopicUpdated,
    #[php(value = "topic_deleted")]
    TopicDeleted,
    #[php(value = "partitions_changed")]
    PartitionsChanged,
    #[php(value = "consumer_group_created")]
    ConsumerGroupCreated,
    #[php(value = "consumer_group_deleted")]
    ConsumerGroupDeleted,
    #[php(value = "consumer_group_member_joined")]
    ConsumerGroupMemberJoined,
    #[php(value = "consumer_group_member_left")]
    ConsumerGroupMemberLeft,
    #[php(value = "consumer_group_partition_handed_over")]
    ConsumerGroupPartitionHandedOver,
    #[php(value = "user_created")]
    UserCreated,
    #[php(value = "user_updated")]
    UserUpdated,
    #[php(value = "user_deleted")]
    UserDeleted,
    #[php(value = "user_password_changed")]
    UserPasswordChanged,
    #[php(value = "user_permissions_updated")]
    UserPermissionsUpdated,
    #[php(value = "personal_access_token_created")]
    PersonalAccessTokenCreated,
    #[php(value = "personal_access_token_deleted")]
    PersonalAccessTokenDeleted,
    #[php(value = "session_registered")]
    SessionRegistered,
    #[php(value = "session_closed")]
    SessionClosed,
}

/// Outcome of a change log query.
///
/// `Ok`: apply the records and keep `next`. `Behind`: the node has not applied the position
/// yet, ask again later or ask another node. `Expired` and `Reset`: rebuild from current state.
#[php_enum]
#[php(name = "Iggy\\MetadataChangeStatus")]
#[derive(Clone, Copy)]
pub enum MetadataChangeStatus {
    #[php(value = "ok")]
    Ok,
    #[php(value = "behind")]
    Behind,
    #[php(value = "expired")]
    Expired,
    #[php(value = "reset")]
    Reset,
}

/// Whether a page carries change records or current state items.
#[php_enum]
#[php(name = "Iggy\\MetadataChangePhase")]
#[derive(Clone, Copy)]
pub enum MetadataChangePhase {
    #[php(value = "changes")]
    Changes,
    #[php(value = "current")]
    Current,
}

/// Why a consumer group member was removed.
#[php_enum]
#[php(name = "Iggy\\MetadataMemberLeftReason")]
#[derive(Clone, Copy)]
pub enum MetadataMemberLeftReason {
    #[php(value = "left")]
    Left,
    #[php(value = "session_ended")]
    SessionEnded,
}

/// Why a personal access token was removed.
#[php_enum]
#[php(name = "Iggy\\MetadataTokenDeletedReason")]
#[derive(Clone, Copy)]
pub enum MetadataTokenDeletedReason {
    #[php(value = "deleted")]
    Deleted,
    #[php(value = "expired")]
    Expired,
}

/// Why a session was closed.
#[php_enum]
#[php(name = "Iggy\\MetadataSessionClosedReason")]
#[derive(Clone, Copy)]
pub enum MetadataSessionClosedReason {
    #[php(value = "logout")]
    Logout,
    #[php(value = "expired")]
    Expired,
    /// The user was deactivated or deleted.
    #[php(value = "revoked")]
    Revoked,
}

/// Why a user's public permission set changed.
#[php_enum]
#[php(name = "Iggy\\MetadataPermissionsUpdatedReason")]
#[derive(Clone, Copy)]
pub enum MetadataPermissionsUpdatedReason {
    #[php(value = "updated")]
    Updated,
    #[php(value = "resource_deleted")]
    ResourceDeleted,
}

/// A stream after a create or update, and a `streams` current state item.
#[php_class]
#[php(name = "Iggy\\MetadataStreamChange")]
pub struct MetadataStreamChange {
    inner: RustMetadataStreamChange,
}

/// A removed stream.
#[php_class]
#[php(name = "Iggy\\MetadataStreamDeleted")]
pub struct MetadataStreamDeleted {
    inner: RustMetadataStreamDeleted,
}

/// A topic after a create or update.
#[php_class]
#[php(name = "Iggy\\MetadataTopicChange")]
pub struct MetadataTopicChange {
    inner: RustMetadataTopicChange,
}

/// A `topics` current state item. Partition counts come as `partitions` items.
#[php_class]
#[php(name = "Iggy\\MetadataTopicItem")]
pub struct MetadataTopicItem {
    inner: RustMetadataTopicItem,
}

/// A removed topic.
#[php_class]
#[php(name = "Iggy\\MetadataTopicDeleted")]
pub struct MetadataTopicDeleted {
    inner: RustMetadataTopicDeleted,
}

/// A partition count change.
#[php_class]
#[php(name = "Iggy\\MetadataPartitionsChanged")]
pub struct MetadataPartitionsChanged {
    inner: RustMetadataPartitionsChanged,
}

/// A `partitions` current state item.
#[php_class]
#[php(name = "Iggy\\MetadataPartitionsItem")]
pub struct MetadataPartitionsItem {
    inner: RustMetadataPartitionsItem,
}

/// A created consumer group with the topic's resulting group count.
#[php_class]
#[php(name = "Iggy\\MetadataConsumerGroupCreated")]
pub struct MetadataConsumerGroupCreated {
    inner: RustMetadataConsumerGroupCreated,
}

/// A removed consumer group with the topic's resulting group count.
#[php_class]
#[php(name = "Iggy\\MetadataConsumerGroupDeleted")]
pub struct MetadataConsumerGroupDeleted {
    inner: RustMetadataConsumerGroupDeleted,
}

/// A `consumer_groups` current state item.
#[php_class]
#[php(name = "Iggy\\MetadataConsumerGroupItem")]
pub struct MetadataConsumerGroupItem {
    inner: RustMetadataConsumerGroupItem,
}

/// A member that joined a consumer group.
#[php_class]
#[php(name = "Iggy\\MetadataMemberJoined")]
pub struct MetadataMemberJoined {
    inner: RustMetadataMemberJoined,
}

/// A member that left a consumer group.
#[php_class]
#[php(name = "Iggy\\MetadataMemberLeft")]
pub struct MetadataMemberLeft {
    inner: RustMetadataMemberLeft,
}

/// A completed partition handover.
#[php_class]
#[php(name = "Iggy\\MetadataPartitionHandedOver")]
pub struct MetadataPartitionHandedOver {
    inner: RustMetadataPartitionHandedOver,
}

/// A `consumer_group_members` current state item.
#[php_class]
#[php(name = "Iggy\\MetadataMemberItem")]
pub struct MetadataMemberItem {
    inner: RustMetadataMemberItem,
}

/// A user after a create or update, and a `users` current state item.
#[php_class]
#[php(name = "Iggy\\MetadataUserChange")]
pub struct MetadataUserChange {
    inner: RustMetadataUserChange,
}

/// A deleted user, or one whose password changed.
#[php_class]
#[php(name = "Iggy\\MetadataUserIdentity")]
pub struct MetadataUserIdentity {
    inner: RustMetadataUserIdentity,
}

/// A user's public permission set after a change.
#[php_class]
#[php(name = "Iggy\\MetadataUserPermissionsUpdated")]
pub struct MetadataUserPermissionsUpdated {
    inner: RustMetadataUserPermissionsUpdated,
}

/// A created personal access token, and a `personal_access_tokens` current state item.
/// Never the token itself.
#[php_class]
#[php(name = "Iggy\\MetadataTokenChange")]
pub struct MetadataTokenChange {
    inner: RustMetadataTokenChange,
}

/// A removed personal access token.
#[php_class]
#[php(name = "Iggy\\MetadataTokenDeleted")]
pub struct MetadataTokenDeleted {
    inner: RustMetadataTokenDeleted,
}

/// A registered session, and a `sessions` current state item.
#[php_class]
#[php(name = "Iggy\\MetadataSession")]
pub struct MetadataSession {
    inner: RustMetadataSession,
}

/// A closed session.
#[php_class]
#[php(name = "Iggy\\MetadataSessionClosed")]
pub struct MetadataSessionClosed {
    inner: RustMetadataSessionClosed,
}

/// A record of a group or kind this build does not know. A mirror stops on it rather than
/// skip a possible state change.
#[php_class]
#[php(name = "Iggy\\MetadataUnknownChange")]
pub struct MetadataUnknownChange {
    group: UnknownCode,
    kind: UnknownCode,
    body: Vec<u8>,
}

/// A current state item of a group this build does not know.
#[php_class]
#[php(name = "Iggy\\MetadataUnknownItem")]
pub struct MetadataUnknownItem {
    group: UnknownCode,
    body: Vec<u8>,
}

#[php_impl]
impl MetadataChanges {
    /// Decodes the response payload of the raw `metadata.changes` command (code 17), as
    /// `Client::sendBinaryRequest()` returns it.
    pub fn decode(payload: Binary<u8>) -> PhpResult<Self> {
        let payload: Vec<u8> = payload.into();
        let (response, consumed) = GetMetadataChangesResponse::decode(&payload)
            .map_err(|_| to_php_exception(IggyError::InvalidFormat))?;
        if consumed != payload.len() {
            return Err(to_php_exception(IggyError::InvalidFormat));
        }
        RustMetadataChanges::try_from(response)
            .map(|inner| Self { inner })
            .map_err(to_php_exception)
    }

    /// Reads the JSON body `GET /metadata/changes` returns.
    pub fn from_json(json: String) -> PhpResult<Self> {
        serde_json::from_str(&json)
            .map(|inner| Self { inner })
            .map_err(|_| to_php_exception(IggyError::InvalidJsonResponse))
    }

    #[php(getter)]
    pub fn status(&self) -> MetadataChangeStatus {
        self.inner.status.into()
    }

    /// `Changes` for `earliest`, `latest` and `after`, `Current` for state pages.
    #[php(getter)]
    pub fn phase(&self) -> MetadataChangePhase {
        self.inner.phase.into()
    }

    /// This node's log id as a decimal string, null before it applied op 1.
    #[php(getter, change_case = "snake_case")]
    pub fn log_id(&self) -> Option<String> {
        self.inner.log_id.map(|log_id| log_id.to_string())
    }

    /// Lowest whole operation from which the requested groups are complete.
    #[php(getter, change_case = "snake_case")]
    pub fn oldest_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.oldest_op)
    }

    /// The requested groups are complete for every timestamp above this value in
    /// microseconds, 0 when nothing was evicted.
    #[php(getter, change_case = "snake_case")]
    pub fn retained_since(&self) -> PhpResult<Zval> {
        unsigned(self.inner.retained_since)
    }

    /// Highest metadata operation the answering node applied.
    #[php(getter, change_case = "snake_case")]
    pub fn head_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.head_op)
    }

    /// The cursor to send back. Present on `Ok`, echoed on `Behind`, null on `Expired` and
    /// `Reset`. The last state page returns the anchor to follow from.
    #[php(getter)]
    pub fn next(&self) -> Option<MetadataChangeCursor> {
        self.inner.next.map(|inner| MetadataChangeCursor { inner })
    }

    /// More entries remain after `next`, or another state page exists.
    #[php(getter, change_case = "snake_case")]
    pub fn has_more(&self) -> bool {
        self.inner.has_more
    }

    /// Change records in `(op, ordinal)` order, empty on state pages.
    #[php(getter)]
    pub fn records(&self) -> Vec<MetadataChangeRecord> {
        self.inner
            .records
            .iter()
            .map(|record| MetadataChangeRecord {
                inner: record.clone(),
            })
            .collect()
    }

    /// Current state items, empty on change pages.
    #[php(getter)]
    pub fn items(&self) -> Vec<MetadataCurrentItem> {
        self.inner
            .items
            .iter()
            .map(|item| MetadataCurrentItem {
                inner: item.clone(),
            })
            .collect()
    }

    /// Continuation of a current state scan, null once it is complete.
    #[php(getter, change_case = "snake_case")]
    pub fn current_page_token(&self) -> Option<MetadataCurrentPageToken> {
        self.inner
            .current_page_token
            .clone()
            .map(|inner| MetadataCurrentPageToken { inner })
    }
}

#[php_impl]
impl MetadataChangeRecord {
    /// The metadata operation that produced the record.
    #[php(getter)]
    pub fn op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.op)
    }

    /// Position of the record within `op`.
    #[php(getter)]
    pub fn ordinal(&self) -> u32 {
        self.inner.ordinal
    }

    /// Prepare checksum of `op` as a decimal string.
    #[php(getter)]
    pub fn checksum(&self) -> String {
        self.inner.checksum.to_string()
    }

    /// Prepare timestamp of `op` in microseconds.
    #[php(getter)]
    pub fn timestamp(&self) -> PhpResult<Zval> {
        unsigned(self.inner.timestamp)
    }

    /// The acting user, null for server actions.
    #[php(getter, change_case = "snake_case")]
    pub fn actor_user_id(&self) -> Option<u32> {
        self.inner.actor_user_id
    }

    /// The group, null for a record this build does not know.
    #[php(getter)]
    pub fn group(&self) -> Option<MetadataChangeGroup> {
        self.inner.change.group().map(Into::into)
    }

    /// The kind, null for a record this build does not know.
    #[php(getter)]
    pub fn kind(&self) -> Option<MetadataChangeKind> {
        self.inner.change.kind().map(Into::into)
    }

    /// The typed body `kind` names, for example a `MetadataStreamChange` for
    /// `StreamCreated` and `StreamUpdated`, or a `MetadataUnknownChange`.
    #[php(getter)]
    pub fn change(&self) -> PhpResult<Zval> {
        change_body(&self.inner.change)
    }
}

#[php_impl]
impl MetadataCurrentItem {
    /// The group, null for an item this build does not know.
    #[php(getter)]
    pub fn group(&self) -> Option<MetadataChangeGroup> {
        self.inner.group().map(Into::into)
    }

    /// The typed body `group` names, or a `MetadataUnknownItem`.
    #[php(getter)]
    pub fn body(&self) -> PhpResult<Zval> {
        item_body(&self.inner)
    }
}

#[php_impl]
impl MetadataChangeCursor {
    /// Builds a cursor from its fields: 128-bit values as decimal strings, `op` as an int or
    /// a decimal string.
    #[php(constructor)]
    pub fn __construct(
        log_id: String,
        op: &Zval,
        checksum: String,
        ordinal: u32,
    ) -> PhpResult<Self> {
        Ok(Self {
            inner: RustMetadataChangeCursor {
                log_id: decimal_u128(&log_id, "log_id")?,
                op: unsigned_argument(op, "op")?,
                checksum: decimal_u128(&checksum, "checksum")?,
                ordinal,
            },
        })
    }

    /// Reads a cursor from its `mc1_` text. Anything else throws `Invalid command` (code 3).
    pub fn parse(text: String) -> PhpResult<Self> {
        RustMetadataChangeCursor::from_str(&text)
            .map(|inner| Self { inner })
            .map_err(to_php_exception)
    }

    /// Prepare checksum of op 1 as a decimal string, which names the history the cursor
    /// belongs to.
    #[php(getter, change_case = "snake_case")]
    pub fn log_id(&self) -> String {
        self.inner.log_id.to_string()
    }

    /// The operation of the last record, 0 before the first record.
    #[php(getter)]
    pub fn op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.op)
    }

    /// Prepare checksum of `op` as a decimal string.
    #[php(getter)]
    pub fn checksum(&self) -> String {
        self.inner.checksum.to_string()
    }

    /// Position of the record within `op`.
    #[php(getter)]
    pub fn ordinal(&self) -> u32 {
        self.inner.ordinal
    }

    pub fn __to_string(&self) -> String {
        self.inner.to_string()
    }
}

#[php_impl]
impl MetadataCurrentPageToken {
    /// Reads a page token from its `cs1_` text. Anything else throws `Invalid command`
    /// (code 3).
    pub fn parse(text: String) -> PhpResult<Self> {
        RustMetadataCurrentPageToken::from_str(&text)
            .map(|inner| Self { inner })
            .map_err(to_php_exception)
    }

    /// The cursor captured before the scan's first page.
    #[php(getter)]
    pub fn anchor(&self) -> MetadataChangeCursor {
        MetadataChangeCursor {
            inner: self.inner.anchor(),
        }
    }

    /// The highest head any page of the scan reported so far.
    #[php(getter, change_case = "snake_case")]
    pub fn required_head(&self) -> PhpResult<Zval> {
        unsigned(self.inner.required_head())
    }

    /// The groups the scan was started with, in code order.
    #[php(getter)]
    pub fn groups(&self) -> Vec<MetadataChangeGroup> {
        self.inner.groups().iter().map(Into::into).collect()
    }

    /// The group of the last item returned.
    #[php(getter, change_case = "snake_case")]
    pub fn last_group(&self) -> MetadataChangeGroup {
        self.inner.last_group().into()
    }

    /// The key of the last item returned, in the page token key layout.
    #[php(getter, change_case = "snake_case")]
    pub fn last_key(&self) -> Binary<u8> {
        Binary::new(self.inner.last_key().to_vec())
    }

    pub fn __to_string(&self) -> String {
        self.inner.to_string()
    }
}

#[php_impl]
impl MetadataChangeStart {
    /// The oldest operation the requested groups still hold completely.
    pub fn earliest() -> Self {
        Self {
            inner: RustMetadataChangeStart::Earliest,
        }
    }

    /// No records, only a cursor at the newest retained record.
    pub fn latest() -> Self {
        Self {
            inner: RustMetadataChangeStart::Latest,
        }
    }

    /// The records after `cursor`.
    pub fn after(cursor: &MetadataChangeCursor) -> Self {
        Self {
            inner: RustMetadataChangeStart::After(cursor.inner),
        }
    }

    /// Current state pages: null starts a scan, a token continues it.
    pub fn current(page_token: Option<&MetadataCurrentPageToken>) -> Self {
        Self {
            inner: RustMetadataChangeStart::Current(page_token.map(|token| token.inner.clone())),
        }
    }
}

#[php_impl]
impl MetadataStreamChange {
    /// Stream id. Ids are reused after a delete.
    #[php(getter, change_case = "snake_case")]
    pub fn stream_id(&self) -> u32 {
        self.inner.stream_id
    }

    /// The operation that created the stream, which tells it from an earlier stream with
    /// the same id.
    #[php(getter, change_case = "snake_case")]
    pub fn stream_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.stream_created_op)
    }

    /// Stream creation time in microseconds.
    #[php(getter, change_case = "snake_case")]
    pub fn created_at(&self) -> PhpResult<Zval> {
        unsigned(self.inner.created_at)
    }

    #[php(getter)]
    pub fn name(&self) -> String {
        self.inner.name.clone()
    }

    /// Options the creating client set, as `StreamDetails::$options` returns them, or null
    /// when `options_unreadable`. Key material is never returned.
    #[php(getter)]
    pub fn options(&self) -> PhpResult<Option<ZBox<ZendHashTable>>> {
        options_view(&self.inner.options, true)
    }

    /// Options the server filled in, including the key source and fingerprints.
    #[php(getter, change_case = "snake_case")]
    pub fn derived_options(&self) -> PhpResult<Option<ZBox<ZendHashTable>>> {
        options_view(&self.inner.options, false)
    }

    /// The server could not read the stream's stored options. Keep the previous values.
    #[php(getter, change_case = "snake_case")]
    pub fn options_unreadable(&self) -> bool {
        matches!(self.inner.options, MetadataOptions::Unreadable)
    }
}

#[php_impl]
impl MetadataStreamDeleted {
    #[php(getter, change_case = "snake_case")]
    pub fn stream_id(&self) -> u32 {
        self.inner.stream_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn stream_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.stream_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn created_at(&self) -> PhpResult<Zval> {
        unsigned(self.inner.created_at)
    }

    #[php(getter)]
    pub fn name(&self) -> String {
        self.inner.name.clone()
    }

    /// The operation of the delete request when the delete completed later, else null.
    #[php(getter, change_case = "snake_case")]
    pub fn requested_op(&self) -> PhpResult<Zval> {
        optional_unsigned(self.inner.requested_op)
    }
}

#[php_impl]
impl MetadataTopicChange {
    #[php(getter, change_case = "snake_case")]
    pub fn stream_id(&self) -> u32 {
        self.inner.stream_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn stream_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.stream_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_id(&self) -> u32 {
        self.inner.topic_id
    }

    /// The operation that created the topic, which tells it from an earlier topic with the
    /// same id.
    #[php(getter, change_case = "snake_case")]
    pub fn topic_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.topic_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn created_at(&self) -> PhpResult<Zval> {
        unsigned(self.inner.created_at)
    }

    #[php(getter)]
    pub fn name(&self) -> String {
        self.inner.name.clone()
    }

    #[php(getter, change_case = "snake_case")]
    pub fn partitions_count(&self) -> u32 {
        self.inner.partitions_count
    }

    /// Options the creating or updating client set, or null when `options_unreadable`.
    #[php(getter)]
    pub fn options(&self) -> PhpResult<Option<ZBox<ZendHashTable>>> {
        options_view(&self.inner.options, true)
    }

    /// Options the server filled in from its catalog defaults.
    #[php(getter, change_case = "snake_case")]
    pub fn derived_options(&self) -> PhpResult<Option<ZBox<ZendHashTable>>> {
        options_view(&self.inner.options, false)
    }

    /// The server could not read the topic's stored options. Keep the previous values.
    #[php(getter, change_case = "snake_case")]
    pub fn options_unreadable(&self) -> bool {
        matches!(self.inner.options, MetadataOptions::Unreadable)
    }
}

#[php_impl]
impl MetadataTopicItem {
    #[php(getter, change_case = "snake_case")]
    pub fn stream_id(&self) -> u32 {
        self.inner.stream_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn stream_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.stream_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_id(&self) -> u32 {
        self.inner.topic_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.topic_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn created_at(&self) -> PhpResult<Zval> {
        unsigned(self.inner.created_at)
    }

    #[php(getter)]
    pub fn name(&self) -> String {
        self.inner.name.clone()
    }

    /// Options the creating or updating client set, or null when `options_unreadable`.
    #[php(getter)]
    pub fn options(&self) -> PhpResult<Option<ZBox<ZendHashTable>>> {
        options_view(&self.inner.options, true)
    }

    /// Options the server filled in from its catalog defaults.
    #[php(getter, change_case = "snake_case")]
    pub fn derived_options(&self) -> PhpResult<Option<ZBox<ZendHashTable>>> {
        options_view(&self.inner.options, false)
    }

    /// The server could not read the topic's stored options.
    #[php(getter, change_case = "snake_case")]
    pub fn options_unreadable(&self) -> bool {
        matches!(self.inner.options, MetadataOptions::Unreadable)
    }
}

#[php_impl]
impl MetadataTopicDeleted {
    #[php(getter, change_case = "snake_case")]
    pub fn stream_id(&self) -> u32 {
        self.inner.stream_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn stream_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.stream_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_id(&self) -> u32 {
        self.inner.topic_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.topic_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn created_at(&self) -> PhpResult<Zval> {
        unsigned(self.inner.created_at)
    }

    #[php(getter)]
    pub fn name(&self) -> String {
        self.inner.name.clone()
    }

    /// The operation of the delete request when the delete completed later, else null.
    #[php(getter, change_case = "snake_case")]
    pub fn requested_op(&self) -> PhpResult<Zval> {
        optional_unsigned(self.inner.requested_op)
    }
}

#[php_impl]
impl MetadataPartitionsChanged {
    #[php(getter, change_case = "snake_case")]
    pub fn stream_id(&self) -> u32 {
        self.inner.stream_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn stream_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.stream_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_id(&self) -> u32 {
        self.inner.topic_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.topic_created_op)
    }

    /// The partition count after the change.
    #[php(getter, change_case = "snake_case")]
    pub fn partitions_count(&self) -> u32 {
        self.inner.partitions_count
    }

    /// The deleted partition ids, ascending, empty on create.
    #[php(getter, change_case = "snake_case")]
    pub fn removed_partition_ids(&self) -> Vec<u32> {
        self.inner.removed_partition_ids.clone()
    }

    /// The operation of the delete request when the delete completed later, else null.
    #[php(getter, change_case = "snake_case")]
    pub fn requested_op(&self) -> PhpResult<Zval> {
        optional_unsigned(self.inner.requested_op)
    }
}

#[php_impl]
impl MetadataPartitionsItem {
    #[php(getter, change_case = "snake_case")]
    pub fn stream_id(&self) -> u32 {
        self.inner.stream_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn stream_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.stream_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_id(&self) -> u32 {
        self.inner.topic_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.topic_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn partitions_count(&self) -> u32 {
        self.inner.partitions_count
    }
}

#[php_impl]
impl MetadataConsumerGroupCreated {
    #[php(getter, change_case = "snake_case")]
    pub fn stream_id(&self) -> u32 {
        self.inner.stream_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn stream_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.stream_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_id(&self) -> u32 {
        self.inner.topic_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.topic_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn group_id(&self) -> u32 {
        self.inner.group_id
    }

    #[php(getter)]
    pub fn name(&self) -> String {
        self.inner.name.clone()
    }

    /// The topic's consumer group count after the change.
    #[php(getter, change_case = "snake_case")]
    pub fn consumer_groups_count(&self) -> u32 {
        self.inner.consumer_groups_count
    }
}

#[php_impl]
impl MetadataConsumerGroupDeleted {
    #[php(getter, change_case = "snake_case")]
    pub fn stream_id(&self) -> u32 {
        self.inner.stream_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn stream_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.stream_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_id(&self) -> u32 {
        self.inner.topic_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.topic_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn group_id(&self) -> u32 {
        self.inner.group_id
    }

    #[php(getter)]
    pub fn name(&self) -> String {
        self.inner.name.clone()
    }

    /// The topic's consumer group count after the change.
    #[php(getter, change_case = "snake_case")]
    pub fn consumer_groups_count(&self) -> u32 {
        self.inner.consumer_groups_count
    }

    /// The operation of the delete request when the delete completed later, else null.
    #[php(getter, change_case = "snake_case")]
    pub fn requested_op(&self) -> PhpResult<Zval> {
        optional_unsigned(self.inner.requested_op)
    }
}

#[php_impl]
impl MetadataConsumerGroupItem {
    #[php(getter, change_case = "snake_case")]
    pub fn stream_id(&self) -> u32 {
        self.inner.stream_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn stream_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.stream_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_id(&self) -> u32 {
        self.inner.topic_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.topic_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn group_id(&self) -> u32 {
        self.inner.group_id
    }

    #[php(getter)]
    pub fn name(&self) -> String {
        self.inner.name.clone()
    }
}

#[php_impl]
impl MetadataMemberJoined {
    #[php(getter, change_case = "snake_case")]
    pub fn stream_id(&self) -> u32 {
        self.inner.stream_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn stream_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.stream_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_id(&self) -> u32 {
        self.inner.topic_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.topic_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn group_id(&self) -> u32 {
        self.inner.group_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn member_id(&self) -> u32 {
        self.inner.member_id
    }

    /// The member's client id as a decimal string.
    #[php(getter, change_case = "snake_case")]
    pub fn client_id(&self) -> String {
        self.inner.client_id.to_string()
    }

    #[php(getter, change_case = "snake_case")]
    pub fn members_count(&self) -> u32 {
        self.inner.members_count
    }

    /// Installed partitions, normally none because a handover installs them.
    #[php(getter, change_case = "snake_case")]
    pub fn partition_ids(&self) -> Vec<u32> {
        self.inner.partition_ids.clone()
    }
}

#[php_impl]
impl MetadataMemberLeft {
    #[php(getter, change_case = "snake_case")]
    pub fn stream_id(&self) -> u32 {
        self.inner.stream_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn stream_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.stream_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_id(&self) -> u32 {
        self.inner.topic_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.topic_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn group_id(&self) -> u32 {
        self.inner.group_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn member_id(&self) -> u32 {
        self.inner.member_id
    }

    /// The member's client id as a decimal string.
    #[php(getter, change_case = "snake_case")]
    pub fn client_id(&self) -> String {
        self.inner.client_id.to_string()
    }

    #[php(getter, change_case = "snake_case")]
    pub fn members_count(&self) -> u32 {
        self.inner.members_count
    }

    #[php(getter)]
    pub fn reason(&self) -> MetadataMemberLeftReason {
        self.inner.reason.into()
    }
}

#[php_impl]
impl MetadataPartitionHandedOver {
    #[php(getter, change_case = "snake_case")]
    pub fn stream_id(&self) -> u32 {
        self.inner.stream_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn stream_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.stream_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_id(&self) -> u32 {
        self.inner.topic_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.topic_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn group_id(&self) -> u32 {
        self.inner.group_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn partition_id(&self) -> u32 {
        self.inner.partition_id
    }

    /// The new owner, null when the last member left.
    #[php(getter, change_case = "snake_case")]
    pub fn member_id(&self) -> Option<u32> {
        self.inner.member_id
    }
}

#[php_impl]
impl MetadataMemberItem {
    #[php(getter, change_case = "snake_case")]
    pub fn stream_id(&self) -> u32 {
        self.inner.stream_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn stream_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.stream_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_id(&self) -> u32 {
        self.inner.topic_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn topic_created_op(&self) -> PhpResult<Zval> {
        unsigned(self.inner.topic_created_op)
    }

    #[php(getter, change_case = "snake_case")]
    pub fn group_id(&self) -> u32 {
        self.inner.group_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn member_id(&self) -> u32 {
        self.inner.member_id
    }

    /// The member's client id as a decimal string.
    #[php(getter, change_case = "snake_case")]
    pub fn client_id(&self) -> String {
        self.inner.client_id.to_string()
    }

    /// Installed partitions, ascending.
    #[php(getter, change_case = "snake_case")]
    pub fn partition_ids(&self) -> Vec<u32> {
        self.inner.partition_ids.clone()
    }
}

#[php_impl]
impl MetadataUserChange {
    /// User id. Ids are reused after a delete.
    #[php(getter, change_case = "snake_case")]
    pub fn user_id(&self) -> u32 {
        self.inner.user_id
    }

    /// User creation time in microseconds, which tells the user from an earlier user with
    /// the same id.
    #[php(getter, change_case = "snake_case")]
    pub fn user_created_at(&self) -> PhpResult<Zval> {
        unsigned(self.inner.user_created_at)
    }

    #[php(getter)]
    pub fn username(&self) -> String {
        self.inner.username.clone()
    }

    /// `active` or `inactive`.
    #[php(getter)]
    pub fn status(&self) -> String {
        self.inner.status.to_string()
    }

    /// The public permission set in the shape `GET /users/{id}` returns, null when the
    /// user has none.
    #[php(getter)]
    pub fn permissions(&self) -> PhpResult<Option<ZBox<ZendHashTable>>> {
        self.inner
            .permissions
            .as_ref()
            .map(permissions_array)
            .transpose()
    }

    /// Options the creating client set, or null when `options_unreadable`.
    #[php(getter)]
    pub fn options(&self) -> PhpResult<Option<ZBox<ZendHashTable>>> {
        options_view(&self.inner.options, true)
    }

    /// Options the server filled in.
    #[php(getter, change_case = "snake_case")]
    pub fn derived_options(&self) -> PhpResult<Option<ZBox<ZendHashTable>>> {
        options_view(&self.inner.options, false)
    }

    /// The server could not read the user's stored options.
    #[php(getter, change_case = "snake_case")]
    pub fn options_unreadable(&self) -> bool {
        matches!(self.inner.options, MetadataOptions::Unreadable)
    }
}

#[php_impl]
impl MetadataUserIdentity {
    #[php(getter, change_case = "snake_case")]
    pub fn user_id(&self) -> u32 {
        self.inner.user_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn user_created_at(&self) -> PhpResult<Zval> {
        unsigned(self.inner.user_created_at)
    }

    #[php(getter)]
    pub fn username(&self) -> String {
        self.inner.username.clone()
    }
}

#[php_impl]
impl MetadataUserPermissionsUpdated {
    #[php(getter, change_case = "snake_case")]
    pub fn user_id(&self) -> u32 {
        self.inner.user_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn user_created_at(&self) -> PhpResult<Zval> {
        unsigned(self.inner.user_created_at)
    }

    #[php(getter)]
    pub fn username(&self) -> String {
        self.inner.username.clone()
    }

    /// The public permission set after the change, null when the user has none.
    #[php(getter)]
    pub fn permissions(&self) -> PhpResult<Option<ZBox<ZendHashTable>>> {
        self.inner
            .permissions
            .as_ref()
            .map(permissions_array)
            .transpose()
    }

    #[php(getter)]
    pub fn reason(&self) -> MetadataPermissionsUpdatedReason {
        self.inner.reason.into()
    }
}

#[php_impl]
impl MetadataTokenChange {
    #[php(getter, change_case = "snake_case")]
    pub fn user_id(&self) -> u32 {
        self.inner.user_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn user_created_at(&self) -> PhpResult<Zval> {
        unsigned(self.inner.user_created_at)
    }

    #[php(getter)]
    pub fn name(&self) -> String {
        self.inner.name.clone()
    }

    /// Expiry in microseconds, null for a token that never expires.
    #[php(getter)]
    pub fn expiry(&self) -> PhpResult<Zval> {
        optional_unsigned(self.inner.expiry)
    }
}

#[php_impl]
impl MetadataTokenDeleted {
    #[php(getter, change_case = "snake_case")]
    pub fn user_id(&self) -> u32 {
        self.inner.user_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn user_created_at(&self) -> PhpResult<Zval> {
        unsigned(self.inner.user_created_at)
    }

    #[php(getter)]
    pub fn name(&self) -> String {
        self.inner.name.clone()
    }

    #[php(getter)]
    pub fn reason(&self) -> MetadataTokenDeletedReason {
        self.inner.reason.into()
    }
}

#[php_impl]
impl MetadataSession {
    #[php(getter, change_case = "snake_case")]
    pub fn user_id(&self) -> u32 {
        self.inner.user_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn user_created_at(&self) -> PhpResult<Zval> {
        unsigned(self.inner.user_created_at)
    }

    /// The session's client id as a decimal string.
    #[php(getter, change_case = "snake_case")]
    pub fn client_id(&self) -> String {
        self.inner.client_id.to_string()
    }

    /// The session epoch.
    #[php(getter)]
    pub fn session(&self) -> PhpResult<Zval> {
        unsigned(self.inner.session)
    }
}

#[php_impl]
impl MetadataSessionClosed {
    #[php(getter, change_case = "snake_case")]
    pub fn user_id(&self) -> u32 {
        self.inner.user_id
    }

    #[php(getter, change_case = "snake_case")]
    pub fn user_created_at(&self) -> PhpResult<Zval> {
        unsigned(self.inner.user_created_at)
    }

    /// The session's client id as a decimal string.
    #[php(getter, change_case = "snake_case")]
    pub fn client_id(&self) -> String {
        self.inner.client_id.to_string()
    }

    /// The session epoch.
    #[php(getter)]
    pub fn session(&self) -> PhpResult<Zval> {
        unsigned(self.inner.session)
    }

    #[php(getter)]
    pub fn reason(&self) -> MetadataSessionClosedReason {
        self.inner.reason.into()
    }
}

#[php_impl]
impl MetadataUnknownChange {
    /// The group code on binary transports, or the group name over HTTP.
    #[php(getter)]
    pub fn group(&self) -> PhpResult<Zval> {
        self.group
            .clone()
            .into_zval(false)
            .map_err(to_php_exception)
    }

    /// The kind code on binary transports, or the kind name over HTTP.
    #[php(getter)]
    pub fn kind(&self) -> PhpResult<Zval> {
        self.kind.clone().into_zval(false).map_err(to_php_exception)
    }

    /// The raw body on binary transports, or the record's whole JSON object over HTTP.
    #[php(getter)]
    pub fn body(&self) -> Binary<u8> {
        Binary::new(self.body.clone())
    }
}

#[php_impl]
impl MetadataUnknownItem {
    /// The group code on binary transports, or the group name over HTTP.
    #[php(getter)]
    pub fn group(&self) -> PhpResult<Zval> {
        self.group
            .clone()
            .into_zval(false)
            .map_err(to_php_exception)
    }

    /// The raw body on binary transports, or the item's whole JSON object over HTTP.
    #[php(getter)]
    pub fn body(&self) -> Binary<u8> {
        Binary::new(self.body.clone())
    }
}

impl From<RustMetadataChanges> for MetadataChanges {
    fn from(inner: RustMetadataChanges) -> Self {
        Self { inner }
    }
}

// Lets a group list default to `[]` in the generated stub.
impl StubLiteral for MetadataChangeGroup {
    fn stub_literal(&self) -> String {
        format!("\\Iggy\\MetadataChangeGroup::{}", self.to_name())
    }
}

impl From<MetadataChangeGroup> for RustMetadataChangeGroup {
    fn from(group: MetadataChangeGroup) -> Self {
        match group {
            MetadataChangeGroup::Streams => Self::Streams,
            MetadataChangeGroup::Topics => Self::Topics,
            MetadataChangeGroup::Partitions => Self::Partitions,
            MetadataChangeGroup::ConsumerGroups => Self::ConsumerGroups,
            MetadataChangeGroup::ConsumerGroupMembers => Self::ConsumerGroupMembers,
            MetadataChangeGroup::Users => Self::Users,
            MetadataChangeGroup::PersonalAccessTokens => Self::PersonalAccessTokens,
            MetadataChangeGroup::Sessions => Self::Sessions,
        }
    }
}

impl From<RustMetadataChangeGroup> for MetadataChangeGroup {
    fn from(group: RustMetadataChangeGroup) -> Self {
        match group {
            RustMetadataChangeGroup::Streams => Self::Streams,
            RustMetadataChangeGroup::Topics => Self::Topics,
            RustMetadataChangeGroup::Partitions => Self::Partitions,
            RustMetadataChangeGroup::ConsumerGroups => Self::ConsumerGroups,
            RustMetadataChangeGroup::ConsumerGroupMembers => Self::ConsumerGroupMembers,
            RustMetadataChangeGroup::Users => Self::Users,
            RustMetadataChangeGroup::PersonalAccessTokens => Self::PersonalAccessTokens,
            RustMetadataChangeGroup::Sessions => Self::Sessions,
        }
    }
}

impl From<RustMetadataChangeKind> for MetadataChangeKind {
    fn from(kind: RustMetadataChangeKind) -> Self {
        match kind {
            RustMetadataChangeKind::StreamCreated => Self::StreamCreated,
            RustMetadataChangeKind::StreamUpdated => Self::StreamUpdated,
            RustMetadataChangeKind::StreamDeleted => Self::StreamDeleted,
            RustMetadataChangeKind::TopicCreated => Self::TopicCreated,
            RustMetadataChangeKind::TopicUpdated => Self::TopicUpdated,
            RustMetadataChangeKind::TopicDeleted => Self::TopicDeleted,
            RustMetadataChangeKind::PartitionsChanged => Self::PartitionsChanged,
            RustMetadataChangeKind::ConsumerGroupCreated => Self::ConsumerGroupCreated,
            RustMetadataChangeKind::ConsumerGroupDeleted => Self::ConsumerGroupDeleted,
            RustMetadataChangeKind::ConsumerGroupMemberJoined => Self::ConsumerGroupMemberJoined,
            RustMetadataChangeKind::ConsumerGroupMemberLeft => Self::ConsumerGroupMemberLeft,
            RustMetadataChangeKind::ConsumerGroupPartitionHandedOver => {
                Self::ConsumerGroupPartitionHandedOver
            }
            RustMetadataChangeKind::UserCreated => Self::UserCreated,
            RustMetadataChangeKind::UserUpdated => Self::UserUpdated,
            RustMetadataChangeKind::UserDeleted => Self::UserDeleted,
            RustMetadataChangeKind::UserPasswordChanged => Self::UserPasswordChanged,
            RustMetadataChangeKind::UserPermissionsUpdated => Self::UserPermissionsUpdated,
            RustMetadataChangeKind::PersonalAccessTokenCreated => Self::PersonalAccessTokenCreated,
            RustMetadataChangeKind::PersonalAccessTokenDeleted => Self::PersonalAccessTokenDeleted,
            RustMetadataChangeKind::SessionRegistered => Self::SessionRegistered,
            RustMetadataChangeKind::SessionClosed => Self::SessionClosed,
        }
    }
}

impl From<RustMetadataChangeStatus> for MetadataChangeStatus {
    fn from(status: RustMetadataChangeStatus) -> Self {
        match status {
            RustMetadataChangeStatus::Ok => Self::Ok,
            RustMetadataChangeStatus::Behind => Self::Behind,
            RustMetadataChangeStatus::Expired => Self::Expired,
            RustMetadataChangeStatus::Reset => Self::Reset,
        }
    }
}

impl From<RustMetadataChangePhase> for MetadataChangePhase {
    fn from(phase: RustMetadataChangePhase) -> Self {
        match phase {
            RustMetadataChangePhase::Changes => Self::Changes,
            RustMetadataChangePhase::Current => Self::Current,
        }
    }
}

impl From<RustMetadataMemberLeftReason> for MetadataMemberLeftReason {
    fn from(reason: RustMetadataMemberLeftReason) -> Self {
        match reason {
            RustMetadataMemberLeftReason::Left => Self::Left,
            RustMetadataMemberLeftReason::SessionEnded => Self::SessionEnded,
        }
    }
}

impl From<RustMetadataTokenDeletedReason> for MetadataTokenDeletedReason {
    fn from(reason: RustMetadataTokenDeletedReason) -> Self {
        match reason {
            RustMetadataTokenDeletedReason::Deleted => Self::Deleted,
            RustMetadataTokenDeletedReason::Expired => Self::Expired,
        }
    }
}

impl From<RustMetadataSessionClosedReason> for MetadataSessionClosedReason {
    fn from(reason: RustMetadataSessionClosedReason) -> Self {
        match reason {
            RustMetadataSessionClosedReason::Logout => Self::Logout,
            RustMetadataSessionClosedReason::Expired => Self::Expired,
            RustMetadataSessionClosedReason::Revoked => Self::Revoked,
        }
    }
}

impl From<RustMetadataPermissionsUpdatedReason> for MetadataPermissionsUpdatedReason {
    fn from(reason: RustMetadataPermissionsUpdatedReason) -> Self {
        match reason {
            RustMetadataPermissionsUpdatedReason::Updated => Self::Updated,
            RustMetadataPermissionsUpdatedReason::ResourceDeleted => Self::ResourceDeleted,
        }
    }
}

/// A group or kind as the transport carried it: a code on binary transports, a name over
/// HTTP.
#[derive(Clone)]
enum UnknownCode {
    Binary(u8),
    Json(String),
}

impl IntoZval for UnknownCode {
    const TYPE: DataType = DataType::Mixed;
    const NULLABLE: bool = false;

    fn set_zval(self, zval: &mut Zval, persistent: bool) -> ConversionResult<()> {
        match self {
            Self::Binary(code) => i64::from(code).set_zval(zval, persistent),
            Self::Json(name) => name.set_zval(zval, persistent),
        }
    }
}

fn change_body(change: &RustMetadataChange) -> PhpResult<Zval> {
    let body = match change {
        RustMetadataChange::StreamCreated(body) | RustMetadataChange::StreamUpdated(body) => {
            MetadataStreamChange {
                inner: body.clone(),
            }
            .into_zval(false)
        }
        RustMetadataChange::StreamDeleted(body) => MetadataStreamDeleted {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataChange::TopicCreated(body) | RustMetadataChange::TopicUpdated(body) => {
            MetadataTopicChange {
                inner: body.clone(),
            }
            .into_zval(false)
        }
        RustMetadataChange::TopicDeleted(body) => MetadataTopicDeleted {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataChange::PartitionsChanged(body) => MetadataPartitionsChanged {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataChange::ConsumerGroupCreated(body) => MetadataConsumerGroupCreated {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataChange::ConsumerGroupDeleted(body) => MetadataConsumerGroupDeleted {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataChange::ConsumerGroupMemberJoined(body) => MetadataMemberJoined {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataChange::ConsumerGroupMemberLeft(body) => MetadataMemberLeft {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataChange::ConsumerGroupPartitionHandedOver(body) => MetadataPartitionHandedOver {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataChange::UserCreated(body) | RustMetadataChange::UserUpdated(body) => {
            MetadataUserChange {
                inner: body.clone(),
            }
            .into_zval(false)
        }
        RustMetadataChange::UserDeleted(body) | RustMetadataChange::UserPasswordChanged(body) => {
            MetadataUserIdentity {
                inner: body.clone(),
            }
            .into_zval(false)
        }
        RustMetadataChange::UserPermissionsUpdated(body) => MetadataUserPermissionsUpdated {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataChange::PersonalAccessTokenCreated(body) => MetadataTokenChange {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataChange::PersonalAccessTokenDeleted(body) => MetadataTokenDeleted {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataChange::SessionRegistered(body) => MetadataSession {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataChange::SessionClosed(body) => MetadataSessionClosed {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataChange::Unknown { group, kind, body } => MetadataUnknownChange {
            group: UnknownCode::Binary(*group),
            kind: UnknownCode::Binary(*kind),
            body: body.to_vec(),
        }
        .into_zval(false),
        RustMetadataChange::UnknownJson { group, kind, body } => MetadataUnknownChange {
            group: UnknownCode::Json(group.clone()),
            kind: UnknownCode::Json(kind.clone()),
            body: body.get().as_bytes().to_vec(),
        }
        .into_zval(false),
    };
    body.map_err(to_php_exception)
}

fn item_body(item: &RustMetadataCurrentItem) -> PhpResult<Zval> {
    let body = match item {
        RustMetadataCurrentItem::Stream(body) => MetadataStreamChange {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataCurrentItem::Topic(body) => MetadataTopicItem {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataCurrentItem::Partitions(body) => MetadataPartitionsItem {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataCurrentItem::ConsumerGroup(body) => MetadataConsumerGroupItem {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataCurrentItem::ConsumerGroupMember(body) => MetadataMemberItem {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataCurrentItem::User(body) => MetadataUserChange {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataCurrentItem::PersonalAccessToken(body) => MetadataTokenChange {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataCurrentItem::Session(body) => MetadataSession {
            inner: body.clone(),
        }
        .into_zval(false),
        RustMetadataCurrentItem::Unknown { group, body } => MetadataUnknownItem {
            group: UnknownCode::Binary(*group),
            body: body.to_vec(),
        }
        .into_zval(false),
        RustMetadataCurrentItem::UnknownJson { group, body } => MetadataUnknownItem {
            group: UnknownCode::Json(group.clone()),
            body: body.get().as_bytes().to_vec(),
        }
        .into_zval(false),
    };
    body.map_err(to_php_exception)
}

fn options_view(
    options: &MetadataOptions,
    explicit: bool,
) -> PhpResult<Option<ZBox<ZendHashTable>>> {
    match options {
        MetadataOptions::Readable(options) => options_array(options, explicit).map(Some),
        MetadataOptions::Unreadable => Ok(None),
    }
}

/// Public permissions as nested arrays keyed like the `GET /users/{id}` JSON, with stream
/// and topic ids as int keys.
fn permissions_array(permissions: &Permissions) -> PhpResult<ZBox<ZendHashTable>> {
    let global = &permissions.global;
    let mut global_array = ZendHashTable::new();
    for (name, allowed) in [
        ("manage_servers", global.manage_servers),
        ("read_servers", global.read_servers),
        ("manage_users", global.manage_users),
        ("read_users", global.read_users),
        ("manage_streams", global.manage_streams),
        ("read_streams", global.read_streams),
        ("manage_topics", global.manage_topics),
        ("read_topics", global.read_topics),
        ("poll_messages", global.poll_messages),
        ("send_messages", global.send_messages),
    ] {
        global_array
            .insert(name, allowed)
            .map_err(to_php_exception)?;
    }
    let mut array = ZendHashTable::new();
    array
        .insert("global", global_array)
        .map_err(to_php_exception)?;
    let streams = match &permissions.streams {
        Some(streams) => {
            let mut streams_array = ZendHashTable::new();
            for (stream_id, stream) in streams {
                streams_array
                    .insert_at_index(id_key(*stream_id)?, stream_permissions_array(stream)?)
                    .map_err(to_php_exception)?;
            }
            Some(streams_array)
        }
        None => None,
    };
    array.insert("streams", streams).map_err(to_php_exception)?;
    Ok(array)
}

fn stream_permissions_array(stream: &StreamPermissions) -> PhpResult<ZBox<ZendHashTable>> {
    let mut array = ZendHashTable::new();
    for (name, allowed) in [
        ("manage_stream", stream.manage_stream),
        ("read_stream", stream.read_stream),
        ("manage_topics", stream.manage_topics),
        ("read_topics", stream.read_topics),
        ("poll_messages", stream.poll_messages),
        ("send_messages", stream.send_messages),
    ] {
        array.insert(name, allowed).map_err(to_php_exception)?;
    }
    let topics = match &stream.topics {
        Some(topics) => {
            let mut topics_array = ZendHashTable::new();
            for (topic_id, topic) in topics {
                let mut topic_array = ZendHashTable::new();
                for (name, allowed) in [
                    ("manage_topic", topic.manage_topic),
                    ("read_topic", topic.read_topic),
                    ("poll_messages", topic.poll_messages),
                    ("send_messages", topic.send_messages),
                ] {
                    topic_array
                        .insert(name, allowed)
                        .map_err(to_php_exception)?;
                }
                topics_array
                    .insert_at_index(id_key(*topic_id)?, topic_array)
                    .map_err(to_php_exception)?;
            }
            Some(topics_array)
        }
        None => None,
    };
    array.insert("topics", topics).map_err(to_php_exception)?;
    Ok(array)
}

fn id_key(id: usize) -> PhpResult<i64> {
    i64::try_from(id).map_err(|_| to_php_exception(IggyError::InvalidFormat))
}

/// An unsigned 64-bit value as an int, or as a decimal string above `PHP_INT_MAX`.
fn unsigned(value: u64) -> PhpResult<Zval> {
    match i64::try_from(value) {
        Ok(number) => number.into_zval(false),
        Err(_) => value.to_string().into_zval(false),
    }
    .map_err(to_php_exception)
}

fn optional_unsigned(value: Option<u64>) -> PhpResult<Zval> {
    match value {
        Some(value) => unsigned(value),
        None => Ok(Zval::new()),
    }
}

fn decimal_u128(text: &str, field: &str) -> PhpResult<u128> {
    let valid = !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
    valid
        .then(|| text.parse::<u128>().ok())
        .flatten()
        .ok_or_else(|| {
            to_php_exception(format!(
                "'{field}' must be an unsigned 128-bit decimal string"
            ))
        })
}
