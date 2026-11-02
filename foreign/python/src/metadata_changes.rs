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

use iggy::prelude::{
    IggyError, IggyTimestamp, MetadataChange as RustMetadataChange,
    MetadataChangeCursor as RustMetadataChangeCursor,
    MetadataChangeGroup as RustMetadataChangeGroup, MetadataChangeKind as RustMetadataChangeKind,
    MetadataChangePhase as RustMetadataChangePhase,
    MetadataChangeRecord as RustMetadataChangeRecord,
    MetadataChangeStart as RustMetadataChangeStart,
    MetadataChangeStatus as RustMetadataChangeStatus, MetadataChanges as RustMetadataChanges,
    MetadataChangesQuery, MetadataCurrentItem as RustMetadataCurrentItem,
    MetadataCurrentPageToken as RustMetadataCurrentPageToken,
    MetadataMemberLeftReason as RustMetadataMemberLeftReason, MetadataOptions,
    MetadataPermissionsUpdatedReason as RustMetadataPermissionsUpdatedReason,
    MetadataSessionClosedReason as RustMetadataSessionClosedReason,
    MetadataTokenDeletedReason as RustMetadataTokenDeletedReason, Permissions as RustPermissions,
    ResourceOptions,
};
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_stub_gen::derive::{
    gen_stub_pyclass, gen_stub_pyclass_complex_enum, gen_stub_pyclass_enum, gen_stub_pymethods,
};
use std::str::FromStr;

use crate::client::to_runtime_error;
use crate::options::options_by_provenance;
use crate::permissions::Permissions;
use crate::user::UserStatus;
use crate::user_headers::UserHeaders;

/// One page of the metadata change log, or of the current state, as
/// `IggyClient.get_metadata_changes` returns it.
///
/// The page is converted once: `records` and `items` hand back the same
/// objects on every access.
#[gen_stub_pyclass]
#[pyclass(frozen)]
pub struct MetadataChanges {
    status: MetadataChangeStatus,
    phase: MetadataChangePhase,
    /// This node's log id, or `None` before it applied op 1.
    #[pyo3(get)]
    log_id: Option<u128>,
    /// Lowest whole operation from which the requested groups are complete.
    #[pyo3(get)]
    oldest_op: u64,
    /// The requested groups are complete for every timestamp above this
    /// value in microseconds, 0 when nothing was evicted.
    #[pyo3(get)]
    retained_since: u64,
    /// Highest metadata operation the answering node applied.
    #[pyo3(get)]
    head_op: u64,
    next: Option<MetadataChangeCursor>,
    /// More entries remain after `next`, or another state page exists.
    #[pyo3(get)]
    has_more: bool,
    records: Vec<Py<MetadataChangeRecord>>,
    items: Vec<Py<MetadataCurrentItem>>,
    current_page_token: Option<MetadataCurrentPageToken>,
}

/// One change record: the committed operation that produced it, its position
/// within that operation and the typed body of what is true after it.
#[gen_stub_pyclass]
#[pyclass(frozen)]
pub struct MetadataChangeRecord {
    /// The metadata operation that produced the record.
    #[pyo3(get)]
    op: u64,
    /// Position of the record within `op`.
    #[pyo3(get)]
    ordinal: u32,
    /// Prepare checksum of `op`.
    #[pyo3(get)]
    checksum: u128,
    /// Prepare timestamp of `op` in microseconds.
    #[pyo3(get)]
    timestamp: u64,
    /// The acting user, `None` for server actions.
    #[pyo3(get)]
    actor_user_id: Option<u32>,
    group: Option<MetadataChangeGroup>,
    kind: Option<MetadataChangeKind>,
    change: Py<PyAny>,
}

/// One current state item of a `MetadataChangeStart.Current` page.
#[gen_stub_pyclass]
#[pyclass(frozen)]
pub struct MetadataCurrentItem {
    group: Option<MetadataChangeGroup>,
    body: Py<PyAny>,
}

/// Position of the last change record a reader consumed or scanned. Send it
/// back in `MetadataChangeStart.After` to read the records after it.
///
/// `str()` gives its `mc1_` text, which `parse` reads back. The 128-bit
/// values are Python ints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen, hash, from_py_object)]
pub struct MetadataChangeCursor {
    /// Prepare checksum of op 1, which names the history the cursor belongs to.
    #[pyo3(get)]
    log_id: u128,
    /// The operation of the last record, 0 before the first record.
    #[pyo3(get)]
    op: u64,
    /// Prepare checksum of `op`.
    #[pyo3(get)]
    checksum: u128,
    /// Position of the record within `op`.
    #[pyo3(get)]
    ordinal: u32,
}

/// Opaque continuation of a current state scan. Send it back unchanged in
/// `MetadataChangeStart.Current` with the same groups.
///
/// `str()` gives its `cs1_` text, which `parse` reads back.
#[derive(Clone, PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen, from_py_object)]
pub struct MetadataCurrentPageToken {
    inner: RustMetadataCurrentPageToken,
}

/// Where `get_metadata_changes` starts reading.
#[derive(Clone)]
#[gen_stub_pyclass_complex_enum]
#[pyclass(from_py_object)]
pub enum MetadataChangeStart {
    /// The oldest operation the requested groups still hold completely.
    Earliest(),
    /// No records, only a cursor at the newest retained record.
    Latest(),
    /// The records after `cursor`.
    After { cursor: MetadataChangeCursor },
    /// Current state pages: `None` starts a scan, a token continues it.
    #[pyo3(constructor = (page_token = None))]
    Current {
        page_token: Option<MetadataCurrentPageToken>,
    },
}

/// Family of change records and the unit of filtering. `str()` gives the
/// name the HTTP API uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, frozen, hash, from_py_object)]
pub enum MetadataChangeGroup {
    Streams,
    Topics,
    Partitions,
    ConsumerGroups,
    ConsumerGroupMembers,
    Users,
    PersonalAccessTokens,
    Sessions,
}

/// Kind of a change record. `str()` gives the name the HTTP API uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, frozen, hash, from_py_object)]
pub enum MetadataChangeKind {
    StreamCreated,
    StreamUpdated,
    StreamDeleted,
    TopicCreated,
    TopicUpdated,
    TopicDeleted,
    PartitionsChanged,
    ConsumerGroupCreated,
    ConsumerGroupDeleted,
    ConsumerGroupMemberJoined,
    ConsumerGroupMemberLeft,
    ConsumerGroupPartitionHandedOver,
    UserCreated,
    UserUpdated,
    UserDeleted,
    UserPasswordChanged,
    UserPermissionsUpdated,
    PersonalAccessTokenCreated,
    PersonalAccessTokenDeleted,
    SessionRegistered,
    SessionClosed,
}

/// Outcome of a change log query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, frozen, hash, from_py_object)]
pub enum MetadataChangeStatus {
    /// Records or items after the requested position. Apply them and keep
    /// `next`.
    Ok,
    /// The answering node has not applied the requested position yet. Ask
    /// again later or ask another node.
    Behind,
    /// The node no longer holds every record after the position. Rebuild
    /// from current state.
    Expired,
    /// The position belongs to another history. Rebuild from current state.
    Reset,
}

/// Whether a page carries change records or current state items.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, frozen, hash, from_py_object)]
pub enum MetadataChangePhase {
    Changes,
    Current,
}

/// Why a consumer group member was removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, frozen, hash, from_py_object)]
pub enum MetadataMemberLeftReason {
    Left,
    SessionEnded,
}

/// Why a personal access token was removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, frozen, hash, from_py_object)]
pub enum MetadataTokenDeletedReason {
    Deleted,
    Expired,
}

/// Why a session was closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, frozen, hash, from_py_object)]
pub enum MetadataSessionClosedReason {
    Logout,
    Expired,
    /// The user was deactivated or deleted.
    Revoked,
}

/// Why a user's public permission set changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, frozen, hash, from_py_object)]
pub enum MetadataPermissionsUpdatedReason {
    /// An `update_permissions` call replaced the set.
    Updated,
    /// A stream or topic delete pruned the user's grants on it.
    ResourceDeleted,
}

/// A stream after a create or update, and a `streams` current state item.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataStreamChange {
    /// Stream id. Ids are reused after a delete.
    #[pyo3(get)]
    stream_id: u32,
    /// The operation that created the stream, which tells it from an earlier
    /// stream with the same id.
    #[pyo3(get)]
    stream_created_op: u64,
    /// Stream creation time as Unix time in microseconds.
    #[pyo3(get)]
    created_at: u64,
    #[pyo3(get)]
    name: String,
    options: Option<ResourceOptions>,
}

/// A removed stream.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataStreamDeleted {
    #[pyo3(get)]
    stream_id: u32,
    #[pyo3(get)]
    stream_created_op: u64,
    #[pyo3(get)]
    created_at: u64,
    #[pyo3(get)]
    name: String,
    /// The operation of the delete request when the delete completed later.
    #[pyo3(get)]
    requested_op: Option<u64>,
}

/// A topic after a create or update.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataTopicChange {
    #[pyo3(get)]
    stream_id: u32,
    #[pyo3(get)]
    stream_created_op: u64,
    #[pyo3(get)]
    topic_id: u32,
    /// The operation that created the topic, which tells it from an earlier
    /// topic with the same id.
    #[pyo3(get)]
    topic_created_op: u64,
    #[pyo3(get)]
    created_at: u64,
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    partitions_count: u32,
    options: Option<ResourceOptions>,
}

/// A `topics` current state item. Partition counts come as `partitions`
/// items.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataTopicItem {
    #[pyo3(get)]
    stream_id: u32,
    #[pyo3(get)]
    stream_created_op: u64,
    #[pyo3(get)]
    topic_id: u32,
    #[pyo3(get)]
    topic_created_op: u64,
    #[pyo3(get)]
    created_at: u64,
    #[pyo3(get)]
    name: String,
    options: Option<ResourceOptions>,
}

/// A removed topic.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataTopicDeleted {
    #[pyo3(get)]
    stream_id: u32,
    #[pyo3(get)]
    stream_created_op: u64,
    #[pyo3(get)]
    topic_id: u32,
    #[pyo3(get)]
    topic_created_op: u64,
    #[pyo3(get)]
    created_at: u64,
    #[pyo3(get)]
    name: String,
    /// The operation of the delete request when the delete completed later.
    #[pyo3(get)]
    requested_op: Option<u64>,
}

/// A partition count change.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataPartitionsChanged {
    #[pyo3(get)]
    stream_id: u32,
    #[pyo3(get)]
    stream_created_op: u64,
    #[pyo3(get)]
    topic_id: u32,
    #[pyo3(get)]
    topic_created_op: u64,
    /// The partition count after the change.
    #[pyo3(get)]
    partitions_count: u32,
    /// The deleted partition ids, ascending, empty on create.
    #[pyo3(get)]
    removed_partition_ids: Vec<u32>,
    /// The operation of the delete request when the delete completed later.
    #[pyo3(get)]
    requested_op: Option<u64>,
}

/// A `partitions` current state item.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataPartitionsItem {
    #[pyo3(get)]
    stream_id: u32,
    #[pyo3(get)]
    stream_created_op: u64,
    #[pyo3(get)]
    topic_id: u32,
    #[pyo3(get)]
    topic_created_op: u64,
    #[pyo3(get)]
    partitions_count: u32,
}

/// A created consumer group with the topic's resulting group count.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataConsumerGroupCreated {
    #[pyo3(get)]
    stream_id: u32,
    #[pyo3(get)]
    stream_created_op: u64,
    #[pyo3(get)]
    topic_id: u32,
    #[pyo3(get)]
    topic_created_op: u64,
    #[pyo3(get)]
    group_id: u32,
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    consumer_groups_count: u32,
}

/// A removed consumer group with the topic's resulting group count.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataConsumerGroupDeleted {
    #[pyo3(get)]
    stream_id: u32,
    #[pyo3(get)]
    stream_created_op: u64,
    #[pyo3(get)]
    topic_id: u32,
    #[pyo3(get)]
    topic_created_op: u64,
    #[pyo3(get)]
    group_id: u32,
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    consumer_groups_count: u32,
    /// The operation of the delete request when the delete completed later.
    #[pyo3(get)]
    requested_op: Option<u64>,
}

/// A `consumer_groups` current state item.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataConsumerGroupItem {
    #[pyo3(get)]
    stream_id: u32,
    #[pyo3(get)]
    stream_created_op: u64,
    #[pyo3(get)]
    topic_id: u32,
    #[pyo3(get)]
    topic_created_op: u64,
    #[pyo3(get)]
    group_id: u32,
    #[pyo3(get)]
    name: String,
}

/// A member that joined a consumer group.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataMemberJoined {
    #[pyo3(get)]
    stream_id: u32,
    #[pyo3(get)]
    stream_created_op: u64,
    #[pyo3(get)]
    topic_id: u32,
    #[pyo3(get)]
    topic_created_op: u64,
    #[pyo3(get)]
    group_id: u32,
    #[pyo3(get)]
    member_id: u32,
    #[pyo3(get)]
    client_id: u128,
    #[pyo3(get)]
    members_count: u32,
    /// Installed partitions, normally none because a handover installs them.
    #[pyo3(get)]
    partition_ids: Vec<u32>,
}

/// A member that left a consumer group.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataMemberLeft {
    #[pyo3(get)]
    stream_id: u32,
    #[pyo3(get)]
    stream_created_op: u64,
    #[pyo3(get)]
    topic_id: u32,
    #[pyo3(get)]
    topic_created_op: u64,
    #[pyo3(get)]
    group_id: u32,
    #[pyo3(get)]
    member_id: u32,
    #[pyo3(get)]
    client_id: u128,
    #[pyo3(get)]
    members_count: u32,
    reason: MetadataMemberLeftReason,
}

/// A completed partition handover.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataPartitionHandedOver {
    #[pyo3(get)]
    stream_id: u32,
    #[pyo3(get)]
    stream_created_op: u64,
    #[pyo3(get)]
    topic_id: u32,
    #[pyo3(get)]
    topic_created_op: u64,
    #[pyo3(get)]
    group_id: u32,
    #[pyo3(get)]
    partition_id: u32,
    /// The new owner, `None` when the last member left.
    #[pyo3(get)]
    member_id: Option<u32>,
}

/// A `consumer_group_members` current state item.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataMemberItem {
    #[pyo3(get)]
    stream_id: u32,
    #[pyo3(get)]
    stream_created_op: u64,
    #[pyo3(get)]
    topic_id: u32,
    #[pyo3(get)]
    topic_created_op: u64,
    #[pyo3(get)]
    group_id: u32,
    #[pyo3(get)]
    member_id: u32,
    #[pyo3(get)]
    client_id: u128,
    /// Installed partitions, ascending.
    #[pyo3(get)]
    partition_ids: Vec<u32>,
}

/// A user after a create or update, and a `users` current state item.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataUserChange {
    /// User id. Ids are reused after a delete.
    #[pyo3(get)]
    user_id: u32,
    /// User creation time in microseconds, which tells the user from an
    /// earlier user with the same id.
    #[pyo3(get)]
    user_created_at: u64,
    #[pyo3(get)]
    username: String,
    status: UserStatus,
    permissions: Option<RustPermissions>,
    options: Option<ResourceOptions>,
}

/// A deleted user, or one whose password changed.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataUserIdentity {
    #[pyo3(get)]
    user_id: u32,
    #[pyo3(get)]
    user_created_at: u64,
    #[pyo3(get)]
    username: String,
}

/// A user's public permission set after a change.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataUserPermissionsUpdated {
    #[pyo3(get)]
    user_id: u32,
    #[pyo3(get)]
    user_created_at: u64,
    #[pyo3(get)]
    username: String,
    permissions: Option<RustPermissions>,
    reason: MetadataPermissionsUpdatedReason,
}

/// A created personal access token, and a `personal_access_tokens` current
/// state item. Never the token itself.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataTokenChange {
    #[pyo3(get)]
    user_id: u32,
    #[pyo3(get)]
    user_created_at: u64,
    #[pyo3(get)]
    name: String,
    /// Expiry in microseconds, `None` for a token that never expires.
    #[pyo3(get)]
    expiry: Option<u64>,
}

/// A removed personal access token.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataTokenDeleted {
    #[pyo3(get)]
    user_id: u32,
    #[pyo3(get)]
    user_created_at: u64,
    #[pyo3(get)]
    name: String,
    reason: MetadataTokenDeletedReason,
}

/// A registered session, and a `sessions` current state item.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataSession {
    #[pyo3(get)]
    user_id: u32,
    #[pyo3(get)]
    user_created_at: u64,
    #[pyo3(get)]
    client_id: u128,
    /// The session epoch.
    #[pyo3(get)]
    session: u64,
}

/// A closed session.
#[derive(PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen)]
pub struct MetadataSessionClosed {
    #[pyo3(get)]
    user_id: u32,
    #[pyo3(get)]
    user_created_at: u64,
    #[pyo3(get)]
    client_id: u128,
    #[pyo3(get)]
    session: u64,
    reason: MetadataSessionClosedReason,
}

/// A record of a group or kind this build does not know. A mirror stops on
/// it rather than skip a possible state change.
#[gen_stub_pyclass]
#[pyclass(frozen)]
pub struct MetadataUnknownChange {
    group: Py<PyAny>,
    kind: Py<PyAny>,
    body: Py<PyAny>,
}

/// A current state item of a group this build does not know.
#[gen_stub_pyclass]
#[pyclass(frozen)]
pub struct MetadataUnknownItem {
    group: Py<PyAny>,
    body: Py<PyAny>,
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataChanges {
    #[getter]
    fn status(&self) -> MetadataChangeStatus {
        self.status
    }

    /// `Changes` for `Earliest`, `Latest` and `After`, `Current` for state
    /// pages.
    #[getter]
    fn phase(&self) -> MetadataChangePhase {
        self.phase
    }

    /// The cursor to send back. Present on `Ok`, echoed on `Behind`, `None`
    /// on `Expired` and `Reset`. The last state page returns the anchor to
    /// follow from.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "MetadataChangeCursor | None"))]
    fn next(&self) -> Option<MetadataChangeCursor> {
        self.next
    }

    /// Change records in `(op, ordinal)` order, empty on state pages.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "builtins.list[MetadataChangeRecord]"))]
    fn records(&self, py: Python<'_>) -> Vec<Py<MetadataChangeRecord>> {
        self.records
            .iter()
            .map(|record| record.clone_ref(py))
            .collect()
    }

    /// Current state items, empty on change pages.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "builtins.list[MetadataCurrentItem]"))]
    fn items(&self, py: Python<'_>) -> Vec<Py<MetadataCurrentItem>> {
        self.items.iter().map(|item| item.clone_ref(py)).collect()
    }

    /// Continuation of a current state scan, `None` once it is complete.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "MetadataCurrentPageToken | None"))]
    fn current_page_token(&self) -> Option<MetadataCurrentPageToken> {
        self.current_page_token.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "MetadataChanges(status={}, phase={}, head_op={}, has_more={}, records={}, items={})",
            self.status.__str__(),
            self.phase.__str__(),
            self.head_op,
            self.has_more,
            self.records.len(),
            self.items.len()
        )
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataChangeRecord {
    /// The group, `None` for a record this build does not know.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "MetadataChangeGroup | None"))]
    fn group(&self) -> Option<MetadataChangeGroup> {
        self.group
    }

    /// The kind, `None` for a record this build does not know.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "MetadataChangeKind | None"))]
    fn kind(&self) -> Option<MetadataChangeKind> {
        self.kind
    }

    /// The typed body `kind` names, or `MetadataUnknownChange`.
    #[getter]
    #[gen_stub(override_return_type(
        type_repr = "MetadataStreamChange | MetadataStreamDeleted | MetadataTopicChange | MetadataTopicDeleted | MetadataPartitionsChanged | MetadataConsumerGroupCreated | MetadataConsumerGroupDeleted | MetadataMemberJoined | MetadataMemberLeft | MetadataPartitionHandedOver | MetadataUserChange | MetadataUserIdentity | MetadataUserPermissionsUpdated | MetadataTokenChange | MetadataTokenDeleted | MetadataSession | MetadataSessionClosed | MetadataUnknownChange"
    ))]
    fn change(&self, py: Python<'_>) -> Py<PyAny> {
        self.change.clone_ref(py)
    }

    fn __repr__(&self) -> String {
        let kind = self
            .kind
            .map_or("unknown", |kind| RustMetadataChangeKind::from(kind).into());
        format!(
            "MetadataChangeRecord(op={}, ordinal={}, kind={kind})",
            self.op, self.ordinal
        )
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataCurrentItem {
    /// The group, `None` for an item this build does not know.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "MetadataChangeGroup | None"))]
    fn group(&self) -> Option<MetadataChangeGroup> {
        self.group
    }

    /// The typed body `group` names, or `MetadataUnknownItem`.
    #[getter]
    #[gen_stub(override_return_type(
        type_repr = "MetadataStreamChange | MetadataTopicItem | MetadataPartitionsItem | MetadataConsumerGroupItem | MetadataMemberItem | MetadataUserChange | MetadataTokenChange | MetadataSession | MetadataUnknownItem"
    ))]
    fn body(&self, py: Python<'_>) -> Py<PyAny> {
        self.body.clone_ref(py)
    }

    fn __repr__(&self) -> String {
        let group = self.group.map_or("unknown", |group| {
            RustMetadataChangeGroup::from(group).into()
        });
        format!("MetadataCurrentItem(group={group})")
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataChangeCursor {
    #[new]
    fn new(log_id: u128, op: u64, checksum: u128, ordinal: u32) -> Self {
        Self {
            log_id,
            op,
            checksum,
            ordinal,
        }
    }

    /// Reads a cursor from its `mc1_` text.
    ///
    /// Raises:
    ///     ValueError: If the text is not a canonical cursor.
    #[staticmethod]
    fn parse(text: &str) -> PyResult<Self> {
        RustMetadataChangeCursor::from_str(text)
            .map(Self::from)
            .map_err(|_| PyValueError::new_err("invalid metadata change cursor"))
    }

    fn __str__(&self) -> String {
        RustMetadataChangeCursor::from(*self).to_string()
    }

    fn __repr__(&self) -> String {
        format!("MetadataChangeCursor.parse('{}')", self.__str__())
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataCurrentPageToken {
    /// Reads a page token from its `cs1_` text.
    ///
    /// Raises:
    ///     ValueError: If the text is not a canonical page token.
    #[staticmethod]
    fn parse(text: &str) -> PyResult<Self> {
        RustMetadataCurrentPageToken::from_str(text)
            .map(|inner| Self { inner })
            .map_err(|_| PyValueError::new_err("invalid metadata current page token"))
    }

    /// The cursor captured before the scan's first page.
    #[getter]
    fn anchor(&self) -> MetadataChangeCursor {
        self.inner.anchor().into()
    }

    /// The highest head any page of the scan reported so far.
    #[getter]
    fn required_head(&self) -> u64 {
        self.inner.required_head()
    }

    /// The groups the scan was started with, in code order.
    #[getter]
    fn groups(&self) -> Vec<MetadataChangeGroup> {
        self.inner.groups().iter().map(Into::into).collect()
    }

    /// The group of the last item returned.
    #[getter]
    fn last_group(&self) -> MetadataChangeGroup {
        self.inner.last_group().into()
    }

    /// The key of the last item returned, in the page token key layout.
    #[getter]
    fn last_key<'a>(&self, py: Python<'a>) -> Bound<'a, PyBytes> {
        PyBytes::new(py, self.inner.last_key())
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }

    fn __repr__(&self) -> String {
        format!("MetadataCurrentPageToken.parse('{}')", self.inner)
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataChangeGroup {
    fn __str__(&self) -> &'static str {
        RustMetadataChangeGroup::from(*self).into()
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataChangeKind {
    fn __str__(&self) -> &'static str {
        RustMetadataChangeKind::from(*self).into()
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataChangeStatus {
    fn __str__(&self) -> &'static str {
        RustMetadataChangeStatus::from(*self).into()
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataChangePhase {
    fn __str__(&self) -> &'static str {
        RustMetadataChangePhase::from(*self).into()
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataMemberLeftReason {
    fn __str__(&self) -> &'static str {
        RustMetadataMemberLeftReason::from(*self).into()
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataTokenDeletedReason {
    fn __str__(&self) -> &'static str {
        RustMetadataTokenDeletedReason::from(*self).into()
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataSessionClosedReason {
    fn __str__(&self) -> &'static str {
        RustMetadataSessionClosedReason::from(*self).into()
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataPermissionsUpdatedReason {
    fn __str__(&self) -> &'static str {
        RustMetadataPermissionsUpdatedReason::from(*self).into()
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataStreamChange {
    /// Options the creating client set explicitly, the same mapping
    /// `StreamDetails.options` returns, or `None` when `options_unreadable`.
    /// The stream's encryption key never appears here.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "UserHeaders | None"))]
    fn options<'a>(&self, py: Python<'a>) -> PyResult<Option<Bound<'a, UserHeaders>>> {
        options_view(py, self.options.as_ref(), true)
    }

    /// Options the server resolved or computed, including the key source and
    /// fingerprints. Same shape as `options`.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "UserHeaders | None"))]
    fn derived_options<'a>(&self, py: Python<'a>) -> PyResult<Option<Bound<'a, UserHeaders>>> {
        options_view(py, self.options.as_ref(), false)
    }

    /// The server could not read the stream's stored options. Keep the
    /// previous values.
    #[getter]
    fn options_unreadable(&self) -> bool {
        self.options.is_none()
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataTopicChange {
    /// Options the creating client set explicitly, the same mapping
    /// `TopicDetails.options` returns, or `None` when `options_unreadable`.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "UserHeaders | None"))]
    fn options<'a>(&self, py: Python<'a>) -> PyResult<Option<Bound<'a, UserHeaders>>> {
        options_view(py, self.options.as_ref(), true)
    }

    /// Options the server resolved for the keys the client did not send.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "UserHeaders | None"))]
    fn derived_options<'a>(&self, py: Python<'a>) -> PyResult<Option<Bound<'a, UserHeaders>>> {
        options_view(py, self.options.as_ref(), false)
    }

    /// The server could not read the topic's stored options. Keep the
    /// previous values.
    #[getter]
    fn options_unreadable(&self) -> bool {
        self.options.is_none()
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataTopicItem {
    /// Options the creating client set explicitly, or `None` when
    /// `options_unreadable`.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "UserHeaders | None"))]
    fn options<'a>(&self, py: Python<'a>) -> PyResult<Option<Bound<'a, UserHeaders>>> {
        options_view(py, self.options.as_ref(), true)
    }

    /// Options the server resolved for the keys the client did not send.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "UserHeaders | None"))]
    fn derived_options<'a>(&self, py: Python<'a>) -> PyResult<Option<Bound<'a, UserHeaders>>> {
        options_view(py, self.options.as_ref(), false)
    }

    /// The server could not read the topic's stored options.
    #[getter]
    fn options_unreadable(&self) -> bool {
        self.options.is_none()
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataMemberLeft {
    #[getter]
    fn reason(&self) -> MetadataMemberLeftReason {
        self.reason
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataUserChange {
    #[getter]
    fn status(&self) -> UserStatus {
        self.status
    }

    /// The public permission set, or `None` when the user has none.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "Permissions | None"))]
    fn permissions(&self) -> Option<Permissions> {
        self.permissions.clone().map(Permissions::from)
    }

    /// Options the creating client set explicitly, or `None` when
    /// `options_unreadable`.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "UserHeaders | None"))]
    fn options<'a>(&self, py: Python<'a>) -> PyResult<Option<Bound<'a, UserHeaders>>> {
        options_view(py, self.options.as_ref(), true)
    }

    /// Options the server resolved for the keys the client did not send.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "UserHeaders | None"))]
    fn derived_options<'a>(&self, py: Python<'a>) -> PyResult<Option<Bound<'a, UserHeaders>>> {
        options_view(py, self.options.as_ref(), false)
    }

    /// The server could not read the user's stored options.
    #[getter]
    fn options_unreadable(&self) -> bool {
        self.options.is_none()
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataUserPermissionsUpdated {
    /// The public permission set after the change, or `None` when the user
    /// has none.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "Permissions | None"))]
    fn permissions(&self) -> Option<Permissions> {
        self.permissions.clone().map(Permissions::from)
    }

    #[getter]
    fn reason(&self) -> MetadataPermissionsUpdatedReason {
        self.reason
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataTokenDeleted {
    #[getter]
    fn reason(&self) -> MetadataTokenDeletedReason {
        self.reason
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataSessionClosed {
    #[getter]
    fn reason(&self) -> MetadataSessionClosedReason {
        self.reason
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataUnknownChange {
    /// The group code on binary transports, or the group name over HTTP.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "builtins.int | builtins.str"))]
    fn group(&self, py: Python<'_>) -> Py<PyAny> {
        self.group.clone_ref(py)
    }

    /// The kind code on binary transports, or the kind name over HTTP.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "builtins.int | builtins.str"))]
    fn kind(&self, py: Python<'_>) -> Py<PyAny> {
        self.kind.clone_ref(py)
    }

    /// The raw body on binary transports, or the record's whole JSON object
    /// over HTTP.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "builtins.bytes | builtins.str"))]
    fn body(&self, py: Python<'_>) -> Py<PyAny> {
        self.body.clone_ref(py)
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MetadataUnknownItem {
    /// The group code on binary transports, or the group name over HTTP.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "builtins.int | builtins.str"))]
    fn group(&self, py: Python<'_>) -> Py<PyAny> {
        self.group.clone_ref(py)
    }

    /// The raw body on binary transports, or the item's whole JSON object
    /// over HTTP.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "builtins.bytes | builtins.str"))]
    fn body(&self, py: Python<'_>) -> Py<PyAny> {
        self.body.clone_ref(py)
    }
}

impl TryFrom<RustMetadataChanges> for MetadataChanges {
    type Error = PyErr;

    fn try_from(changes: RustMetadataChanges) -> PyResult<Self> {
        Python::attach(|py| {
            let mut records = Vec::with_capacity(changes.records.len());
            for record in changes.records {
                records.push(change_record(py, record)?);
            }
            let mut items = Vec::with_capacity(changes.items.len());
            for item in changes.items {
                items.push(current_item(py, item)?);
            }
            Ok(Self {
                status: changes.status.into(),
                phase: changes.phase.into(),
                log_id: changes.log_id,
                oldest_op: changes.oldest_op,
                retained_since: changes.retained_since,
                head_op: changes.head_op,
                next: changes.next.map(Into::into),
                has_more: changes.has_more,
                records,
                items,
                current_page_token: changes
                    .current_page_token
                    .map(|inner| MetadataCurrentPageToken { inner }),
            })
        })
    }
}

impl From<RustMetadataChangeCursor> for MetadataChangeCursor {
    fn from(cursor: RustMetadataChangeCursor) -> Self {
        Self {
            log_id: cursor.log_id,
            op: cursor.op,
            checksum: cursor.checksum,
            ordinal: cursor.ordinal,
        }
    }
}

impl From<MetadataChangeCursor> for RustMetadataChangeCursor {
    fn from(cursor: MetadataChangeCursor) -> Self {
        Self {
            log_id: cursor.log_id,
            op: cursor.op,
            checksum: cursor.checksum,
            ordinal: cursor.ordinal,
        }
    }
}

impl From<&MetadataChangeStart> for RustMetadataChangeStart {
    fn from(start: &MetadataChangeStart) -> Self {
        match start {
            MetadataChangeStart::Earliest() => Self::Earliest,
            MetadataChangeStart::Latest() => Self::Latest,
            MetadataChangeStart::After { cursor } => Self::After((*cursor).into()),
            MetadataChangeStart::Current { page_token } => {
                Self::Current(page_token.as_ref().map(|token| token.inner.clone()))
            }
        }
    }
}

macro_rules! mirror_enum {
    ($python:ident, $rust:ident, $($variant:ident),+) => {
        impl From<$rust> for $python {
            fn from(value: $rust) -> Self {
                match value {
                    $($rust::$variant => Self::$variant,)+
                }
            }
        }

        impl From<$python> for $rust {
            fn from(value: $python) -> Self {
                match value {
                    $($python::$variant => Self::$variant,)+
                }
            }
        }
    };
}

mirror_enum!(
    MetadataChangeGroup,
    RustMetadataChangeGroup,
    Streams,
    Topics,
    Partitions,
    ConsumerGroups,
    ConsumerGroupMembers,
    Users,
    PersonalAccessTokens,
    Sessions
);
mirror_enum!(
    MetadataChangeKind,
    RustMetadataChangeKind,
    StreamCreated,
    StreamUpdated,
    StreamDeleted,
    TopicCreated,
    TopicUpdated,
    TopicDeleted,
    PartitionsChanged,
    ConsumerGroupCreated,
    ConsumerGroupDeleted,
    ConsumerGroupMemberJoined,
    ConsumerGroupMemberLeft,
    ConsumerGroupPartitionHandedOver,
    UserCreated,
    UserUpdated,
    UserDeleted,
    UserPasswordChanged,
    UserPermissionsUpdated,
    PersonalAccessTokenCreated,
    PersonalAccessTokenDeleted,
    SessionRegistered,
    SessionClosed
);
mirror_enum!(
    MetadataChangeStatus,
    RustMetadataChangeStatus,
    Ok,
    Behind,
    Expired,
    Reset
);
mirror_enum!(
    MetadataChangePhase,
    RustMetadataChangePhase,
    Changes,
    Current
);
mirror_enum!(
    MetadataMemberLeftReason,
    RustMetadataMemberLeftReason,
    Left,
    SessionEnded
);
mirror_enum!(
    MetadataTokenDeletedReason,
    RustMetadataTokenDeletedReason,
    Deleted,
    Expired
);
mirror_enum!(
    MetadataSessionClosedReason,
    RustMetadataSessionClosedReason,
    Logout,
    Expired,
    Revoked
);
mirror_enum!(
    MetadataPermissionsUpdatedReason,
    RustMetadataPermissionsUpdatedReason,
    Updated,
    ResourceDeleted
);

/// Build the query `get_metadata_changes` sends and refuse it before any
/// I/O when the Rust SDK would.
///
/// # Errors
///
/// Raises `IggyError` with the `InvalidCommand` code for a query
/// `MetadataChangesQuery::validate` refuses, or for a time bound the
/// platform clock cannot hold.
pub(crate) fn validated_query(
    start: &MetadataChangeStart,
    groups: Option<Vec<MetadataChangeGroup>>,
    since: Option<u64>,
    until: Option<u64>,
    limit: Option<u32>,
) -> PyResult<MetadataChangesQuery> {
    let timestamp = |micros: u64| {
        IggyTimestamp::checked_from_micros(micros)
            .ok_or_else(|| to_runtime_error(IggyError::InvalidCommand))
    };
    let defaults = MetadataChangesQuery::default();
    let query = MetadataChangesQuery {
        start: start.into(),
        groups: groups.into_iter().flatten().map(Into::into).collect(),
        since: since.map(timestamp).transpose()?,
        until: until.map(timestamp).transpose()?,
        limit: limit.unwrap_or(defaults.limit),
    };
    query.validate().map_err(to_runtime_error)?;
    Ok(query)
}

fn change_record(
    py: Python<'_>,
    record: RustMetadataChangeRecord,
) -> PyResult<Py<MetadataChangeRecord>> {
    let kind = record.change.kind();
    Py::new(
        py,
        MetadataChangeRecord {
            op: record.op,
            ordinal: record.ordinal,
            checksum: record.checksum,
            timestamp: record.timestamp,
            actor_user_id: record.actor_user_id,
            group: kind.map(|kind| kind.group().into()),
            kind: kind.map(Into::into),
            change: change_body(py, record.change)?,
        },
    )
}

fn current_item(
    py: Python<'_>,
    item: RustMetadataCurrentItem,
) -> PyResult<Py<MetadataCurrentItem>> {
    Py::new(
        py,
        MetadataCurrentItem {
            group: item.group().map(Into::into),
            body: item_body(py, item)?,
        },
    )
}

fn change_body(py: Python<'_>, change: RustMetadataChange) -> PyResult<Py<PyAny>> {
    match change {
        RustMetadataChange::StreamCreated(body) | RustMetadataChange::StreamUpdated(body) => {
            MetadataStreamChange {
                stream_id: body.stream_id,
                stream_created_op: body.stream_created_op,
                created_at: body.created_at,
                name: body.name,
                options: readable_options(body.options),
            }
            .into_py_any(py)
        }
        RustMetadataChange::StreamDeleted(body) => MetadataStreamDeleted {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            created_at: body.created_at,
            name: body.name,
            requested_op: body.requested_op,
        }
        .into_py_any(py),
        RustMetadataChange::TopicCreated(body) | RustMetadataChange::TopicUpdated(body) => {
            MetadataTopicChange {
                stream_id: body.stream_id,
                stream_created_op: body.stream_created_op,
                topic_id: body.topic_id,
                topic_created_op: body.topic_created_op,
                created_at: body.created_at,
                name: body.name,
                partitions_count: body.partitions_count,
                options: readable_options(body.options),
            }
            .into_py_any(py)
        }
        RustMetadataChange::TopicDeleted(body) => MetadataTopicDeleted {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            created_at: body.created_at,
            name: body.name,
            requested_op: body.requested_op,
        }
        .into_py_any(py),
        RustMetadataChange::PartitionsChanged(body) => MetadataPartitionsChanged {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            partitions_count: body.partitions_count,
            removed_partition_ids: body.removed_partition_ids,
            requested_op: body.requested_op,
        }
        .into_py_any(py),
        RustMetadataChange::ConsumerGroupCreated(body) => MetadataConsumerGroupCreated {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            group_id: body.group_id,
            name: body.name,
            consumer_groups_count: body.consumer_groups_count,
        }
        .into_py_any(py),
        RustMetadataChange::ConsumerGroupDeleted(body) => MetadataConsumerGroupDeleted {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            group_id: body.group_id,
            name: body.name,
            consumer_groups_count: body.consumer_groups_count,
            requested_op: body.requested_op,
        }
        .into_py_any(py),
        RustMetadataChange::ConsumerGroupMemberJoined(body) => MetadataMemberJoined {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            group_id: body.group_id,
            member_id: body.member_id,
            client_id: body.client_id,
            members_count: body.members_count,
            partition_ids: body.partition_ids,
        }
        .into_py_any(py),
        RustMetadataChange::ConsumerGroupMemberLeft(body) => MetadataMemberLeft {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            group_id: body.group_id,
            member_id: body.member_id,
            client_id: body.client_id,
            members_count: body.members_count,
            reason: body.reason.into(),
        }
        .into_py_any(py),
        RustMetadataChange::ConsumerGroupPartitionHandedOver(body) => MetadataPartitionHandedOver {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            group_id: body.group_id,
            partition_id: body.partition_id,
            member_id: body.member_id,
        }
        .into_py_any(py),
        RustMetadataChange::UserCreated(body) | RustMetadataChange::UserUpdated(body) => {
            MetadataUserChange {
                user_id: body.user_id,
                user_created_at: body.user_created_at,
                username: body.username,
                status: body.status.into(),
                permissions: body.permissions,
                options: readable_options(body.options),
            }
            .into_py_any(py)
        }
        RustMetadataChange::UserDeleted(body) | RustMetadataChange::UserPasswordChanged(body) => {
            MetadataUserIdentity {
                user_id: body.user_id,
                user_created_at: body.user_created_at,
                username: body.username,
            }
            .into_py_any(py)
        }
        RustMetadataChange::UserPermissionsUpdated(body) => MetadataUserPermissionsUpdated {
            user_id: body.user_id,
            user_created_at: body.user_created_at,
            username: body.username,
            permissions: body.permissions,
            reason: body.reason.into(),
        }
        .into_py_any(py),
        RustMetadataChange::PersonalAccessTokenCreated(body) => MetadataTokenChange {
            user_id: body.user_id,
            user_created_at: body.user_created_at,
            name: body.name,
            expiry: body.expiry,
        }
        .into_py_any(py),
        RustMetadataChange::PersonalAccessTokenDeleted(body) => MetadataTokenDeleted {
            user_id: body.user_id,
            user_created_at: body.user_created_at,
            name: body.name,
            reason: body.reason.into(),
        }
        .into_py_any(py),
        RustMetadataChange::SessionRegistered(body) => MetadataSession {
            user_id: body.user_id,
            user_created_at: body.user_created_at,
            client_id: body.client_id,
            session: body.session,
        }
        .into_py_any(py),
        RustMetadataChange::SessionClosed(body) => MetadataSessionClosed {
            user_id: body.user_id,
            user_created_at: body.user_created_at,
            client_id: body.client_id,
            session: body.session,
            reason: body.reason.into(),
        }
        .into_py_any(py),
        RustMetadataChange::Unknown { group, kind, body } => MetadataUnknownChange {
            group: group.into_py_any(py)?,
            kind: kind.into_py_any(py)?,
            body: PyBytes::new(py, &body).into_any().unbind(),
        }
        .into_py_any(py),
        RustMetadataChange::UnknownJson { group, kind, body } => MetadataUnknownChange {
            group: group.into_py_any(py)?,
            kind: kind.into_py_any(py)?,
            body: body.get().into_py_any(py)?,
        }
        .into_py_any(py),
    }
}

fn item_body(py: Python<'_>, item: RustMetadataCurrentItem) -> PyResult<Py<PyAny>> {
    match item {
        RustMetadataCurrentItem::Stream(body) => MetadataStreamChange {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            created_at: body.created_at,
            name: body.name,
            options: readable_options(body.options),
        }
        .into_py_any(py),
        RustMetadataCurrentItem::Topic(body) => MetadataTopicItem {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            created_at: body.created_at,
            name: body.name,
            options: readable_options(body.options),
        }
        .into_py_any(py),
        RustMetadataCurrentItem::Partitions(body) => MetadataPartitionsItem {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            partitions_count: body.partitions_count,
        }
        .into_py_any(py),
        RustMetadataCurrentItem::ConsumerGroup(body) => MetadataConsumerGroupItem {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            group_id: body.group_id,
            name: body.name,
        }
        .into_py_any(py),
        RustMetadataCurrentItem::ConsumerGroupMember(body) => MetadataMemberItem {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            group_id: body.group_id,
            member_id: body.member_id,
            client_id: body.client_id,
            partition_ids: body.partition_ids,
        }
        .into_py_any(py),
        RustMetadataCurrentItem::User(body) => MetadataUserChange {
            user_id: body.user_id,
            user_created_at: body.user_created_at,
            username: body.username,
            status: body.status.into(),
            permissions: body.permissions,
            options: readable_options(body.options),
        }
        .into_py_any(py),
        RustMetadataCurrentItem::PersonalAccessToken(body) => MetadataTokenChange {
            user_id: body.user_id,
            user_created_at: body.user_created_at,
            name: body.name,
            expiry: body.expiry,
        }
        .into_py_any(py),
        RustMetadataCurrentItem::Session(body) => MetadataSession {
            user_id: body.user_id,
            user_created_at: body.user_created_at,
            client_id: body.client_id,
            session: body.session,
        }
        .into_py_any(py),
        RustMetadataCurrentItem::Unknown { group, body } => MetadataUnknownItem {
            group: group.into_py_any(py)?,
            body: PyBytes::new(py, &body).into_any().unbind(),
        }
        .into_py_any(py),
        RustMetadataCurrentItem::UnknownJson { group, body } => MetadataUnknownItem {
            group: group.into_py_any(py)?,
            body: body.get().into_py_any(py)?,
        }
        .into_py_any(py),
    }
}

fn readable_options(options: MetadataOptions) -> Option<ResourceOptions> {
    match options {
        MetadataOptions::Readable(options) => Some(options),
        MetadataOptions::Unreadable => None,
    }
}

fn options_view<'a>(
    py: Python<'a>,
    options: Option<&ResourceOptions>,
    explicit: bool,
) -> PyResult<Option<Bound<'a, UserHeaders>>> {
    options
        .map(|options| options_by_provenance(py, options, explicit))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use iggy::prelude::UserStatus as RustUserStatus;
    use iggy_binary_protocol::requests::system::GetMetadataChangesRequest;
    use iggy_binary_protocol::responses::system::{
        GetMetadataChangesResponse, WireMetadataChangeRecord, WireMetadataItem,
    };
    use iggy_binary_protocol::{WireDecode, WireEncode};
    use pyo3::types::{PyDict, PyString};
    use serde_json::{Map, Value};

    const FIXTURE: &str = include_str!("../tests/fixtures/metadata_changes.json");
    const ENVELOPE_KEYS: [&str; 7] = [
        "op",
        "ordinal",
        "checksum",
        "timestamp",
        "actor_user_id",
        "group",
        "kind",
    ];
    const OPTIONAL_BODY_KEYS: [&str; 3] = ["requested_op", "member_id", "expiry"];

    #[test]
    fn given_record_vectors_when_converting_binary_and_json_should_match_expected_json() {
        Python::initialize();
        Python::attach(|py| {
            for vector in vectors("records") {
                let bytes = decode_hex(&vector["record_hex"]);
                let (wire, consumed) =
                    WireMetadataChangeRecord::decode(&bytes).expect("record decodes");
                assert_eq!(consumed, bytes.len());
                let binary = RustMetadataChangeRecord::try_from(wire).expect("record converts");
                let json: RustMetadataChangeRecord =
                    serde_json::from_value(vector["expected"].clone()).expect("record parses");

                for record in [binary, json] {
                    let page = page_of(py, vec![record], vec![]);
                    let records = page.getattr("records").expect("records");
                    assert_record(&records.get_item(0).expect("record"), &vector["expected"]);
                }
            }
        });
    }

    #[test]
    fn given_item_vectors_when_converting_binary_and_json_should_match_expected_json() {
        Python::initialize();
        Python::attach(|py| {
            for vector in vectors("items") {
                let bytes = decode_hex(&vector["item_hex"]);
                let (wire, consumed) = WireMetadataItem::decode(&bytes).expect("item decodes");
                assert_eq!(consumed, bytes.len());
                let binary = RustMetadataCurrentItem::try_from(wire).expect("item converts");
                let json: RustMetadataCurrentItem =
                    serde_json::from_value(vector["expected"].clone()).expect("item parses");

                for item in [binary, json] {
                    let page = page_of(py, vec![], vec![item]);
                    let items = page.getattr("items").expect("items");
                    assert_item(&items.get_item(0).expect("item"), &vector["expected"]);
                }
            }
        });
    }

    #[test]
    fn given_response_vectors_when_converting_binary_and_json_should_match_expected_json() {
        Python::initialize();
        Python::attach(|py| {
            for vector in vectors("responses") {
                let bytes = decode_hex(&vector["response_hex"]);
                let (wire, consumed) =
                    GetMetadataChangesResponse::decode(&bytes).expect("response decodes");
                assert_eq!(consumed, bytes.len());
                let binary = RustMetadataChanges::try_from(wire).expect("response converts");
                let json: RustMetadataChanges =
                    serde_json::from_value(vector["expected"].clone()).expect("response parses");

                for changes in [binary, json] {
                    let page = Bound::new(py, MetadataChanges::try_from(changes).expect("page"))
                        .expect("page wraps");
                    assert_page(page.as_any(), &vector["expected"]);
                }
            }
        });
    }

    #[test]
    fn given_large_values_when_converting_should_keep_every_bit() {
        Python::initialize();
        let fixture: Value = serde_json::from_str(FIXTURE).expect("fixture parses");
        let vector = &fixture["large_values"];
        Python::attach(|py| {
            let bytes = decode_hex(&vector["record_hex"]);
            let (wire, _) = WireMetadataChangeRecord::decode(&bytes).expect("record decodes");
            let record = RustMetadataChangeRecord::try_from(wire).expect("record converts");
            let page = page_of(py, vec![record], vec![]);
            let record = page
                .getattr("records")
                .and_then(|records| records.get_item(0))
                .expect("record");
            let change = record.getattr("change").expect("change");

            assert_eq!(attribute::<u64>(&record, "op"), decimal(&vector["op"]));
            assert_eq!(
                attribute::<u64>(&record, "timestamp"),
                decimal(&vector["timestamp"])
            );
            assert_eq!(
                attribute::<u32>(&record, "ordinal"),
                vector["ordinal"]
                    .as_u64()
                    .map(|value| value as u32)
                    .expect("ordinal")
            );
            assert_eq!(
                attribute::<u128>(&record, "checksum"),
                hex(&vector["checksum"])
            );
            assert_eq!(
                attribute::<u64>(&change, "user_created_at"),
                decimal(&vector["user_created_at"])
            );
            assert_eq!(
                attribute::<u64>(&change, "session"),
                decimal(&vector["session"])
            );
            assert_eq!(
                attribute::<u128>(&change, "client_id"),
                hex(&vector["client_id"])
            );
        });
    }

    #[test]
    fn given_request_vectors_when_building_query_should_encode_fixture_bytes_and_http_query() {
        Python::initialize();
        for vector in vectors("requests") {
            let input = &vector["input"];
            let start = match input["start"].as_str().expect("start") {
                "earliest" => MetadataChangeStart::Earliest(),
                "latest" => MetadataChangeStart::Latest(),
                "after" => MetadataChangeStart::After {
                    cursor: RustMetadataChangeCursor::from_str(
                        input["cursor"].as_str().expect("cursor"),
                    )
                    .expect("cursor parses")
                    .into(),
                },
                "current" => MetadataChangeStart::Current {
                    page_token: input["page_token"]
                        .as_str()
                        .map(|text| MetadataCurrentPageToken {
                            inner: RustMetadataCurrentPageToken::from_str(text)
                                .expect("token parses"),
                        }),
                },
                other => panic!("unknown start {other}"),
            };
            let groups = input["groups"]
                .as_array()
                .expect("groups")
                .iter()
                .map(|name| {
                    RustMetadataChangeGroup::from_str(name.as_str().expect("group name"))
                        .expect("known group")
                        .into()
                })
                .collect();

            let query = validated_query(
                &start,
                Some(groups),
                input["since"].as_u64(),
                input["until"].as_u64(),
                input["limit"].as_u64().map(|limit| limit as u32),
            )
            .expect("query is accepted");

            assert_eq!(
                GetMetadataChangesRequest::from(&query).to_bytes().to_vec(),
                decode_hex(&vector["request_hex"]),
                "{}",
                vector["case"]
            );
            let http_query = query
                .http_query_pairs()
                .into_iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join("&");
            assert_eq!(http_query, vector["http_query"].as_str().expect("query"));
        }
    }

    #[test]
    fn given_refused_bodies_and_responses_when_decoding_should_fail() {
        let fixture: Value = serde_json::from_str(FIXTURE).expect("fixture parses");
        let refused = &fixture["refused"];
        for vector in refused["record_bodies"].as_array().expect("bodies") {
            let wire = WireMetadataChangeRecord {
                op: 1,
                ordinal: 0,
                checksum: 0,
                timestamp: 0,
                actor_user_id: None,
                group: vector["group"].as_u64().expect("group") as u8,
                kind: vector["kind"].as_u64().expect("kind") as u8,
                body: Bytes::from(decode_hex(&vector["body_hex"])),
            };
            assert!(
                RustMetadataChangeRecord::try_from(wire).is_err(),
                "{}",
                vector["case"]
            );
        }
        for vector in refused["responses"].as_array().expect("responses") {
            let bytes = decode_hex(&vector["hex"]);
            let decoded = GetMetadataChangesResponse::decode(&bytes)
                .map_err(|_| ())
                .and_then(|(response, _)| RustMetadataChanges::try_from(response).map_err(|_| ()));
            assert!(decoded.is_err(), "{}", vector["case"]);
        }
    }

    #[test]
    fn given_unknown_records_and_items_when_reading_twice_should_return_the_same_objects() {
        Python::initialize();
        let vector = vectors("responses")
            .into_iter()
            .find(|vector| vector["case"] == "unknown_kind_and_group")
            .expect("unknown vector");
        let bytes = decode_hex(&vector["response_hex"]);
        let (wire, _) = GetMetadataChangesResponse::decode(&bytes).expect("response decodes");
        let binary = RustMetadataChanges::try_from(wire).expect("response converts");
        let json: RustMetadataChanges =
            serde_json::from_value(vector["expected"].clone()).expect("response parses");
        let items = vec![
            RustMetadataCurrentItem::Unknown {
                group: 200,
                body: Bytes::from_static(&[0xde, 0xad]),
            },
            serde_json::from_str(r#"{"group":"future","value":1}"#).expect("item parses"),
        ];
        Python::attach(|py| {
            let records = binary.records.into_iter().chain(json.records).collect();
            let page = page_of(py, records, items);
            let records = page.getattr("records").expect("records");
            assert_eq!(records.len().expect("length"), 4);
            for record in records.try_iter().expect("records iterate") {
                let change = record.and_then(|record| record.getattr("change"));
                assert_same_on_every_read(&change.expect("change"), &["group", "kind", "body"]);
            }
            let items = page.getattr("items").expect("items");
            assert_eq!(items.len().expect("length"), 2);
            for item in items.try_iter().expect("items iterate") {
                let body = item.and_then(|item| item.getattr("body"));
                assert_same_on_every_read(&body.expect("body"), &["group", "body"]);
            }
        });
    }

    fn page_of<'py>(
        py: Python<'py>,
        records: Vec<RustMetadataChangeRecord>,
        items: Vec<RustMetadataCurrentItem>,
    ) -> Bound<'py, PyAny> {
        let changes = RustMetadataChanges {
            status: RustMetadataChangeStatus::Ok,
            phase: RustMetadataChangePhase::Changes,
            log_id: None,
            oldest_op: 0,
            retained_since: 0,
            head_op: 0,
            next: None,
            has_more: false,
            records,
            items,
            current_page_token: None,
        };
        Bound::new(
            py,
            MetadataChanges::try_from(changes).expect("page converts"),
        )
        .expect("page wraps")
        .into_any()
    }

    fn assert_page(page: &Bound<'_, PyAny>, expected: &Value) {
        assert_eq!(text(page, "status"), expected["status"]);
        assert_eq!(text(page, "phase"), expected["phase"]);
        assert_eq!(
            attribute::<Option<u128>>(page, "log_id"),
            expected["log_id"]
                .as_str()
                .map(|_| hex(&expected["log_id"]))
        );
        for key in ["oldest_op", "retained_since", "head_op"] {
            assert_eq!(
                attribute::<u64>(page, key),
                decimal(&expected[key]),
                "{key}"
            );
        }
        assert_eq!(
            optional_text(page, "next"),
            expected["next"].as_str().map(str::to_owned)
        );
        assert_eq!(
            attribute::<bool>(page, "has_more"),
            expected["has_more"].as_bool().expect("has_more")
        );
        let records = page.getattr("records").expect("records");
        let expected_records = expected["records"].as_array().expect("records");
        assert_eq!(records.len().expect("length"), expected_records.len());
        for (index, expected) in expected_records.iter().enumerate() {
            assert_record(&records.get_item(index).expect("record"), expected);
        }
        let items = page.getattr("items").expect("items");
        let expected_items = expected["items"].as_array().expect("items");
        assert_eq!(items.len().expect("length"), expected_items.len());
        for (index, expected) in expected_items.iter().enumerate() {
            assert_item(&items.get_item(index).expect("item"), expected);
        }
        assert_eq!(
            optional_text(page, "current_page_token"),
            expected["current_page_token"].as_str().map(str::to_owned)
        );
    }

    fn assert_record(record: &Bound<'_, PyAny>, expected: &Value) {
        let fields = expected.as_object().expect("record object");
        assert_eq!(attribute::<u64>(record, "op"), decimal(&fields["op"]));
        assert_eq!(
            attribute::<u32>(record, "ordinal"),
            fields["ordinal"]
                .as_u64()
                .map(|value| value as u32)
                .expect("ordinal")
        );
        assert_eq!(
            attribute::<u128>(record, "checksum"),
            hex(&fields["checksum"])
        );
        assert_eq!(
            attribute::<u64>(record, "timestamp"),
            decimal(&fields["timestamp"])
        );
        assert_eq!(
            attribute::<Option<u32>>(record, "actor_user_id"),
            fields
                .get("actor_user_id")
                .and_then(Value::as_u64)
                .map(|value| value as u32)
        );
        let change = record.getattr("change").expect("change");
        if record.getattr("kind").expect("kind").is_none() {
            assert!(record.getattr("group").expect("group").is_none());
            assert_unknown(&change, fields);
            return;
        }
        assert_eq!(text(record, "group"), fields["group"]);
        assert_eq!(text(record, "kind"), fields["kind"]);
        assert_body(&change, fields, &ENVELOPE_KEYS);
    }

    fn assert_item(item: &Bound<'_, PyAny>, expected: &Value) {
        let fields = expected.as_object().expect("item object");
        let body = item.getattr("body").expect("body");
        if item.getattr("group").expect("group").is_none() {
            assert_unknown(&body, fields);
            return;
        }
        assert_eq!(text(item, "group"), fields["group"]);
        assert_body(&body, fields, &["group"]);
    }

    /// An unknown record or item keeps its codes and its raw body: base64 in
    /// the binary rendering, the whole object over JSON.
    fn assert_unknown(unknown: &Bound<'_, PyAny>, fields: &Map<String, Value>) {
        let py = unknown.py();
        assert_eq!(text(unknown, "group"), fields["group"]);
        if fields.contains_key("kind") {
            assert_eq!(text(unknown, "kind"), fields["kind"]);
        }
        let body = unknown.getattr("body").expect("body");
        if let Ok(json) = body.extract::<String>() {
            let parsed: Value = serde_json::from_str(&json).expect("raw json");
            assert_eq!(parsed, Value::Object(fields.clone()));
        } else {
            let encoded = py
                .import("base64")
                .and_then(|base64| base64.call_method1("b64encode", (body,)))
                .and_then(|encoded| encoded.call_method0("decode"))
                .and_then(|encoded| encoded.extract::<String>())
                .expect("base64 body");
            assert_eq!(encoded, fields["body"].as_str().expect("body text"));
        }
    }

    fn assert_body(body: &Bound<'_, PyAny>, fields: &Map<String, Value>, skipped: &[&str]) {
        for (key, value) in fields
            .iter()
            .filter(|(key, _)| !skipped.contains(&key.as_str()))
        {
            match key.as_str() {
                "options" => assert_options(body, value),
                "permissions" => {
                    let permissions = body.getattr("permissions").expect("permissions");
                    let expected: Option<RustPermissions> =
                        serde_json::from_value(value.clone()).expect("permissions parse");
                    let actual = (!permissions.is_none()).then(|| {
                        permissions
                            .extract::<Permissions>()
                            .map_err(Into::<PyErr>::into)
                            .expect("permissions extract")
                            .inner
                    });
                    assert_eq!(actual, expected);
                }
                "status" => {
                    let expected = RustUserStatus::from_str(value.as_str().expect("status"))
                        .expect("known status");
                    assert_eq!(attribute::<UserStatus>(body, key), expected.into());
                }
                "client_id" => assert_eq!(attribute::<u128>(body, key), hex(value)),
                "reason" => assert_eq!(text(body, key), *value),
                _ => match value {
                    Value::Number(_) => assert_eq!(attribute::<u64>(body, key), decimal(value)),
                    Value::String(text) => assert_eq!(&attribute::<String>(body, key), text),
                    Value::Bool(flag) => assert_eq!(attribute::<bool>(body, key), *flag),
                    Value::Array(_) => assert_eq!(
                        attribute::<Vec<u32>>(body, key),
                        serde_json::from_value::<Vec<u32>>(value.clone()).expect("ids")
                    ),
                    Value::Null => assert!(body.getattr(key.as_str()).expect(key).is_none()),
                    Value::Object(_) => panic!("unexpected object field {key}"),
                },
            }
        }
        for key in OPTIONAL_BODY_KEYS {
            if !fields.contains_key(key)
                && let Ok(value) = body.getattr(key)
            {
                assert!(value.is_none(), "{key} must be absent");
            }
        }
    }

    /// Compare both provenance getters with the fixture's option map, or
    /// the unreadable marker when the map is null.
    fn assert_options(body: &Bound<'_, PyAny>, expected: &Value) {
        let py = body.py();
        if expected.is_null() {
            assert!(attribute::<bool>(body, "options_unreadable"));
            assert!(body.getattr("options").expect("options").is_none());
            assert!(body.getattr("derived_options").expect("derived").is_none());
            return;
        }
        assert!(!attribute::<bool>(body, "options_unreadable"));
        let expected = expected.as_object().expect("option map");
        for (getter, explicit) in [("options", true), ("derived_options", false)] {
            let actual = body
                .getattr(getter)
                .and_then(|headers| headers.call_method0("to_scalar_dict"))
                .expect("scalar options");
            let actual = actual.cast::<PyDict>().expect("dict");
            let wanted = PyDict::new(py);
            for (key, option) in expected
                .iter()
                .filter(|(_, option)| option["explicit"].as_bool() == Some(explicit))
            {
                let text = option["value"].as_str().expect("value text");
                let value: Bound<'_, PyAny> = match option["kind"].as_str() {
                    Some("uint64") => text
                        .parse::<u64>()
                        .expect("u64")
                        .into_bound_py_any(py)
                        .expect("int"),
                    Some("uint32") => text
                        .parse::<u32>()
                        .expect("u32")
                        .into_bound_py_any(py)
                        .expect("int"),
                    Some("bool") => text
                        .parse::<bool>()
                        .expect("bool")
                        .into_bound_py_any(py)
                        .expect("bool"),
                    _ => PyString::new(py, text).into_any(),
                };
                wanted.set_item(key, value).expect("set item");
            }
            assert!(
                actual.eq(&wanted).expect("comparable"),
                "{getter}: {actual} != {wanted}"
            );
        }
    }

    fn assert_same_on_every_read(object: &Bound<'_, PyAny>, names: &[&str]) {
        for name in names {
            let first = object.getattr(*name).expect("first read");
            let second = object.getattr(*name).expect("second read");
            assert!(first.is(&second), "{name} is rebuilt on every read");
        }
    }

    fn vectors(section: &str) -> Vec<Value> {
        let fixture: Value = serde_json::from_str(FIXTURE).expect("fixture parses");
        fixture[section].as_array().expect("vector list").clone()
    }

    fn attribute<'py, T: FromPyObjectOwned<'py>>(object: &Bound<'py, PyAny>, name: &str) -> T {
        object
            .getattr(name)
            .expect("attribute exists")
            .extract::<T>()
            .map_err(Into::<PyErr>::into)
            .expect("attribute extracts")
    }

    fn text(object: &Bound<'_, PyAny>, name: &str) -> String {
        object
            .getattr(name)
            .and_then(|value| value.str())
            .map(|value| value.to_string())
            .expect("attribute text")
    }

    fn optional_text(object: &Bound<'_, PyAny>, name: &str) -> Option<String> {
        let value = object.getattr(name).expect("attribute exists");
        (!value.is_none()).then(|| value.str().expect("text").to_string())
    }

    /// A u64 the fixture writes as a JSON number, or as decimal text above
    /// 2^53.
    fn decimal(value: &Value) -> u64 {
        value
            .as_u64()
            .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
            .expect("u64 value")
    }

    fn hex(value: &Value) -> u128 {
        u128::from_str_radix(value.as_str().expect("hex text"), 16).expect("u128 hex")
    }

    fn decode_hex(text: &Value) -> Vec<u8> {
        let text = text.as_str().expect("hex text");
        (0..text.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&text[index..index + 2], 16).expect("hex byte"))
            .collect()
    }
}
