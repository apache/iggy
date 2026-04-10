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

use crate::{Pipeline, PipelineEntry, Status, VsrAction, VsrConsensus};
use iggy_binary_protocol::Operation;
use iggy_common::sharding::IggyNamespace;
use message_bus::MessageBus;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaneKind {
    Metadata,
    Partitions,
}

impl PlaneKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::Partitions => "partitions",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaRole {
    Primary,
    Backup,
}

impl ReplicaRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Backup => "backup",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewChangeReason {
    NormalHeartbeatTimeout,
    ViewChangeStatusTimeout,
    ReceivedStartViewChange,
    ReceivedDoViewChange,
}

impl ViewChangeReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NormalHeartbeatTimeout => "normal_heartbeat_timeout",
            Self::ViewChangeStatusTimeout => "view_change_status_timeout",
            Self::ReceivedStartViewChange => "received_start_view_change",
            Self::ReceivedDoViewChange => "received_do_view_change",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IgnoreReason {
    NotPrimary,
    NotNormal,
    Syncing,
    NewerView,
    OlderView,
    OldPrepare,
    UnknownPrepare,
    DuplicateAck,
    ChecksumMismatch,
    InvalidOperation,
    ApplyFailed,
    PersistFailed,
}

impl IgnoreReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotPrimary => "not_primary",
            Self::NotNormal => "not_normal",
            Self::Syncing => "syncing",
            Self::NewerView => "newer_view",
            Self::OlderView => "older_view",
            Self::OldPrepare => "old_prepare",
            Self::UnknownPrepare => "unknown_prepare",
            Self::DuplicateAck => "duplicate_ack",
            Self::ChecksumMismatch => "checksum_mismatch",
            Self::InvalidOperation => "invalid_operation",
            Self::ApplyFailed => "apply_failed",
            Self::PersistFailed => "persist_failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlActionKind {
    SendStartViewChange,
    SendDoViewChange,
    SendStartView,
    SendPrepareOk,
    SendPrepare,
    RebuildPipeline,
    CommitJournal,
    SendCommit,
}

impl ControlActionKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SendStartViewChange => "send_start_view_change",
            Self::SendDoViewChange => "send_do_view_change",
            Self::SendStartView => "send_start_view",
            Self::SendPrepareOk => "send_prepare_ok",
            Self::SendPrepare => "send_prepare",
            Self::RebuildPipeline => "rebuild_pipeline",
            Self::CommitJournal => "commit_journal",
            Self::SendCommit => "send_commit",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimEventKind {
    ClientRequestReceived,
    PrepareQueued,
    PrepareAcked,
    OperationCommitted,
    ClientReplyEmitted,
    ViewChangeStarted,
    PrimaryElected,
    ReplicaStateChanged,
    NamespaceProgressUpdated,
    ControlMessageScheduled,
}

impl SimEventKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClientRequestReceived => "ClientRequestReceived",
            Self::PrepareQueued => "PrepareQueued",
            Self::PrepareAcked => "PrepareAcked",
            Self::OperationCommitted => "OperationCommitted",
            Self::ClientReplyEmitted => "ClientReplyEmitted",
            Self::ViewChangeStarted => "ViewChangeStarted",
            Self::PrimaryElected => "PrimaryElected",
            Self::ReplicaStateChanged => "ReplicaStateChanged",
            Self::NamespaceProgressUpdated => "NamespaceProgressUpdated",
            Self::ControlMessageScheduled => "ControlMessageScheduled",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamespaceLogContext {
    pub raw: u64,
    pub stream_id: Option<u32>,
    pub topic_id: Option<u32>,
    pub partition_id: Option<u32>,
}

impl NamespaceLogContext {
    #[must_use]
    pub fn from_raw(plane: PlaneKind, raw: u64) -> Self {
        if matches!(plane, PlaneKind::Metadata) {
            return Self {
                raw,
                stream_id: None,
                topic_id: None,
                partition_id: None,
            };
        }

        let namespace = IggyNamespace::from_raw(raw);
        #[allow(clippy::cast_possible_truncation)]
        let stream_id = namespace.stream_id() as u32;
        #[allow(clippy::cast_possible_truncation)]
        let topic_id = namespace.topic_id() as u32;
        #[allow(clippy::cast_possible_truncation)]
        let partition_id = namespace.partition_id() as u32;

        Self {
            raw,
            stream_id: Some(stream_id),
            topic_id: Some(topic_id),
            partition_id: Some(partition_id),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaLogContext {
    pub plane: PlaneKind,
    pub cluster_id: u128,
    pub replica_id: u8,
    pub namespace: NamespaceLogContext,
    pub view: u32,
    pub log_view: u32,
    pub commit: u64,
    pub status: Status,
    pub role: ReplicaRole,
}

impl ReplicaLogContext {
    #[must_use]
    pub fn from_consensus<B, P>(consensus: &VsrConsensus<B, P>, plane: PlaneKind) -> Self
    where
        B: MessageBus,
        P: Pipeline<Entry = PipelineEntry>,
    {
        let role = if consensus.is_primary() {
            ReplicaRole::Primary
        } else {
            ReplicaRole::Backup
        };

        Self {
            plane,
            cluster_id: consensus.cluster(),
            replica_id: consensus.replica(),
            namespace: NamespaceLogContext::from_raw(plane, consensus.namespace()),
            view: consensus.view(),
            log_view: consensus.log_view(),
            commit: consensus.commit_max(),
            status: consensus.status(),
            role,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestLogEvent {
    pub replica: ReplicaLogContext,
    pub client_id: u128,
    pub request_id: u64,
    pub operation: Operation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrepareLogEvent {
    pub replica: ReplicaLogContext,
    pub op: u64,
    pub parent_checksum: u128,
    pub prepare_checksum: u128,
    pub client_id: u128,
    pub request_id: u64,
    pub operation: Operation,
    pub pipeline_depth: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckLogEvent {
    pub replica: ReplicaLogContext,
    pub op: u64,
    pub prepare_checksum: u128,
    pub ack_from_replica: u8,
    pub ack_count: usize,
    pub quorum: usize,
    pub quorum_reached: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitLogEvent {
    pub replica: ReplicaLogContext,
    pub op: u64,
    pub client_id: u128,
    pub request_id: u64,
    pub operation: Operation,
    pub pipeline_depth: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewChangeLogEvent {
    pub replica: ReplicaLogContext,
    pub old_view: u32,
    pub new_view: u32,
    pub reason: ViewChangeReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlActionLogEvent {
    pub replica: ReplicaLogContext,
    pub action: ControlActionKind,
    pub target_replica: Option<u8>,
    pub op: Option<u64>,
    pub commit: Option<u64>,
}

impl ControlActionLogEvent {
    #[must_use]
    pub const fn from_vsr_action(replica: ReplicaLogContext, action: &VsrAction) -> Self {
        match *action {
            VsrAction::SendStartViewChange { .. } => Self {
                replica,
                action: ControlActionKind::SendStartViewChange,
                target_replica: None,
                op: None,
                commit: None,
            },
            VsrAction::SendDoViewChange {
                target, op, commit, ..
            } => Self {
                replica,
                action: ControlActionKind::SendDoViewChange,
                target_replica: Some(target),
                op: Some(op),
                commit: Some(commit),
            },
            VsrAction::SendStartView { op, commit, .. } => Self {
                replica,
                action: ControlActionKind::SendStartView,
                target_replica: None,
                op: Some(op),
                commit: Some(commit),
            },
            VsrAction::SendPrepareOk {
                target, from_op, ..
            } => Self {
                replica,
                action: ControlActionKind::SendPrepareOk,
                target_replica: Some(target),
                op: Some(from_op),
                commit: None,
            },
            VsrAction::RetransmitPrepares { .. } => Self {
                replica,
                action: ControlActionKind::SendPrepare,
                target_replica: None,
                op: None,
                commit: None,
            },
            VsrAction::RebuildPipeline { from_op, to_op, .. } => Self {
                replica,
                action: ControlActionKind::RebuildPipeline,
                target_replica: None,
                op: Some(from_op),
                commit: Some(to_op),
            },
            VsrAction::CommitJournal => Self {
                replica,
                action: ControlActionKind::CommitJournal,
                target_replica: None,
                op: None,
                commit: None,
            },
            VsrAction::SendCommit { commit, .. } => Self {
                replica,
                action: ControlActionKind::SendCommit,
                target_replica: None,
                op: None,
                commit: Some(commit),
            },
        }
    }
}

pub trait StructuredSimEvent {
    fn emit(&self, sim_event: SimEventKind);
}

#[must_use]
pub const fn status_as_str(status: Status) -> &'static str {
    match status {
        Status::Normal => "normal",
        Status::ViewChange => "view_change",
        Status::Recovering => "recovering",
    }
}

#[must_use]
pub const fn operation_as_str(operation: Operation) -> &'static str {
    match operation {
        Operation::Reserved => "reserved",
        Operation::CreateStream => "create_stream",
        Operation::UpdateStream => "update_stream",
        Operation::DeleteStream => "delete_stream",
        Operation::PurgeStream => "purge_stream",
        Operation::CreateTopic => "create_topic",
        Operation::UpdateTopic => "update_topic",
        Operation::DeleteTopic => "delete_topic",
        Operation::PurgeTopic => "purge_topic",
        Operation::CreatePartitions => "create_partitions",
        Operation::DeletePartitions => "delete_partitions",
        Operation::DeleteSegments => "delete_segments",
        Operation::CreateConsumerGroup => "create_consumer_group",
        Operation::DeleteConsumerGroup => "delete_consumer_group",
        Operation::CreateUser => "create_user",
        Operation::UpdateUser => "update_user",
        Operation::DeleteUser => "delete_user",
        Operation::ChangePassword => "change_password",
        Operation::UpdatePermissions => "update_permissions",
        Operation::CreatePersonalAccessToken => "create_personal_access_token",
        Operation::DeletePersonalAccessToken => "delete_personal_access_token",
        Operation::SendMessages => "send_messages",
        Operation::StoreConsumerOffset => "store_consumer_offset",
    }
}

#[must_use]
pub const fn namespace_component(component: Option<u32>) -> u32 {
    match component {
        Some(value) => value,
        None => 0,
    }
}

pub fn emit_sim_event<T>(sim_event: SimEventKind, event: &T)
where
    T: StructuredSimEvent,
{
    event.emit(sim_event);
}

pub fn emit_replica_event(sim_event: SimEventKind, ctx: &ReplicaLogContext) {
    tracing::event!(
        target: "iggy.sim",
        tracing::Level::DEBUG,
        sim_event = sim_event.as_str(),
        plane = ctx.plane.as_str(),
        cluster_id = ctx.cluster_id,
        replica_id = ctx.replica_id,
        namespace_raw = ctx.namespace.raw,
        stream_id = namespace_component(ctx.namespace.stream_id),
        topic_id = namespace_component(ctx.namespace.topic_id),
        partition_id = namespace_component(ctx.namespace.partition_id),
        view = ctx.view,
        log_view = ctx.log_view,
        commit = ctx.commit,
        status = status_as_str(ctx.status),
        role = ctx.role.as_str(),
    );
}

pub fn emit_namespace_progress_event(
    sim_event: SimEventKind,
    ctx: &ReplicaLogContext,
    op: u64,
    pipeline_depth: usize,
) {
    tracing::event!(
        target: "iggy.sim",
        tracing::Level::DEBUG,
        sim_event = sim_event.as_str(),
        plane = ctx.plane.as_str(),
        cluster_id = ctx.cluster_id,
        replica_id = ctx.replica_id,
        namespace_raw = ctx.namespace.raw,
        stream_id = namespace_component(ctx.namespace.stream_id),
        topic_id = namespace_component(ctx.namespace.topic_id),
        partition_id = namespace_component(ctx.namespace.partition_id),
        view = ctx.view,
        log_view = ctx.log_view,
        commit = ctx.commit,
        status = status_as_str(ctx.status),
        role = ctx.role.as_str(),
        op,
        pipeline_depth,
    );
}

impl StructuredSimEvent for RequestLogEvent {
    fn emit(&self, sim_event: SimEventKind) {
        let ctx = self.replica;
        tracing::event!(
            target: "iggy.sim",
            tracing::Level::DEBUG,
            sim_event = sim_event.as_str(),
            plane = ctx.plane.as_str(),
            cluster_id = ctx.cluster_id,
            replica_id = ctx.replica_id,
            namespace_raw = ctx.namespace.raw,
            stream_id = namespace_component(ctx.namespace.stream_id),
            topic_id = namespace_component(ctx.namespace.topic_id),
            partition_id = namespace_component(ctx.namespace.partition_id),
            view = ctx.view,
            log_view = ctx.log_view,
            commit = ctx.commit,
            status = status_as_str(ctx.status),
            role = ctx.role.as_str(),
            client_id = self.client_id,
            request_id = self.request_id,
            operation = operation_as_str(self.operation),
        );
    }
}

impl StructuredSimEvent for PrepareLogEvent {
    fn emit(&self, sim_event: SimEventKind) {
        let ctx = self.replica;
        tracing::event!(
            target: "iggy.sim",
            tracing::Level::DEBUG,
            sim_event = sim_event.as_str(),
            plane = ctx.plane.as_str(),
            cluster_id = ctx.cluster_id,
            replica_id = ctx.replica_id,
            namespace_raw = ctx.namespace.raw,
            stream_id = namespace_component(ctx.namespace.stream_id),
            topic_id = namespace_component(ctx.namespace.topic_id),
            partition_id = namespace_component(ctx.namespace.partition_id),
            view = ctx.view,
            log_view = ctx.log_view,
            commit = ctx.commit,
            status = status_as_str(ctx.status),
            role = ctx.role.as_str(),
            op = self.op,
            parent_checksum = self.parent_checksum,
            prepare_checksum = self.prepare_checksum,
            client_id = self.client_id,
            request_id = self.request_id,
            operation = operation_as_str(self.operation),
            pipeline_depth = self.pipeline_depth,
        );
    }
}

impl StructuredSimEvent for AckLogEvent {
    fn emit(&self, sim_event: SimEventKind) {
        let ctx = self.replica;
        tracing::event!(
            target: "iggy.sim",
            tracing::Level::DEBUG,
            sim_event = sim_event.as_str(),
            plane = ctx.plane.as_str(),
            cluster_id = ctx.cluster_id,
            replica_id = ctx.replica_id,
            namespace_raw = ctx.namespace.raw,
            stream_id = namespace_component(ctx.namespace.stream_id),
            topic_id = namespace_component(ctx.namespace.topic_id),
            partition_id = namespace_component(ctx.namespace.partition_id),
            view = ctx.view,
            log_view = ctx.log_view,
            commit = ctx.commit,
            status = status_as_str(ctx.status),
            role = ctx.role.as_str(),
            op = self.op,
            prepare_checksum = self.prepare_checksum,
            ack_from_replica = self.ack_from_replica,
            ack_count = self.ack_count,
            quorum = self.quorum,
            quorum_reached = self.quorum_reached,
        );
    }
}

impl StructuredSimEvent for CommitLogEvent {
    fn emit(&self, sim_event: SimEventKind) {
        let ctx = self.replica;
        tracing::event!(
            target: "iggy.sim",
            tracing::Level::DEBUG,
            sim_event = sim_event.as_str(),
            plane = ctx.plane.as_str(),
            cluster_id = ctx.cluster_id,
            replica_id = ctx.replica_id,
            namespace_raw = ctx.namespace.raw,
            stream_id = namespace_component(ctx.namespace.stream_id),
            topic_id = namespace_component(ctx.namespace.topic_id),
            partition_id = namespace_component(ctx.namespace.partition_id),
            view = ctx.view,
            log_view = ctx.log_view,
            commit = ctx.commit,
            status = status_as_str(ctx.status),
            role = ctx.role.as_str(),
            op = self.op,
            client_id = self.client_id,
            request_id = self.request_id,
            operation = operation_as_str(self.operation),
            pipeline_depth = self.pipeline_depth,
        );
    }
}

impl StructuredSimEvent for ViewChangeLogEvent {
    fn emit(&self, sim_event: SimEventKind) {
        let ctx = self.replica;
        tracing::event!(
            target: "iggy.sim",
            tracing::Level::DEBUG,
            sim_event = sim_event.as_str(),
            plane = ctx.plane.as_str(),
            cluster_id = ctx.cluster_id,
            replica_id = ctx.replica_id,
            namespace_raw = ctx.namespace.raw,
            stream_id = namespace_component(ctx.namespace.stream_id),
            topic_id = namespace_component(ctx.namespace.topic_id),
            partition_id = namespace_component(ctx.namespace.partition_id),
            view = ctx.view,
            log_view = ctx.log_view,
            commit = ctx.commit,
            status = status_as_str(ctx.status),
            role = ctx.role.as_str(),
            old_view = self.old_view,
            new_view = self.new_view,
            reason = self.reason.as_str(),
        );
    }
}

impl StructuredSimEvent for ControlActionLogEvent {
    fn emit(&self, sim_event: SimEventKind) {
        let ctx = self.replica;
        tracing::event!(
            target: "iggy.sim",
            tracing::Level::DEBUG,
            sim_event = sim_event.as_str(),
            plane = ctx.plane.as_str(),
            cluster_id = ctx.cluster_id,
            replica_id = ctx.replica_id,
            namespace_raw = ctx.namespace.raw,
            stream_id = namespace_component(ctx.namespace.stream_id),
            topic_id = namespace_component(ctx.namespace.topic_id),
            partition_id = namespace_component(ctx.namespace.partition_id),
            view = ctx.view,
            log_view = ctx.log_view,
            commit = ctx.commit,
            status = status_as_str(ctx.status),
            role = ctx.role.as_str(),
            action = self.action.as_str(),
            target_replica = self.target_replica.unwrap_or_default(),
            op = self.op.unwrap_or_default(),
            action_commit = self.commit.unwrap_or_default(),
        );
    }
}
