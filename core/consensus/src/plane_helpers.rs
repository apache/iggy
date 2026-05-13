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

use crate::client_table::{Notify, RequestStatus};
use crate::{
    Consensus, IgnoreReason, Pipeline, PipelineEntry, PlaneKind, PrepareOkOutcome, Sequencer,
    Status, VsrConsensus,
};
use iggy_binary_protocol::consensus::iobuf::Owned;
use iggy_binary_protocol::{
    Command2, Message, PrepareHeader, PrepareOkHeader, ReplyHeader, RequestHeader,
};
use message_bus::{MessageBus, SendError};
use std::ops::AsyncFnOnce;

// TODO: Rework all of those helpers, once the boundaries are more clear and we have a better picture of the commonalities between all of the planes.

/// Shared request preflight: duplicate detection + pending registration.
///
/// Returns `Some(Notify)` if the request is new and should proceed through
/// consensus. Returns `None` if the request was already handled (duplicate
/// reply sent, in-progress, stale, or session error), the caller should
/// return early.
#[allow(clippy::future_not_send)]
pub async fn request_preflight<B, P>(
    consensus: &VsrConsensus<B, P>,
    client_id: u128,
    session: u64,
    request: u64,
) -> Option<Notify>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let status = consensus
        .client_table()
        .borrow()
        .check_request(client_id, session, request);
    match status {
        RequestStatus::Duplicate(cached_reply) => {
            // Best-effort resend, client may have disconnected.
            let _ = consensus
                .message_bus()
                .send_to_client(client_id, cached_reply.into_generic().into_frozen())
                .await;
            None
        }
        RequestStatus::InProgress
        | RequestStatus::Stale
        | RequestStatus::NoSession
        | RequestStatus::SessionMismatch { .. }
        | RequestStatus::RequestGap { .. }
        | RequestStatus::AlreadyRegistered { .. } => None,
        RequestStatus::New => {
            let notify = consensus
                .client_table()
                .borrow_mut()
                .register_pending(client_id, request);
            Some(notify)
        }
    }
}

/// Shared register preflight: duplicate detection for `Operation::Register`.
///
/// Returns `Some(Notify)` if the register is new and should proceed through
/// consensus. Returns `None` if the client is already registered (session
/// number sent back) or the register is already in progress.
#[allow(clippy::future_not_send, clippy::unused_async)]
pub async fn register_preflight<B, P>(
    consensus: &VsrConsensus<B, P>,
    client_id: u128,
) -> Option<Notify>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let status = consensus.client_table().borrow().check_register(client_id);
    match status {
        RequestStatus::AlreadyRegistered { session } => {
            // Synthesize a register reply with the existing session.
            // The caller can extract session from reply.header().commit.
            tracing::debug!(
                client_id,
                session,
                "register_preflight: client already registered, ignoring"
            );
            None
        }
        RequestStatus::InProgress => None,
        RequestStatus::New => {
            let notify = consensus
                .client_table()
                .borrow_mut()
                .register_pending(client_id, 0);
            Some(notify)
        }
        // check_register only returns AlreadyRegistered, InProgress, or New.
        other => {
            tracing::warn!(client_id, ?other, "register_preflight: unexpected status");
            None
        }
    }
}

/// Shared pipeline-first request flow used by metadata and partitions.
///
/// # Panics
/// - If the caller is not the primary.
/// - If the consensus status is not normal.
/// - If the consensus is syncing.
#[allow(clippy::future_not_send)]
pub async fn pipeline_prepare_common<C, F>(
    consensus: &C,
    plane: PlaneKind,
    prepare: C::Message<C::ReplicateHeader>,
    on_replicate: F,
) where
    C: Consensus,
    F: AsyncFnOnce(C::Message<C::ReplicateHeader>) -> (),
{
    assert!(!consensus.is_follower(), "on_request: primary only");
    assert!(consensus.is_normal(), "on_request: status must be normal");
    assert!(!consensus.is_syncing(), "on_request: must not be syncing");

    consensus.verify_pipeline();
    consensus.pipeline_message(plane, &prepare);
    on_replicate(prepare).await;
}

/// Shared commit-based old-prepare fence.
///
/// Uses `commit_min` (locally executed), not `commit_max`. A backup may know
/// that op 50 is committed (`commit_max = 50`) but only have executed up to
/// op 14 (`commit_min = 14`). A retransmitted prepare for op 15 must NOT be
/// fenced out, the backup still needs it in the WAL for `commit_journal`.
#[must_use]
pub const fn fence_old_prepare_by_commit<B, P>(
    consensus: &VsrConsensus<B, P>,
    header: &PrepareHeader,
) -> bool
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    header.op <= consensus.commit_min()
}

/// Shared chain-replication forwarding to the next replica.
///
/// Borrows the message, makes a deep copy for the wire, and lets the caller
/// retain ownership for journal append.
///
/// # Errors
///
/// Returns `SendError` if the bus fails to deliver to the next replica.
/// Callers decide error policy (VSR retransmits from WAL via prepare timeout).
///
/// # Panics
/// - If `header.command` is not `Command2::Prepare`.
/// - If `header.op <= consensus.commit_min()`.
/// - If the computed next replica equals self.
#[allow(clippy::future_not_send)]
pub async fn replicate_to_next_in_chain<B, P>(
    consensus: &VsrConsensus<B, P>,
    message: &Message<PrepareHeader>,
) -> Result<(), SendError>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let header = *message.header();

    assert_eq!(header.command, Command2::Prepare);
    assert!(header.op > consensus.commit_min());

    let next = (consensus.replica() + 1) % consensus.replica_count();
    let primary = consensus.primary_index(header.view);

    if next == primary {
        return Ok(());
    }

    assert_ne!(next, consensus.replica());

    // Chain replication to the next replica is N=1, so the freeze-once
    // trick does not apply: the caller has already appended `message` to
    // its local journal (durability-before-ack) and kept a reference for
    // this forward, so we deep_copy a fresh Frozen here. Future refactor
    // could freeze once and share the backing with the journal path.
    let frozen = message.deep_copy().into_generic().into_frozen();
    consensus.message_bus().send_to_replica(next, frozen).await
}

/// Shared preflight checks for `on_replicate`.
///
/// Returns current op on success.
///
/// # Errors
/// Returns a static error string if the replica is syncing, not in normal
/// status, or the message has a newer view.
///
/// # Panics
/// If `header.command` is not `Command2::Prepare`.
pub fn replicate_preflight<B, P>(
    consensus: &VsrConsensus<B, P>,
    header: &PrepareHeader,
) -> Result<u64, IgnoreReason>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    assert_eq!(header.command, Command2::Prepare);

    if consensus.is_syncing() {
        return Err(IgnoreReason::Syncing);
    }

    let current_op = consensus.sequencer().current_sequence();

    if consensus.status() != Status::Normal {
        return Err(IgnoreReason::NotNormal);
    }

    if header.view > consensus.view() {
        return Err(IgnoreReason::NewerView);
    }

    if consensus.is_follower() {
        consensus.advance_commit_max(header.commit);
    }

    Ok(current_op)
}

/// Shared preflight checks for `on_ack`.
///
/// # Errors
/// Returns a static error string if the replica is not primary or not in
/// normal status.
pub fn ack_preflight<B, P>(consensus: &VsrConsensus<B, P>) -> Result<(), IgnoreReason>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    if !consensus.is_primary() {
        return Err(IgnoreReason::NotPrimary);
    }

    if consensus.status() != Status::Normal {
        return Err(IgnoreReason::NotNormal);
    }

    Ok(())
}

/// Shared quorum tracking flow for ack handling.
///
/// After recording the ack, walks forward from `current_commit + 1` advancing
/// the commit number only while consecutive ops have achieved quorum. This
/// prevents committing ops that have gaps in quorum acknowledgment.
pub fn ack_quorum_reached<B, P>(
    consensus: &VsrConsensus<B, P>,
    plane: PlaneKind,
    ack: &PrepareOkHeader,
) -> bool
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    if !matches!(
        consensus.handle_prepare_ok(plane, ack),
        PrepareOkOutcome::Accepted {
            quorum_reached: true,
            ..
        }
    ) {
        return false;
    }

    let pipeline = consensus.pipeline().borrow();
    let mut new_commit = consensus.commit_max();
    while let Some(entry) = pipeline.entry_by_op(new_commit + 1) {
        if !entry.ok_quorum_received {
            break;
        }
        new_commit += 1;
    }
    drop(pipeline);

    if new_commit > consensus.commit_max() {
        consensus.advance_commit_max(new_commit);
        return true;
    }

    false
}

/// Drain and return committable prepares from the pipeline head.
///
/// Entries are drained only from the head and only while their op is covered
/// by the current commit frontier.
///
/// # Panics
/// If `head()` returns `Some` but `pop()` returns `None` (unreachable).
pub fn drain_committable_prefix<B, P>(consensus: &VsrConsensus<B, P>) -> Vec<PipelineEntry>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let commit = consensus.commit_max();
    let mut drained = Vec::new();
    let mut pipeline = consensus.pipeline().borrow_mut();

    while let Some(head_op) = pipeline.head().map(|entry| entry.header.op) {
        if head_op > commit {
            break;
        }

        let entry = pipeline
            .pop()
            .expect("drain_committable_prefix: head exists");
        drained.push(entry);
    }

    drained
}

/// Shared reply-message construction for committed prepare.
///
/// # Panics
/// If the constructed message buffer is not valid.
#[allow(clippy::needless_pass_by_value, clippy::cast_possible_truncation)]
pub fn build_reply_message<B, P>(
    consensus: &VsrConsensus<B, P>,
    prepare_header: &PrepareHeader,
    body: bytes::Bytes,
) -> Message<ReplyHeader>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let header_size = std::mem::size_of::<ReplyHeader>();
    let total_size = header_size + body.len();
    let mut buffer = bytes::BytesMut::zeroed(total_size);

    let header = bytemuck::checked::try_from_bytes_mut::<ReplyHeader>(&mut buffer[..header_size])
        .expect("zeroed bytes are valid");
    *header = ReplyHeader {
        checksum: 0,
        checksum_body: 0,
        cluster: consensus.cluster(),
        size: total_size as u32,
        view: consensus.view(),
        release: 0,
        command: Command2::Reply,
        replica: consensus.replica(),
        reserved_frame: [0; 66],
        request_checksum: prepare_header.request_checksum,
        context: 0,
        client: prepare_header.client,
        op: prepare_header.op,
        // Use the prepare's op, not commit_max. This value drives eviction
        // ordering in ClientTable, it must be deterministic across replicas.
        commit: prepare_header.op,
        timestamp: prepare_header.timestamp,
        request: prepare_header.request,
        operation: prepare_header.operation,
        namespace: prepare_header.namespace,
        ..Default::default()
    };

    if !body.is_empty() {
        buffer[header_size..].copy_from_slice(&body);
    }

    // TODO: Remove this copy once replies stop round-tripping through `Bytes`
    // and the binary protocol uses `Owned` end-to-end.
    Message::try_from(Owned::<4096>::copy_from_slice(buffer.as_ref()))
        .expect("reply buffer must contain a valid reply message")
}

/// Reply for fast paths that skip the VSR pipeline (e.g. `AckLevel::NoAck`).
///
/// Stamps `op` and `commit` with `commit_max` — monotonic, so
/// `ClientTable::commit_reply` regression checks always pass.
///
/// # Panics
/// If the constructed message buffer is not valid.
#[allow(clippy::needless_pass_by_value, clippy::cast_possible_truncation)]
pub fn build_reply_from_request<B, P>(
    consensus: &VsrConsensus<B, P>,
    request_header: &RequestHeader,
    body: bytes::Bytes,
) -> Message<ReplyHeader>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let header_size = std::mem::size_of::<ReplyHeader>();
    let total_size = header_size + body.len();
    let mut buffer = bytes::BytesMut::zeroed(total_size);

    let commit = consensus.commit_max();
    let header = bytemuck::checked::try_from_bytes_mut::<ReplyHeader>(&mut buffer[..header_size])
        .expect("zeroed bytes are valid");
    *header = ReplyHeader {
        checksum: 0,
        checksum_body: 0,
        cluster: consensus.cluster(),
        size: total_size as u32,
        view: consensus.view(),
        release: 0,
        command: Command2::Reply,
        replica: consensus.replica(),
        reserved_frame: [0; 66],
        request_checksum: request_header.request_checksum,
        context: 0,
        client: request_header.client,
        op: commit,
        commit,
        timestamp: request_header.timestamp,
        request: request_header.request,
        operation: request_header.operation,
        namespace: request_header.namespace,
        ..Default::default()
    };

    if !body.is_empty() {
        buffer[header_size..].copy_from_slice(&body);
    }

    Message::try_from(Owned::<4096>::copy_from_slice(buffer.as_ref()))
        .expect("reply buffer must contain a valid reply message")
}

/// Verify hash chain would not break if we add this header.
///
/// # Panics
/// If both headers share the same view and `current.parent != previous.checksum`.
pub fn panic_if_hash_chain_would_break_in_same_view(
    previous: &PrepareHeader,
    current: &PrepareHeader,
) {
    // If both headers are in the same view, parent must chain correctly.
    if previous.view == current.view {
        assert_eq!(
            current.parent, previous.checksum,
            "hash chain broken in same view: op={} parent={} expected={}",
            current.op, current.parent, previous.checksum
        );
    }
}

// TODO: Figure out how to make this check the journal if it contains the prepare.
/// # Panics
/// - If `header.command` is not `Command2::Prepare`.
/// - If `header.view > consensus.view()`.
#[allow(clippy::cast_possible_truncation, clippy::future_not_send)]
pub async fn send_prepare_ok<B, P>(
    consensus: &VsrConsensus<B, P>,
    header: &PrepareHeader,
    is_persisted: Option<bool>,
) where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    assert_eq!(header.command, Command2::Prepare);

    if consensus.status() != Status::Normal {
        return;
    }

    if consensus.is_syncing() {
        return;
    }

    if is_persisted == Some(false) {
        return;
    }

    assert!(
        header.view <= consensus.view(),
        "send_prepare_ok: prepare view {} > our view {}",
        header.view,
        consensus.view()
    );

    if header.op > consensus.sequencer().current_sequence() {
        return;
    }

    let prepare_ok_header = PrepareOkHeader {
        command: Command2::PrepareOk,
        cluster: consensus.cluster(),
        replica: consensus.replica(),
        view: consensus.view(),
        op: header.op,
        commit: consensus.commit_max(),
        timestamp: header.timestamp,
        parent: header.parent,
        prepare_checksum: header.checksum,
        request: header.request,
        operation: header.operation,
        namespace: header.namespace,
        size: std::mem::size_of::<PrepareOkHeader>() as u32,
        ..Default::default()
    };

    let message: Message<PrepareOkHeader> =
        Message::<PrepareOkHeader>::new(std::mem::size_of::<PrepareOkHeader>())
            .transmute_header(|_, new| *new = prepare_ok_header);
    let primary = consensus.primary_index(consensus.view());

    consensus
        .send_or_loopback(primary, message.into_generic())
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Consensus, LocalPipeline};
    use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
    use iggy_binary_protocol::consensus::iobuf::Frozen;
    use message_bus::SendError;

    #[derive(Debug, Default)]
    struct NoopBus;

    impl MessageBus for NoopBus {
        async fn send_to_client(
            &self,
            _client_id: u128,
            _data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            Ok(())
        }

        async fn send_to_replica(
            &self,
            _replica: u8,
            _data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            Ok(())
        }
    }

    fn prepare_message(op: u64, parent: u128, checksum: u128) -> Message<PrepareHeader> {
        Message::<PrepareHeader>::new(std::mem::size_of::<PrepareHeader>()).transmute_header(
            |_, new| {
                *new = PrepareHeader {
                    command: Command2::Prepare,
                    op,
                    parent,
                    checksum,
                    ..Default::default()
                };
            },
        )
    }

    #[test]
    fn loopback_push_and_drain() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();

        let mut buf = Vec::new();
        consensus.drain_loopback_into(&mut buf);
        assert!(buf.is_empty());

        let msg = Message::<PrepareOkHeader>::new(std::mem::size_of::<PrepareOkHeader>());
        consensus.push_loopback(msg.into_generic());
        consensus.drain_loopback_into(&mut buf);
        assert_eq!(buf.len(), 1);
        buf.clear();
        consensus.drain_loopback_into(&mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn loopback_cleared_on_view_change_reset() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();

        let msg = Message::<PrepareOkHeader>::new(std::mem::size_of::<PrepareOkHeader>());
        consensus.push_loopback(msg.into_generic());
        consensus.reset_view_change_state();
        let mut buf = Vec::new();
        consensus.drain_loopback_into(&mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn send_prepare_ok_pushes_to_loopback_when_primary() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();

        let prepare_header = PrepareHeader {
            command: Command2::Prepare,
            cluster: 1,
            view: 0,
            op: 0,
            checksum: 42,
            ..Default::default()
        };

        futures::executor::block_on(send_prepare_ok(&consensus, &prepare_header, Some(true)));

        let mut buf = Vec::new();
        consensus.drain_loopback_into(&mut buf);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0].header().command, Command2::PrepareOk);

        let typed: Message<PrepareOkHeader> = buf
            .remove(0)
            .try_into_typed()
            .expect("loopback message must be PrepareOk");
        assert_eq!(typed.header().command, Command2::PrepareOk);
    }

    #[test]
    fn loopback_cleared_on_complete_view_change_as_primary() {
        use iggy_binary_protocol::{DoViewChangeHeader, StartViewChangeHeader};

        // 3 replicas, replica 0 is primary for view 0 (and view 3: 3 % 3 = 0).
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();

        // SVC from replica 1 for view 3.
        // Replica 0 advances to view 3 (reset_view_change_state clears loopback),
        // records own SVC + DVC, and records replica 1's SVC. DVC quorum needs 2, have 1.
        let svc = StartViewChangeHeader {
            checksum: 0,
            checksum_body: 0,
            cluster: 0,
            size: 0,
            view: 3,
            release: 0,
            command: Command2::StartViewChange,
            replica: 1,
            reserved_frame: [0; 66],
            namespace: 0,
            reserved: [0; 120],
        };
        let _ = consensus.handle_start_view_change(PlaneKind::Metadata, &svc);

        // Simulate an in-flight loopback message queued between SVC and DVC quorum.
        let stale_msg = Message::<PrepareOkHeader>::new(std::mem::size_of::<PrepareOkHeader>());
        consensus.push_loopback(stale_msg.into_generic());

        // DVC from replica 2 for view 3 -- quorum reached, complete_view_change_as_primary fires.
        let dvc = DoViewChangeHeader {
            checksum: 0,
            checksum_body: 0,
            cluster: 0,
            size: 0,
            view: 3,
            release: 0,
            command: Command2::DoViewChange,
            replica: 2,
            reserved_frame: [0; 66],
            op: 0,
            commit: 0,
            namespace: 0,
            log_view: 0,
            reserved: [0; 100],
        };
        let actions = consensus.handle_do_view_change(PlaneKind::Metadata, &dvc);

        // View change completed: should have SendStartView action.
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, crate::VsrAction::SendStartView { .. })),
            "expected SendStartView action after DVC quorum"
        );

        // The stale loopback message must have been cleared.
        let mut buf = Vec::new();
        consensus.drain_loopback_into(&mut buf);
        assert!(
            buf.is_empty(),
            "loopback queue must be empty after view change completion"
        );
    }

    #[test]
    fn send_prepare_ok_sends_to_bus_when_not_primary() {
        let consensus = VsrConsensus::new(1, 1, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();

        let prepare_header = PrepareHeader {
            command: Command2::Prepare,
            cluster: 1,
            view: 0,
            op: 0,
            checksum: 42,
            ..Default::default()
        };

        futures::executor::block_on(send_prepare_ok(&consensus, &prepare_header, Some(true)));

        let mut buf = Vec::new();
        consensus.drain_loopback_into(&mut buf);
        assert!(buf.is_empty());
    }

    struct SpyBus {
        sent: std::cell::RefCell<Vec<(u8, Frozen<MESSAGE_ALIGN>)>>,
    }

    impl SpyBus {
        fn new() -> Self {
            Self {
                sent: std::cell::RefCell::new(Vec::new()),
            }
        }
    }

    #[allow(clippy::future_not_send)]
    impl MessageBus for SpyBus {
        async fn send_to_client(
            &self,
            _client_id: u128,
            _data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            Ok(())
        }
        async fn send_to_replica(
            &self,
            replica: u8,
            data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            self.sent.borrow_mut().push((replica, data));
            Ok(())
        }
    }

    #[test]
    fn send_or_loopback_routes_self_to_queue() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, SpyBus::new(), LocalPipeline::new());
        consensus.init();

        let msg = Message::<PrepareOkHeader>::new(std::mem::size_of::<PrepareOkHeader>());
        futures::executor::block_on(consensus.send_or_loopback(0, msg.into_generic()));

        let mut buf = Vec::new();
        consensus.drain_loopback_into(&mut buf);
        assert_eq!(buf.len(), 1);
        assert!(consensus.message_bus().sent.borrow().is_empty());
    }

    #[test]
    fn send_or_loopback_routes_other_to_bus() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, SpyBus::new(), LocalPipeline::new());
        consensus.init();

        let msg = Message::<PrepareOkHeader>::new(std::mem::size_of::<PrepareOkHeader>());
        futures::executor::block_on(consensus.send_or_loopback(1, msg.into_generic()));

        let mut buf = Vec::new();
        consensus.drain_loopback_into(&mut buf);
        assert!(buf.is_empty());

        let sent = consensus.message_bus().sent.borrow();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, 1);
    }

    #[test]
    fn drains_head_prefix_by_commit_frontier() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();

        consensus.pipeline_message(PlaneKind::Metadata, &prepare_message(1, 0, 10));
        consensus.pipeline_message(PlaneKind::Metadata, &prepare_message(2, 10, 20));
        consensus.pipeline_message(PlaneKind::Metadata, &prepare_message(3, 20, 30));

        consensus.advance_commit_max(3);

        let drained = drain_committable_prefix(&consensus);
        let drained_ops: Vec<_> = drained.into_iter().map(|entry| entry.header.op).collect();
        assert_eq!(drained_ops, vec![1, 2, 3]);
        assert!(consensus.pipeline().borrow().is_empty());
    }

    #[test]
    fn drains_only_up_to_commit_frontier_even_without_quorum_flags() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();

        consensus.pipeline_message(PlaneKind::Metadata, &prepare_message(5, 0, 50));
        consensus.pipeline_message(PlaneKind::Metadata, &prepare_message(6, 50, 60));
        consensus.pipeline_message(PlaneKind::Metadata, &prepare_message(7, 60, 70));

        consensus.advance_commit_max(6);
        let drained = drain_committable_prefix(&consensus);
        let drained_ops: Vec<_> = drained.into_iter().map(|entry| entry.header.op).collect();

        assert_eq!(drained_ops, vec![5, 6]);
        assert_eq!(
            consensus
                .pipeline()
                .borrow()
                .head()
                .map(|entry| entry.header.op),
            Some(7)
        );
    }
}
