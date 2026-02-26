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

use crate::{Consensus, Pipeline, PipelineEntry, Sequencer, Status, VsrConsensus};
use iggy_common::header::{Command2, GenericHeader, PrepareHeader, PrepareOkHeader, ReplyHeader};
use iggy_common::message::Message;
use message_bus::MessageBus;
use std::ops::AsyncFnOnce;

// TODO: Rework all of those helpers, once the boundaries are more clear and we have a better picture of the commonalities between all of the planes.

/// Shared pipeline-first request flow used by metadata and partitions.
pub async fn pipeline_prepare_common<C, F>(
    consensus: &C,
    prepare: C::Message<C::ReplicateHeader>,
    on_replicate: F,
) where
    C: Consensus,
    C::Message<C::ReplicateHeader>: Clone,
    F: AsyncFnOnce(C::Message<C::ReplicateHeader>) -> (),
{
    assert!(!consensus.is_follower(), "on_request: primary only");
    assert!(consensus.is_normal(), "on_request: status must be normal");
    assert!(!consensus.is_syncing(), "on_request: must not be syncing");

    consensus.verify_pipeline();
    consensus.pipeline_message(prepare.clone());
    on_replicate(prepare.clone()).await;
}

/// Shared commit-based old-prepare fence.
pub fn fence_old_prepare_by_commit<B, P>(
    consensus: &VsrConsensus<B, P>,
    header: &PrepareHeader,
) -> bool
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
    P: Pipeline<Message = Message<PrepareHeader>, Entry = PipelineEntry>,
{
    header.op <= consensus.commit()
}

/// Shared chain-replication forwarding to the next replica.
pub async fn replicate_to_next_in_chain<B, P>(
    consensus: &VsrConsensus<B, P>,
    message: Message<PrepareHeader>,
) where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
    P: Pipeline<Message = Message<PrepareHeader>, Entry = PipelineEntry>,
{
    let header = message.header();

    assert_eq!(header.command, Command2::Prepare);
    assert!(header.op > consensus.commit());

    let next = (consensus.replica() + 1) % consensus.replica_count();
    let primary = consensus.primary_index(header.view);

    if next == primary {
        return;
    }

    assert_ne!(next, consensus.replica());

    // TODO: Propagate send error instead of panicking; requires bus error design.
    consensus
        .message_bus()
        .send_to_replica(next, message.into_generic())
        .await
        .unwrap();
}

/// Shared preflight checks for `on_replicate`.
///
/// Returns current op on success.
pub fn replicate_preflight<B, P>(
    consensus: &VsrConsensus<B, P>,
    header: &PrepareHeader,
) -> Result<u64, &'static str>
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
    P: Pipeline<Message = Message<PrepareHeader>, Entry = PipelineEntry>,
{
    assert_eq!(header.command, Command2::Prepare);

    if consensus.is_syncing() {
        return Err("sync");
    }

    let current_op = consensus.sequencer().current_sequence();

    if consensus.status() != Status::Normal {
        return Err("not normal state");
    }

    if header.view > consensus.view() {
        return Err("newer view");
    }

    if consensus.is_follower() {
        consensus.advance_commit_number(header.commit);
    }

    Ok(current_op)
}

/// Shared preflight checks for `on_ack`.
pub fn ack_preflight<B, P>(consensus: &VsrConsensus<B, P>) -> Result<(), &'static str>
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
    P: Pipeline<Message = Message<PrepareHeader>, Entry = PipelineEntry>,
{
    if !consensus.is_primary() {
        return Err("not primary");
    }

    if consensus.status() != Status::Normal {
        return Err("not normal");
    }

    Ok(())
}

/// Shared quorum tracking flow for ack handling.
///
/// After recording the ack, walks forward from `current_commit + 1` advancing
/// the commit number only while consecutive ops have achieved quorum. This
/// prevents committing ops that have gaps in quorum acknowledgment.
pub fn ack_quorum_reached<B, P>(consensus: &VsrConsensus<B, P>, ack: &PrepareOkHeader) -> bool
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
    P: Pipeline<Message = Message<PrepareHeader>, Entry = PipelineEntry>,
{
    if !consensus.handle_prepare_ok(ack) {
        return false;
    }

    let pipeline = consensus.pipeline().borrow();
    let mut new_commit = consensus.commit();
    while let Some(entry) = pipeline.message_by_op(new_commit + 1) {
        if !entry.ok_quorum_received {
            break;
        }
        new_commit += 1;
    }
    drop(pipeline);

    if new_commit > consensus.commit() {
        consensus.advance_commit_number(new_commit);
        return true;
    }

    false
}

/// Drain and return committable prepares from the pipeline head.
///
/// Entries are drained only from the head and only while their op is covered
/// by the current commit frontier.
pub fn drain_committable_prefix<B, P>(consensus: &VsrConsensus<B, P>) -> Vec<PipelineEntry>
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
    P: Pipeline<Message = Message<PrepareHeader>, Entry = PipelineEntry>,
{
    let commit = consensus.commit();
    let mut drained = Vec::new();
    let mut pipeline = consensus.pipeline().borrow_mut();

    while let Some(head_op) = pipeline.head().map(|entry| entry.header.op) {
        if head_op > commit {
            break;
        }

        let entry = pipeline
            .pop_message()
            .expect("drain_committable_prefix: head exists");
        drained.push(entry);
    }

    drained
}

/// Shared reply-message construction for committed prepare.
pub fn build_reply_message<B, P>(
    consensus: &VsrConsensus<B, P>,
    prepare_header: &PrepareHeader,
) -> Message<ReplyHeader>
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
    P: Pipeline<Message = Message<PrepareHeader>, Entry = PipelineEntry>,
{
    Message::<ReplyHeader>::new(std::mem::size_of::<ReplyHeader>()).transmute_header(|_, new| {
        *new = ReplyHeader {
            checksum: 0,
            checksum_body: 0,
            cluster: consensus.cluster(),
            size: std::mem::size_of::<ReplyHeader>() as u32,
            view: consensus.view(),
            release: 0,
            command: Command2::Reply,
            replica: consensus.replica(),
            reserved_frame: [0; 66],
            request_checksum: prepare_header.request_checksum,
            context: 0,
            op: prepare_header.op,
            commit: consensus.commit(),
            timestamp: prepare_header.timestamp,
            request: prepare_header.request,
            operation: prepare_header.operation,
            namespace: prepare_header.namespace,
            ..Default::default()
        };
    })
}

/// Verify hash chain would not break if we add this header.
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
pub async fn send_prepare_ok<B, P>(
    consensus: &VsrConsensus<B, P>,
    header: &PrepareHeader,
    is_persisted: Option<bool>,
) where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
    P: Pipeline<Message = Message<PrepareHeader>, Entry = PipelineEntry>,
{
    assert_eq!(header.command, Command2::Prepare);

    if consensus.status() != Status::Normal {
        return;
    }

    if consensus.is_syncing() {
        return;
    }

    if let Some(false) = is_persisted {
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
        commit: consensus.commit(),
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
    let generic_message = message.into_generic();
    let primary = consensus.primary_index(consensus.view());

    // TODO: Propagate send errors instead of panicking; requires bus error design.
    if primary == consensus.replica() {
        // TODO: Queue for self-processing or call handle_prepare_ok directly.
        // TODO: This is temporal, to test simulator, but we should send message to ourselves properly.
        consensus
            .message_bus()
            .send_to_replica(primary, generic_message)
            .await
            .unwrap();
    } else {
        consensus
            .message_bus()
            .send_to_replica(primary, generic_message)
            .await
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Consensus, LocalPipeline};
    use iggy_common::IggyError;

    #[derive(Debug, Default)]
    struct NoopBus;

    impl MessageBus for NoopBus {
        type Client = u128;
        type Replica = u8;
        type Data = Message<GenericHeader>;
        type Sender = ();

        fn add_client(&mut self, _client: Self::Client, _sender: Self::Sender) -> bool {
            true
        }

        fn remove_client(&mut self, _client: Self::Client) -> bool {
            true
        }

        fn add_replica(&mut self, _replica: Self::Replica) -> bool {
            true
        }

        fn remove_replica(&mut self, _replica: Self::Replica) -> bool {
            true
        }

        async fn send_to_client(
            &self,
            _client_id: Self::Client,
            _data: Self::Data,
        ) -> Result<(), IggyError> {
            Ok(())
        }

        async fn send_to_replica(
            &self,
            _replica: Self::Replica,
            _data: Self::Data,
        ) -> Result<(), IggyError> {
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
    fn drains_head_prefix_by_commit_frontier() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();

        consensus.pipeline_message(prepare_message(1, 0, 10));
        consensus.pipeline_message(prepare_message(2, 10, 20));
        consensus.pipeline_message(prepare_message(3, 20, 30));

        consensus.advance_commit_number(3);

        let drained = drain_committable_prefix(&consensus);
        let drained_ops: Vec<_> = drained.into_iter().map(|entry| entry.header.op).collect();
        assert_eq!(drained_ops, vec![1, 2, 3]);
        assert!(consensus.pipeline().borrow().is_empty());
    }

    #[test]
    fn drains_only_up_to_commit_frontier_even_without_quorum_flags() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, NoopBus, LocalPipeline::new());
        consensus.init();

        consensus.pipeline_message(prepare_message(5, 0, 50));
        consensus.pipeline_message(prepare_message(6, 50, 60));
        consensus.pipeline_message(prepare_message(7, 60, 70));

        consensus.advance_commit_number(6);
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
