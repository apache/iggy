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

use super::*;
use crate::shards_table::PapayaShardsTable;
use crate::{
    PartitionConsensusConfig, Receiver, ReplicaTopology, ShardIdentity, shard_mesh_channels,
};
use consensus::{LocalPipeline, PlaneKind, StateTransferStage};
use iggy_binary_protocol::{
    CommitHeader, DoViewChangeHeader, StartViewChangeHeader, StartViewHeader,
};
use iggy_common::{IggyByteSize, PartitionStats, variadic};
use journal::prepare_journal::PrepareJournal;
use journal::superblock::{PingPongSuperblock, SuperblockContents};
use message_bus::{BusMessage, SendError};
use metadata::impls::metadata::{IggySnapshot, PartitionPlacementView, StreamsFrontend};
use metadata::stm::stream::Streams;
use metadata::stm::user::Users;
use metadata::{IggyMetadata, MuxStateMachine};
use partitions::{IggyPartitions, PartitionPathLayout, PartitionsConfig};
use server_common::MESSAGE_ALIGN;
use server_common::iobuf::Frozen;
use server_common::sharding::{METADATA_GROUP, ShardId};
use std::cell::{Cell, RefCell};
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Poll;

const CLUSTER: u128 = 7;
const REPLICAS: u8 = 3;
const METADATA_VIEW: u32 = 3;
const PARTITION_VIEW: u32 = 4;
const CREATED_REVISION: u64 = 11;
const COMMITTED: u64 = 13;
/// The primary of `PARTITION_VIEW`.
const SOURCE: u8 = 1;
const TARGET: u8 = 2;
const NONCE: u128 = 17;

#[derive(Debug, Default)]
struct RecordingBus {
    sent: RefCell<Vec<(u8, PrimaryRebalanceHeader)>>,
    now: Cell<Option<Instant>>,
    disconnected: Cell<bool>,
}

#[allow(clippy::future_not_send, clippy::unused_async_trait_impl)]
impl MessageBus for RecordingBus {
    fn monotonic_now(&self) -> Instant {
        self.now.get().unwrap_or_else(Instant::now)
    }

    fn track_background(&self, _handle: message_bus::JoinHandle<()>) {}

    async fn send_to_client(
        &self,
        _client_id: u128,
        _data: impl Into<BusMessage>,
    ) -> Result<(), SendError> {
        panic!("readiness exchange must not send client traffic");
    }

    async fn send_to_replica(
        &self,
        replica: u8,
        frame: Frozen<MESSAGE_ALIGN>,
    ) -> Result<(), SendError> {
        if self.disconnected.get() {
            return Err(SendError::ReplicaNotConnected(replica));
        }
        assert_eq!(frame.len(), HEADER_SIZE);
        let mut message = Message::<PrimaryRebalanceHeader>::new(HEADER_SIZE);
        message.as_mut_slice().copy_from_slice(frame.as_slice());
        message.header().validate().unwrap();
        message.header().verify_frame().unwrap();
        self.sent.borrow_mut().push((replica, *message.header()));
        Ok(())
    }

    fn set_connection_lost_fn(&self, _callback: message_bus::ConnectionLostFn) {}
    fn set_replica_forward_fn(&self, _callback: message_bus::ReplicaForwardFn) {}
}

type TestShard<SB = PingPongSuperblock> = IggyShard<
    Rc<RecordingBus>,
    PrepareJournal,
    IggySnapshot,
    MuxStateMachine<variadic!(Users, Streams)>,
    PapayaShardsTable,
    SB,
>;

fn owner() -> (TestShard, Receiver<ShardFrame>) {
    owner_with_superblock(None)
}

fn owner_with_superblock<SB: SuperblockStore>(
    superblock: Option<Rc<SB>>,
) -> (TestShard<SB>, Receiver<ShardFrame>) {
    owner_at(superblock, 0, REPLICAS, METADATA_VIEW)
}

fn owner_at<SB: SuperblockStore>(
    superblock: Option<Rc<SB>>,
    replica: u8,
    replica_count: u8,
    view: u32,
) -> (TestShard<SB>, Receiver<ShardFrame>) {
    let bus = Rc::new(RecordingBus::default());
    bus.now.set(Some(Instant::now()));
    let mut consensus = VsrConsensus::new(
        CLUSTER,
        replica,
        replica_count,
        METADATA_GROUP,
        Rc::clone(&bus),
        LocalPipeline::new(),
    );
    consensus.set_view(view);
    consensus.init();
    let segment_size = IggyByteSize::from(1_048_576_u64);
    let partitions = IggyPartitions::new(
        ShardId::new(0),
        PartitionsConfig {
            messages_required_to_save: 100,
            size_of_messages_required_to_save: segment_size,
            validate_checksum: true,
            segment_size,
            preallocate_segments: false,
            key_context: Arc::default(),
            path_layout: PartitionPathLayout::default(),
        },
    );
    let metadata = IggyMetadata::new(
        Some(consensus),
        None,
        None,
        superblock,
        MuxStateMachine::<variadic!(Users, Streams)>::default(),
        None,
    )
    .with_applied_frontier(Arc::new(
        metadata::AppliedFrontier::default()
            .with_partition_freshness(bus.monotonic_now(), Duration::from_secs(5)),
    ));
    let mut shard = TestShard::<SB>::without_inbox(
        ShardIdentity::new(0, "primary-rebalance-test".to_owned()),
        Rc::clone(&bus),
        metadata,
        partitions,
        PapayaShardsTable::new(),
        PartitionConsensusConfig::new(CLUSTER, ReplicaTopology::new(replica, replica_count), bus),
    );
    let (senders, mut inboxes, _reply_inboxes) = shard_mesh_channels(1, 1, 1);
    shard.attach_senders(senders);
    (shard, inboxes[0].take().unwrap())
}

fn arm<SB: SuperblockStore>(shard: &TestShard<SB>, phase: MovePhase) -> PrimaryRebalanceHeader {
    let namespace = IggyNamespace::new(1, 1, 0);
    let consensus = shard.plane.metadata().consensus.as_ref().unwrap();
    let mut probe = probe_header(consensus, NONCE);
    probe.namespace = namespace.inner();
    probe.created_revision = CREATED_REVISION;
    probe.partition_view = PARTITION_VIEW;
    probe.commit = COMMITTED;
    probe.kind = if phase == MovePhase::Capture {
        PrimaryRebalanceKind::SourceProbe
    } else {
        PrimaryRebalanceKind::PartitionProbe
    };
    *shard.primary_rebalance.borrow_mut() = PrimaryRebalance {
        view: Some(METADATA_VIEW),
        peers: vec![Peer::default(); usize::from(REPLICAS)],
        eligible: vec![true; usize::from(REPLICAS)],
        pending: Some(PendingMove {
            plan: PartitionPrimaryTransfer {
                namespace,
                created_revision: CREATED_REVISION,
                view: PARTITION_VIEW,
                target: TARGET,
            },
            probe,
            phase,
            since: Instant::now(),
        }),
        ..PrimaryRebalance::default()
    };
    probe.kind = if phase == MovePhase::Capture {
        PrimaryRebalanceKind::SourceReply
    } else {
        PrimaryRebalanceKind::PartitionReply
    };
    probe.replica = if phase == MovePhase::Capture {
        consensus.primary_index(PARTITION_VIEW)
    } else {
        TARGET
    };
    probe.ready = 1;
    probe
}

#[compio::test]
async fn given_pending_target_probe_when_reply_is_stale_or_mismatched_should_not_grant() {
    for changed in [
        "cluster",
        "nonce",
        "metadata view",
        "namespace",
        "incarnation",
        "partition view",
        "commit",
        "target",
        "expired",
    ] {
        let (shard, inbox) = owner();
        let mut reply = arm(&shard, MovePhase::Probe);
        match changed {
            "cluster" => reply.cluster += 1,
            "nonce" => reply.nonce += 1,
            "metadata view" => reply.view += 1,
            "namespace" => reply.namespace += 1,
            "incarnation" => reply.created_revision += 1,
            "partition view" => reply.partition_view += 1,
            "commit" => reply.commit += 1,
            "target" => reply.replica = 0,
            "expired" => {
                shard
                    .primary_rebalance
                    .borrow_mut()
                    .pending
                    .as_mut()
                    .unwrap()
                    .since = Instant::now()
                    .checked_sub(PROBE_TIMEOUT + PROBE_INTERVAL)
                    .unwrap();
            }
            _ => unreachable!(),
        }
        shard.on_primary_rebalance(reply).await;
        assert!(
            inbox.try_recv().is_err(),
            "{changed} reply granted a transfer"
        );
        assert!(shard.bus.sent.borrow().is_empty());
        assert!(
            shard
                .primary_rebalance
                .borrow()
                .pending
                .as_ref()
                .is_some_and(|pending| pending.phase == MovePhase::Probe)
        );
    }
}

#[compio::test]
async fn given_primary_frontier_when_target_confirms_it_should_grant_the_source_primary_once() {
    let (shard, inbox) = owner();
    let mut source = arm(&shard, MovePhase::Capture);
    source.commit = COMMITTED + 1;
    let mut wrong_source = source;
    wrong_source.replica = TARGET;
    shard.on_primary_rebalance(wrong_source).await;
    assert!(shard.bus.sent.borrow().is_empty());
    assert!(inbox.try_recv().is_err());

    shard.on_primary_rebalance(source).await;
    let (target, probe) = shard.bus.sent.borrow()[0];
    assert_eq!(target, TARGET);
    assert_eq!(probe.kind, PrimaryRebalanceKind::PartitionProbe);
    assert_eq!(probe.commit, source.commit);
    assert_eq!(probe.nonce, source.nonce);
    assert_eq!(probe.replica, 0);
    assert!(inbox.try_recv().is_err());
    shard.on_primary_rebalance(source).await;
    assert_eq!(shard.bus.sent.borrow().len(), 1);

    let mut reply = probe;
    reply.kind = PrimaryRebalanceKind::PartitionReply;
    reply.replica = TARGET;
    reply.ready = 1;
    shard.on_primary_rebalance(reply).await;
    assert!(
        inbox.try_recv().is_err(),
        "the coordinator must not grant its own replica of the partition"
    );
    let (source_replica, grant) = shard.bus.sent.borrow()[1];
    assert_eq!(source_replica, source.replica);
    assert_eq!(grant.kind, PrimaryRebalanceKind::PrimaryTransfer);
    assert_eq!(grant.replica, 0);
    assert_eq!(grant.view, METADATA_VIEW);
    assert_eq!(grant.nonce, NONCE);
    assert_eq!(grant.namespace, probe.namespace);
    assert_eq!(grant.created_revision, CREATED_REVISION);
    assert_eq!(grant.partition_view, PARTITION_VIEW);
    assert_eq!(grant.target, TARGET);
    shard.on_primary_rebalance(reply).await;
    assert_eq!(
        shard.bus.sent.borrow().len(),
        2,
        "a replay must not issue a second grant"
    );

    let (source_node, source_inbox) =
        owner_at::<PingPongSuperblock>(None, source_replica, REPLICAS, METADATA_VIEW);
    let received_at = source_node.bus.monotonic_now();
    source_node.on_primary_rebalance(grant).await;
    let ShardFrame::Lifecycle(LifecycleFrame::PrimaryRebalance(frame)) =
        source_inbox.try_recv().unwrap()
    else {
        panic!("the source must route the grant to the partition's shard");
    };
    let LocalRebalance::Transfer { namespace, request } = *frame else {
        panic!("a grant must yield a transfer");
    };
    assert_eq!(namespace.inner(), probe.namespace);
    assert_eq!(request.created_revision, CREATED_REVISION);
    assert_eq!(request.expected_view, PARTITION_VIEW);
    assert_eq!(request.target, TARGET);
    assert_eq!(
        request.expires_at,
        received_at + GRANT_TIMEOUT,
        "the source must time the grant on its own clock"
    );
}

#[compio::test]
async fn given_a_grant_frame_when_its_sender_is_not_the_ready_metadata_primary_should_drop_it() {
    for changed in ["none", "cluster", "metadata view", "sender", "not ready"] {
        let (source_node, inbox) =
            owner_at::<PingPongSuperblock>(None, SOURCE, REPLICAS, METADATA_VIEW);
        let consensus = source_node.plane.metadata().consensus.as_ref().unwrap();
        let mut grant = probe_header(consensus, NONCE);
        grant.replica = consensus.primary_index(METADATA_VIEW);
        grant.kind = PrimaryRebalanceKind::PrimaryTransfer;
        grant.namespace = IggyNamespace::new(1, 1, 0).inner();
        grant.created_revision = CREATED_REVISION;
        grant.partition_view = PARTITION_VIEW;
        grant.target = TARGET;
        match changed {
            "none" => {}
            "cluster" => grant.cluster += 1,
            "metadata view" => grant.view += 1,
            "sender" => grant.replica = TARGET,
            "not ready" => consensus.begin_view_probe(),
            _ => unreachable!(),
        }
        source_node.on_primary_rebalance(grant).await;
        assert_eq!(inbox.try_recv().is_ok(), changed == "none", "{changed}");
    }
}

#[compio::test]
async fn given_the_coordinator_leads_the_partition_when_target_confirms_it_should_grant_through_its_own_ingress()
 {
    let (shard, inbox) = owner();
    let mut reply = arm(&shard, MovePhase::Probe);
    let consensus = shard.plane.metadata().consensus.as_ref().unwrap();
    // The coordinator leads the metadata view, so it leads this partition view too.
    let led_view = METADATA_VIEW;
    assert_eq!(consensus.primary_index(led_view), consensus.replica());
    {
        let mut state = shard.primary_rebalance.borrow_mut();
        let pending = state.pending.as_mut().unwrap();
        pending.plan.view = led_view;
        pending.probe.partition_view = led_view;
    }
    reply.partition_view = led_view;
    shard.on_primary_rebalance(reply).await;
    assert!(
        shard.bus.sent.borrow().is_empty(),
        "a local grant must not leave the node"
    );
    let ShardFrame::Lifecycle(LifecycleFrame::PrimaryRebalance(frame)) = inbox.try_recv().unwrap()
    else {
        panic!("the grant must enter the metadata owner's inbox");
    };
    let frame = *frame;
    let LocalRebalance::Wire(grant) = &frame else {
        panic!("a local grant must take the wire path");
    };
    assert_eq!(grant.kind, PrimaryRebalanceKind::PrimaryTransfer);
    shard.on_local_rebalance(frame).await;
    let ShardFrame::Lifecycle(LifecycleFrame::PrimaryRebalance(frame)) = inbox.try_recv().unwrap()
    else {
        panic!("the checked grant must reach the partition's shard");
    };
    let LocalRebalance::Transfer { request, .. } = *frame else {
        panic!("a grant must yield a transfer");
    };
    assert_eq!(request.expected_view, led_view);
    assert_eq!(request.target, TARGET);
}

#[compio::test]
async fn given_target_rejection_when_reply_is_correlated_should_drop_the_move_and_ignore_late_success()
 {
    let (shard, inbox) = owner();
    let mut reply = arm(&shard, MovePhase::Probe);
    reply.ready = 0;
    shard.on_primary_rebalance(reply).await;
    assert!(shard.primary_rebalance.borrow().pending.is_none());
    reply.ready = 1;
    shard.on_primary_rebalance(reply).await;
    assert!(inbox.try_recv().is_err());
    assert!(shard.bus.sent.borrow().is_empty());
}

/// Ticks the coordinator at `at` with every peer freshly ready, so only the
/// pending move can change.
#[allow(clippy::future_not_send)]
async fn tick_with_ready_peers(shard: &TestShard, at: Instant) {
    for peer in &mut shard.primary_rebalance.borrow_mut().peers {
        peer.ready_at = Some(at);
    }
    shard.bus.now.set(Some(at));
    shard.tick_primary_rebalance().await;
}

#[compio::test]
async fn given_a_sent_grant_when_the_new_view_is_unseen_should_hold_the_move_until_it_shows_or_times_out()
 {
    for shows in [true, false] {
        let (shard, _inbox) = owner();
        arm(&shard, MovePhase::Transfer);
        let metadata = shard.plane.metadata();
        let namespace = IggyNamespace::new(1, 1, 0);
        let streams = metadata.mux_stm.streams();
        streams.seed_namespace(namespace, namespace.inner(), PARTITION_VIEW);
        let created_revision = streams.created_revision_for_namespace(namespace).unwrap();
        let observe = |view| {
            metadata.observe_partition_views(&[PartitionPlacementView {
                namespace,
                created_revision,
                view,
            }]);
        };
        observe(PARTITION_VIEW);
        let sent_at = shard.bus.monotonic_now();
        {
            let mut state = shard.primary_rebalance.borrow_mut();
            let pending = state.pending.as_mut().unwrap();
            pending.plan.created_revision = created_revision;
            pending.since = sent_at;
        }
        let pending = || shard.primary_rebalance.borrow().pending.is_some();

        tick_with_ready_peers(&shard, sent_at + GRANT_TIMEOUT * 2).await;
        assert!(
            pending(),
            "an expired grant may have started a view change that is not visible yet"
        );
        if shows {
            observe(PARTITION_VIEW + 1);
            tick_with_ready_peers(&shard, sent_at + GRANT_TIMEOUT * 2).await;
        } else {
            tick_with_ready_peers(&shard, sent_at + TRANSFER_TIMEOUT).await;
            assert!(pending(), "the move lasts the whole transfer timeout");
            tick_with_ready_peers(&shard, sent_at + TRANSFER_TIMEOUT + Duration::from_nanos(1))
                .await;
        }
        assert!(!pending(), "shows={shows}");
    }
}

#[compio::test]
async fn given_an_unsent_grant_when_the_coordinator_ticks_should_plan_again_before_the_transfer_timeout()
 {
    let (shard, _inbox) = owner();
    let reply = arm(&shard, MovePhase::Probe);
    let metadata = shard.plane.metadata();
    let streams = metadata.mux_stm.streams();
    // Both partitions of the seeded topic start on the source, so a move
    // stays planned after the refused one.
    let last = IggyNamespace::new(1, 1, 1);
    streams.seed_namespace(last, last.inner(), PARTITION_VIEW);
    let views: Vec<_> = (0..=last.partition_id())
        .map(|partition| {
            let namespace = IggyNamespace::new(1, 1, partition);
            PartitionPlacementView {
                namespace,
                created_revision: streams.created_revision_for_namespace(namespace).unwrap(),
                view: PARTITION_VIEW,
            }
        })
        .collect();
    metadata.observe_partition_views(&views);
    shard
        .primary_rebalance
        .borrow_mut()
        .pending
        .as_mut()
        .unwrap()
        .plan
        .created_revision = views[0].created_revision;

    shard.bus.disconnected.set(true);
    shard.on_primary_rebalance(reply).await;
    shard.bus.disconnected.set(false);
    assert!(shard.bus.sent.borrow().is_empty());

    let replan_after = STABLE_INTERVAL * 2;
    assert!(replan_after < TRANSFER_TIMEOUT);
    tick_with_ready_peers(&shard, shard.bus.monotonic_now() + replan_after).await;
    assert!(
        shard
            .bus
            .sent
            .borrow()
            .iter()
            .any(|(replica, probe)| *replica == SOURCE
                && probe.kind == PrimaryRebalanceKind::SourceProbe),
        "the coordinator must plan again before the transfer timeout"
    );
}

fn suspect_primary(group: u64, view: u32) -> MessageBag {
    MessageBag::StartViewChange(
        Message::<StartViewChangeHeader>::new(HEADER_SIZE).transmute_header(
            |_, header: &mut StartViewChangeHeader| {
                header.cluster = CLUSTER;
                header.group = group;
                header.view = view;
                header.replica = 1;
                header.command = Command::StartViewChange;
                header.size = u32::try_from(HEADER_SIZE).unwrap();
            },
        ),
    )
}

#[compio::test]
async fn given_pending_target_probe_when_metadata_authority_changes_should_not_grant_while_handler_suspends()
 {
    let superblock = Rc::new(HeldSuperblock::default());
    let (shard, inbox) = owner_with_superblock(Some(Rc::clone(&superblock)));
    let reply = arm(&shard, MovePhase::Probe);
    let mut dispatch =
        pin!(shard.dispatch_message(suspect_primary(METADATA_GROUP, METADATA_VIEW + 1), None));
    assert_eq!(futures::poll!(&mut dispatch), Poll::Pending);
    assert!(superblock.entered.get());
    shard.on_primary_rebalance(reply).await;
    assert!(
        shard.bus.sent.borrow().is_empty(),
        "a reply handled during the suspension granted a move"
    );
    assert!(inbox.try_recv().is_err());
    assert!(shard.primary_rebalance.borrow().pending.is_none());
}

#[derive(Default)]
struct HeldSuperblock {
    entered: Cell<bool>,
}

#[allow(clippy::future_not_send)]
impl SuperblockStore for HeldSuperblock {
    async fn write(&self, _payload: &[u8]) -> std::io::Result<()> {
        self.entered.set(true);
        std::future::pending().await
    }

    fn read_latest(&self) -> impl Future<Output = std::io::Result<SuperblockContents>> {
        std::future::ready(Ok(SuperblockContents::Empty))
    }
}

#[test]
fn given_pending_move_when_unrelated_or_current_traffic_arrives_should_preserve_authority() {
    let partition_group = IggyNamespace::new(1, 1, 0).inner();
    let cases = [
        (partition_group, METADATA_VIEW + 1, false),
        (METADATA_GROUP, METADATA_VIEW - 1, false),
        // Includes a late vote for the view this replica already leads.
        (METADATA_GROUP, METADATA_VIEW, false),
        (METADATA_GROUP, METADATA_VIEW + 1, true),
    ];
    for command in [
        Command::StartViewChange,
        Command::DoViewChange,
        Command::Commit,
    ] {
        for (group, view, should_revoke) in cases {
            let (shard, _inbox) = owner();
            arm(&shard, MovePhase::Transfer);
            let message = match command {
                Command::StartViewChange => suspect_primary(group, view),
                Command::DoViewChange => MessageBag::DoViewChange(
                    Message::<DoViewChangeHeader>::new(HEADER_SIZE).transmute_header(
                        |_, header: &mut DoViewChangeHeader| {
                            header.cluster = CLUSTER;
                            header.group = group;
                            header.view = view;
                            header.replica = 1;
                            header.command = command;
                            header.size = u32::try_from(HEADER_SIZE).unwrap();
                        },
                    ),
                ),
                _ => {
                    MessageBag::Commit(Message::<CommitHeader>::new(HEADER_SIZE).transmute_header(
                        |_, header: &mut CommitHeader| {
                            header.cluster = CLUSTER;
                            header.group = group;
                            header.view = view;
                            header.replica = 1;
                            header.command = command;
                            header.size = u32::try_from(HEADER_SIZE).unwrap();
                        },
                    ))
                }
            };
            shard.fence_rebalance_authority(&message);
            assert_eq!(
                shard.primary_rebalance.borrow().pending.is_none(),
                should_revoke,
                "group={group}, view={view}, command={command:?}"
            );
        }
    }
}

#[compio::test]
async fn given_a_new_authority_when_peers_have_not_answered_should_keep_them_eligible_until_their_first_probe_times_out()
 {
    let (shard, _inbox) = owner_at::<PingPongSuperblock>(None, 0, REPLICAS, 0);
    let epoch = shard.bus.monotonic_now();
    let eligible = || shard.primary_rebalance.borrow().eligible.clone();
    shard.tick_primary_rebalance().await;
    assert_eq!(
        eligible(),
        [true; 3],
        "placement must not narrow to the coordinator before any peer answers"
    );

    let (replica, mut not_ready) = shard.bus.sent.borrow()[0];
    not_ready.replica = replica;
    not_ready.kind = PrimaryRebalanceKind::MetadataReply;
    shard.on_primary_rebalance(not_ready).await;
    shard.tick_primary_rebalance().await;
    assert_eq!(eligible(), [true, false, true]);

    shard.bus.now.set(Some(epoch + PROBE_TIMEOUT));
    shard.tick_primary_rebalance().await;
    assert_eq!(eligible(), [true, false, true]);
    shard
        .bus
        .now
        .set(Some(epoch + PROBE_TIMEOUT + Duration::from_nanos(1)));
    shard.tick_primary_rebalance().await;
    assert_eq!(
        eligible(),
        [true, false, false],
        "an unanswered first probe ends the presumption at its timeout"
    );
}

#[test]
fn given_health_challenge_when_reply_is_overdue_or_replayed_should_not_extend_readiness() {
    let (shard, _inbox) = owner();
    let consensus = shard.plane.metadata().consensus.as_ref().unwrap();
    let mut reply = probe_header(consensus, NONCE);
    reply.kind = PrimaryRebalanceKind::MetadataReply;
    reply.replica = TARGET;
    reply.ready = 1;
    for changed in ["nonce", "view", "replica", "expired", "unsent"] {
        let mut state = PrimaryRebalance {
            view: Some(METADATA_VIEW),
            peers: vec![Peer::default(); usize::from(REPLICAS)],
            ..PrimaryRebalance::default()
        };
        let previous = Instant::now()
            .checked_sub(PROBE_TIMEOUT + PROBE_INTERVAL)
            .unwrap();
        state.peers[usize::from(TARGET)] = Peer {
            nonce: Some(NONCE),
            sent_at: Some(Instant::now()),
            ready_at: Some(previous),
            answered_at: None,
            replied: false,
        };
        let mut invalid = reply;
        match changed {
            "nonce" => invalid.nonce += 1,
            "view" => invalid.view += 1,
            "replica" => invalid.replica = REPLICAS,
            "expired" => state.peers[usize::from(TARGET)].sent_at = Some(previous),
            "unsent" => state.peers[usize::from(TARGET)].sent_at = None,
            _ => unreachable!(),
        }
        state.observe_peer(&invalid, Instant::now());
        assert_eq!(
            state.peers[usize::from(TARGET)].ready_at,
            Some(previous),
            "{changed}"
        );
        assert_eq!(
            state.peers[usize::from(TARGET)].nonce,
            Some(NONCE),
            "{changed}"
        );
        state.peers[usize::from(TARGET)].sent_at = Some(Instant::now());
        state.observe_peer(&reply, Instant::now());
        let accepted_at = state.peers[usize::from(TARGET)].ready_at;
        assert!(accepted_at.is_some_and(|ready| ready > previous));
        assert_eq!(state.peers[usize::from(TARGET)].nonce, None);
        reply.ready = 0;
        state.observe_peer(&reply, Instant::now());
        assert_eq!(state.peers[usize::from(TARGET)].ready_at, accepted_at);
        reply.ready = 1;
    }
}

fn ready_partition(replica: u8) -> (IggyPartition<Rc<RecordingBus>>, PrimaryRebalanceHeader) {
    let (shard, _inbox) = owner();
    let probe = arm(&shard, MovePhase::Probe);
    let mut consensus = VsrConsensus::new(
        CLUSTER,
        replica,
        REPLICAS,
        probe.namespace,
        Rc::clone(&shard.bus),
        LocalPipeline::new(),
    );
    consensus.set_view(PARTITION_VIEW);
    consensus.init();
    consensus.restore_commit_state(COMMITTED, COMMITTED);
    let mut partition = IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        shard.plane.partitions().config().segment_size,
    );
    partition.set_created_revision(CREATED_REVISION);
    (partition, probe)
}

#[test]
fn given_partition_probe_when_local_history_is_not_ready_should_refuse() {
    for changed in [
        "incarnation",
        "view",
        "observed newer view",
        "prefix",
        "unapplied",
        "recovering",
        "unproved",
        "transfer",
        "ceded",
        "recovery barrier",
        "transition",
    ] {
        let (mut partition, mut probe) = ready_partition(TARGET);
        assert!(
            partition_ready(&partition, &probe),
            "ready fixture: {changed}"
        );
        match changed {
            "incarnation" => probe.created_revision += 1,
            "view" => probe.partition_view += 1,
            "observed newer view" => partition.consensus().observe_newer_view(PARTITION_VIEW + 1),
            "prefix" => probe.commit += 1,
            "unapplied" => partition.consensus().advance_commit_max(COMMITTED + 1),
            "recovering" => partition.consensus().begin_view_probe(),
            "unproved" => partition.consensus().fence_view_log(COMMITTED, COMMITTED),
            "transfer" => partition
                .consensus()
                .set_state_transfer_stage(StateTransferStage::AwaitingTarget),
            "ceded" => partition.consensus().init_as_backup(),
            "recovery barrier" => partition.consensus().set_recovery_barrier(COMMITTED + 1),
            "transition" => {
                partition.defer_view_truncation((COMMITTED + 1)..=u64::MAX, Vec::new(), None);
            }
            _ => unreachable!(),
        }
        assert!(!partition_ready(&partition, &probe), "{changed}");
    }
}

#[test]
fn given_busy_source_history_when_capturing_frontier_should_allow_drain_but_not_target_readiness() {
    let (mut partition, probe) = ready_partition(SOURCE);
    assert!(partition.consensus().is_primary());
    partition.consensus().advance_commit_max(COMMITTED + 1);
    assert!(partition_history_ready(&partition, &probe));
    assert!(!partition_ready(&partition, &probe));
    let request = PreparedPrimaryTransfer {
        created_revision: CREATED_REVISION,
        expected_view: PARTITION_VIEW,
        target: TARGET,
        expires_at: Instant::now() + GRANT_TIMEOUT,
    };
    assert!(partition.defer_primary_transfer(request));
    assert!(partition.history_is_busy());
    assert!(partition_history_ready(&partition, &probe));
    assert!(!partition_ready(&partition, &probe));
}

#[compio::test]
async fn given_three_or_five_replicas_when_a_quorum_answers_not_ready_should_credit_probe_send_times()
 {
    for replicas in [3, 5] {
        let (shard, _inbox) = owner_at::<PingPongSuperblock>(None, 0, replicas, 0);
        let consensus = shard.plane.metadata().consensus.as_ref().unwrap();
        let frontier = shard.plane.metadata().applied_frontier();
        let epoch = shard.bus.monotonic_now();
        shard.tick_primary_rebalance().await;
        assert_eq!(shard.bus.sent.borrow().len(), usize::from(replicas - 1));
        assert!(
            shard
                .bus
                .sent
                .borrow()
                .iter()
                .all(|(_, header)| header.quorum_age_ns
                    == PrimaryRebalanceHeader::NO_QUORUM_EVIDENCE)
        );
        assert!(!frontier.partition_requests_allowed(epoch));
        shard.bus.now.set(Some(epoch + Duration::from_secs(3)));
        for peer in 1..=2 {
            let mut reply = shard.bus.sent.borrow()[usize::from(peer - 1)].1;
            reply.replica = peer;
            reply.kind = PrimaryRebalanceKind::MetadataReply;
            reply.ready = 0;
            // The second challenge was sent later; receipt time is identical.
            if peer == 2 {
                shard.primary_rebalance.borrow_mut().peers[usize::from(peer)].sent_at =
                    Some(epoch + Duration::from_secs(2));
            }
            shard.on_primary_rebalance(reply).await;
            assert_eq!(
                frontier.partition_requests_allowed(shard.bus.monotonic_now()),
                replicas == 3 || peer == 2
            );
        }
        let deadline = epoch + Duration::from_secs(if replicas == 3 { 10 } else { 8 });
        assert!(
            frontier
                .partition_requests_allowed(deadline.checked_sub(Duration::from_nanos(1)).unwrap())
        );
        assert!(!frontier.partition_requests_allowed(deadline));
        assert_eq!(
            shard
                .primary_rebalance
                .borrow()
                .quorum_evidence(consensus.quorum_replication()),
            Some(epoch + Duration::from_secs(if replicas == 3 { 2 } else { 0 }))
        );
        consensus.begin_view_probe();
        shard.tick_primary_rebalance().await;
        assert!(
            frontier
                .partition_requests_allowed(deadline.checked_sub(Duration::from_nanos(1)).unwrap()),
            "authority loss must not shorten credit"
        );
        assert!(!frontier.partition_requests_allowed(deadline));
    }
}

#[compio::test]
async fn given_a_refused_probe_when_the_interval_elapses_should_probe_again_without_waiting_for_the_timeout()
 {
    let (shard, _inbox) = owner_at::<PingPongSuperblock>(None, 0, REPLICAS, 0);
    let epoch = shard.bus.monotonic_now();
    shard.bus.disconnected.set(true);
    shard.tick_primary_rebalance().await;
    shard.bus.disconnected.set(false);
    shard.bus.now.set(Some(
        (epoch + PROBE_INTERVAL)
            .checked_sub(Duration::from_nanos(1))
            .unwrap(),
    ));
    shard.tick_primary_rebalance().await;
    assert!(
        shard.bus.sent.borrow().is_empty(),
        "a refused probe must still pace the peer"
    );
    shard.bus.now.set(Some(epoch + PROBE_INTERVAL));
    shard.tick_primary_rebalance().await;
    assert_eq!(shard.bus.sent.borrow().len(), usize::from(REPLICAS - 1));
}

#[compio::test]
async fn given_admission_opening_when_peers_answered_earlier_probes_should_reprobe_each_once_with_evidence()
 {
    let (primary, _inbox) = owner_at::<PingPongSuperblock>(None, 0, REPLICAS, 0);
    let (backup, _backup_inbox) = owner_at::<PingPongSuperblock>(None, 1, REPLICAS, 0);
    let epoch = primary.bus.monotonic_now();
    let advance = |millis| {
        primary
            .bus
            .now
            .set(Some(epoch + Duration::from_millis(millis)));
    };
    let sent = |index: usize| primary.bus.sent.borrow()[index];
    let answer = |index: usize| {
        let (replica, mut reply) = sent(index);
        reply.replica = replica;
        reply.kind = PrimaryRebalanceKind::MetadataReply;
        reply
    };

    primary.tick_primary_rebalance().await;
    advance(10);
    primary.on_primary_rebalance(answer(0)).await;
    assert!(
        primary
            .plane
            .metadata()
            .applied_frontier()
            .partition_requests_allowed(primary.bus.monotonic_now())
    );
    advance(20);
    primary.tick_primary_rebalance().await;
    assert_eq!(
        primary.bus.sent.borrow().len(),
        3,
        "only the peer that already answered is probed again"
    );
    let (replica, reprobe) = sent(2);
    assert_eq!(replica, 1);
    assert_ne!(
        reprobe.quorum_age_ns,
        PrimaryRebalanceHeader::NO_QUORUM_EVIDENCE
    );

    let backup_frontier = backup.plane.metadata().applied_frontier();
    backup.on_primary_rebalance(sent(0).1).await;
    assert!(!backup_frontier.partition_requests_allowed(backup.bus.monotonic_now()));
    backup.on_primary_rebalance(reprobe).await;
    assert!(backup_frontier.partition_requests_allowed(backup.bus.monotonic_now()));

    advance(30);
    primary.on_primary_rebalance(answer(1)).await;
    advance(40);
    primary.tick_primary_rebalance().await;
    assert_eq!(primary.bus.sent.borrow().len(), 4);
    let (replica, reprobe) = sent(3);
    assert_eq!(replica, 2);
    assert_ne!(
        reprobe.quorum_age_ns,
        PrimaryRebalanceHeader::NO_QUORUM_EVIDENCE
    );

    advance(50);
    primary.on_primary_rebalance(answer(2)).await;
    primary.on_primary_rebalance(answer(3)).await;
    advance(60);
    primary.tick_primary_rebalance().await;
    assert_eq!(
        primary.bus.sent.borrow().len(),
        4,
        "one opening re-probes each peer once"
    );
}

#[compio::test]
async fn given_backup_credit_when_probes_are_stale_replayed_or_lack_evidence_should_not_extend_it()
{
    let (shard, _inbox) = owner_at::<PingPongSuperblock>(None, 1, 3, 0);
    let consensus = shard.plane.metadata().consensus.as_ref().unwrap();
    let frontier = shard.plane.metadata().applied_frontier();
    let epoch = shard.bus.monotonic_now();
    let mut probe = probe_header(consensus, 100);
    probe.replica = 0;
    probe.quorum_age_ns = 0;
    let mut previous = probe;
    previous.nonce -= 1;
    shard.on_primary_rebalance(previous).await;
    shard.on_primary_rebalance(probe).await;
    assert!(frontier.partition_requests_allowed(epoch));
    shard.bus.now.set(Some(epoch + Duration::from_secs(1)));
    for altered in ["replay", "reordered", "view", "cluster", "primary"] {
        let mut invalid = probe;
        match altered {
            "replay" => {}
            "reordered" => invalid.nonce -= 1,
            "view" => invalid.view += 1,
            "cluster" => invalid.cluster += 1,
            "primary" => invalid.replica = 2,
            _ => unreachable!(),
        }
        shard.on_primary_rebalance(invalid).await;
        assert!(
            !frontier.partition_requests_allowed(epoch + Duration::from_secs(8)),
            "{altered}"
        );
    }
    shard.primary_rebalance.borrow_mut().reset_authority();
    shard.on_primary_rebalance(probe).await;
    assert!(!frontier.partition_requests_allowed(epoch + Duration::from_secs(8)));
    probe.nonce += 1;
    probe.quorum_age_ns = PrimaryRebalanceHeader::NO_QUORUM_EVIDENCE;
    shard.on_primary_rebalance(probe).await;
    probe.quorum_age_ns = 0;
    shard.on_primary_rebalance(probe).await;
    assert!(
        !frontier.partition_requests_allowed(epoch + Duration::from_secs(8)),
        "an absent-evidence probe is consumed too"
    );
    probe.nonce += 1;
    probe.quorum_age_ns = u64::try_from(Duration::from_secs(8).as_nanos()).unwrap();
    shard.on_primary_rebalance(probe).await;
    assert!(!frontier.partition_requests_allowed(epoch + Duration::from_secs(8)));
    probe.nonce += 1;
    probe.quorum_age_ns = u64::try_from(Duration::from_secs(1).as_nanos()).unwrap();
    shard.on_primary_rebalance(probe).await;
    assert!(
        !frontier.partition_requests_allowed(epoch + Duration::from_secs(8)),
        "receipt must not hide the advertised evidence age"
    );
    probe.nonce += 1;
    probe.quorum_age_ns = 0;
    shard.on_primary_rebalance(probe).await;
    assert!(frontier.partition_requests_allowed(epoch + Duration::from_secs(8)));
    assert!(!frontier.partition_requests_allowed(epoch + Duration::from_secs(9)));
}

#[compio::test]
async fn given_an_adopted_view_when_its_head_is_not_applied_and_advertised_should_not_credit_a_probe()
 {
    let (shard, _inbox) = owner_at::<PingPongSuperblock>(None, 1, 3, 0);
    let consensus = shard.plane.metadata().consensus.as_ref().unwrap();
    let frontier = shard.plane.metadata().applied_frontier();
    let epoch = shard.bus.monotonic_now();
    let start = Message::<StartViewHeader>::new(HEADER_SIZE).transmute_header(
        |_, header: &mut StartViewHeader| {
            header.cluster = CLUSTER;
            header.command = Command::StartView;
            header.size = u32::try_from(HEADER_SIZE).unwrap();
            header.group = METADATA_GROUP;
            header.view = 3;
            header.replica = 0;
            header.op = COMMITTED;
            header.commit = COMMITTED;
        },
    );
    let mut header = *start.header();
    consensus.handle_start_view(PlaneKind::Metadata, &header, &[]);
    assert_eq!(consensus.adopted_view_head(), Some((3, COMMITTED)));
    header.replica = 2;
    header.op += 10;
    consensus.handle_start_view(PlaneKind::Metadata, &header, &[]);
    assert_eq!(
        consensus.adopted_view_head(),
        Some((3, COMMITTED)),
        "rejected offers cannot raise the floor"
    );
    let mut probe = probe_header(consensus, NONCE);
    probe.replica = 0;
    probe.quorum_age_ns = 0;
    probe.commit = COMMITTED - 1;
    frontier.advance(COMMITTED - 1);
    shard.on_primary_rebalance(probe).await;
    assert!(!frontier.partition_requests_allowed(epoch));
    probe.nonce += 1;
    probe.commit = COMMITTED;
    shard.on_primary_rebalance(probe).await;
    assert!(!frontier.partition_requests_allowed(epoch));
    frontier.advance(COMMITTED);
    shard.on_primary_rebalance(probe).await;
    assert!(
        !frontier.partition_requests_allowed(epoch),
        "an earlier discarded packet cannot become fresh by replay"
    );
    probe.nonce += 1;
    shard.on_primary_rebalance(probe).await;
    assert!(frontier.partition_requests_allowed(epoch));
}

#[test]
fn given_empty_primary_restart_with_lower_nonce_when_metadata_advances_should_resume_without_accepting_old_probes()
 {
    let (shard, _inbox) = owner();
    let consensus = shard.plane.metadata().consensus.as_ref().unwrap();
    let mut state = PrimaryRebalance::default();
    let epoch = Instant::now();
    let at = |seconds| epoch + Duration::from_secs(seconds);
    let mut probe = probe_header(consensus, 100);
    assert_eq!(state.accept_probe(&probe, at(0)), None);
    probe.nonce = 1;
    assert_eq!(state.accept_probe(&probe, at(1)), None);
    probe.commit = 1;
    assert_eq!(
        state.accept_probe(&probe, at(2)),
        Some(at(0)),
        "the lower nonce must not have been accepted"
    );
    let mut old = probe;
    old.commit = 0;
    old.nonce = 101;
    assert_eq!(state.accept_probe(&old, at(3)), None);
    state.reset_authority();
    assert_eq!(state.accept_probe(&probe, at(4)), None);
    probe.nonce += 1;
    assert_eq!(
        state.accept_probe(&probe, at(5)),
        Some(at(2)),
        "replay protection must survive the authority reset"
    );
}

#[compio::test]
async fn given_a_delayed_probe_when_crediting_a_backup_should_age_its_evidence_from_the_previous_probe()
 {
    let (shard, _inbox) = owner_at::<PingPongSuperblock>(None, 1, 3, 0);
    let consensus = shard.plane.metadata().consensus.as_ref().unwrap();
    let frontier = shard.plane.metadata().applied_frontier();
    let epoch = shard.bus.monotonic_now();
    let window = Duration::from_secs(5) + metadata::AppliedFrontier::PARTITION_FRESHNESS_MARGIN;
    let mut probe = probe_header(consensus, NONCE);
    probe.replica = 0;
    probe.quorum_age_ns = 0;
    shard.on_primary_rebalance(probe).await;
    assert!(
        !frontier.partition_requests_allowed(epoch),
        "nothing bounds when the first probe of a view left the primary"
    );
    let transit = Duration::from_secs(3);
    shard.bus.now.set(Some(epoch + transit));
    probe.nonce += 1;
    shard.on_primary_rebalance(probe).await;
    assert!(
        frontier.partition_requests_allowed(
            (epoch + window)
                .checked_sub(Duration::from_nanos(1))
                .unwrap()
        )
    );
    assert!(
        !frontier.partition_requests_allowed(epoch + window),
        "the transit of the probe must not extend the credit"
    );
}

#[compio::test]
async fn given_an_overdue_probe_when_probing_again_should_withhold_evidence_until_the_peer_replies()
{
    let (shard, _inbox) = owner_at::<PingPongSuperblock>(None, 0, REPLICAS, 0);
    let epoch = shard.bus.monotonic_now();
    let advance = |millis| {
        shard
            .bus
            .now
            .set(Some(epoch + Duration::from_millis(millis)));
    };
    let sent = |index: usize| shard.bus.sent.borrow()[index];
    let answer = |index: usize| {
        let (replica, mut reply) = sent(index);
        reply.replica = replica;
        reply.kind = PrimaryRebalanceKind::MetadataReply;
        reply
    };
    let carries_evidence =
        |index: usize| sent(index).1.quorum_age_ns != PrimaryRebalanceHeader::NO_QUORUM_EVIDENCE;

    shard.tick_primary_rebalance().await;
    advance(10);
    shard.on_primary_rebalance(answer(0)).await;
    advance(3_100);
    shard.tick_primary_rebalance().await;
    assert_eq!(sent(2).0, 1);
    assert!(carries_evidence(2), "a peer that answered gets evidence");
    assert_eq!(sent(3).0, 2);
    assert!(
        !carries_evidence(3),
        "the overdue probe can still reach the peer after this one"
    );
    advance(3_200);
    shard.on_primary_rebalance(answer(3)).await;
    advance(4_200);
    shard.tick_primary_rebalance().await;
    assert_eq!(sent(4).0, 2);
    assert!(carries_evidence(4), "the reply restores the evidence");
}
