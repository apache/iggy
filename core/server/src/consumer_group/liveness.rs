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

//! Shard 0's periodic pass: backups report bound sessions to the metadata
//! primary, which renews leases and commits a Logout for each expired member.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use consensus::{Consensus, MetadataHandle};
use iggy_binary_protocol::{
    Command, ConsensusHeader, ConsumerSession, ConsumerSessionHeartbeatHeader, HEADER_SIZE,
    MAX_CONSUMER_SESSIONS_PER_HEARTBEAT, PrepareHeader, WireEncode,
};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::SendError;
use message_bus::lifecycle::ShutdownToken;
use metadata::impls::metadata::StreamsFrontend;
use server_common::Message;
use server_common::sharding::CONSUMER_SESSION_REPORT_TIMEOUT;
use shard::Receiver;
use tracing::{info, trace};

use crate::consumer_group::lease::ConsumerGroupLiveness;
use crate::shell::{ServerShard, ShellBus, ShellShard};

const MAX_LOGOUTS_PER_PASS: usize = 256;
const HEARTBEAT_RETRY_INTERVAL: Duration = Duration::from_millis(1);

#[derive(Debug, PartialEq, Eq)]
enum Pass {
    Stopped,
    HitCap,
    Drained,
}

pub async fn run(
    shard: Rc<ServerShard>,
    liveness: Rc<RefCell<ConsumerGroupLiveness>>,
    stop: Receiver<()>,
    interval: Duration,
    timeout: Duration,
) {
    let shutdown = shard.bus.token();
    'running: while compio::time::timeout(interval, stop.recv()).await.is_err() {
        loop {
            match report_and_expire(&shard, &liveness, &stop, &shutdown, timeout).await {
                Pass::Stopped => break 'running,
                Pass::HitCap => {}
                Pass::Drained => break,
            }
        }
    }
}

async fn report_and_expire<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    liveness: &RefCell<ConsumerGroupLiveness>,
    stop: &Receiver<()>,
    shutdown: &ShutdownToken,
    timeout: Duration,
) -> Pass
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let metadata = shard.plane.metadata();
    let Some(consensus) = metadata.consensus.as_ref() else {
        return Pass::Drained;
    };
    let clients = shard.gather_consumer_sessions().await;
    let now = Instant::now();
    let view = consensus.view();
    let serving = consensus.is_normal() && !consensus.is_transferring();
    let primary_view = (consensus.is_primary() && serving).then_some(view);
    liveness.borrow_mut().observe_view(primary_view);
    if !consensus.is_normal() || (consensus.is_primary() && !serving) {
        return Pass::Drained;
    }

    let incomplete = !clients.complete;
    let sessions = clients.clients;

    if primary_view.is_some() {
        let table = metadata.client_table.borrow();
        let members = metadata.mux_stm.streams().read(|inner| {
            inner
                .items
                .iter()
                .flat_map(|(_, stream)| {
                    stream.topics.iter().flat_map(|(_, topic)| {
                        topic.consumer_groups.values().flat_map(|group| {
                            group.members.iter().map(|(_, member)| {
                                (
                                    member.client_id,
                                    table.get_epoch(member.client_id).or(member.session),
                                )
                            })
                        })
                    })
                })
                .fold(
                    BTreeMap::<u128, Option<u64>>::new(),
                    |mut members, (client_id, session)| {
                        members
                            .entry(client_id)
                            .and_modify(|current| *current = (*current).max(session))
                            .or_insert(session);
                        members
                    },
                )
        });
        drop(table);
        let mut tracker = liveness.borrow_mut();
        tracker.reconcile(view, &members, now);
        if incomplete {
            tracker.defer_expiry(now, timeout, consensus.replica());
        }
        for session in sessions {
            tracker.renew(session, now);
        }
    } else {
        // A backup can lag a freshly joined group. Report all bound sessions;
        // filtering through its replicated memberships would drop valid renewals.
        // Even an empty gather must report incompleteness to the primary.
        return report_sessions(shard, liveness, &sessions, incomplete, stop, shutdown).await;
    }

    expire_sessions(shard, liveness, stop, shutdown, timeout, now).await
}

async fn report_sessions<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    liveness: &RefCell<ConsumerGroupLiveness>,
    sessions: &[ConsumerSession],
    incomplete: bool,
    stop: &Receiver<()>,
    shutdown: &ShutdownToken,
) -> Pass
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Some(consensus) = shard.plane.metadata().consensus.as_ref() else {
        return Pass::Drained;
    };
    let batch_size = (shard.bus_max_message_size().saturating_sub(HEADER_SIZE)
        / ConsumerSession::ENCODED_SIZE)
        .min(MAX_CONSUMER_SESSIONS_PER_HEARTBEAT);
    if batch_size == 0 {
        return Pass::Drained;
    }
    let view = consensus.view();
    let session_count = sessions.len().max(1);
    let (offset, incomplete) = {
        let mut tracker = liveness.borrow_mut();
        let incomplete = incomplete || tracker.report_incomplete;
        tracker.report_incomplete = true;
        (tracker.report_offset % session_count, incomplete)
    };
    let (before, after) = sessions.split_at(offset);
    let mut batches = after.chunks(batch_size).chain(before.chunks(batch_size));
    let first = batches.next().unwrap_or_default();
    let send = async {
        let mut offset = offset;
        for batch in std::iter::once(first).chain(batches) {
            loop {
                if stop.try_recv().is_ok() || shutdown.is_triggered() {
                    return Pass::Stopped;
                }
                if consensus.view() != view || !consensus.is_normal() || consensus.is_primary() {
                    return Pass::Drained;
                }
                let heartbeat = heartbeat_message(
                    consensus.cluster(),
                    view,
                    consensus.replica(),
                    batch,
                    incomplete,
                );
                match shard
                    .bus
                    .send_to_replica(consensus.primary_index(view), heartbeat.into_frozen())
                    .await
                {
                    Ok(()) => {
                        offset = (offset + batch.len()) % session_count;
                        liveness.borrow_mut().report_offset = offset;
                        break;
                    }
                    Err(SendError::Backpressure | SendError::ReplicaForwardFailed(_)) => {
                        shard.bus.sleep(HEARTBEAT_RETRY_INTERVAL).await;
                    }
                    Err(error) => {
                        trace!(
                            ?error,
                            "consumer session heartbeat could not reach metadata primary"
                        );
                        return Pass::Drained;
                    }
                }
            }
        }
        liveness.borrow_mut().report_incomplete = false;
        Pass::Drained
    };
    shard::bus_timeout(&shard.bus, CONSUMER_SESSION_REPORT_TIMEOUT, send)
        .await
        .unwrap_or(Pass::Drained)
}

async fn expire_sessions<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    liveness: &RefCell<ConsumerGroupLiveness>,
    stop: &Receiver<()>,
    shutdown: &ShutdownToken,
    timeout: Duration,
    now: Instant,
) -> Pass
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let metadata = shard.plane.metadata();
    let Some(consensus) = metadata.consensus.as_ref() else {
        return Pass::Drained;
    };
    let view = consensus.view();
    let expired = {
        let mut tracker = liveness.borrow_mut();
        if tracker
            .last_incomplete
            .is_some_and(|last| now.saturating_duration_since(last) >= timeout)
        {
            tracker.last_incomplete = None;
            tracker.last_incomplete_warning = None;
            info!(?timeout, "consumer-group session expiry resumed");
        }
        tracker
            .leases
            .iter()
            .filter(|(_, lease)| now.saturating_duration_since(lease.last_seen) >= timeout)
            .take(MAX_LOGOUTS_PER_PASS)
            .map(|(&client_id, lease)| (client_id, lease.session))
            .collect::<Vec<_>>()
    };
    let hit_cap = expired.len() == MAX_LOGOUTS_PER_PASS;
    let mut removed = 0;
    let mut deferred = false;
    for (client_id, session) in expired {
        // Submission is not cancel-safe. Stop between logouts and recheck every
        // fence after the preceding commit, which can yield to a new heartbeat.
        if stop.try_recv().is_ok() || shutdown.is_triggered() {
            return Pass::Stopped;
        }
        if consensus.view() != view || !metadata.is_caught_up_primary() {
            return Pass::Drained;
        }
        if !liveness
            .borrow()
            .expired(view, client_id, session, now, timeout)
        {
            continue;
        }
        match metadata
            .submit_expired_logout_in_process(client_id, session)
            .await
        {
            Ok(Some(_)) => {
                removed += 1;
                info!(client_id, ?session, "expired consumer-group session");
            }
            Ok(None) => {}
            Err(error) => {
                deferred = true;
                trace!(client_id, ?error, "consumer-group session cleanup deferred");
            }
        }
    }
    if hit_cap && removed > 0 && !deferred {
        Pass::HitCap
    } else {
        Pass::Drained
    }
}

#[allow(clippy::cast_possible_truncation)]
fn heartbeat_message(
    cluster: u128,
    view: u32,
    replica: u8,
    sessions: &[ConsumerSession],
    incomplete: bool,
) -> Message<ConsumerSessionHeartbeatHeader> {
    let mut body = BytesMut::with_capacity(sessions.len() * ConsumerSession::ENCODED_SIZE);
    for session in sessions {
        session.encode(&mut body);
    }
    let size = HEADER_SIZE + body.len();
    let mut message = Message::<ConsumerSessionHeartbeatHeader>::new(size).transmute_header(
        |_, header: &mut ConsumerSessionHeartbeatHeader| {
            header.command = Command::ConsumerSessionHeartbeat;
            header.cluster = cluster;
            header.view = view;
            header.replica = replica;
            header.incomplete = u8::from(incomplete);
            header.size = size as u32;
            header.checksum_body = u128::from(iggy_common::calculate_checksum(&body));
            header.seal();
        },
    );
    message.as_mut_slice()[HEADER_SIZE..size].copy_from_slice(&body);
    message
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::test_support::{SpyBus, TestShard, prepare_message, test_shard};
    use consensus::client_table::ClientTable;
    use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
    use iggy_binary_protocol::requests::consumer_groups::CreateConsumerGroupRequest;
    use iggy_binary_protocol::requests::streams::CreateStreamRequest;
    use iggy_binary_protocol::requests::topics::{
        CreateTopicRequest, CreateTopicWithAssignmentsRequest,
    };
    use iggy_binary_protocol::{Operation, WireDecode, WireIdentifier, WireName, WireOptions};
    use journal::prepare_journal::PrepareJournal;
    use message_bus::lifecycle::Shutdown;
    use metadata::stm::StateMachine;
    use metadata::stm::consumer_group::JoinConsumerGroupRequest;
    use server_common::MessageBag;
    use server_common::sharding::METADATA_GROUP;

    const CLIENT: u128 = 7;
    const SESSION: u64 = 11;
    const VIEW: u32 = 3;
    const CLUSTER: u128 = 42;
    const TIMEOUT: Duration = Duration::from_secs(30);

    #[compio::test]
    async fn heartbeat_batches_fit_the_transport_limit_without_losing_sessions() {
        const SMALL_FRAME_CAP: usize = 4096;
        let sessions = (0..=MAX_CONSUMER_SESSIONS_PER_HEARTBEAT)
            .map(|index| ConsumerSession {
                client_id: CLIENT + index as u128,
                session: SESSION,
            })
            .collect::<Vec<_>>();
        for cap in [
            HEADER_SIZE + ConsumerSession::ENCODED_SIZE,
            SMALL_FRAME_CAP,
            HEADER_SIZE + MAX_CONSUMER_SESSIONS_PER_HEARTBEAT * ConsumerSession::ENCODED_SIZE,
        ] {
            let bus = SpyBus::default();
            let shard = Rc::new(test_shard(&bus, 1, 3, 1));
            shard.set_bus_max_message_size(cap);
            let tracker = RefCell::new(ConsumerGroupLiveness::default());
            let (_stop, receiver) = shard::channel(1);
            let (_signal, shutdown) = Shutdown::new();
            assert_eq!(
                report_sessions(&shard, &tracker, &sessions, false, &receiver, &shutdown).await,
                Pass::Drained
            );
            let frames = bus.replica_sends.borrow();
            assert!(frames.iter().all(|(_, frame)| frame.len() <= cap));
            let received = frames
                .iter()
                .flat_map(|(_, frame)| {
                    frame[HEADER_SIZE..]
                        .as_chunks::<{ ConsumerSession::ENCODED_SIZE }>()
                        .0
                        .iter()
                })
                .map(|bytes| ConsumerSession::decode(bytes).unwrap().0)
                .collect::<Vec<_>>();
            assert_eq!(received, sessions);
            assert!(!tracker.borrow().report_incomplete);
        }
    }

    #[compio::test]
    async fn heartbeat_reports_retry_a_full_peer_queue_until_the_tail_is_sent() {
        const PEER_QUEUE_CAPACITY: usize = 2;
        let sessions = (0..=MAX_CONSUMER_SESSIONS_PER_HEARTBEAT * PEER_QUEUE_CAPACITY)
            .map(|index| ConsumerSession {
                client_id: CLIENT + index as u128,
                session: SESSION,
            })
            .collect::<Vec<_>>();
        let bus = SpyBus::default();
        bus.replica_send_capacity.set(Some(PEER_QUEUE_CAPACITY));
        let shard = Rc::new(test_shard(&bus, 1, 3, 1));
        let tracker = RefCell::new(ConsumerGroupLiveness::default());
        let (_stop, receiver) = shard::channel(1);
        let (_signal, shutdown) = Shutdown::new();
        let send = report_sessions(&shard, &tracker, &sessions, false, &receiver, &shutdown);
        let drain = async {
            let mut received = Vec::new();
            while received.len() < sessions.len() {
                for (_, frame) in bus.replica_sends.borrow_mut().drain(..) {
                    received.extend(
                        frame[HEADER_SIZE..]
                            .as_chunks::<{ ConsumerSession::ENCODED_SIZE }>()
                            .0
                            .iter()
                            .map(|bytes| ConsumerSession::decode(bytes).unwrap().0),
                    );
                }
                if received.len() < sessions.len() {
                    compio::time::sleep(HEARTBEAT_RETRY_INTERVAL).await;
                }
            }
            received
        };
        let (pass, received) =
            compio::time::timeout(TIMEOUT, async { futures::join!(send, drain) })
                .await
                .expect("heartbeat sender must yield to the peer writer");
        assert_eq!(pass, Pass::Drained);
        assert_eq!(received, sessions);
        assert!(!tracker.borrow().report_incomplete);
    }

    #[compio::test]
    async fn timed_out_reports_resume_at_the_unsent_session_and_defer_expiry() {
        let sessions = (0..=MAX_CONSUMER_SESSIONS_PER_HEARTBEAT)
            .map(|index| ConsumerSession {
                client_id: CLIENT + index as u128,
                session: SESSION,
            })
            .collect::<Vec<_>>();
        let bus = SpyBus::default();
        bus.replica_send_capacity.set(Some(1));
        let shard = Rc::new(test_shard(&bus, 1, 3, 1));
        let tracker = RefCell::new(ConsumerGroupLiveness::default());
        let (stop, receiver) = shard::channel(1);
        let (_signal, shutdown) = Shutdown::new();
        assert_eq!(
            report_sessions(&shard, &tracker, &sessions, false, &receiver, &shutdown).await,
            Pass::Drained
        );
        assert!(tracker.borrow().report_incomplete);
        assert_eq!(
            tracker.borrow().report_offset,
            MAX_CONSUMER_SESSIONS_PER_HEARTBEAT
        );
        bus.replica_sends.borrow_mut().clear();
        bus.replica_send_capacity.set(None);
        assert_eq!(
            report_sessions(&shard, &tracker, &sessions, false, &receiver, &shutdown).await,
            Pass::Drained
        );
        {
            let frames = bus.replica_sends.borrow();
            assert_eq!(
                ConsumerSession::decode(&frames[0].1[HEADER_SIZE..])
                    .unwrap()
                    .0,
                sessions[MAX_CONSUMER_SESSIONS_PER_HEARTBEAT]
            );
            assert!(frames.iter().all(|(_, frame)| {
                frame[std::mem::offset_of!(ConsumerSessionHeartbeatHeader, incomplete)] == 1
            }));
        }
        assert!(!tracker.borrow().report_incomplete);
        bus.replica_sends.borrow_mut().clear();
        stop.try_send(()).unwrap();
        assert_eq!(
            report_sessions(&shard, &tracker, &sessions, false, &receiver, &shutdown).await,
            Pass::Stopped
        );
        assert!(bus.replica_sends.borrow().is_empty());
    }

    #[compio::test]
    async fn mixed_legacy_membership_epochs_do_not_block_automatic_expiry() {
        let (_dir, shard) = shard_with_members(1).await;
        let metadata = shard.plane.metadata();
        let topic = CreateTopicWithAssignmentsRequest {
            request: CreateTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                partitions_count: 1,
                name: WireName::new("legacy").unwrap(),
                options: WireOptions::empty(),
            },
            created_view: 0,
            derived_options: WireOptions::empty(),
            partitions: vec![CreatedPartitionAssignment {
                partition_id: 0,
                consensus_group_id: 2,
            }],
        };
        let group = CreateConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(1),
            name: WireName::new("legacy").unwrap(),
        };
        let legacy_join = JoinConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(1),
            group_id: WireIdentifier::numeric(0),
            client_id: CLIENT,
            in_flight: Vec::new(),
            session: None,
        };
        for (operation, body) in [
            (Operation::CreateTopicWithAssignments, topic.to_bytes()),
            (Operation::CreateConsumerGroup, group.to_bytes()),
            (Operation::JoinConsumerGroup, legacy_join.to_bytes()),
        ] {
            assert_eq!(
                metadata
                    .mux_stm
                    .update(prepare_message(operation, CLIENT, 1, &body))
                    .unwrap()
                    .code,
                0
            );
        }
        assert_eq!(metadata.client_table.borrow().get_epoch(CLIENT), None);
        let tracker = RefCell::new(ConsumerGroupLiveness::default());
        let (_stop, receiver) = shard::channel(1);
        let (_signal, shutdown) = Shutdown::new();
        report_and_expire(&shard, &tracker, &receiver, &shutdown, TIMEOUT).await;
        assert_eq!(tracker.borrow().leases[&CLIENT].session, Some(SESSION));
        let expired_at = Instant::now() + TIMEOUT * 2;
        assert_eq!(
            expire_sessions(&shard, &tracker, &receiver, &shutdown, TIMEOUT, expired_at).await,
            Pass::Drained
        );
        assert!(
            metadata
                .mux_stm
                .streams()
                .consumer_group_memberships(CLIENT)
                .is_empty()
        );
    }

    #[compio::test]
    async fn transferring_backup_still_reports_while_transferring_primary_does_not() {
        for replica in [0, 1] {
            let bus = SpyBus::default();
            let shard = Rc::new(test_shard(&bus, replica, 3, 1));
            let consensus = shard.plane.metadata().consensus.as_ref().unwrap();
            consensus.begin_state_transfer_await();
            assert!(consensus.is_normal());
            assert!(consensus.is_transferring());
            let (_stop, receiver) = shard::channel(1);
            let (_signal, shutdown) = Shutdown::new();
            let tracker = RefCell::new(ConsumerGroupLiveness::default());
            assert_eq!(
                report_and_expire(&shard, &tracker, &receiver, &shutdown, TIMEOUT).await,
                Pass::Drained
            );
            if replica == 0 {
                assert!(bus.replica_sends.borrow().is_empty());
            } else {
                let (target, header) = bus.sole_replica_send::<ConsumerSessionHeartbeatHeader>();
                assert_eq!(target, 0);
                assert_eq!(header.replica, replica);
                assert_eq!(header.view, consensus.view());
                assert_eq!(header.incomplete, 1);
            }
        }
    }

    #[test]
    fn one_lost_report_with_gather_delays_does_not_expire_live_members() {
        const INTERVAL: Duration = Duration::from_secs(20);
        const VALID_TIMEOUT: Duration = Duration::from_secs(67);
        let now = Instant::now();
        let mut tracker = ConsumerGroupLiveness::default();
        tracker.reconcile(VIEW, &BTreeMap::from([(CLIENT, Some(SESSION))]), now);
        let next_report = now
            + (INTERVAL
                + server_common::sharding::LIST_CLIENTS_GATHER_TIMEOUT
                + CONSUMER_SESSION_REPORT_TIMEOUT)
                * 2;
        assert!(!tracker.expired(VIEW, CLIENT, Some(SESSION), next_report, VALID_TIMEOUT));
        tracker.receive(
            CLUSTER,
            Some(VIEW),
            &heartbeat_message(
                CLUSTER,
                VIEW,
                1,
                &[ConsumerSession {
                    client_id: CLIENT,
                    session: SESSION,
                }],
                false,
            ),
            next_report,
            VALID_TIMEOUT,
        );
        assert!(!tracker.expired(
            VIEW,
            CLIENT,
            Some(SESSION),
            now + VALID_TIMEOUT,
            VALID_TIMEOUT
        ));
        assert!(tracker.expired(
            VIEW,
            CLIENT,
            Some(SESSION),
            next_report + VALID_TIMEOUT,
            VALID_TIMEOUT
        ));
    }

    #[compio::test]
    async fn capped_expiry_with_a_fenced_candidate_continues_but_deferral_stops() {
        let (_dir, shard) = shard_with_members(MAX_LOGOUTS_PER_PASS + 1).await;
        let metadata = shard.plane.metadata();
        let now = Instant::now();
        let members = (0..=MAX_LOGOUTS_PER_PASS)
            .map(|index| (CLIENT + index as u128, Some(SESSION)))
            .collect();
        let tracker = RefCell::new(ConsumerGroupLiveness::default());
        tracker.borrow_mut().reconcile(0, &members, now);
        metadata
            .mux_stm
            .streams()
            .refresh_consumer_group_session(CLIENT, SESSION + 1);
        let (_stop, receiver) = shard::channel(1);
        let (_signal, shutdown) = Shutdown::new();

        tracker.borrow_mut().defer_expiry(now + TIMEOUT, TIMEOUT, 1);
        assert_eq!(
            expire_sessions(
                &shard,
                &tracker,
                &receiver,
                &shutdown,
                TIMEOUT,
                now + TIMEOUT
            )
            .await,
            Pass::Drained
        );
        assert_eq!(metadata.consensus.as_ref().unwrap().commit_min(), 0);

        assert_eq!(
            expire_sessions(
                &shard,
                &tracker,
                &receiver,
                &shutdown,
                TIMEOUT,
                now + TIMEOUT * 2
            )
            .await,
            Pass::HitCap
        );
        assert_eq!(
            metadata.consensus.as_ref().unwrap().commit_min(),
            (MAX_LOGOUTS_PER_PASS - 1) as u64
        );
        assert_eq!(
            metadata.mux_stm.streams().consumer_group_session(CLIENT),
            Some(SESSION + 1)
        );
        assert_eq!(
            metadata
                .mux_stm
                .streams()
                .consumer_group_session(CLIENT + MAX_LOGOUTS_PER_PASS as u128),
            Some(SESSION)
        );

        let remaining = BTreeMap::from([
            (CLIENT, Some(SESSION + 1)),
            (CLIENT + MAX_LOGOUTS_PER_PASS as u128, Some(SESSION)),
        ]);
        tracker
            .borrow_mut()
            .reconcile(0, &remaining, now + TIMEOUT * 2);
        assert_eq!(
            expire_sessions(
                &shard,
                &tracker,
                &receiver,
                &shutdown,
                TIMEOUT,
                now + TIMEOUT * 2
            )
            .await,
            Pass::Drained
        );
        assert_eq!(
            metadata
                .mux_stm
                .streams()
                .consumer_group_session(CLIENT + MAX_LOGOUTS_PER_PASS as u128),
            None
        );
        assert_eq!(
            metadata.mux_stm.streams().consumer_group_session(CLIENT),
            Some(SESSION + 1)
        );
    }

    async fn shard_with_members(count: usize) -> (tempfile::TempDir, Rc<TestShard>) {
        let dir = tempfile::tempdir().unwrap();
        let mut shard = test_shard(&SpyBus::default(), 0, 1, 1);
        let metadata = shard.plane.metadata_mut();
        metadata.journal = Some(
            PrepareJournal::open(&dir.path().join("journal.wal"), 0)
                .await
                .unwrap(),
        );
        metadata.client_table = RefCell::new(ClientTable::new(1));
        let create_stream = CreateStreamRequest {
            name: WireName::new("stream").unwrap(),
            options: WireOptions::empty(),
        };
        let create_topic = CreateTopicWithAssignmentsRequest {
            request: CreateTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                partitions_count: 1,
                name: WireName::new("topic").unwrap(),
                options: WireOptions::empty(),
            },
            created_view: 0,
            derived_options: WireOptions::empty(),
            partitions: vec![CreatedPartitionAssignment {
                partition_id: 0,
                consensus_group_id: 1,
            }],
        };
        let create_group = CreateConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            name: WireName::new("group").unwrap(),
        };
        for (operation, body) in [
            (Operation::CreateStream, create_stream.to_bytes()),
            (
                Operation::CreateTopicWithAssignments,
                create_topic.to_bytes(),
            ),
            (Operation::CreateConsumerGroup, create_group.to_bytes()),
        ] {
            assert_eq!(
                metadata
                    .mux_stm
                    .update(prepare_message(operation, CLIENT, 1, &body))
                    .unwrap()
                    .code,
                0
            );
        }
        for index in 0..count {
            let client_id = CLIENT + index as u128;
            let join = JoinConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                group_id: WireIdentifier::numeric(0),
                client_id,
                in_flight: Vec::new(),
                session: None,
            };
            assert_eq!(
                metadata
                    .mux_stm
                    .update(prepare_message(
                        Operation::JoinConsumerGroup,
                        client_id,
                        1,
                        &join.to_bytes()
                    ))
                    .unwrap()
                    .code,
                0
            );
            metadata
                .mux_stm
                .streams()
                .refresh_consumer_group_session(client_id, SESSION);
        }
        (dir, Rc::new(shard))
    }

    #[test]
    fn recovered_members_receive_full_timeout_including_missing_client_table_entries() {
        let now = Instant::now();
        let mut tracker = ConsumerGroupLiveness::default();
        tracker.reconcile(
            VIEW,
            &BTreeMap::from([(CLIENT, Some(SESSION)), (CLIENT + 1, None)]),
            now,
        );
        assert!(!tracker.expired(VIEW, CLIENT, Some(SESSION), now + TIMEOUT / 2, TIMEOUT));
        assert!(tracker.expired(VIEW, CLIENT, Some(SESSION), now + TIMEOUT, TIMEOUT));
        assert!(tracker.expired(VIEW, CLIENT + 1, None, now + TIMEOUT, TIMEOUT));
        tracker.reconcile(VIEW, &BTreeMap::new(), now + TIMEOUT);
        assert!(tracker.leases.is_empty());
    }

    #[test]
    fn remote_heartbeats_renew_only_matching_sessions() {
        let now = Instant::now();
        let mut tracker = ConsumerGroupLiveness::default();
        tracker.reconcile(
            VIEW,
            &BTreeMap::from([(CLIENT, Some(SESSION)), (CLIENT + 1, Some(SESSION))]),
            now,
        );
        let message = heartbeat_message(
            CLUSTER,
            VIEW,
            2,
            &[ConsumerSession {
                client_id: CLIENT,
                session: SESSION,
            }],
            false,
        );
        tracker.receive(CLUSTER, Some(VIEW), &message, now + TIMEOUT / 2, TIMEOUT);
        assert!(!tracker.expired(VIEW, CLIENT, Some(SESSION), now + TIMEOUT, TIMEOUT));
        assert!(tracker.expired(VIEW, CLIENT + 1, Some(SESSION), now + TIMEOUT, TIMEOUT));
        assert!(tracker.expired(VIEW, CLIENT, Some(SESSION), now + TIMEOUT * 2, TIMEOUT));

        tracker.reconcile(
            VIEW,
            &BTreeMap::from([(CLIENT, Some(SESSION + 1))]),
            now + TIMEOUT,
        );
        tracker.receive(CLUSTER, Some(VIEW), &message, now + TIMEOUT * 2, TIMEOUT);
        assert!(tracker.expired(VIEW, CLIENT, Some(SESSION + 1), now + TIMEOUT * 2, TIMEOUT));
    }

    #[test]
    fn promotion_grants_full_grace_and_rejects_old_view_or_foreign_heartbeats() {
        let now = Instant::now();
        let members = BTreeMap::from([(CLIENT, Some(SESSION))]);
        let mut tracker = ConsumerGroupLiveness::default();
        tracker.reconcile(VIEW, &members, now);
        tracker.observe_view(None);
        tracker.reconcile(VIEW + 1, &members, now + TIMEOUT);
        assert!(!tracker.expired(VIEW + 1, CLIENT, Some(SESSION), now + TIMEOUT, TIMEOUT));
        for (cluster, view) in [(CLUSTER + 1, VIEW + 1), (CLUSTER, VIEW)] {
            let message = heartbeat_message(
                cluster,
                view,
                2,
                &[ConsumerSession {
                    client_id: CLIENT,
                    session: SESSION,
                }],
                false,
            );
            tracker.receive(
                CLUSTER,
                Some(VIEW + 1),
                &message,
                now + TIMEOUT * 2,
                TIMEOUT,
            );
        }
        assert!(tracker.expired(VIEW + 1, CLIENT, Some(SESSION), now + TIMEOUT * 2, TIMEOUT));
        assert!(!tracker.expired(VIEW, CLIENT, Some(SESSION), now + TIMEOUT * 2, TIMEOUT));
    }

    #[test]
    fn unknown_sessions_do_not_grow_the_tracker_and_malformed_batches_do_not_renew() {
        let now = Instant::now();
        let mut tracker = ConsumerGroupLiveness::default();
        tracker.reconcile(VIEW, &BTreeMap::from([(CLIENT, Some(SESSION))]), now);
        for sessions in [
            vec![ConsumerSession {
                client_id: CLIENT + 1,
                session: SESSION,
            }],
            vec![
                ConsumerSession {
                    client_id: CLIENT,
                    session: SESSION,
                },
                ConsumerSession {
                    client_id: 0,
                    session: 0,
                },
            ],
        ] {
            let message = heartbeat_message(CLUSTER, VIEW, 2, &sessions, false);
            tracker.receive(CLUSTER, Some(VIEW), &message, now + TIMEOUT, TIMEOUT);
        }
        assert_eq!(tracker.leases.len(), 1);
        assert!(tracker.expired(VIEW, CLIENT, Some(SESSION), now + TIMEOUT, TIMEOUT));
    }

    #[test]
    fn incomplete_local_gathers_grant_another_timeout_before_expiry() {
        let now = Instant::now();
        let mut tracker = ConsumerGroupLiveness::default();
        let members = BTreeMap::from([(CLIENT, Some(SESSION))]);
        tracker.reconcile(VIEW, &members, now);
        tracker.defer_expiry(now + TIMEOUT, TIMEOUT, 0);
        tracker.reconcile(VIEW, &members, now + TIMEOUT + TIMEOUT / 2);
        assert!(!tracker.expired(
            VIEW,
            CLIENT,
            Some(SESSION),
            now + TIMEOUT + TIMEOUT / 2,
            TIMEOUT
        ));
        assert!(tracker.expired(VIEW, CLIENT, Some(SESSION), now + TIMEOUT * 2, TIMEOUT));
    }

    #[test]
    fn incomplete_remote_gathers_defer_expiry_even_without_sessions() {
        let now = Instant::now();
        let mut tracker = ConsumerGroupLiveness::default();
        tracker.reconcile(VIEW, &BTreeMap::from([(CLIENT, Some(SESSION))]), now);
        let incomplete = heartbeat_message(CLUSTER, VIEW, 2, &[], true);
        let routed = MessageBag::try_from(incomplete.into_generic())
            .unwrap()
            .into_generic()
            .try_into_typed::<ConsumerSessionHeartbeatHeader>()
            .unwrap();
        for tick in 1..=3 {
            let received = now + TIMEOUT * tick;
            tracker.receive(CLUSTER, Some(VIEW), &routed, received, TIMEOUT);
            assert!(!tracker.expired(VIEW, CLIENT, Some(SESSION), received, TIMEOUT));
        }
        let complete = heartbeat_message(CLUSTER, VIEW, 2, &[], false);
        tracker.receive(
            CLUSTER,
            Some(VIEW),
            &complete,
            now + TIMEOUT * 3 + TIMEOUT / 2,
            TIMEOUT,
        );
        assert!(!tracker.expired(
            VIEW,
            CLIENT,
            Some(SESSION),
            now + TIMEOUT * 3 + TIMEOUT / 2,
            TIMEOUT
        ));
        assert!(
            tracker.expired(VIEW, CLIENT, Some(SESSION), now + TIMEOUT * 4, TIMEOUT),
            "a crashed or recovered node must stop deferring expiry"
        );
    }

    #[test]
    fn foreign_or_malformed_incomplete_reports_cannot_defer_expiry() {
        let now = Instant::now();
        let mut tracker = ConsumerGroupLiveness::default();
        tracker.reconcile(VIEW, &BTreeMap::from([(CLIENT, Some(SESSION))]), now);
        for message in [
            heartbeat_message(CLUSTER + 1, VIEW, 2, &[], true),
            heartbeat_message(CLUSTER, VIEW + 1, 2, &[], true),
            heartbeat_message(
                CLUSTER,
                VIEW,
                2,
                &[ConsumerSession {
                    client_id: 0,
                    session: SESSION,
                }],
                true,
            ),
        ] {
            tracker.receive(CLUSTER, Some(VIEW), &message, now + TIMEOUT, TIMEOUT);
        }
        assert!(tracker.expired(VIEW, CLIENT, Some(SESSION), now + TIMEOUT, TIMEOUT));
    }

    #[test]
    fn bounded_heartbeat_round_trips_through_replica_routing() {
        let sessions = vec![
            ConsumerSession {
                client_id: CLIENT,
                session: SESSION
            };
            MAX_CONSUMER_SESSIONS_PER_HEARTBEAT
        ];
        let message = heartbeat_message(CLUSTER, VIEW, 2, &sessions, false);
        assert_eq!(
            u128::from(iggy_common::calculate_checksum(message.body())),
            message.header().checksum_body
        );
        let bag = MessageBag::try_from(message.into_generic()).unwrap();
        assert_eq!(bag.command(), Command::ConsumerSessionHeartbeat);
        assert_eq!(
            bag.routing(),
            (iggy_binary_protocol::Operation::Reserved, METADATA_GROUP)
        );
        let typed = bag
            .into_generic()
            .try_into_typed::<ConsumerSessionHeartbeatHeader>()
            .unwrap();
        assert_eq!(
            typed.body().len(),
            sessions.len() * ConsumerSession::ENCODED_SIZE
        );
        let mut corrupted = typed.into_generic();
        corrupted.as_mut_slice()[std::mem::offset_of!(ConsumerSessionHeartbeatHeader, view)] ^= 1;
        assert!(MessageBag::try_from(corrupted).is_err());
    }
}
