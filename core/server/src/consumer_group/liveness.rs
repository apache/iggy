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
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::pin::pin;
use std::rc::Rc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use consensus::{Consensus, MetadataHandle, VsrConsensus};
use futures::StreamExt;
use futures::future::{Either, select};
use iggy_binary_protocol::requests::system::{
    FinalizeSessionRequest, MAX_SESSIONS_PER_RETIREMENT, RetireSessionsRequest, RetirementDebt,
    SessionIdentity,
};
use iggy_binary_protocol::{
    Command, ConsensusHeader, ConsumerSession, ConsumerSessionHeartbeatHeader, GenericHeader,
    HEADER_SIZE, MAX_CONSUMER_SESSIONS_PER_HEARTBEAT, Operation, PrepareHeader, ReplyHeader,
    RoutedRequestHeader, WireEncode,
};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::SendError;
use message_bus::lifecycle::ShutdownToken;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::Streams;
use partitions::RetirementDebtStatus;
use server_common::Message;
use server_common::sharding::{CONSUMER_SESSION_REPORT_TIMEOUT, IggyNamespace};
use shard::Receiver;
use tracing::{debug, info, trace, warn};

use crate::consumer_group::lease::{
    ConsumerGroupLiveness, MemberSet, RetirementGroups, RetirementProgress, Unprovable, Withheld,
    encode_retirement_progress,
};
use crate::shell::{ShellBus, ShellShard};

const MAX_LOGOUTS_PER_PASS: usize = 256;
const MAX_RETIREMENTS_PER_PASS: usize = 128;
const HEARTBEAT_RETRY_INTERVAL: Duration = Duration::from_millis(1);
const RETIREMENT_SWEEP_INTERVAL: Duration = Duration::from_millis(100);
/// Retirement reads and barriers one sweep keeps in flight. One shard's inbox
/// serves every other request too.
const RETIREMENT_ATTEMPTS_IN_FLIGHT: usize = 8;
/// How long a stored partition may stay unproven, after its retry, before
/// this node reports it unprovable.
const UNPROVABLE_AFTER: Duration = Duration::from_secs(2);
/// How long the metadata primary waits for an expected reporter before it
/// finalizes with retirement debt.
const RETIREMENT_DEBT_GRACE: Duration = Duration::from_secs(2);

#[derive(Debug, PartialEq, Eq)]
enum Pass {
    Stopped,
    HitCap,
    Drained,
}

pub async fn run<B, MJ, S, SB>(
    shard: Rc<ShellShard<B, MJ, S, SB>>,
    liveness: Rc<RefCell<ConsumerGroupLiveness>>,
    stop: Receiver<()>,
    shutdown: ShutdownToken,
    interval: Duration,
    timeout: Duration,
    system_path: String,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let mut next_report = shard.bus.monotonic_now() + interval;
    'running: loop {
        let pending_retirement = shard
            .plane
            .metadata()
            .client_table
            .borrow()
            .ended_sessions()
            .next()
            .is_some()
            || !liveness.borrow().retirement_debts.unpaid.is_empty();
        let report_wait = next_report.saturating_duration_since(shard.bus.monotonic_now());
        let wait = if pending_retirement {
            report_wait.min(RETIREMENT_SWEEP_INTERVAL)
        } else {
            report_wait
        };
        if shard::bus_timeout(&shard.bus, wait, stop.recv())
            .await
            .is_some()
            || shutdown.is_triggered()
        {
            break;
        }
        if shard.bus.monotonic_now() >= next_report {
            loop {
                match report_and_expire(
                    &shard,
                    &liveness,
                    &stop,
                    &shutdown,
                    interval,
                    timeout,
                    &system_path,
                )
                .await
                {
                    Pass::Stopped => break 'running,
                    Pass::HitCap => {}
                    Pass::Drained => break,
                }
            }
            next_report = shard.bus.monotonic_now() + interval;
        } else {
            retire_sessions(&shard, &liveness, &stop, &shutdown, interval, &system_path).await;
        }
        pay_retirement_debts(&shard, &liveness, &stop, &shutdown).await;
    }
}

/// Pay, for the partitions this node leads, the retirement debts committed
/// finalizations recorded. Reading a debt also arms the session gate of every
/// local replica, so a backup that becomes primary already refuses sessions.
async fn pay_retirement_debts<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    liveness: &RefCell<ConsumerGroupLiveness>,
    stop: &Receiver<()>,
    shutdown: &ShutdownToken,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let metadata = shard.plane.metadata();
    let Some(consensus) = metadata.consensus.as_ref() else {
        return;
    };
    absorb_retirement_debts(
        metadata.mux_stm.streams(),
        liveness,
        consensus.replica(),
        consensus.replica_count(),
    );
    let batch: Vec<(IggyNamespace, u64, u64)> = {
        let mut tracker = liveness.borrow_mut();
        let debts = &mut tracker.retirement_debts;
        let rotated: Box<dyn Iterator<Item = _>> = match debts.cursor {
            Some(after) => Box::new(
                debts
                    .unpaid
                    .range((Bound::Excluded(after), Bound::Unbounded))
                    .chain(debts.unpaid.range(..=after)),
            ),
            None => Box::new(debts.unpaid.iter()),
        };
        let batch: Vec<_> = rotated
            .take(MAX_RETIREMENTS_PER_PASS)
            .map(|(namespace, (created_revision, debt_op))| {
                (*namespace, *created_revision, *debt_op)
            })
            .collect();
        debts.cursor = batch.last().map(|(namespace, _, _)| *namespace);
        batch
    };
    if batch.is_empty() || stop.try_recv().is_ok() || shutdown.is_triggered() {
        return;
    }
    let mut running = futures::stream::iter(batch.iter().map(
        |&(namespace, created_revision, debt_op)| async move {
            let settled = pay_retirement_debt(shard, namespace, created_revision, debt_op).await;
            (namespace, created_revision, debt_op, settled)
        },
    ))
    .buffer_unordered(RETIREMENT_ATTEMPTS_IN_FLIGHT);
    let mut deadline = pin!(shard.bus.sleep(CONSUMER_SESSION_REPORT_TIMEOUT));
    while let Either::Left((Some((namespace, created_revision, debt_op, settled)), _)) =
        select(running.next(), deadline.as_mut()).await
    {
        let Some(settled) = settled else {
            continue;
        };
        let mut tracker = liveness.borrow_mut();
        let debts = &mut tracker.retirement_debts;
        if debts.unpaid.get(&namespace) == Some(&(created_revision, debt_op)) {
            debts.unpaid.remove(&namespace);
            if settled == DebtSettlement::Paid {
                debts.paid.insert(namespace, (created_revision, debt_op));
            } else {
                debts.paid.remove(&namespace);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DebtSettlement {
    Paid,
    Gone,
}

/// Merge the debts recorded since the last call into the unpaid set,
/// rescanning committed metadata when the recent ones are no longer kept.
fn absorb_retirement_debts(
    streams: &Streams,
    liveness: &RefCell<ConsumerGroupLiveness>,
    node: u8,
    nodes: u8,
) {
    let mut tracker = liveness.borrow_mut();
    let debts = &mut tracker.retirement_debts;
    let recent = debts
        .seen_through
        .and_then(|seen_through| streams.retirement_debts_since(seen_through, node, nodes));
    let (entries, newest) = recent.unwrap_or_else(|| {
        debts.unpaid.clear();
        streams.retirement_debts_stored_by(node, nodes)
    });
    for (namespace, created_revision, debt_op) in entries {
        if debts
            .paid
            .get(&namespace)
            .is_some_and(|&(paid_revision, paid_op)| {
                paid_revision == created_revision && paid_op >= debt_op
            })
        {
            continue;
        }
        let entry = debts
            .unpaid
            .entry(namespace)
            .or_insert((created_revision, debt_op));
        if entry.0 < created_revision || (entry.0 == created_revision && entry.1 < debt_op) {
            *entry = (created_revision, debt_op);
        }
    }
    debts.seen_through = Some(newest);
}

/// `Some(Paid)` once a committed payment covers `debt_op`, `Some(Gone)` when
/// the incarnation no longer exists, `None` while it is still owed. Only the
/// node whose replica is a normal primary with an idle pipeline pays.
async fn pay_retirement_debt<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    namespace: IggyNamespace,
    created_revision: u64,
    debt_op: u64,
) -> Option<DebtSettlement>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let read = || {
        shard.partition_read(
            namespace,
            shard::PartitionRead::RetirementDebt {
                created_revision,
                debt_op,
            },
        )
    };
    let metadata = shard.plane.metadata();
    let sessions = match read().await {
        Some(shard::PartitionReadReply::RetirementDebt(RetirementDebtStatus::Paid)) => {
            return Some(DebtSettlement::Paid);
        }
        Some(shard::PartitionReadReply::RetirementDebt(RetirementDebtStatus::Unpaid(Some(
            sessions,
        )))) => sessions,
        Some(shard::PartitionReadReply::NotFound) => {
            return metadata
                .mux_stm
                .streams()
                .retirement_debt_for(namespace, created_revision)
                .is_none()
                .then_some(DebtSettlement::Gone);
        }
        _ => return None,
    };
    let consensus = metadata.consensus.as_ref()?;
    let applied = consensus.commit_min();
    if applied < debt_op {
        return None;
    }
    // A session absent from the table at `applied` but registered at or
    // below it was finalized; newer ones are left alone.
    let mut identities: Vec<SessionIdentity> = {
        let table = metadata.client_table.borrow();
        sessions
            .into_iter()
            .filter(|&(client_id, session)| {
                session <= applied && table.session_epoch(client_id) != Some(session)
            })
            .map(|(client_id, session)| SessionIdentity {
                client_id,
                session,
                metadata_watermark: debt_op.max(session),
            })
            .collect()
    };
    identities.sort_unstable_by_key(|identity| identity.client_id);
    let batch_limit = MAX_SESSIONS_PER_RETIREMENT.min(
        shard
            .bus_max_message_size()
            .saturating_sub(HEADER_SIZE + size_of::<u64>())
            / SessionIdentity::ENCODED_SIZE,
    );
    if batch_limit == 0 {
        return None;
    }
    let batches: Vec<&[SessionIdentity]> = if identities.is_empty() {
        vec![&[]]
    } else {
        identities.chunks(batch_limit).collect()
    };
    let last = batches.len() - 1;
    // Each batch must commit before the next: the last one's `paid_through`
    // vouches for every earlier batch.
    for (index, identities) in batches.into_iter().enumerate() {
        let body = RetireSessionsRequest {
            identities: identities.to_vec(),
            paid_through: if index == last { debt_op } else { 0 },
        }
        .to_bytes();
        let request =
            retire_sessions_frame(consensus.cluster(), namespace, debt_op, applied, &body);
        let ticket = shard.partition_submit(namespace, request).ok()?;
        let reply = shard.await_partition_submit(ticket).await?;
        if !reply_committed(&reply) {
            return None;
        }
    }
    let paid = matches!(
        read().await,
        Some(shard::PartitionReadReply::RetirementDebt(
            RetirementDebtStatus::Paid
        ))
    );
    if paid {
        shard.metrics().record_retirement_debt_paid();
    }
    paid.then_some(DebtSettlement::Paid)
}

fn reply_committed(reply: &Message<GenericHeader>) -> bool {
    reply
        .as_slice()
        .get(..size_of::<ReplyHeader>())
        .and_then(|bytes| bytemuck::checked::try_from_bytes::<ReplyHeader>(bytes).ok())
        .is_some_and(|header| header.status == 0)
}

async fn report_and_expire<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    liveness: &RefCell<ConsumerGroupLiveness>,
    stop: &Receiver<()>,
    shutdown: &ShutdownToken,
    report_interval: Duration,
    timeout: Duration,
    system_path: &str,
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
    let now = shard.bus.monotonic_now();
    let view = consensus.view();
    let serving = consensus.is_normal() && !consensus.is_transferring();
    let primary_view = (consensus.is_primary() && serving).then_some(view);
    liveness.borrow_mut().observe_view(serving.then_some(view));
    if !consensus.is_normal() || (consensus.is_primary() && !serving) {
        return Pass::Drained;
    }

    let incomplete = !clients.complete;
    let mut sessions = clients.clients;
    sessions.extend(liveness.borrow_mut().local_activity(now, timeout));

    if primary_view.is_some() {
        let table = metadata.client_table.borrow();
        let members: BTreeMap<u128, Option<u64>> = table
            .client_ids()
            .filter_map(|client_id| {
                table
                    .get_epoch(client_id)
                    .map(|session| (client_id, Some(session)))
            })
            .collect();
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
        let pass = report_sessions(shard, liveness, &sessions, incomplete, stop, shutdown).await;
        if pass != Pass::Stopped {
            retire_sessions(
                shard,
                liveness,
                stop,
                shutdown,
                report_interval,
                system_path,
            )
            .await;
        }
        return pass;
    }

    retire_sessions(
        shard,
        liveness,
        stop,
        shutdown,
        report_interval,
        system_path,
    )
    .await;
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

async fn retire_sessions<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    liveness: &RefCell<ConsumerGroupLiveness>,
    stop: &Receiver<()>,
    shutdown: &ShutdownToken,
    report_interval: Duration,
    system_path: &str,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let mut deadline = pin!(shard.bus.sleep(CONSUMER_SESSION_REPORT_TIMEOUT));
    for _ in 0..MAX_LOGOUTS_PER_PASS {
        let before = shard.plane.metadata().client_table.borrow().count();
        let retirement = pin!(retire_session(
            shard,
            liveness,
            stop,
            shutdown,
            report_interval,
            system_path,
        ));
        match select(retirement, deadline.as_mut()).await {
            Either::Left(((), _)) => {}
            Either::Right(((), retirement)) => {
                // Finalization can own an admitted WAL append. Canceling it
                // strands that op and blocks subsequent metadata requests.
                retirement.await;
                break;
            }
        }
        if shard.plane.metadata().client_table.borrow().count() >= before {
            break;
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn retire_session<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    liveness: &RefCell<ConsumerGroupLiveness>,
    stop: &Receiver<()>,
    shutdown: &ShutdownToken,
    report_interval: Duration,
    system_path: &str,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let metadata = shard.plane.metadata();
    let Some(consensus) = metadata.consensus.as_ref() else {
        return;
    };
    if !consensus.is_normal()
        || consensus.is_transferring()
        || consensus.commit_min() != consensus.commit_max()
    {
        return;
    }
    let view = consensus.view();
    liveness.borrow_mut().observe_view(Some(view));
    let identity = metadata
        .client_table
        .borrow()
        .ended_sessions()
        .min_by_key(|identity| (identity.metadata_watermark, identity.client_id));
    let Some(identity) = identity else {
        liveness.borrow_mut().reconcile_retirement(None, 0);
        return;
    };
    let revision = metadata
        .mux_stm
        .streams()
        .read(|inner| inner.namespace_revision);
    liveness
        .borrow_mut()
        .reconcile_retirement(Some(identity), revision);
    // The metadata primary need not belong to the common reporting quorum.
    if consensus.is_primary()
        && let Some(debt) = settle_retirement(shard, liveness, revision)
    {
        finalize_session(shard, identity, revision, debt).await;
        return;
    }
    let Some(reported) = liveness
        .borrow()
        .retirement
        .as_ref()
        .map(|progress| progress.reported)
    else {
        return;
    };
    if !reported
        && !sweep_retirement(
            shard,
            liveness,
            stop,
            shutdown,
            identity,
            revision,
            system_path,
        )
        .await
    {
        return;
    }
    if consensus.view() != view
        || !consensus.is_normal()
        || consensus.is_transferring()
        || metadata
            .mux_stm
            .streams()
            .read(|inner| inner.namespace_revision)
            != revision
    {
        return;
    }
    let groups = retirement_groups(shard, liveness, revision);
    let withheld = {
        let tracker = liveness.borrow();
        let Some(progress) = &tracker.retirement else {
            return;
        };
        withheld_vouches(&groups, &progress.unprovable)
    };
    if consensus.is_primary() {
        let debt = {
            let mut tracker = liveness.borrow_mut();
            let Some(progress) = &mut tracker.retirement else {
                return;
            };
            progress
                .reporters
                .insert(consensus.replica(), Some(Rc::new(withheld)));
            drop(tracker);
            settle_retirement(shard, liveness, revision)
        };
        if let Some(debt) = debt {
            finalize_session(shard, identity, revision, debt).await;
        }
        return;
    }
    if !liveness
        .borrow_mut()
        .retirement
        .as_mut()
        .is_some_and(|progress| {
            // The primary can receive this report before it starts the sweep.
            progress.report_due(
                shard.bus.monotonic_now(),
                report_interval.min(RETIREMENT_SWEEP_INTERVAL),
            )
        })
    {
        return;
    }
    let body = encode_retirement_progress(
        ConsumerSession {
            client_id: identity.client_id,
            session: identity.session,
        },
        Some(&withheld),
        shard.bus_max_message_size().saturating_sub(HEADER_SIZE),
    );
    let message = heartbeat_frame(consensus.cluster(), view, consensus.replica(), &body, false)
        .transmute_header(|old, header| {
            *header = old;
            header.command = Command::SessionRetirementProgress;
            header.namespace_revision = revision;
            header.seal();
        });
    if let Err(error) = shard
        .bus
        .send_to_replica(consensus.primary_index(view), message.into_frozen())
        .await
    {
        trace!(?error, "session retirement report deferred");
    }
}

/// One bounded pass of this node's sweep for `identity`. Every stored
/// namespace is tried once, then the unproven ones are retried until each is
/// proven or has stayed unproven for [`UNPROVABLE_AFTER`]. Returns whether the
/// sweep is complete.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn sweep_retirement<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    liveness: &RefCell<ConsumerGroupLiveness>,
    stop: &Receiver<()>,
    shutdown: &ShutdownToken,
    identity: SessionIdentity,
    revision: u64,
    system_path: &str,
) -> bool
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let metadata = shard.plane.metadata();
    let Some(consensus) = metadata.consensus.as_ref() else {
        return false;
    };
    let view = consensus.view();
    let mut identities: Vec<_> = metadata.client_table.borrow().ended_sessions().collect();
    identities.sort_unstable_by_key(|identity| (identity.metadata_watermark, identity.client_id));
    let batch_limit = MAX_SESSIONS_PER_RETIREMENT.min(
        shard.bus_max_message_size().saturating_sub(HEADER_SIZE) / SessionIdentity::ENCODED_SIZE,
    );
    if batch_limit == 0 {
        return false;
    }
    identities.truncate(batch_limit);
    let body = RetireSessionsRequest {
        identities,
        paid_through: 0,
    }
    .to_bytes();
    let namespaces = stored_namespaces(shard, liveness, revision);
    let (cursor, attempts) = {
        let tracker = liveness.borrow();
        let Some(progress) = &tracker.retirement else {
            return false;
        };
        let suspect = |namespace: &IggyNamespace| tracker.retirement_suspects.contains(namespace);
        let attempts: Vec<(IggyNamespace, u64, bool)> = if progress.cursor < namespaces.len() {
            namespaces
                .iter()
                .skip(progress.cursor)
                .take(MAX_RETIREMENTS_PER_PASS)
                .map(|&(namespace, created_revision)| {
                    (namespace, created_revision, suspect(&namespace))
                })
                .collect()
        } else {
            progress
                .pending
                .iter()
                .take(MAX_RETIREMENTS_PER_PASS)
                .map(|(namespace, (created_revision, _, _))| {
                    (*namespace, *created_revision, suspect(namespace))
                })
                .collect()
        };
        (progress.cursor, attempts)
    };
    if stop.try_recv().is_ok()
        || shutdown.is_triggered()
        || consensus.view() != view
        || !consensus.is_normal()
    {
        return false;
    }
    // A group without a live member quorum stalls only its own attempt: the
    // pass deadline settles every attempt that answered in time.
    let mut running = futures::stream::iter(attempts.iter().map(
        |&(namespace, created_revision, suspect)| {
            let body = (!suspect).then_some(&body[..]);
            async move {
                let retired = prove_retired(
                    shard,
                    liveness,
                    identity,
                    body,
                    namespace,
                    created_revision,
                    system_path,
                )
                .await;
                (namespace, retired)
            }
        },
    ))
    .buffer_unordered(RETIREMENT_ATTEMPTS_IN_FLIGHT);
    let mut proven = BTreeSet::new();
    {
        let mut deadline = pin!(shard.bus.sleep(CONSUMER_SESSION_REPORT_TIMEOUT));
        while let Either::Left((Some((namespace, retired)), _)) =
            select(running.next(), deadline.as_mut()).await
        {
            if retired {
                proven.insert(namespace);
            }
        }
    }
    drop(running);
    if consensus.view() != view || !consensus.is_normal() {
        return false;
    }
    let now = shard.bus.monotonic_now();
    let mut tracker = liveness.borrow_mut();
    let ConsumerGroupLiveness {
        retirement,
        retirement_suspects,
        ..
    } = &mut *tracker;
    let Some(progress) = retirement else {
        return false;
    };
    if progress.identity != identity || progress.revision != revision {
        return false;
    }
    let first_round = cursor < namespaces.len();
    for (namespace, created_revision, _) in &attempts {
        if proven.contains(namespace) {
            progress.pending.remove(namespace);
        } else if first_round {
            progress
                .pending
                .insert(*namespace, (*created_revision, now, false));
        } else if let Some((_, _, retried)) = progress.pending.get_mut(namespace) {
            *retried = true;
        }
    }
    if first_round {
        progress.cursor = cursor + attempts.len();
    }
    if progress.cursor < namespaces.len() {
        return false;
    }
    let RetirementProgress {
        pending,
        unprovable,
        ..
    } = progress;
    pending.retain(|namespace, (created_revision, first_attempt, retried)| {
        let given_up = *retried
            && (retirement_suspects.contains(namespace)
                || now.saturating_duration_since(*first_attempt) >= UNPROVABLE_AFTER);
        if given_up {
            unprovable.insert(RetirementDebt {
                namespace: namespace.inner(),
                created_revision: *created_revision,
            });
        }
        !given_up
    });
    if !pending.is_empty() {
        return false;
    }
    progress.reported = true;
    *retirement_suspects = progress
        .unprovable
        .iter()
        .map(|debt| IggyNamespace::from_raw(debt.namespace))
        .collect();
    if !progress.unprovable.is_empty() {
        debug!(
            client_id = identity.client_id,
            unprovable = progress.unprovable.len(),
            "session retirement could not be proven on partitions without a live member quorum"
        );
    }
    true
}

/// The namespaces this node stores, cached per namespace revision. Each node
/// vouches only for the copies it stores.
fn stored_namespaces<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    liveness: &RefCell<ConsumerGroupLiveness>,
    revision: u64,
) -> Rc<[(IggyNamespace, u64)]>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let metadata = shard.plane.metadata();
    let mut tracker = liveness.borrow_mut();
    if let Some((cached_revision, namespaces)) = &tracker.retirement_namespaces
        && *cached_revision == revision
    {
        return Rc::clone(namespaces);
    }
    tracker.retirement_fences.clear();
    tracker.retirement_suspects.clear();
    let (node, nodes) = metadata.consensus.as_ref().map_or((0, 1), |consensus| {
        (consensus.replica(), consensus.replica_count())
    });
    let namespaces: Rc<[(IggyNamespace, u64)]> = metadata.mux_stm.streams().read(|inner| {
        let mut namespaces = Vec::new();
        for (_, stream) in &inner.items {
            for (topic_id, topic) in &stream.topics {
                for partition in topic
                    .partitions
                    .iter()
                    .filter(|partition| partition.replica_set(nodes).member(node).is_some())
                {
                    namespaces.push((
                        IggyNamespace::new(stream.id, topic_id, partition.id),
                        partition.created_revision,
                    ));
                }
            }
        }
        Rc::from(namespaces)
    });
    tracker.retirement_namespaces = Some((revision, Rc::clone(&namespaces)));
    namespaces
}

/// Whether this node's copy of `namespace` holds `identity` retired: an
/// applied barrier, or a durable fence over a failed incarnation. Without a
/// `body` it only reads, so a group that cannot commit never holds a slot.
async fn prove_retired<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    liveness: &RefCell<ConsumerGroupLiveness>,
    identity: SessionIdentity,
    body: Option<&[u8]>,
    namespace: IggyNamespace,
    created_revision: u64,
    system_path: &str,
) -> bool
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let read = || {
        shard.partition_read(
            namespace,
            shard::PartitionRead::SessionRetired {
                identity,
                created_revision,
            },
        )
    };
    match read().await {
        Some(shard::PartitionReadReply::SessionRetired(true)) => return true,
        Some(shard::PartitionReadReply::SessionRetirementFailed {
            created_revision: failed_revision,
        }) if failed_revision == created_revision => {
            if liveness.borrow().retirement_fences.get(&namespace) == Some(&created_revision) {
                return true;
            }
            return match crate::partition_helpers::record_partition_retirement_fence(
                system_path,
                namespace,
                created_revision,
            )
            .await
            {
                Ok(()) => {
                    liveness
                        .borrow_mut()
                        .retirement_fences
                        .insert(namespace, created_revision);
                    true
                }
                Err(error) => {
                    tracing::warn!(%error, namespace_raw = namespace.inner(),
                        "retirement waits for durable partition fence");
                    false
                }
            };
        }
        _ => {}
    }
    let (Some(body), Some(consensus)) = (body, shard.plane.metadata().consensus.as_ref()) else {
        return false;
    };
    let request = retire_sessions_frame(
        consensus.cluster(),
        namespace,
        identity.metadata_watermark,
        consensus.commit_min(),
        body,
    );
    if let Ok(ticket) = shard.partition_submit(namespace, request) {
        let _ = shard.await_partition_submit(ticket).await;
    }
    matches!(
        read().await,
        Some(shard::PartitionReadReply::SessionRetired(true))
    )
}

/// A server-originated `RetireSession` request for `namespace`.
fn retire_sessions_frame(
    cluster: u128,
    namespace: IggyNamespace,
    request: u64,
    metadata_watermark: u64,
    body: &[u8],
) -> Message<RoutedRequestHeader> {
    let size = HEADER_SIZE + body.len();
    let mut message = Message::<RoutedRequestHeader>::new(size).transmute_header(
        |_, header: &mut RoutedRequestHeader| {
            header.command = Command::Request;
            header.operation = Operation::RetireSession;
            header.cluster = cluster;
            header.client = message_bus::AUTO_COMMIT_CLIENT_ID;
            header.session = 1;
            header.request = request;
            header.metadata_watermark = metadata_watermark;
            header.group = namespace.inner();
            header.size = u32::try_from(size).expect("retirement frame fits u32");
        },
    );
    message.as_mut_slice()[HEADER_SIZE..].copy_from_slice(body);
    message
}

/// Settle the tracked retirement from the reports so far: the debt of every
/// partition whose members did not vouch for it, or `None` to keep waiting.
/// A partition goes into debt only once every node expected to report has, or
/// after [`RETIREMENT_DEBT_GRACE`].
fn settle_retirement<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    liveness: &RefCell<ConsumerGroupLiveness>,
    revision: u64,
) -> Option<Vec<RetirementDebt>>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let metadata = shard.plane.metadata();
    let nodes = metadata.consensus.as_ref()?.replica_count();
    let groups = retirement_groups(shard, liveness, revision);
    let now = shard.bus.monotonic_now();
    let mut tracker = liveness.borrow_mut();
    let ConsumerGroupLiveness {
        retirement,
        retirement_absent,
        ..
    } = &mut *tracker;
    let progress = retirement.as_mut()?;
    let debt = retirement_settlement(&groups, &progress.reporters, nodes)?;
    let reported = progress
        .reporters
        .keys()
        .fold(0, |mask, &node| mask | node_bit(node));
    let expected = all_nodes(nodes) & !*retirement_absent;
    if !debt.is_empty() && reported & expected != expected {
        let since = *progress.debt_settleable_since.get_or_insert(now);
        if now.saturating_duration_since(since) < RETIREMENT_DEBT_GRACE {
            return None;
        }
    }
    *retirement_absent = all_nodes(nodes) & !reported;
    Some(debt)
}

fn retirement_groups<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    liveness: &RefCell<ConsumerGroupLiveness>,
    revision: u64,
) -> Rc<RetirementGroups>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let mut tracker = liveness.borrow_mut();
    if let Some((cached_revision, groups)) = &tracker.retirement_groups
        && *cached_revision == revision
    {
        return Rc::clone(groups);
    }
    let metadata = shard.plane.metadata();
    let nodes = metadata
        .consensus
        .as_ref()
        .map_or(1, VsrConsensus::replica_count);
    let groups = Rc::new(build_retirement_groups(metadata.mux_stm.streams(), nodes));
    tracker.retirement_groups = Some((revision, Rc::clone(&groups)));
    groups
}

fn build_retirement_groups(streams: &Streams, nodes: u8) -> RetirementGroups {
    streams.read(|inner| {
        let mut groups = RetirementGroups::default();
        let mut by_mask = BTreeMap::new();
        for (stream_id, stream) in &inner.items {
            for (topic_id, topic) in &stream.topics {
                for partition in &topic.partitions {
                    let members = partition.replica_set(nodes);
                    let mask = members
                        .nodes()
                        .iter()
                        .fold(0, |mask, &node| mask | node_bit(node));
                    let index = *by_mask.entry(mask).or_insert_with(|| {
                        groups.sets.push(MemberSet {
                            mask,
                            quorum: u32::try_from(consensus::quorum_replication(members.len()))
                                .unwrap_or(u32::MAX),
                            partitions: Vec::new(),
                        });
                        groups.sets.len() - 1
                    });
                    let debt = RetirementDebt {
                        namespace: IggyNamespace::new(stream_id, topic_id, partition.id).inner(),
                        created_revision: partition.created_revision,
                    };
                    groups.sets[index].partitions.push(debt);
                    groups.by_partition.insert(debt, index);
                }
            }
        }
        groups
    })
}

/// `None` without a replication quorum of the cluster. Otherwise every
/// partition whose vouching members fall short of a replication quorum of its
/// member set: those were not proven retired and owe a retirement.
fn retirement_settlement(
    groups: &RetirementGroups,
    reporters: &BTreeMap<u8, Unprovable>,
    nodes: u8,
) -> Option<Vec<RetirementDebt>> {
    if reporters.len() < consensus::quorum_replication(nodes) {
        return None;
    }
    let vouching = reporters
        .iter()
        .filter(|(_, withheld)| withheld.is_some())
        .fold(0, |mask, (&node, _)| mask | node_bit(node));
    let vouchers: Vec<u32> = groups
        .sets
        .iter()
        .map(|set| {
            let withheld_sets = reporters
                .iter()
                .filter(|(node, withheld)| {
                    set.mask & node_bit(**node) != 0
                        && withheld
                            .as_ref()
                            .is_some_and(|withheld| withheld.masks.contains(&set.mask))
                })
                .count();
            (vouching & set.mask)
                .count_ones()
                .saturating_sub(u32::try_from(withheld_sets).unwrap_or(u32::MAX))
        })
        .collect();
    let mut debt = Vec::new();
    for (set, &vouchers) in groups.sets.iter().zip(&vouchers) {
        if vouchers < set.quorum {
            debt.extend_from_slice(&set.partitions);
        }
    }
    let mut withheld: BTreeMap<RetirementDebt, (usize, u32)> = BTreeMap::new();
    for (&node, entries) in reporters {
        for entry in entries
            .iter()
            .flat_map(|withheld| withheld.partitions.iter())
        {
            if let Some(&index) = groups.by_partition.get(entry)
                && vouchers[index] >= groups.sets[index].quorum
                && groups.sets[index].mask & node_bit(node) != 0
            {
                withheld.entry(*entry).or_insert((index, 0)).1 += 1;
            }
        }
    }
    for (entry, (index, count)) in withheld {
        if vouchers[index].saturating_sub(count) < groups.sets[index].quorum {
            debt.push(entry);
        }
    }
    Some(debt)
}

/// The vouches a node withholds for `unprovable`, as whole member sets where
/// it could prove none of a set's partitions.
fn withheld_vouches(groups: &RetirementGroups, unprovable: &BTreeSet<RetirementDebt>) -> Withheld {
    let mut by_set: BTreeMap<usize, Vec<RetirementDebt>> = BTreeMap::new();
    let mut withheld = Withheld::default();
    for entry in unprovable {
        match groups.by_partition.get(entry) {
            Some(&index) => by_set.entry(index).or_default().push(*entry),
            None => withheld.partitions.push(*entry),
        }
    }
    for (index, entries) in by_set {
        let set = &groups.sets[index];
        if entries.len() == set.partitions.len() {
            withheld.masks.push(set.mask);
        } else {
            withheld.partitions.extend(entries);
        }
    }
    withheld
}

const fn node_bit(node: u8) -> u32 {
    match 1_u32.checked_shl(node as u32) {
        Some(bit) => bit,
        None => 0,
    }
}

const fn all_nodes(nodes: u8) -> u32 {
    match 1_u32.checked_shl(nodes as u32) {
        Some(bit) => bit - 1,
        None => u32::MAX,
    }
}

async fn finalize_session<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    identity: SessionIdentity,
    revision: u64,
    debt: Vec<RetirementDebt>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let metadata = shard.plane.metadata();
    let encoded = FinalizeSessionRequest {
        identity,
        namespace_revision: revision,
        debt: Vec::new(),
    }
    .encoded_size()
        + size_of::<u32>()
        + debt.len() * RetirementDebt::ENCODED_SIZE;
    if HEADER_SIZE + encoded > shard.bus_max_message_size() {
        warn!(
            client_id = identity.client_id,
            debt = debt.len(),
            "session finalization deferred: its retirement debt exceeds one message"
        );
        return;
    }
    if !debt.is_empty() {
        debug!(
            client_id = identity.client_id,
            debt = debt.len(),
            "finalizing session with partition retirement debt"
        );
    }
    if let Err(error) = metadata
        .submit_session_finalization(identity, revision, debt)
        .await
    {
        trace!(
            ?error,
            client_id = identity.client_id,
            "session finalization deferred"
        );
    }
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
    heartbeat_frame(cluster, view, replica, &body, incomplete)
}

#[allow(clippy::cast_possible_truncation)]
fn heartbeat_frame(
    cluster: u128,
    view: u32,
    replica: u8,
    body: &[u8],
    incomplete: bool,
) -> Message<ConsumerSessionHeartbeatHeader> {
    let size = HEADER_SIZE + body.len();
    let mut message = Message::<ConsumerSessionHeartbeatHeader>::new(size).transmute_header(
        |_, header: &mut ConsumerSessionHeartbeatHeader| {
            header.command = Command::ConsumerSessionHeartbeat;
            header.cluster = cluster;
            header.view = view;
            header.replica = replica;
            header.incomplete = u8::from(incomplete);
            header.size = size as u32;
            header.checksum_body = u128::from(iggy_common::calculate_checksum(body));
            header.seal();
        },
    );
    message.as_mut_slice()[HEADER_SIZE..size].copy_from_slice(body);
    message
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::test_support::{
        SpyBus, TestShard, prepare_message, register_reply, test_shard,
    };
    use consensus::client_table::ClientTable;
    use consensus::{LocalPipeline, PartitionsHandle, Sequencer, ViewRestore, VsrConsensus};
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
    use metadata::stm::stream::TruncatePartitionRequest;
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
    async fn membership_epochs_do_not_replace_the_durable_session_registry() {
        let (_dir, shard) = shard_with_members(1).await;
        let metadata = shard.plane.metadata();
        let topic = CreateTopicWithAssignmentsRequest {
            request: CreateTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                partitions_count: 1,
                name: WireName::new("legacy").unwrap(),
                options: WireOptions::empty(),
            },
            derived_options: WireOptions::empty(),
            partitions: vec![CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 0,
                consensus_group_id: 2,
                members: Vec::new(),
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

            session: 1,
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
        assert_eq!(
            metadata.client_table.borrow().get_epoch(CLIENT),
            Some(SESSION)
        );
        let tracker = RefCell::new(ConsumerGroupLiveness::default());
        let (_stop, receiver) = shard::channel(1);
        let (_signal, shutdown) = Shutdown::new();
        report_and_expire(&shard, &tracker, &receiver, &shutdown, TIMEOUT, TIMEOUT, "").await;
        assert_eq!(tracker.borrow().leases[&CLIENT].session, Some(SESSION));
        let expired_at = Instant::now() + TIMEOUT * 2;
        assert_eq!(
            expire_sessions(&shard, &tracker, &receiver, &shutdown, TIMEOUT, expired_at).await,
            Pass::Drained
        );
        assert_eq!(
            metadata
                .mux_stm
                .streams()
                .consumer_group_memberships(CLIENT),
            []
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
                report_and_expire(&shard, &tracker, &receiver, &shutdown, TIMEOUT, TIMEOUT, "")
                    .await,
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
        metadata.mux_stm.streams().refresh_consumer_group_session(
            CLIENT,
            SESSION + 1,
            SESSION + 1,
            iggy_common::IggyTimestamp::default(),
            None,
        );
        let mut table = ClientTable::new(MAX_LOGOUTS_PER_PASS + 1);
        for index in 0..=MAX_LOGOUTS_PER_PASS {
            let client_id = CLIENT + index as u128;
            let epoch = if index == 0 { SESSION + 1 } else { SESSION };
            table
                .commit_register(client_id, 0, [0x5a; 32], register_reply(client_id, epoch))
                .unwrap();
        }
        *metadata.client_table.borrow_mut() = table;
        let consensus = metadata.consensus.as_ref().unwrap();
        consensus.advance_commit_max(SESSION + 1);
        consensus.advance_commit_min(SESSION + 1);
        consensus.sequencer().set_sequence(SESSION + 1);
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
        assert_eq!(
            metadata.consensus.as_ref().unwrap().commit_min(),
            SESSION + 1
        );

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
            SESSION + 1 + (MAX_LOGOUTS_PER_PASS - 1) as u64
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
        shard_with_members_on_partitions(count, 1).await
    }

    async fn shard_with_members_on_partitions(
        count: usize,
        partitions_count: u32,
    ) -> (tempfile::TempDir, Rc<TestShard>) {
        let dir = tempfile::tempdir().unwrap();
        let mut shard = Rc::try_unwrap(super::super::tests::partition_read_shard(1))
            .ok()
            .unwrap();
        let metadata = shard.plane.metadata_mut();
        metadata.journal = Some(
            PrepareJournal::open(&dir.path().join("journal.wal"), 0)
                .await
                .unwrap(),
        );
        metadata.client_table = RefCell::new(ClientTable::new(count));
        let consensus = metadata.consensus.as_ref().unwrap();
        consensus.restore_commit_state(SESSION, SESSION);
        consensus.sequencer().set_sequence(SESSION);
        let create_stream = CreateStreamRequest {
            name: WireName::new("stream").unwrap(),
            options: WireOptions::empty(),
        };
        let create_topic = CreateTopicWithAssignmentsRequest {
            request: CreateTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                partitions_count,
                name: WireName::new("topic").unwrap(),
                options: WireOptions::empty(),
            },
            derived_options: WireOptions::empty(),
            partitions: (0..partitions_count)
                .map(|partition_id| CreatedPartitionAssignment {
                    initial_view: 0,
                    partition_id,
                    consensus_group_id: u64::from(partition_id) + 1,
                    members: Vec::new(),
                })
                .collect(),
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
            metadata
                .client_table
                .borrow_mut()
                .commit_register(client_id, 0, [0x5a; 32], register_reply(client_id, SESSION))
                .unwrap();
            let join = JoinConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                group_id: WireIdentifier::numeric(0),
                client_id,

                session: 1,
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
            metadata.mux_stm.streams().refresh_consumer_group_session(
                client_id,
                SESSION,
                SESSION,
                iggy_common::IggyTimestamp::default(),
                None,
            );
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

    #[compio::test]
    async fn retirement_sweep_discards_quorum_from_the_previous_view() {
        const END_OP: u64 = SESSION + 1;
        let (_directory, mut shard) = shard_with_members(1).await;
        let mut consensus = VsrConsensus::new(
            1,
            0,
            3,
            METADATA_GROUP,
            shard.bus.clone(),
            LocalPipeline::new(),
        );
        consensus.restore_view(ViewRestore {
            durable_view: Some((VIEW, VIEW)),
            view_fallback: None,
            seed_view: None,
        });
        consensus.init();
        consensus.restore_commit_state(END_OP, END_OP);
        consensus.sequencer().set_sequence(END_OP);
        Rc::get_mut(&mut shard)
            .unwrap()
            .plane
            .metadata_mut()
            .consensus = Some(consensus);
        let metadata = shard.plane.metadata();
        assert!(
            metadata
                .client_table
                .borrow_mut()
                .end_session(CLIENT, 0, SESSION, END_OP)
        );
        let identity = metadata
            .client_table
            .borrow()
            .ended_sessions()
            .next()
            .unwrap();
        let revision = metadata
            .mux_stm
            .streams()
            .read(|inner| inner.namespace_revision);
        let tracker = RefCell::new(ConsumerGroupLiveness::default());
        {
            let mut tracker = tracker.borrow_mut();
            tracker.observe_view(Some(VIEW - 1));
            tracker.reconcile_retirement(Some(identity), revision);
            let progress = tracker.retirement.as_mut().unwrap();
            progress.reported = true;
            progress
                .reporters
                .extend([(0, Some(Rc::default())), (1, Some(Rc::default()))]);
        }
        let (stop, receiver) = shard::channel(1);
        stop.try_send(()).unwrap();
        let (_signal, shutdown) = Shutdown::new();
        let sweep = Box::pin(retire_sessions(
            &shard, &tracker, &receiver, &shutdown, TIMEOUT, "",
        ));
        assert!(
            futures::poll!(sweep).is_ready(),
            "old-view quorum must not start a session finalization"
        );
        let tracker = tracker.borrow();
        let progress = tracker.retirement.as_ref().unwrap();
        assert!(!progress.reported);
        assert!(progress.reporters.is_empty());
        assert_eq!(progress.cursor, 0);
        assert_eq!(
            metadata
                .consensus
                .as_ref()
                .unwrap()
                .sequencer()
                .current_sequence(),
            END_OP
        );
        assert_eq!(
            metadata.client_table.borrow().ended_sessions().next(),
            Some(identity)
        );
    }

    #[test]
    fn given_a_partition_without_member_reports_when_finalizing_should_owe_debt_not_block() {
        let streams = Streams::default();
        let stored = server_common::sharding::IggyNamespace::new(0, 0, 0);
        let sibling = server_common::sharding::IggyNamespace::new(0, 0, 1);
        let everywhere = server_common::sharding::IggyNamespace::new(1, 0, 0);
        for (namespace, group) in [(stored, 1), (sibling, 2)] {
            streams.seed_namespace_on(
                namespace,
                group,
                0,
                iggy_common::Durability::Persisted,
                &[1, 3, 4],
            );
        }
        streams.seed_namespace_on(
            everywhere,
            3,
            0,
            iggy_common::Durability::Persisted,
            &[0, 1, 2, 3, 4],
        );
        let debt = |namespace: server_common::sharding::IggyNamespace| RetirementDebt {
            namespace: namespace.inner(),
            created_revision: streams.created_revision_for_namespace(namespace).unwrap(),
        };
        let groups = build_retirement_groups(&streams, 5);
        let settle = |reporters: &[(u8, Unprovable)]| {
            retirement_settlement(&groups, &reporters.iter().cloned().collect(), 5).map(
                |mut debt| {
                    debt.sort_unstable();
                    debt
                },
            )
        };
        let vouches = |node: u8| (node, Some(Rc::<Withheld>::default()));
        let withholds = |node: u8, unprovable: &[RetirementDebt]| {
            (
                node,
                Some(Rc::new(withheld_vouches(
                    &groups,
                    &unprovable.iter().copied().collect(),
                ))),
            )
        };
        let mut both = vec![debt(stored), debt(sibling)];
        both.sort_unstable();
        assert_eq!(
            settle(&[vouches(0), vouches(1), vouches(2)]),
            Some(both.clone()),
            "a cluster quorum holding one member finalizes and leaves the partitions owing"
        );
        assert_eq!(
            settle(&[vouches(0), vouches(1), vouches(3)]),
            Some(Vec::new())
        );
        assert_eq!(
            settle(&[vouches(1), vouches(3)]),
            None,
            "the cluster-wide quorum still applies"
        );
        assert_eq!(
            settle(&[
                vouches(0),
                withholds(1, &[debt(stored)]),
                vouches(3),
                (4, None)
            ]),
            Some(vec![debt(stored)]),
            "an unprovable or withheld report is not a vouch"
        );
        assert_eq!(
            settle(&[
                vouches(0),
                withholds(1, &[debt(stored), debt(sibling)]),
                vouches(3),
            ]),
            Some(both),
            "a withheld member set counts against every partition in it"
        );
        assert_eq!(
            settle(&[
                vouches(0),
                vouches(1),
                vouches(3),
                withholds(4, &[debt(stored)]),
            ]),
            Some(Vec::new()),
            "two of three members still vouch"
        );
    }

    #[test]
    fn given_withheld_vouches_when_reporting_progress_should_round_trip_or_withhold_all() {
        let session = ConsumerSession {
            client_id: CLIENT,
            session: SESSION,
        };
        let withheld = Withheld {
            masks: vec![0b1010],
            partitions: vec![RetirementDebt {
                namespace: 5,
                created_revision: 6,
            }],
        };
        let report = |body: &[u8]| {
            heartbeat_frame(CLUSTER, VIEW, 1, body, false).transmute_header(|old, header| {
                *header = old;
                header.command = Command::SessionRetirementProgress;
                header.namespace_revision = 1;
                header.seal();
            })
        };
        let identity = iggy_binary_protocol::requests::system::SessionIdentity {
            client_id: CLIENT,
            session: SESSION,
            metadata_watermark: 3,
        };
        for (sent, body_max, expected) in [
            (Some(&withheld), 1024, Some(Rc::new(withheld.clone()))),
            (Some(&Withheld::default()), 1024, Some(Rc::default())),
            (Some(&withheld), 2 * ConsumerSession::ENCODED_SIZE, None),
            (None, 1024, None),
        ] {
            let mut tracker = ConsumerGroupLiveness::default();
            tracker.observe_view(Some(VIEW));
            tracker.reconcile_retirement(Some(identity), 1);
            let body = encode_retirement_progress(session, sent, body_max);
            tracker.receive(CLUSTER, Some(VIEW), &report(&body), Instant::now(), TIMEOUT);
            assert_eq!(
                tracker.retirement.as_ref().unwrap().reporters.get(&1),
                Some(&expected)
            );
        }
    }

    #[test]
    fn retirement_quorum_discards_reports_from_an_older_namespace_revision() {
        let identity = iggy_binary_protocol::requests::system::SessionIdentity {
            client_id: CLIENT,
            session: SESSION,
            metadata_watermark: 3,
        };
        let report = |revision| {
            heartbeat_message(
                CLUSTER,
                VIEW,
                1,
                &[ConsumerSession {
                    client_id: CLIENT,
                    session: SESSION,
                }],
                false,
            )
            .transmute_header(|old, header| {
                *header = old;
                header.command = Command::SessionRetirementProgress;
                header.namespace_revision = revision;
                header.seal();
            })
        };
        let mut tracker = ConsumerGroupLiveness::default();
        tracker.observe_view(Some(VIEW));
        tracker.reconcile_retirement(Some(identity), 1);
        tracker.receive(CLUSTER, Some(VIEW), &report(1), Instant::now(), TIMEOUT);
        assert_eq!(tracker.retirement.as_ref().unwrap().reporters.len(), 1);

        tracker.reconcile_retirement(Some(identity), 2);
        assert!(tracker.retirement.as_ref().unwrap().reporters.is_empty());
        tracker.receive(CLUSTER, Some(VIEW), &report(1), Instant::now(), TIMEOUT);
        assert!(tracker.retirement.as_ref().unwrap().reporters.is_empty());
        tracker.receive(CLUSTER, Some(VIEW), &report(2), Instant::now(), TIMEOUT);
        assert_eq!(tracker.retirement.as_ref().unwrap().reporters.len(), 1);
    }

    #[compio::test]
    async fn given_retirement_budget_expires_during_finalization_when_logging_in_should_make_progress()
     {
        const LOGIN_TIMEOUT: Duration = Duration::from_secs(5);
        let (_directory, shard) = shard_with_members(2).await;
        let metadata = shard.plane.metadata();
        metadata.mux_stm.users().ensure_root_user("root", "hash");
        metadata
            .client_table
            .borrow_mut()
            .commit_capacity(3)
            .unwrap();
        for client_id in [CLIENT, CLIENT + 1] {
            assert!(
                metadata
                    .client_table
                    .borrow_mut()
                    .end_session(client_id, 0, SESSION, SESSION)
            );
        }
        let identity = metadata
            .client_table
            .borrow()
            .ended_sessions()
            .find(|identity| identity.client_id == CLIENT)
            .unwrap();
        let consensus = metadata.consensus.as_ref().unwrap();
        let revision = metadata
            .mux_stm
            .streams()
            .read(|inner| inner.namespace_revision);
        let tracker = RefCell::new(ConsumerGroupLiveness::default());
        {
            let mut tracker = tracker.borrow_mut();
            tracker.observe_view(Some(consensus.view()));
            tracker.reconcile_retirement(Some(identity), revision);
            let progress = tracker.retirement.as_mut().unwrap();
            progress.reported = true;
            progress
                .reporters
                .insert(consensus.replica(), Some(Rc::default()));
        }
        let (_stop, receiver) = shard::channel(1);
        let (_signal, shutdown) = Shutdown::new();
        // Expire at the finalization's first I/O yield, after it claims an op.
        shard.bus.instant_timers.set(true);
        retire_sessions(&shard, &tracker, &receiver, &shutdown, TIMEOUT, "").await;
        shard.bus.instant_timers.set(false);

        metadata.resume_stranded_commits().await;
        let login = compio::time::timeout(
            LOGIN_TIMEOUT,
            metadata.submit_register_in_process(
                CLIENT + 2,
                0,
                iggy_binary_protocol::requests::users::login_register::RegisterRequest {
                    user_created_at: 1,
                    bind_verifier: [0x6b; 32],
                },
            ),
        )
        .await
        .expect("a retirement deadline must not strand a prepare ahead of the next login")
        .expect("finalization must release capacity for the new login");
        assert_eq!(login.epoch, SESSION + 2);
        assert_eq!(metadata.client_table.borrow().count(), 2);
        assert!(metadata.client_table.borrow().get_epoch(CLIENT).is_none());
        assert_eq!(
            metadata
                .client_table
                .borrow()
                .ended_sessions()
                .next()
                .map(|identity| identity.client_id),
            Some(CLIENT + 1),
            "the expired sweep must defer the next session's finalization"
        );
        assert_eq!(
            consensus.commit_min(),
            consensus.sequencer().current_sequence()
        );
    }

    #[compio::test]
    async fn given_failed_partition_when_retiring_should_allow_new_sessions() {
        let (directory, shard) = shard_with_members(1).await;
        let metadata = shard.plane.metadata();
        let namespace = server_common::sharding::IggyNamespace::new(0, 0, 0);
        let created_revision = metadata
            .mux_stm
            .streams()
            .created_revision_for_namespace(namespace)
            .unwrap();
        assert!(
            metadata
                .client_table
                .borrow_mut()
                .end_session(CLIENT, 0, SESSION, SESSION)
        );
        shard.plane.partitions().fence(namespace, created_revision);
        let tracker = RefCell::new(ConsumerGroupLiveness::default());
        let (_stop, receiver) = shard::channel(1);
        let (_signal, shutdown) = Shutdown::new();
        super::super::tests::run_with_partition_message_pump(
            &shard,
            retire_sessions(
                &shard,
                &tracker,
                &receiver,
                &shutdown,
                TIMEOUT,
                directory.path().to_str().unwrap(),
            ),
        )
        .await;
        assert_eq!(
            metadata.client_table.borrow().count(),
            0,
            "one failed partition must not fill the session registry permanently"
        );
        assert_eq!(
            crate::partition_helpers::partition_retirement_fence(
                directory.path().to_str().unwrap(),
                namespace,
            )
            .await
            .unwrap(),
            Some(created_revision)
        );
        metadata
            .client_table
            .borrow_mut()
            .commit_register(
                CLIENT + 1,
                0,
                [0x6b; 32],
                register_reply(CLIENT + 1, SESSION + 2),
            )
            .expect("retirement must release capacity for a new login");
    }

    #[compio::test]
    async fn given_failed_fence_write_when_retiring_should_retain_session_until_retry() {
        let (directory, shard) = shard_with_members(1).await;
        let system_path = directory.path().to_str().unwrap();
        let metadata = shard.plane.metadata();
        assert!(
            metadata
                .client_table
                .borrow_mut()
                .end_session(CLIENT, 0, SESSION, SESSION)
        );
        let namespace = server_common::sharding::IggyNamespace::new(0, 0, 0);
        let created_revision = metadata
            .mux_stm
            .streams()
            .created_revision_for_namespace(namespace)
            .unwrap();
        shard.plane.partitions().fence(namespace, created_revision);
        let obstruction = directory.path().join("partition-initialization");
        std::fs::write(&obstruction, b"not a directory").unwrap();
        let tracker = RefCell::new(ConsumerGroupLiveness::default());
        let (_stop, receiver) = shard::channel(1);
        let (_signal, shutdown) = Shutdown::new();
        super::super::tests::run_with_partition_message_pump(
            &shard,
            retire_sessions(&shard, &tracker, &receiver, &shutdown, TIMEOUT, system_path),
        )
        .await;
        assert_eq!(
            metadata.client_table.borrow().count(),
            1,
            "an unpersisted fence is not a retirement proof"
        );
        std::fs::remove_file(obstruction).unwrap();
        super::super::tests::run_with_partition_message_pump(
            &shard,
            retire_sessions(&shard, &tracker, &receiver, &shutdown, TIMEOUT, system_path),
        )
        .await;
        assert_eq!(metadata.client_table.borrow().count(), 0);
    }

    #[compio::test]
    async fn given_teardown_or_stale_failure_when_retiring_should_not_fence_current_incarnation() {
        for failed_revision in [None, Some(0)] {
            let (directory, shard) = shard_with_members(1).await;
            let metadata = shard.plane.metadata();
            assert!(
                metadata
                    .client_table
                    .borrow_mut()
                    .end_session(CLIENT, 0, SESSION, SESSION)
            );
            let namespace = server_common::sharding::IggyNamespace::new(0, 0, 0);
            if let Some(revision) = failed_revision {
                shard.plane.partitions().fence(namespace, revision);
            } else {
                shard.plane.partitions().tombstone(namespace);
            }
            let tracker = RefCell::new(ConsumerGroupLiveness::default());
            let (_stop, receiver) = shard::channel(1);
            let (_signal, shutdown) = Shutdown::new();
            super::super::tests::run_with_partition_message_pump(
                &shard,
                retire_sessions(
                    &shard,
                    &tracker,
                    &receiver,
                    &shutdown,
                    TIMEOUT,
                    directory.path().to_str().unwrap(),
                ),
            )
            .await;
            assert_eq!(metadata.client_table.borrow().count(), 1);
            assert_eq!(
                crate::partition_helpers::partition_retirement_fence(
                    directory.path().to_str().unwrap(),
                    namespace,
                )
                .await
                .unwrap(),
                None
            );
        }
    }

    #[compio::test]
    async fn given_segment_deletion_when_retiring_should_keep_sweep_progress() {
        let (_directory, shard) = shard_with_members(1).await;
        let metadata = shard.plane.metadata();
        assert!(
            metadata
                .client_table
                .borrow_mut()
                .end_session(CLIENT, 0, SESSION, SESSION)
        );
        let identity = metadata
            .client_table
            .borrow()
            .ended_sessions()
            .next()
            .unwrap();
        let mut tracker = ConsumerGroupLiveness::default();
        tracker.reconcile_retirement(
            Some(identity),
            metadata
                .mux_stm
                .streams()
                .read(|inner| inner.namespace_revision),
        );
        let progress = tracker.retirement.as_mut().unwrap();
        progress.cursor = 1;
        progress.reporters.insert(1, Some(Rc::default()));
        let truncate = TruncatePartitionRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partition_id: 0,
            up_to_offset: 0,
            expected_history: None,
        };
        for _ in 0..3 {
            assert_eq!(
                metadata
                    .mux_stm
                    .update(prepare_message(
                        Operation::TruncatePartition,
                        CLIENT,
                        1,
                        &truncate.to_bytes()
                    ))
                    .unwrap()
                    .code,
                0
            );
            tracker.reconcile_retirement(
                Some(identity),
                metadata
                    .mux_stm
                    .streams()
                    .read(|inner| inner.namespace_revision),
            );
            let progress = tracker.retirement.as_ref().unwrap();
            assert_eq!(
                progress.cursor, 1,
                "segment deletion must not restart session retirement"
            );
            assert!(progress.reporters.contains_key(&1));
        }
    }

    /// Pins the release pace of the registry capacity of ended sessions. Every
    /// session takes its own sweep over all allocated partitions, at most
    /// `MAX_RETIREMENTS_PER_PASS` per pass, so above that many partitions each
    /// further session costs `ceil(P / MAX_RETIREMENTS_PER_PASS) - 1` passes.
    /// Failed partitions report through their fence and need no barrier, so the
    /// count covers only the sweep and the finalization.
    #[compio::test]
    async fn given_ended_sessions_across_partitions_when_sweeping_should_release_one_session_per_sweep()
     {
        const SESSIONS: usize = 3;
        const MANY_PARTITIONS: u32 = 1000;
        const PASSES_MAX: usize = 64;
        let one_pass = u32::try_from(MAX_RETIREMENTS_PER_PASS).unwrap();
        for partitions_count in [one_pass, MANY_PARTITIONS] {
            let sweep_passes = partitions_count.div_ceil(one_pass) as usize;
            let (directory, shard) =
                shard_with_members_on_partitions(SESSIONS, partitions_count).await;
            // A real budget ends a pass by the clock, so the disk speed of the
            // fence writes would move every release.
            shard.bus.frozen_timers.set(true);
            let system_path = directory.path().to_str().unwrap();
            let metadata = shard.plane.metadata();
            let streams = metadata.mux_stm.streams();
            for partition_id in 0..partitions_count {
                let namespace =
                    server_common::sharding::IggyNamespace::new(0, 0, partition_id as usize);
                let created_revision = streams.created_revision_for_namespace(namespace).unwrap();
                shard.plane.partitions().fence(namespace, created_revision);
            }
            for index in 0..SESSIONS {
                assert!(metadata.client_table.borrow_mut().end_session(
                    CLIENT + index as u128,
                    0,
                    SESSION,
                    SESSION
                ));
            }
            let tracker = RefCell::new(ConsumerGroupLiveness::default());
            let (_stop, receiver) = shard::channel(1);
            let (_signal, shutdown) = Shutdown::new();
            let mut release_passes = Vec::with_capacity(SESSIONS);
            for pass in 1..=PASSES_MAX {
                let before = metadata.client_table.borrow().count();
                compio::time::timeout(
                    Duration::from_secs(30),
                    super::super::tests::run_with_partition_message_pump(
                        &shard,
                        retire_sessions(
                            &shard,
                            &tracker,
                            &receiver,
                            &shutdown,
                            TIMEOUT,
                            system_path,
                        ),
                    ),
                )
                .await
                .expect("a frozen virtual budget must still obey the external test deadline");
                let after = metadata.client_table.borrow().count();
                release_passes.extend(std::iter::repeat_n(pass, before - after));
                if after == 0 {
                    break;
                }
            }
            // A sweep starts in the pass that released the session before it.
            let expected: Vec<usize> = (0..SESSIONS)
                .map(|released| sweep_passes + released * (sweep_passes - 1))
                .collect();
            assert_eq!(
                release_passes, expected,
                "P={partitions_count}: each slot must be released in the pass where its sweep \
                 covers every partition"
            );
        }
    }

    #[compio::test]
    async fn given_a_recorded_debt_when_paying_should_retire_the_finalized_session_and_open_the_gate()
     {
        const STRANDED: u128 = CLIENT + 7;
        const STRANDED_SESSION: u64 = 5;
        const DEBT_OP: u64 = 9;
        let (_directory, shard) = shard_with_members(1).await;
        let metadata = shard.plane.metadata();
        let namespace = IggyNamespace::new(0, 0, 0);
        let created_revision = metadata
            .mux_stm
            .streams()
            .created_revision_for_namespace(namespace)
            .unwrap();
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            namespace.inner(),
            shard.bus.clone(),
            LocalPipeline::new(),
        );
        consensus.init();
        let mut partition = partitions::IggyPartition::with_in_memory_storage(
            std::sync::Arc::new(iggy_common::PartitionStats::default()),
            consensus,
            shard.plane.partitions().config().segment_size,
        );
        partition.set_created_revision(created_revision);
        shard.plane.partitions().insert(namespace, partition);
        shard::shards_table::ShardsTable::insert(
            shard.shards_table(),
            namespace,
            server_common::sharding::PartitionLocation::new(
                server_common::sharding::ShardId::new(0),
                created_revision,
            ),
        );
        let mut messages = server_common::send_messages::IggyMessages::with_capacity(1);
        messages.push(server_common::send_messages::IggyMessage {
            header: server_common::send_messages::IggyMessageHeader::default(),
            payload: bytes::Bytes::from_static(b"stranded"),
            user_headers: None,
        });
        let batch =
            server_common::send_messages::SendMessagesOwned::from_messages(namespace, &messages)
                .unwrap();
        let mut body = vec![0; batch.header.total_size()];
        batch.header.encode_into(&mut body);
        body[iggy_binary_protocol::batch::BATCH_HEADER_SIZE..].copy_from_slice(&batch.blob);
        let send = crate::dispatch::test_support::request_message(
            Operation::SendMessages,
            STRANDED,
            STRANDED_SESSION,
            1,
            &body,
        )
        .transmute_header(|original, header: &mut RoutedRequestHeader| {
            *header = original;
            header.cluster = 1;
            header.group = namespace.inner();
            header.partition_incarnation = created_revision;
        });
        super::super::tests::run_with_partition_message_pump(&shard, async {
            let ticket = shard.partition_submit(namespace, send).unwrap();
            let reply = shard.await_partition_submit(ticket).await.unwrap();
            assert!(reply_committed(&reply), "the stranded session writes once");
        })
        .await;
        let slice_holds = |shard: &TestShard| {
            shard
                .plane
                .partitions()
                .get_by_ns(&namespace)
                .unwrap()
                .observe_retirement_debt(0)
        };
        assert_eq!(
            slice_holds(&shard),
            RetirementDebtStatus::Paid,
            "no debt is owed before one is recorded"
        );
        assert_eq!(
            shard
                .plane
                .partitions()
                .get_by_ns(&namespace)
                .unwrap()
                .observe_retirement_debt(1),
            RetirementDebtStatus::Unpaid(Some(vec![(STRANDED, STRANDED_SESSION)]))
        );
        metadata.mux_stm.streams().record_retirement_debt(
            &[RetirementDebt {
                namespace: namespace.inner(),
                created_revision,
            }],
            DEBT_OP,
        );

        let tracker = RefCell::new(ConsumerGroupLiveness::default());
        let (_stop, receiver) = shard::channel(1);
        let (_signal, shutdown) = Shutdown::new();
        super::super::tests::run_with_partition_message_pump(
            &shard,
            pay_retirement_debts(&shard, &tracker, &receiver, &shutdown),
        )
        .await;

        let partitions = shard.plane.partitions();
        let partition = partitions.get_by_ns(&namespace).unwrap();
        assert!(
            !partition.owes_session_retirement(),
            "the payment committed and opened the gate"
        );
        assert_eq!(
            partition.observe_retirement_debt(DEBT_OP),
            RetirementDebtStatus::Paid
        );
        assert_eq!(
            partition.observe_retirement_debt(DEBT_OP + 1),
            RetirementDebtStatus::Unpaid(Some(Vec::new())),
            "the finalized session left the retry slice"
        );
        let tracker = tracker.borrow();
        assert!(tracker.retirement_debts.unpaid.is_empty());
        assert_eq!(
            tracker.retirement_debts.paid.get(&namespace),
            Some(&(created_revision, DEBT_OP))
        );
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
        for command in [
            Command::ConsumerSessionHeartbeat,
            Command::SessionRetirementProgress,
        ] {
            let message = heartbeat_message(CLUSTER, VIEW, 2, &sessions, false).transmute_header(
                |old, header| {
                    *header = old;
                    header.command = command;
                    header.seal();
                },
            );
            assert_eq!(
                u128::from(iggy_common::calculate_checksum(message.body())),
                message.header().checksum_body
            );
            let bag = MessageBag::try_from(message.into_generic()).unwrap();
            assert_eq!(bag.command(), command);
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
            corrupted.as_mut_slice()[std::mem::offset_of!(ConsumerSessionHeartbeatHeader, view)] ^=
                1;
            assert!(MessageBag::try_from(corrupted).is_err());
        }
    }
}
