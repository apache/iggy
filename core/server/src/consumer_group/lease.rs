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

//! Volatile, primary-owned leases for registered logical sessions.
//! A new primary and each newly observed session get a full timeout. Only
//! server-observed connections renew leases, never recovered client-table rows.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::{Rc, Weak};
use std::time::{Duration, Instant};

use bytes::{BufMut, BytesMut};
use iggy_binary_protocol::requests::system::{RetirementDebt, SessionIdentity};
use iggy_binary_protocol::{
    Command, ConsumerSession, ConsumerSessionHeartbeatHeader, MAX_CONSUMER_SESSIONS_PER_HEARTBEAT,
    WireDecode, WireEncode,
};
use server_common::Message;
use server_common::sharding::IggyNamespace;
use tracing::{debug, warn};

#[derive(Debug)]
pub(super) struct Lease {
    pub(super) session: Option<u64>,
    pub(super) last_seen: Instant,
}

pub struct SessionActivity {
    session: ConsumerSession,
    last_seen: Cell<Instant>,
}

impl SessionActivity {
    pub(crate) fn new(session: ConsumerSession) -> Self {
        Self {
            session,
            last_seen: Cell::new(Instant::now()),
        }
    }

    pub(crate) fn touch(&self) {
        self.last_seen.set(Instant::now());
    }
}

/// What a reporter could not prove retired among the partitions it stores.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct Withheld {
    /// Member sets none of whose partitions it could prove.
    pub(super) masks: Vec<u32>,
    pub(super) partitions: Vec<RetirementDebt>,
}

/// A reporter's withheld vouches. `None` vouches for none of its partitions.
pub(super) type Unprovable = Option<Rc<Withheld>>;

pub(super) struct RetirementProgress {
    pub(super) identity: SessionIdentity,
    pub(super) revision: u64,
    pub(super) cursor: usize,
    /// Swept namespaces not yet proven: created revision, first attempt, and
    /// whether a retry followed it.
    pub(super) pending: BTreeMap<IggyNamespace, (u64, Instant, bool)>,
    pub(super) unprovable: BTreeSet<RetirementDebt>,
    pub(super) reported: bool,
    last_report: Option<Instant>,
    pub(super) reporters: BTreeMap<u8, Unprovable>,
    pub(super) debt_settleable_since: Option<Instant>,
}

impl RetirementProgress {
    pub(super) fn report_due(&mut self, now: Instant, interval: Duration) -> bool {
        if self
            .last_report
            .is_some_and(|last| now.saturating_duration_since(last) < interval)
        {
            return false;
        }
        self.last_report = Some(now);
        true
    }
}

type RetirementNamespaces = Rc<[(IggyNamespace, u64)]>;

/// Committed partitions grouped by member set, for counting vouching reporters
/// once per set instead of once per partition.
#[derive(Default)]
pub(super) struct RetirementGroups {
    pub(super) sets: Vec<MemberSet>,
    pub(super) by_partition: BTreeMap<RetirementDebt, usize>,
}

pub(super) struct MemberSet {
    pub(super) mask: u32,
    pub(super) quorum: u32,
    pub(super) partitions: Vec<RetirementDebt>,
}

/// Unpaid retirement debts of partitions this node stores, and the newest
/// debt op absorbed into them.
#[derive(Default)]
pub(super) struct RetirementDebts {
    pub(super) seen_through: Option<u64>,
    /// `(created_revision, debt_op)` by namespace.
    pub(super) unpaid: BTreeMap<IggyNamespace, (u64, u64)>,
    /// Debts confirmed paid, so a rescan does not visit them again.
    pub(super) paid: BTreeMap<IggyNamespace, (u64, u64)>,
    pub(super) cursor: Option<IggyNamespace>,
}

/// Bounded by the session registry, independent of heartbeat volume.
#[derive(Default)]
pub struct ConsumerGroupLiveness {
    view: Option<u32>,
    local_sessions: BTreeMap<u128, Weak<SessionActivity>>,
    pub(super) leases: BTreeMap<u128, Lease>,
    pub(super) last_incomplete: Option<Instant>,
    pub(super) last_incomplete_warning: Option<Instant>,
    pub(super) report_offset: usize,
    pub(super) report_incomplete: bool,
    pub(super) retirement: Option<RetirementProgress>,
    pub(super) retirement_namespaces: Option<(u64, RetirementNamespaces)>,
    pub(super) retirement_fences: BTreeMap<IggyNamespace, u64>,
    /// Namespaces the last completed sweep could not prove: the next sweep
    /// gives each of them one attempt instead of the full wait.
    pub(super) retirement_suspects: BTreeSet<IggyNamespace>,
    pub(super) retirement_groups: Option<(u64, Rc<RetirementGroups>)>,
    /// Nodes missing from the reports of the last finalization, which the
    /// next one does not wait for before recording debt.
    pub(super) retirement_absent: u32,
    pub(super) retirement_debts: RetirementDebts,
}

impl ConsumerGroupLiveness {
    pub(crate) fn observe_local_session(&mut self, activity: &Rc<SessionActivity>) {
        self.local_sessions
            .retain(|_, activity| activity.strong_count() != 0);
        self.local_sessions
            .insert(activity.session.client_id, Rc::downgrade(activity));
    }

    pub(super) fn local_activity(
        &mut self,
        now: Instant,
        timeout: Duration,
    ) -> Vec<ConsumerSession> {
        let mut sessions = Vec::new();
        self.local_sessions.retain(|_, activity| {
            let Some(activity) = activity.upgrade() else {
                return false;
            };
            if now.saturating_duration_since(activity.last_seen.get()) < timeout {
                sessions.push(activity.session);
            }
            true
        });
        sessions
    }

    pub(super) fn observe_view(&mut self, view: Option<u32>) {
        if self.view != view {
            self.leases.clear();
            self.last_incomplete = None;
            self.last_incomplete_warning = None;
            self.retirement = None;
            self.view = view;
        }
    }

    pub(super) fn reconcile(
        &mut self,
        view: u32,
        members: &BTreeMap<u128, Option<u64>>,
        now: Instant,
    ) {
        self.observe_view(Some(view));
        self.leases
            .retain(|client_id, _| members.contains_key(client_id));
        for (&client_id, &session) in members {
            let lease = self.leases.entry(client_id).or_insert(Lease {
                session,
                last_seen: now,
            });
            if lease.session != session {
                *lease = Lease {
                    session,
                    last_seen: now,
                };
            }
        }
    }

    pub(super) fn renew(&mut self, session: ConsumerSession, now: Instant) {
        if let Some(lease) = self.leases.get_mut(&session.client_id)
            && lease.session == Some(session.session)
        {
            lease.last_seen = now;
        }
    }

    pub(super) fn reconcile_retirement(
        &mut self,
        identity: Option<SessionIdentity>,
        revision: u64,
    ) {
        if self.retirement.as_ref().map(|progress| progress.identity) != identity {
            self.retirement = identity.map(|identity| RetirementProgress {
                identity,
                revision,
                cursor: 0,
                pending: BTreeMap::new(),
                unprovable: BTreeSet::new(),
                reported: false,
                last_report: None,
                reporters: BTreeMap::new(),
                debt_settleable_since: None,
            });
        } else if let Some(progress) = &mut self.retirement
            && progress.revision != revision
        {
            progress.revision = revision;
            progress.cursor = 0;
            progress.pending.clear();
            progress.unprovable.clear();
            progress.reported = false;
            progress.last_report = None;
            progress.reporters.clear();
            progress.debt_settleable_since = None;
        }
    }

    pub(super) fn defer_expiry(&mut self, now: Instant, timeout: Duration, replica: u8) {
        // Recovered memberships do not identify their hosting replica. Until
        // every reporting node can enumerate its clients, absence is uncertain.
        if self
            .last_incomplete_warning
            .is_none_or(|last| now.saturating_duration_since(last) >= timeout)
        {
            warn!(
                replica,
                ?timeout,
                "incomplete consumer session report; deferring session expiry"
            );
            self.last_incomplete_warning = Some(now);
        }
        self.last_incomplete = Some(now);
    }

    pub(crate) fn receive(
        &mut self,
        cluster: u128,
        primary_view: Option<u32>,
        message: &Message<ConsumerSessionHeartbeatHeader>,
        now: Instant,
        timeout: Duration,
    ) {
        self.observe_view(primary_view);
        let header = message.header();
        if primary_view != Some(header.view) || header.cluster != cluster {
            return;
        }
        if header.command == Command::SessionRetirementProgress {
            if header.incomplete != 0 {
                return;
            }
            let Some((session, unprovable)) = decode_retirement_progress(message.body()) else {
                debug!(
                    replica = header.replica,
                    "invalid session retirement progress body"
                );
                return;
            };
            if let Some(progress) = &mut self.retirement
                && header.namespace_revision == progress.revision
                && session.client_id == progress.identity.client_id
                && session.session == progress.identity.session
            {
                progress.reporters.insert(header.replica, unprovable);
            }
            return;
        }
        // Validate the whole batch before refreshing anything.
        let (sessions, remainder) = message
            .body()
            .as_chunks::<{ ConsumerSession::ENCODED_SIZE }>();
        if !remainder.is_empty()
            || sessions.len() > iggy_binary_protocol::MAX_CONSUMER_SESSIONS_PER_HEARTBEAT
            || header.incomplete > 1
        {
            return;
        }
        if let Err(error) = sessions
            .iter()
            .try_for_each(|bytes| ConsumerSession::decode(bytes).map(|_| ()))
        {
            debug!(
                ?error,
                replica = header.replica,
                "invalid consumer session heartbeat body"
            );
            return;
        }
        if header.incomplete != 0 {
            self.defer_expiry(now, timeout, header.replica);
        }
        for bytes in sessions {
            if let Ok((session, _)) = ConsumerSession::decode(bytes) {
                self.renew(session, now);
            }
        }
    }

    pub(super) fn expired(
        &self,
        view: u32,
        client_id: u128,
        session: Option<u64>,
        now: Instant,
        timeout: Duration,
    ) -> bool {
        self.view == Some(view)
            && self.last_incomplete.is_none_or(|last_incomplete| {
                now.saturating_duration_since(last_incomplete) >= timeout
            })
            && self.leases.get(&client_id).is_some_and(|lease| {
                lease.session == session
                    && now.saturating_duration_since(lease.last_seen) >= timeout
            })
    }
}

const WITHHELD_PARTITION: u64 = 1;
const WITHHELD_MEMBER_SET: u64 = 2;
const WITHHELD_ALL: u64 = 3;

/// A retirement progress body: the session, then one 24-byte record per
/// withheld vouch, `namespace u64 | created_revision u64 | tag u64`, or
/// `mask u64 | 0 | tag` for a member set. The records keep the body whole
/// heartbeat sessions; a list that does not fit is sent as one record
/// withholding every vouch.
pub(super) fn encode_retirement_progress(
    session: ConsumerSession,
    withheld: Option<&Withheld>,
    body_max: usize,
) -> BytesMut {
    let records = withheld.map_or(1, |withheld| {
        withheld.masks.len() + withheld.partitions.len()
    });
    let fits = withheld.is_some()
        && records < MAX_CONSUMER_SESSIONS_PER_HEARTBEAT
        && (records + 1) * ConsumerSession::ENCODED_SIZE <= body_max;
    let mut body = BytesMut::with_capacity((records + 1) * ConsumerSession::ENCODED_SIZE);
    session.encode(&mut body);
    let mut record = |first: u64, second: u64, tag: u64| {
        body.put_u64_le(first);
        body.put_u64_le(second);
        body.put_u64_le(tag);
    };
    match withheld {
        Some(withheld) if fits => {
            for &mask in &withheld.masks {
                record(u64::from(mask), 0, WITHHELD_MEMBER_SET);
            }
            for debt in &withheld.partitions {
                record(debt.namespace, debt.created_revision, WITHHELD_PARTITION);
            }
        }
        _ => record(0, 0, WITHHELD_ALL),
    }
    body
}

fn decode_retirement_progress(body: &[u8]) -> Option<(ConsumerSession, Unprovable)> {
    let (records, remainder) = body.as_chunks::<{ ConsumerSession::ENCODED_SIZE }>();
    let (session, records) = records.split_first()?;
    if !remainder.is_empty() {
        return None;
    }
    let (session, _) = ConsumerSession::decode(session).ok()?;
    let mut withheld = Withheld::default();
    for record in records {
        let (first, rest) = record.split_first_chunk::<8>()?;
        let (second, tag) = rest.split_first_chunk::<8>()?;
        let (first, second) = (u64::from_le_bytes(*first), u64::from_le_bytes(*second));
        match u64::from_le_bytes(tag.try_into().ok()?) {
            WITHHELD_PARTITION => withheld.partitions.push(RetirementDebt {
                namespace: first,
                created_revision: second,
            }),
            WITHHELD_MEMBER_SET if second == 0 => {
                withheld.masks.push(u32::try_from(first).ok()?);
            }
            WITHHELD_ALL if records.len() == 1 && first == 0 && second == 0 => {
                return Some((session, None));
            }
            _ => return None,
        }
    }
    Some((session, Some(Rc::new(withheld))))
}

#[cfg(test)]
mod tests {
    use super::{ConsumerGroupLiveness, SessionActivity};
    use iggy_binary_protocol::ConsumerSession;
    use std::rc::Rc;
    use std::time::{Duration, Instant};

    #[test]
    fn retirement_reports_are_paced_and_restart_after_view_or_revision_changes() {
        const INTERVAL: Duration = Duration::from_secs(5);
        let identity = iggy_binary_protocol::requests::system::SessionIdentity {
            client_id: 1,
            session: 2,
            metadata_watermark: 3,
        };
        let mut tracker = ConsumerGroupLiveness::default();
        tracker.observe_view(Some(1));
        tracker.reconcile_retirement(Some(identity), 1);
        let now = Instant::now();
        let progress = tracker.retirement.as_mut().unwrap();
        assert!(progress.report_due(now, INTERVAL));
        assert!(!progress.report_due(now + Duration::from_millis(100), INTERVAL));
        assert!(progress.report_due(now + INTERVAL, INTERVAL));
        tracker.reconcile_retirement(Some(identity), 2);
        assert!(
            tracker
                .retirement
                .as_mut()
                .unwrap()
                .report_due(now + INTERVAL, INTERVAL)
        );
        tracker.observe_view(Some(2));
        tracker.reconcile_retirement(Some(identity), 2);
        assert!(
            tracker
                .retirement
                .as_mut()
                .unwrap()
                .report_due(now + INTERVAL, INTERVAL)
        );
    }

    #[test]
    fn local_sessions_report_recent_activity_and_release_dropped_descriptors() {
        const TIMEOUT: Duration = Duration::from_secs(10);
        let identity = ConsumerSession {
            client_id: 1,
            session: 2,
        };
        let activity = Rc::new(SessionActivity::new(identity));
        let mut tracker = ConsumerGroupLiveness::default();
        tracker.observe_local_session(&activity);
        let now = Instant::now();
        assert_eq!(tracker.local_activity(now, TIMEOUT), vec![identity]);
        assert_eq!(tracker.local_activity(now + TIMEOUT, TIMEOUT), []);
        activity.touch();
        assert_eq!(
            tracker.local_activity(Instant::now(), TIMEOUT),
            vec![identity]
        );
        drop(activity);
        assert_eq!(tracker.local_activity(now, TIMEOUT), []);
        assert!(tracker.local_sessions.is_empty());
    }
}
