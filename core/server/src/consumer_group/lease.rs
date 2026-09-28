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

//! Volatile, primary-owned leases for persisted consumer-group memberships.
//! A new primary and each newly observed session get a full timeout. Only
//! server-observed connections renew leases, never recovered client-table rows.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use iggy_binary_protocol::{ConsumerSession, ConsumerSessionHeartbeatHeader, WireDecode};
use server_common::Message;
use tracing::{debug, warn};

#[derive(Debug)]
pub(super) struct Lease {
    pub(super) session: Option<u64>,
    pub(super) last_seen: Instant,
}

/// Bounded by the current consumer-group members, independent of heartbeat volume.
#[derive(Default)]
pub struct ConsumerGroupLiveness {
    view: Option<u32>,
    pub(super) leases: BTreeMap<u128, Lease>,
    pub(super) last_incomplete: Option<Instant>,
    pub(super) last_incomplete_warning: Option<Instant>,
    pub(super) report_offset: usize,
    pub(super) report_incomplete: bool,
}

impl ConsumerGroupLiveness {
    pub(super) fn observe_view(&mut self, view: Option<u32>) {
        if self.view != view {
            self.leases.clear();
            self.last_incomplete = None;
            self.last_incomplete_warning = None;
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
        // Validate the whole batch before refreshing anything.
        let sessions = message
            .body()
            .as_chunks::<{ ConsumerSession::ENCODED_SIZE }>()
            .0;
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
