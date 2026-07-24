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

//! In-flight admission for awaited partition writes: the per-session /
//! shard-global budget guard. Coupled to the router body cap because the shard
//! inbox is one shared bounded channel, so admission is correctness-adjacent.

use std::cell::Cell;

use crate::http::error::PartitionWriteError;

/// In-flight admission token for one awaited partition write. One guard owns
/// both releases (session + global) so success, every error return, the reply
/// timeout, and handler cancellation (the client hanging up mid-await drops
/// this future) all decrement through the same `Drop`.
pub(in crate::http) struct InFlightWriteGuard<'a> {
    session_in_flight: &'a Cell<u32>,
    global_in_flight: &'a Cell<u32>,
}

impl Drop for InFlightWriteGuard<'_> {
    fn drop(&mut self) {
        self.session_in_flight.set(self.session_in_flight.get() - 1);
        self.global_in_flight.set(self.global_in_flight.get() - 1);
    }
}

/// Admit one awaited partition write against the per-session cap and the
/// shard-0 global budget, incrementing both counters only when both pass. The
/// session cap is checked first so a session that saturates itself reads as
/// its own 429 rather than as server-wide pressure.
///
/// Both caps come from `[http_admission]`. `max_global` bounds both starvation
/// terms every admitted write imposes on shard 0 - budget x `max_request_size`
/// worst-case buffered bytes, budget x per-request decode/encode/HS256 CPU on
/// the core that also pumps consensus. `max_per_session` bounds one credential's
/// slice of that budget, so a session that outruns its own commits reads its own
/// 429 before it can spill onto the shared budget. `?ack=none` produces are
/// admitted through the same caps: they install no reply slot, but still park
/// inside dispatch while pinning their buffered body, so leaving them uncapped
/// would bypass both terms.
pub(in crate::http) fn admit_partition_write<'a>(
    session_in_flight: &'a Cell<u32>,
    global_in_flight: &'a Cell<u32>,
    max_per_session: u32,
    max_global: u32,
) -> Result<InFlightWriteGuard<'a>, PartitionWriteError> {
    if session_in_flight.get() >= max_per_session {
        return Err(PartitionWriteError::TooManyInFlight);
    }
    if global_in_flight.get() >= max_global {
        return Err(PartitionWriteError::ServerBusy);
    }
    session_in_flight.set(session_in_flight.get() + 1);
    global_in_flight.set(global_in_flight.get() + 1);
    Ok(InFlightWriteGuard {
        session_in_flight,
        global_in_flight,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SESSION_CAP: u32 = 32;
    const GLOBAL_CAP: u32 = 128;

    /// Admit at the fixture caps, so each test reads the counting behavior
    /// without restating the caps on every call.
    fn admit<'a>(
        session_in_flight: &'a Cell<u32>,
        global_in_flight: &'a Cell<u32>,
    ) -> Result<InFlightWriteGuard<'a>, PartitionWriteError> {
        admit_partition_write(session_in_flight, global_in_flight, SESSION_CAP, GLOBAL_CAP)
    }

    #[test]
    fn in_flight_write_guard_decrements_both_counters_on_drop() {
        let session = Cell::new(0);
        let global = Cell::new(0);
        let guard = admit(&session, &global).expect("below both caps");
        assert_eq!(session.get(), 1);
        assert_eq!(global.get(), 1);
        drop(guard);
        assert_eq!(session.get(), 0);
        assert_eq!(global.get(), 0);
    }

    #[test]
    fn admission_at_session_cap_rejects_with_too_many_in_flight() {
        let session = Cell::new(SESSION_CAP);
        let global = Cell::new(0);
        assert!(matches!(
            admit(&session, &global),
            Err(PartitionWriteError::TooManyInFlight)
        ));
        // A refusal must not leak a partial increment on either counter.
        assert_eq!(session.get(), SESSION_CAP);
        assert_eq!(global.get(), 0);
    }

    #[test]
    fn admission_at_global_budget_rejects_with_server_busy() {
        let session = Cell::new(0);
        let global = Cell::new(GLOBAL_CAP);
        assert!(matches!(
            admit(&session, &global),
            Err(PartitionWriteError::ServerBusy)
        ));
        assert_eq!(session.get(), 0);
        assert_eq!(global.get(), GLOBAL_CAP);
    }

    #[test]
    fn interleaved_admission_reopens_exactly_released_session_slots() {
        let session = Cell::new(0);
        let global = Cell::new(0);
        let mut guards = Vec::new();
        for _ in 0..SESSION_CAP {
            guards.push(admit(&session, &global).expect("below both caps"));
        }
        assert!(matches!(
            admit(&session, &global),
            Err(PartitionWriteError::TooManyInFlight)
        ));
        let released = 3;
        guards.truncate((SESSION_CAP - released) as usize);
        assert_eq!(session.get(), SESSION_CAP - released);
        for _ in 0..released {
            guards.push(admit(&session, &global).expect("released slots"));
        }
        assert!(matches!(
            admit(&session, &global),
            Err(PartitionWriteError::TooManyInFlight)
        ));
        drop(guards);
        assert_eq!(session.get(), 0);
        assert_eq!(global.get(), 0);
    }

    #[test]
    fn global_budget_spans_sessions_and_reopens_after_release() {
        let global = Cell::new(0);
        let session_count = GLOBAL_CAP.div_ceil(SESSION_CAP) as usize;
        let sessions: Vec<Cell<u32>> = (0..session_count).map(|_| Cell::new(0)).collect();
        let mut guards = Vec::new();
        'fill: for session in &sessions {
            for _ in 0..SESSION_CAP {
                match admit(session, &global) {
                    Ok(guard) => guards.push(guard),
                    Err(PartitionWriteError::ServerBusy) => break 'fill,
                    Err(other) => {
                        panic!("only the global budget may refuse this fill, got {other:?}")
                    }
                }
            }
        }
        assert_eq!(global.get(), GLOBAL_CAP);
        // A fresh session is refused on the shared budget, not its own cap.
        let fresh = Cell::new(0);
        assert!(matches!(
            admit(&fresh, &global),
            Err(PartitionWriteError::ServerBusy)
        ));
        drop(guards.pop());
        let readmitted = admit(&fresh, &global).expect("budget slot released");
        assert_eq!(fresh.get(), 1);
        assert_eq!(global.get(), GLOBAL_CAP);
        drop(readmitted);
    }
}
