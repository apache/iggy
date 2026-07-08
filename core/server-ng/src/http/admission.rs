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

/// Per-session cap on concurrently awaited partition writes (produce /
/// consumer-offset). Bounds how much of [`MAX_IN_FLIGHT_WRITES_GLOBAL`] one
/// credential can occupy, so a session that outruns its own commits saturates
/// itself (429, its own backpressure signal) before it can starve every other
/// session out of the shared budget.
const MAX_IN_FLIGHT_WRITES_PER_SESSION: u32 = 32;

/// Shard-0-global budget for concurrently awaited partition writes, across all
/// sessions. Every admitted write parks a handler for up to
/// [`PARTITION_WRITE_REPLY_TIMEOUT`] while pinning its buffered body, and its
/// decode/encode/HS256 CPU runs on the same single-threaded core that pumps
/// consensus. The budget therefore bounds both starvation terms: budget x
/// `max_request_size` bounds the worst-case buffered bytes, and budget x
/// per-request CPU bounds how far admitted HTTP work can delay the consensus
/// pump. `?ack=none` produces are admitted through the same caps: they install
/// no reply slot and never await a commit, but they still park inside dispatch
/// for the routable-wait budget while pinning their buffered body, so leaving
/// them uncapped would bypass both terms. A session that saturates its own cap
/// reads its own 429 before it can spill onto the shared budget.
const MAX_IN_FLIGHT_WRITES_GLOBAL: u32 = 128;

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
pub(in crate::http) fn admit_partition_write<'a>(
    session_in_flight: &'a Cell<u32>,
    global_in_flight: &'a Cell<u32>,
) -> Result<InFlightWriteGuard<'a>, PartitionWriteError> {
    if session_in_flight.get() >= MAX_IN_FLIGHT_WRITES_PER_SESSION {
        return Err(PartitionWriteError::TooManyInFlight);
    }
    if global_in_flight.get() >= MAX_IN_FLIGHT_WRITES_GLOBAL {
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

    #[test]
    fn in_flight_write_guard_decrements_both_counters_on_drop() {
        let session = Cell::new(0);
        let global = Cell::new(0);
        let guard = admit_partition_write(&session, &global).expect("below both caps");
        assert_eq!(session.get(), 1);
        assert_eq!(global.get(), 1);
        drop(guard);
        assert_eq!(session.get(), 0);
        assert_eq!(global.get(), 0);
    }

    #[test]
    fn admission_at_session_cap_rejects_with_too_many_in_flight() {
        let session = Cell::new(MAX_IN_FLIGHT_WRITES_PER_SESSION);
        let global = Cell::new(0);
        assert!(matches!(
            admit_partition_write(&session, &global),
            Err(PartitionWriteError::TooManyInFlight)
        ));
        // A refusal must not leak a partial increment on either counter.
        assert_eq!(session.get(), MAX_IN_FLIGHT_WRITES_PER_SESSION);
        assert_eq!(global.get(), 0);
    }

    #[test]
    fn admission_at_global_budget_rejects_with_server_busy() {
        let session = Cell::new(0);
        let global = Cell::new(MAX_IN_FLIGHT_WRITES_GLOBAL);
        assert!(matches!(
            admit_partition_write(&session, &global),
            Err(PartitionWriteError::ServerBusy)
        ));
        assert_eq!(session.get(), 0);
        assert_eq!(global.get(), MAX_IN_FLIGHT_WRITES_GLOBAL);
    }

    #[test]
    fn interleaved_admission_reopens_exactly_released_session_slots() {
        let session = Cell::new(0);
        let global = Cell::new(0);
        let mut guards = Vec::new();
        for _ in 0..MAX_IN_FLIGHT_WRITES_PER_SESSION {
            guards.push(admit_partition_write(&session, &global).expect("below both caps"));
        }
        assert!(matches!(
            admit_partition_write(&session, &global),
            Err(PartitionWriteError::TooManyInFlight)
        ));
        let released = 3;
        guards.truncate((MAX_IN_FLIGHT_WRITES_PER_SESSION - released) as usize);
        assert_eq!(session.get(), MAX_IN_FLIGHT_WRITES_PER_SESSION - released);
        for _ in 0..released {
            guards.push(admit_partition_write(&session, &global).expect("released slots"));
        }
        assert!(matches!(
            admit_partition_write(&session, &global),
            Err(PartitionWriteError::TooManyInFlight)
        ));
        drop(guards);
        assert_eq!(session.get(), 0);
        assert_eq!(global.get(), 0);
    }

    #[test]
    fn global_budget_spans_sessions_and_reopens_after_release() {
        let global = Cell::new(0);
        let session_count =
            MAX_IN_FLIGHT_WRITES_GLOBAL.div_ceil(MAX_IN_FLIGHT_WRITES_PER_SESSION) as usize;
        let sessions: Vec<Cell<u32>> = (0..session_count).map(|_| Cell::new(0)).collect();
        let mut guards = Vec::new();
        'fill: for session in &sessions {
            for _ in 0..MAX_IN_FLIGHT_WRITES_PER_SESSION {
                match admit_partition_write(session, &global) {
                    Ok(guard) => guards.push(guard),
                    Err(PartitionWriteError::ServerBusy) => break 'fill,
                    Err(other) => {
                        panic!("only the global budget may refuse this fill, got {other:?}")
                    }
                }
            }
        }
        assert_eq!(global.get(), MAX_IN_FLIGHT_WRITES_GLOBAL);
        // A fresh session is refused on the shared budget, not its own cap.
        let fresh = Cell::new(0);
        assert!(matches!(
            admit_partition_write(&fresh, &global),
            Err(PartitionWriteError::ServerBusy)
        ));
        drop(guards.pop());
        let readmitted = admit_partition_write(&fresh, &global).expect("budget slot released");
        assert_eq!(fresh.get(), 1);
        assert_eq!(global.get(), MAX_IN_FLIGHT_WRITES_GLOBAL);
        drop(readmitted);
    }
}
