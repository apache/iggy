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

//! Stopping the process on an environmental failure that has no in-process answer.

/// Why the process is stopping. The discriminant is the exit status, one per
/// condition; `1` stays the binary's generic startup failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FatalReason {
    /// A prepare's WAL append failed and the op it had claimed could not be handed
    /// back. The durable log is intact up to the previous op, so recovery re-derives
    /// the frontier and restarting is the repair.
    UnreconcilableLogFrontier = 2,
    /// The superblock stayed unwritable past the configured fail-stop window.
    /// The replica was already fenced quorum-invisible, so exiting hands the
    /// wedge to a supervisor instead of a log reader.
    SuperblockWedged = 3,
}

impl FatalReason {
    #[must_use]
    pub const fn exit_status(self) -> u8 {
        self as u8
    }
}

/// Log `message` and terminate the process.
///
/// For an environmental failure where stopping IS the answer, rather than an error
/// threaded up a stack whose top knows less than this leaf does. Not for bugs in
/// this process, which are `assert!` / `panic!` and say so.
///
/// `exit`, not `panic!`: a panic unwinds one shard of a thread-per-core runtime and
/// leaves its siblings serving, which is the half-alive state this exists to avoid.
/// Skipping destructors is wanted here, since the reason for stopping is that
/// further writes cannot be trusted.
pub fn fatal(reason: FatalReason, message: &str) -> ! {
    tracing::error!(
        target: "iggy.consensus.diag",
        reason = ?reason,
        exit_status = reason.exit_status(),
        "{message}"
    );
    std::process::exit(i32::from(reason.exit_status()));
}
