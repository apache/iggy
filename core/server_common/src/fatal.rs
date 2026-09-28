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

use nix::errno::Errno;
use nix::sys::resource::{Resource, getrlimit};
use std::io::{self, Write};

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
    /// A storage open for a write or a sync failed because the process
    /// (`EMFILE`) or the host (`ENFILE`) has no free file descriptor. The shard
    /// cannot persist that write, and serving on only fails each later write in
    /// turn.
    DescriptorsExhausted = 4,
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
///
/// The reason also goes straight to stderr. The tracing appenders are
/// non-blocking workers, and `exit` stops them before they flush, so without
/// this write a supervisor's journal never learns why the process stopped. The
/// write result is ignored because `eprintln!` would panic on a broken stderr.
pub fn fatal(reason: FatalReason, message: &str) -> ! {
    tracing::error!(
        target: "iggy.fatal.diag",
        reason = ?reason,
        exit_status = reason.exit_status(),
        "{message}"
    );
    let _ = writeln!(
        std::io::stderr().lock(),
        "iggy fatal: reason={reason:?} exit_status={}: {message}",
        reason.exit_status()
    );
    std::process::exit(i32::from(reason.exit_status()));
}

/// Storage results that stop the process when no file descriptor is free.
///
/// Only for opens that write, create, truncate or sync, and for read-only
/// opens that are one step of such a write: the existence probes of segment
/// setup, or an open that exists only to sync. A failure there leaves the
/// write half done. Other reads keep their own error path: a failed read fails
/// only its request, and the descriptors that fill the table can be client
/// sockets or other reads that close again soon.
/// Accept loops never stop here, see `message_bus::accept`.
pub trait ExitOnDescriptorExhaustion: Sized {
    /// Pass the result through, unless it failed with `EMFILE` or `ENFILE`: then
    /// stop the process with [`FatalReason::DescriptorsExhausted`]. `operation`
    /// names what was being opened, for the fatal message.
    #[must_use]
    fn exit_on_descriptor_exhaustion(self, operation: impl FnOnce() -> String) -> Self;
}

impl<T> ExitOnDescriptorExhaustion for io::Result<T> {
    fn exit_on_descriptor_exhaustion(self, operation: impl FnOnce() -> String) -> Self {
        if let Err(error) = &self
            && is_descriptor_exhaustion(error)
        {
            exit_descriptors_exhausted(error, &operation());
        }
        self
    }
}

/// `EMFILE` (this process is at its `RLIMIT_NOFILE`) or `ENFILE` (the host is
/// at its system-wide file table limit).
#[must_use]
pub fn is_descriptor_exhaustion(error: &io::Error) -> bool {
    error
        .raw_os_error()
        .is_some_and(|code| code == Errno::EMFILE as i32 || code == Errno::ENFILE as i32)
}

fn exit_descriptors_exhausted(error: &io::Error, operation: &str) -> ! {
    let limits = getrlimit(Resource::RLIMIT_NOFILE).map_or_else(
        |errno| format!("RLIMIT_NOFILE unreadable: {errno}"),
        |(soft, hard)| format!("RLIMIT_NOFILE soft={soft} hard={hard}"),
    );
    fatal(
        FatalReason::DescriptorsExhausted,
        &format!("no free file descriptor while {operation}: {error} ({limits})"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_emfile_or_enfile_when_classifying_should_report_exhaustion() {
        for errno in [Errno::EMFILE, Errno::ENFILE] {
            assert!(is_descriptor_exhaustion(&io::Error::from_raw_os_error(
                errno as i32
            )));
        }
    }

    #[test]
    fn given_other_error_when_classifying_should_not_report_exhaustion() {
        assert!(!is_descriptor_exhaustion(&io::Error::from_raw_os_error(
            Errno::ENOENT as i32
        )));
        assert!(!is_descriptor_exhaustion(&io::Error::other(
            "Too many open files"
        )));
    }

    #[test]
    fn given_non_exhaustion_error_when_checking_result_should_pass_it_through() {
        let result: io::Result<()> = Err(io::Error::from_raw_os_error(Errno::ENOENT as i32));

        let checked = result.exit_on_descriptor_exhaustion(|| "opening a test file".to_owned());

        assert_eq!(
            checked.map_err(|error| error.kind()),
            Err(io::ErrorKind::NotFound)
        );
    }
}
