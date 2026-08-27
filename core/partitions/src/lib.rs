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

#![allow(clippy::future_not_send)]

mod iggy_index;
mod iggy_index_reader;
mod iggy_index_writer;
mod iggy_partition;
mod iggy_partitions;
mod journal;
mod log;
mod messages_writer;
pub mod offset_storage;
mod poll_plan;
mod segment;
pub mod state_transfer;
mod types;

use iggy_binary_protocol::PrepareHeader;
use iggy_common::IggyError;
pub use iggy_index::IggyIndex;
pub use iggy_index_reader::IggyIndexReader;
pub use iggy_index_writer::IggyIndexWriter;
pub use iggy_partition::{IggyPartition, PurgeError, SegmentRemoval};
pub use iggy_partitions::IggyPartitions;
pub use journal::{EVICTED_RING_BYTES_MAX, EVICTED_RING_CAPACITY};
pub use messages_writer::MessagesWriter;
pub use offset_storage::delete_persisted_offset;
pub use poll_plan::{AutoCommitApplied, PollPlan};
pub use segment::Segment;
use server_common::Message;
pub use server_common::send_messages::{IggyMessage, IggyMessageHeader, IggyMessages};
pub use types::{
    AppendResult, FatalCommit, Fragment, PartitionOffsets, PartitionPathLayout, PartitionsConfig,
    PollFragments, PollQueryResult, PollingArgs, PollingConsumer, REPAIR_RETRY_TICKS,
    RepairConclusion, RepairSession, SendMessagesResult,
};

#[cfg(target_os = "linux")]
use nix::libc::EOPNOTSUPP;
#[cfg(target_os = "linux")]
use server_common::segment_io::SegmentIoMode;
#[cfg(target_os = "linux")]
use std::io;
#[cfg(target_os = "linux")]
use std::sync::Once;
#[cfg(target_os = "linux")]
use tracing::error;

/// A partition's message log, named so a caller can carry one across a rebuild.
///
/// Exists for the simulator, which has no segment files and so must hold the log
/// itself for a restarted replica to come back with its data (see
/// [`IggyPartition::adopt_retained_log`]). Names the only journal
/// `IggyPartition::log` is instantiated with rather than widening anything.
#[cfg(any(test, feature = "simulator"))]
pub type RetainedPartitionLog =
    log::SegmentedLog<journal::PartitionJournal<journal::PartitionJournalMemStorage>>;

/// Everything a partition hands its own next incarnation across a simulated
/// restart.
#[cfg(any(test, feature = "simulator"))]
pub struct RetainedPartitionState {
    pub log: RetainedPartitionLog,
    /// Offset counter the previous incarnation had proved durable.
    pub durable_offset: u64,
    /// Highest offset it had written, durable or not.
    pub write_offset: u64,
    /// Whether that incarnation ever stamped an offset, i.e. whether the two
    /// numbers above describe an offset space at all.
    pub offset_space_used: bool,
}

/// Partition-level data plane operations.
///
/// `send_messages` MUST only append to the partition journal (prepare phase),
/// without committing/persisting to disk.
pub trait Partition {
    fn append_messages(
        &mut self,
        message: Message<PrepareHeader>,
    ) -> impl Future<Output = Result<AppendResult, IggyError>>;

    /// # Errors
    /// Returns `IggyError::FeatureUnavailable` by default.
    fn store_consumer_offset(
        &self,
        consumer: PollingConsumer,
        offset: u64,
    ) -> Result<(), IggyError> {
        let _ = (consumer, offset);
        Err(IggyError::FeatureUnavailable)
    }

    fn get_consumer_offset(&self, consumer: PollingConsumer) -> Option<u64> {
        let _ = consumer;
        None
    }

    fn offsets(&self) -> PartitionOffsets {
        PartitionOffsets::default()
    }
}

#[cfg(target_os = "linux")]
const WRITE_IO_CONFIG_KEY: &str = "system.segment.write_io";

/// Names the `write_io` knob when a segment write is rejected because the
/// filesystem underneath it cannot serve `RWF_DONTCACHE`.
///
/// The boot probe only reaches directories that exist at boot, so a per-stream
/// submount added later fails here instead, with an errno that says nothing
/// about the config that asked for the flag. Deliberately not fatal: one
/// stream's mount must not take the node down. Reported once per process,
/// because the remedy is a config edit plus a restart and a per-commit line
/// would only bury it.
#[cfg(target_os = "linux")]
pub(crate) fn report_uncached_write_unsupported(
    write_io: SegmentIoMode,
    error: &io::Error,
    file_path: &str,
) {
    static REPORTED: Once = Once::new();

    if !is_uncached_write_unsupported(write_io, error) {
        return;
    }

    REPORTED.call_once(|| {
        error!(
            target: "iggy.partitions.storage",
            file = file_path,
            config_key = WRITE_IO_CONFIG_KEY,
            "segment write rejected RWF_DONTCACHE, so this filesystem cannot serve \
             {WRITE_IO_CONFIG_KEY} = \"uncached\": set {WRITE_IO_CONFIG_KEY} = \"buffered\", \
             or keep segments on ext4/XFS under Linux >= 6.14"
        );
    });
}

#[cfg(target_os = "linux")]
fn is_uncached_write_unsupported(write_io: SegmentIoMode, error: &io::Error) -> bool {
    write_io == SegmentIoMode::Uncached && error.raw_os_error() == Some(EOPNOTSUPP)
}

/// Shared fixtures for the tests that exercise the uncached (`RWF_DONTCACHE`)
/// segment write path.
#[cfg(all(test, target_os = "linux"))]
mod uncached_test_support {
    use nix::sys::statfs;
    use server_common::uncached_io::{
        UncachedIoError, probe_uncached_write, require_uncached_io_tests,
    };
    use std::path::Path;
    use tempfile::TempDir;

    const SHM_DIR: &str = "/dev/shm";
    const REQUIRE_ENV: &str = "IGGY_REQUIRE_UNCACHED_IO";

    /// A directory whose filesystem takes `RWF_DONTCACHE`, or `None` with the
    /// reason reported. `tempdir()` lands on tmpfs on many boxes and tmpfs
    /// refuses the flag, so the fixture sits next to the test binary instead.
    pub async fn uncached_scratch_dir() -> Option<TempDir> {
        let exe = std::env::current_exe().expect("current_exe");
        let base = exe.parent().expect("test binary has a parent directory");
        let directory = tempfile::tempdir_in(base).expect("scratch directory next to the binary");
        match probe_uncached_write(directory.path()).await {
            Ok(()) => Some(directory),
            Err(error @ UncachedIoError::Unsupported { .. }) => {
                skip_or_fail(&error.to_string());
                None
            }
            Err(error) => panic!("uncached probe hit an I/O error: {error}"),
        }
    }

    /// A tmpfs directory: the negative control for the uncached path, since
    /// tmpfs is the filesystem guaranteed to refuse `RWF_DONTCACHE` while
    /// taking the very same buffered write.
    pub fn tmpfs_scratch_dir() -> Option<TempDir> {
        let shm = Path::new(SHM_DIR);
        let is_tmpfs =
            statfs::statfs(shm).is_ok_and(|stat| stat.filesystem_type() == statfs::TMPFS_MAGIC);
        if !is_tmpfs {
            skip_or_fail(&format!("{SHM_DIR} is not tmpfs"));
            return None;
        }
        match tempfile::tempdir_in(shm) {
            Ok(directory) => Some(directory),
            Err(error) => {
                skip_or_fail(&format!(
                    "cannot create a directory under {SHM_DIR}: {error}"
                ));
                None
            }
        }
    }

    /// Loud enough to spot in a scrolling CI log, and a hard failure wherever
    /// the uncached path is required to have run. A silent skip on a build
    /// tree that sits on overlayfs is how this whole suite goes green without
    /// ever submitting an uncached write.
    pub fn skip_or_fail(reason: &str) {
        assert!(
            !require_uncached_io_tests(),
            "### {REQUIRE_ENV}=1 forbids skipping: {reason} ###"
        );
        eprintln!("######## SKIPPING UNCACHED TEST: {reason} ########");
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::is_uncached_write_unsupported;
    use nix::libc::{EIO, EOPNOTSUPP};
    use server_common::segment_io::SegmentIoMode;
    use std::io;

    #[test]
    fn given_uncached_mode_when_write_is_rejected_as_unsupported_should_blame_the_knob() {
        assert!(is_uncached_write_unsupported(
            SegmentIoMode::Uncached,
            &io::Error::from_raw_os_error(EOPNOTSUPP)
        ));
    }

    #[test]
    fn given_buffered_mode_when_write_is_rejected_as_unsupported_should_stay_silent() {
        assert!(!is_uncached_write_unsupported(
            SegmentIoMode::Buffered,
            &io::Error::from_raw_os_error(EOPNOTSUPP)
        ));
    }

    #[test]
    fn given_uncached_mode_when_write_fails_for_another_reason_should_stay_silent() {
        assert!(!is_uncached_write_unsupported(
            SegmentIoMode::Uncached,
            &io::Error::from_raw_os_error(EIO)
        ));
    }
}
