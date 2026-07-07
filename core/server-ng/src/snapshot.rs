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

//! Diagnostic snapshot collection (`GET_SNAPSHOT_FILE` / `POST /snapshot`).

mod procdump;

use std::io;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Instant;

use async_zip::base::write::ZipFileWriter;
use async_zip::{Compression, ZipEntryBuilder};
use configs::system::SystemConfig;
use futures::channel::oneshot;
use iggy_common::{IggyDuration, IggyError, SnapshotCompression, SystemSnapshotType};
use tracing::{error, info, warn};

/// Single-flight admission for snapshot collection. Each collection detaches
/// an uncancellable OS thread plus `ps`/`top` child processes; neither
/// transport's request-admission path bounds this, so `collect` refuses a
/// second concurrent collection rather than let privileged requests pile up
/// unbounded threads.
static SNAPSHOT_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

/// Collect a diagnostic snapshot as an in-memory ZIP archive.
///
/// Collection shells out to system tools (`top -H -b -n 1` alone runs for
/// seconds) and walks `/proc`, so it runs on a dedicated OS thread: every
/// shard drives its compio event loop (shard 0 additionally drives
/// consensus) and must never block, and the runtime's blocking fallback
/// pool is disabled (`thread_pool_limit(0)`), which rules out
/// `spawn_blocking` and `compio::process`. The caller only awaits the
/// result handoff, so its task parks without stalling the shard.
///
/// Single-flight: at most one collection runs at a time (see
/// [`SNAPSHOT_IN_PROGRESS`]); a concurrent request busy-rejects with
/// [`IggyError::SnapshotFileCompletionFailed`].
pub async fn collect(
    system_config: Arc<SystemConfig>,
    compression: SnapshotCompression,
    snapshot_types: Vec<SystemSnapshotType>,
) -> Result<Vec<u8>, IggyError> {
    let snapshot_types = if snapshot_types.contains(&SystemSnapshotType::All) {
        if snapshot_types.len() > 1 {
            error!("when using the `all` snapshot type, no other types can be specified");
            return Err(IggyError::InvalidCommand);
        }
        SystemSnapshotType::all_snapshot_types()
    } else {
        snapshot_types
    };

    // Deliberate busy-reject: a collection is already running. Reuses the
    // snapshot-domain error (no dedicated busy code) so both transports surface
    // the same "no archive produced" outcome.
    let Some(in_progress) = SnapshotInProgressGuard::acquire() else {
        return Err(IggyError::SnapshotFileCompletionFailed);
    };

    let (result_sender, result_receiver) = oneshot::channel();
    thread::Builder::new()
        .name("iggy-snapshot".to_string())
        .spawn(move || {
            // Held for the whole collection: the guard releases the single-flight
            // flag when this thread exits (any exit, including panic). Moving it
            // in (rather than holding it in the awaiting future) keeps the flag
            // set even if the requester disconnects and drops the receiver, so a
            // detached-but-still-running collector still blocks a second one.
            let _in_progress = in_progress;
            // A dropped receiver means the requester disconnected while
            // collecting; there is nobody left to deliver to.
            let _ = result_sender.send(collect_blocking(
                &system_config,
                compression,
                &snapshot_types,
            ));
        })
        // On spawn failure the closure is dropped, dropping `in_progress` and
        // releasing the flag, so a failed spawn cannot wedge it set.
        .map_err(|error| {
            error!(%error, "failed to spawn snapshot collector thread");
            IggyError::SnapshotFileCompletionFailed
        })?;
    result_receiver
        .await
        .map_err(|_| IggyError::SnapshotFileCompletionFailed)?
}

/// RAII guard for the [`SNAPSHOT_IN_PROGRESS`] single-flight flag. Releasing on
/// drop (rather than a manual store) means a panicking or early-returning
/// collector thread cannot leave the flag stuck `true` and lock out every
/// future snapshot.
struct SnapshotInProgressGuard;

impl SnapshotInProgressGuard {
    /// Acquire the flag, or `None` if a collection is already in progress.
    fn acquire() -> Option<Self> {
        SNAPSHOT_IN_PROGRESS
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            .then_some(Self)
    }
}

impl Drop for SnapshotInProgressGuard {
    fn drop(&mut self) {
        SNAPSHOT_IN_PROGRESS.store(false, Ordering::Release);
    }
}

fn collect_blocking(
    system_config: &SystemConfig,
    compression: SnapshotCompression,
    snapshot_types: &[SystemSnapshotType],
) -> Result<Vec<u8>, IggyError> {
    let started = Instant::now();
    let mut entries = Vec::with_capacity(snapshot_types.len());
    for snapshot_type in snapshot_types {
        match capture(snapshot_type, system_config) {
            Ok(content) => entries.push((format!("{snapshot_type}.txt"), content)),
            // Parity with the legacy collector: a failed section is logged and
            // skipped so the rest of the archive still ships.
            Err(error) => {
                error!(%snapshot_type, %error, "failed to capture snapshot section");
            }
        }
    }
    let archive = build_zip(&entries, compression)?;
    info!(
        types = ?snapshot_types,
        size = archive.len(),
        elapsed = %IggyDuration::new(started.elapsed()),
        "snapshot collected"
    );
    Ok(archive)
}

fn capture(
    snapshot_type: &SystemSnapshotType,
    system_config: &SystemConfig,
) -> io::Result<Vec<u8>> {
    match snapshot_type {
        SystemSnapshotType::FilesystemOverview => {
            command_stdout(Command::new("ls").args(["-la", "/tmp", "/proc"]))
        }
        SystemSnapshotType::ProcessList => process_list(),
        SystemSnapshotType::ResourceUsage => {
            command_stdout(Command::new("top").args(["-H", "-b", "-n", "1"]))
        }
        SystemSnapshotType::Test => command_stdout(Command::new("echo").arg("test")),
        SystemSnapshotType::ServerLogs => server_logs(system_config),
        SystemSnapshotType::ServerConfig => server_config(system_config),
        // `collect` expands `All` before handing off to the collector.
        SystemSnapshotType::All => Err(io::Error::other("`all` must be expanded by the caller")),
    }
}

fn command_stdout(command: &mut Command) -> io::Result<Vec<u8>> {
    let output = command.output()?;
    if !output.stderr.is_empty() {
        warn!(
            program = ?command.get_program(),
            stderr = %String::from_utf8_lossy(&output.stderr),
            "snapshot command reported errors"
        );
    }
    Ok(output.stdout)
}

fn process_list() -> io::Result<Vec<u8>> {
    let ps_output = Command::new("ps").arg("aux").output()?;
    let mut content = b"=== Process List (ps aux) ===\n".to_vec();
    content.extend_from_slice(&ps_output.stdout);
    content.extend_from_slice(b"\n\n=== Detailed Process Information ===\n");
    content.extend_from_slice(procdump::proc_info()?.as_bytes());
    Ok(content)
}

fn server_logs(system_config: &SystemConfig) -> io::Result<Vec<u8>> {
    // Mirror the logger's path derivation (server_common `Logging::late_init`):
    // it canonicalizes the configured subdirectory before joining the system
    // path, so a relative `logging.path` that already exists resolves against the
    // CWD. Skipping the canonicalize here would read a different (often empty)
    // directory than the one the logger actually writes to.
    let logs_subdirectory = PathBuf::from(&system_config.logging.path);
    let logs_subdirectory = logs_subdirectory
        .canonicalize()
        .unwrap_or(logs_subdirectory);
    let logs_path = PathBuf::from(system_config.get_system_path()).join(logs_subdirectory);
    let mut log_files = Vec::new();
    for entry in std::fs::read_dir(&logs_path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            log_files.push((metadata.modified()?, entry.path()));
        }
    }
    // Oldest first, so the concatenation reads chronologically (the legacy
    // collector's `ls -tr | xargs cat`).
    log_files.sort_by_key(|(modified, _)| *modified);
    let mut content = Vec::new();
    for (_, path) in log_files {
        content.extend_from_slice(&std::fs::read(&path)?);
    }
    Ok(content)
}

fn server_config(system_config: &SystemConfig) -> io::Result<Vec<u8>> {
    let config_path = PathBuf::from(system_config.get_runtime_path()).join("current_config.toml");
    std::fs::read(config_path)
}

fn build_zip(
    entries: &[(String, Vec<u8>)],
    compression: SnapshotCompression,
) -> Result<Vec<u8>, IggyError> {
    let compression = zip_compression(compression);
    // The writer targets an in-memory Vec, so every poll completes
    // immediately; `block_on` never parks the collector thread on I/O.
    futures::executor::block_on(async {
        let mut zip_writer = ZipFileWriter::new(Vec::new());
        for (filename, content) in entries {
            let entry = ZipEntryBuilder::new(filename.clone().into(), compression);
            zip_writer
                .write_entry_whole(entry, content)
                .await
                .map_err(|error| {
                    error!(filename, %error, "failed to write snapshot zip entry");
                    IggyError::SnapshotFileCompletionFailed
                })?;
        }
        zip_writer.close().await.map_err(|error| {
            error!(%error, "failed to finalize snapshot zip");
            IggyError::SnapshotFileCompletionFailed
        })
    })
}

const fn zip_compression(compression: SnapshotCompression) -> Compression {
    match compression {
        SnapshotCompression::Stored => Compression::Stored,
        SnapshotCompression::Deflated => Compression::Deflate,
        SnapshotCompression::Bzip2 => Compression::Bz,
        SnapshotCompression::Zstd => Compression::Zstd,
        SnapshotCompression::Lzma => Compression::Lzma,
        SnapshotCompression::Xz => Compression::Xz,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_collection_in_progress_when_collecting_then_it_busy_rejects() {
        // Hold the single-flight flag to stand in for a running collection, then
        // assert a concurrent `collect` refuses (returning without spawning a
        // second collector thread) rather than piling up threads.
        let held = SnapshotInProgressGuard::acquire().expect("flag starts free");
        let result = futures::executor::block_on(collect(
            Arc::new(SystemConfig::default()),
            SnapshotCompression::Stored,
            vec![SystemSnapshotType::Test],
        ));
        assert!(matches!(
            result,
            Err(IggyError::SnapshotFileCompletionFailed)
        ));
        // Dropping the guard releases the flag for the next collection.
        drop(held);
        assert!(
            SnapshotInProgressGuard::acquire().is_some(),
            "flag must be free after the guard drops"
        );
    }
}
