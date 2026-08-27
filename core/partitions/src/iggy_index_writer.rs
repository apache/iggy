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

use compio::fs::{File, OpenOptions};
use compio::io::AsyncWriteAtExt;
use iggy_common::IggyError;
use server_common::segment_io::SegmentIoMode;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{error, trace};

#[cfg(target_os = "linux")]
use crate::report_uncached_write_unsupported;
#[cfg(target_os = "linux")]
use compio::driver::ToSharedFd;
#[cfg(target_os = "linux")]
use server_common::uncached_io::write_all_at_uncached;

#[derive(Debug)]
pub struct IggyIndexWriter {
    file_path: String,
    file: File,
    index_size_bytes: Rc<AtomicU64>,
    fsync: bool,
    #[cfg(target_os = "linux")]
    write_io: SegmentIoMode,
}

impl IggyIndexWriter {
    /// Creates an index writer backed by the sparse index file at `file_path`.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened, synchronized, or queried for
    /// metadata, or if the on-disk length does not match the seeded size counter.
    pub async fn new(
        file_path: &str,
        index_size_bytes: Rc<AtomicU64>,
        fsync: bool,
        write_io: SegmentIoMode,
        file_exists: bool,
    ) -> Result<Self, IggyError> {
        // Config validation rejects uncached writes off Linux (no io_uring
        // `rw_flags` there), so the mode is only ever `Buffered` here.
        #[cfg(not(target_os = "linux"))]
        debug_assert_eq!(write_io, SegmentIoMode::Buffered);

        let mut opts = OpenOptions::new();
        opts.write(true);
        if !file_exists {
            opts.create(true);
        }
        let file = opts
            .open(file_path)
            .await
            .map_err(|_| IggyError::CannotReadFile)?;

        if file_exists {
            file.sync_all()
                .await
                .map_err(|_| IggyError::CannotWriteToFile)?;

            let actual_index_size = file
                .metadata()
                .await
                .map_err(|_| IggyError::CannotReadFileMetadata)?
                .len();

            // Refusal rationale documented on `IggyError::SegmentSizeMismatchAtOpen`.
            let expected_index_size = index_size_bytes.load(Ordering::Relaxed);
            if actual_index_size != expected_index_size {
                error!(
                    target: "iggy.partitions.storage",
                    file = file_path,
                    on_disk_size = actual_index_size,
                    expected_size = expected_index_size,
                    "sparse index file size does not match the seeded size at open"
                );
                return Err(IggyError::SegmentSizeMismatchAtOpen(
                    actual_index_size,
                    expected_index_size,
                ));
            }
        }

        let size = index_size_bytes.load(Ordering::Relaxed);
        trace!(
            target: "iggy.partitions.storage",
            file = file_path,
            size,
            "opened sparse index file for writing"
        );

        Ok(Self {
            file_path: file_path.to_owned(),
            file,
            index_size_bytes,
            fsync,
            #[cfg(target_os = "linux")]
            write_io,
        })
    }

    /// Appends encoded sparse index bytes at the current write cursor and
    /// returns how many bytes landed. The cursor is left where it was: the
    /// caller advances it with `advance` once the companion segment save has
    /// also succeeded.
    ///
    /// # Errors
    ///
    /// Returns an error if the index bytes cannot be written or synced to disk.
    pub(crate) async fn save_indexes(&self, indexes: Vec<u8>) -> Result<u64, IggyError> {
        if indexes.is_empty() {
            return Ok(0);
        }

        let len = indexes.len();
        let position = self.index_size_bytes.load(Ordering::Relaxed);
        self.write_all(indexes, position).await.map_err(|error| {
            #[cfg(target_os = "linux")]
            report_uncached_write_unsupported(self.write_io, &error, self.file_path.as_str());
            #[cfg(not(target_os = "linux"))]
            let _ = error;
            IggyError::CannotSaveIndexToSegment
        })?;

        if self.fsync {
            self.fsync().await?;
        }

        trace!(
            target: "iggy.partitions.storage",
            file = self.file_path.as_str(),
            bytes = len,
            position,
            "saved sparse index bytes to file"
        );
        Ok(len as u64)
    }

    /// Move the write cursor forward over `bytes` that are now durable. Split
    /// out of the save so the index and segment cursors advance together, only
    /// once both halves have succeeded.
    pub(crate) fn advance(&self, bytes: u64) {
        self.index_size_bytes.fetch_add(bytes, Ordering::Release);
    }

    /// Flushes buffered index file contents to disk.
    ///
    /// Uses `fdatasync` (data only): index files are append-only and the
    /// size change is tracked in datasync metadata on Linux, so the inode
    /// metadata fsync of `sync_all` adds latency without correctness gain.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be synchronized.
    pub async fn fsync(&self) -> Result<(), IggyError> {
        self.file
            .sync_data()
            .await
            .map_err(|_| IggyError::CannotWriteToFile)?;
        Ok(())
    }

    /// Uncached writes take our own `io_uring` op: compio's carries no
    /// `rw_flags`, and `RWF_DONTCACHE` is a per-write flag, not an open flag.
    async fn write_all(&self, indexes: Vec<u8>, position: u64) -> std::io::Result<()> {
        #[cfg(target_os = "linux")]
        if self.write_io == SegmentIoMode::Uncached {
            return write_all_at_uncached(&self.file.to_shared_fd(), indexes, position)
                .await
                .0;
        }
        (&self.file).write_all_at(indexes, position).await.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "linux")]
    use crate::uncached_test_support::{tmpfs_scratch_dir, uncached_scratch_dir};

    const INDEX_ENTRY_LEN: usize = 24;

    #[compio::test]
    async fn given_seeded_size_diverging_from_disk_when_opening_existing_file_should_return_size_mismatch_error()
     {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("segment.index");
        std::fs::write(&path, [7u8; 96]).unwrap();

        let result = IggyIndexWriter::new(
            path.to_str().unwrap(),
            Rc::new(AtomicU64::new(32)),
            false,
            SegmentIoMode::Buffered,
            true,
        )
        .await;

        assert!(matches!(
            result,
            Err(IggyError::SegmentSizeMismatchAtOpen(96, 32))
        ));
    }

    #[cfg(target_os = "linux")]
    async fn open_writer(
        directory: &tempfile::TempDir,
        name: &str,
        write_io: SegmentIoMode,
        size: &Rc<AtomicU64>,
    ) -> IggyIndexWriter {
        let path = directory.path().join(name);
        IggyIndexWriter::new(
            path.to_str().expect("utf-8 path"),
            Rc::clone(size),
            false,
            write_io,
            false,
        )
        .await
        .expect("open index writer")
    }

    #[cfg(target_os = "linux")]
    #[compio::test]
    async fn given_uncached_mode_when_saving_indexes_should_append_entries_and_advance_cursor() {
        let Some(directory) = uncached_scratch_dir().await else {
            return;
        };
        let size = Rc::new(AtomicU64::new(0));
        let writer = open_writer(&directory, "segment.index", SegmentIoMode::Uncached, &size).await;

        // Three entries appended one flush at a time, the way the partition
        // writes them: each append lands in a partially filled block.
        let mut expected = Vec::new();
        for entry in 1u8..=3 {
            let bytes = vec![entry; INDEX_ENTRY_LEN];
            let saved = writer.save_indexes(bytes.clone()).await.unwrap();
            writer.advance(saved);
            expected.extend_from_slice(&bytes);
        }

        assert_eq!(size.load(Ordering::Relaxed), expected.len() as u64);
        assert_eq!(
            std::fs::read(directory.path().join("segment.index")).unwrap(),
            expected
        );
    }

    /// The negative control: tmpfs refuses `RWF_DONTCACHE` while taking the
    /// identical buffered write, so only a submission that really carries the
    /// flag can fail here. Without this, deleting the uncached branch of
    /// `write_all` leaves every other test in this file green.
    #[cfg(target_os = "linux")]
    #[compio::test]
    async fn given_tmpfs_when_saving_indexes_should_fail_uncached_but_succeed_buffered() {
        let Some(directory) = tmpfs_scratch_dir() else {
            return;
        };
        let entry = vec![9u8; INDEX_ENTRY_LEN];

        let uncached_size = Rc::new(AtomicU64::new(0));
        let uncached = open_writer(
            &directory,
            "uncached.index",
            SegmentIoMode::Uncached,
            &uncached_size,
        )
        .await;
        let error = uncached.save_indexes(entry.clone()).await.expect_err(
            "tmpfs must reject RWF_DONTCACHE; a write that succeeds here never carried the flag",
        );
        assert!(
            matches!(error, IggyError::CannotSaveIndexToSegment),
            "{error}"
        );

        let buffered_size = Rc::new(AtomicU64::new(0));
        let buffered = open_writer(
            &directory,
            "buffered.index",
            SegmentIoMode::Buffered,
            &buffered_size,
        )
        .await;
        let saved = buffered
            .save_indexes(entry.clone())
            .await
            .expect("the same directory takes buffered writes");
        assert_eq!(saved, entry.len() as u64);
    }
}
