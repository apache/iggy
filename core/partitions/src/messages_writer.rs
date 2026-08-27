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

use compio::{
    fs::{File, OpenOptions},
    io::AsyncWriteAtExt,
};
use iggy_common::{IggyByteSize, IggyError};
use server_common::iobuf::{Frozen, IOV_MAX};
use server_common::segment_io::SegmentIoMode;
use std::{
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
};
use tracing::{error, warn};

#[cfg(target_os = "linux")]
use crate::report_uncached_write_unsupported;
#[cfg(target_os = "linux")]
use compio::driver::ToSharedFd;
#[cfg(target_os = "linux")]
use nix::fcntl::{FallocateFlags, fallocate};
#[cfg(target_os = "linux")]
use server_common::uncached_io::write_vectored_all_at_uncached;

#[derive(Debug)]
pub struct MessagesWriter {
    file_path: String,
    file: File,
    messages_size_bytes: Rc<AtomicU64>,
    fsync: bool,
    #[cfg(target_os = "linux")]
    write_io: SegmentIoMode,
}

impl MessagesWriter {
    /// Creates a messages writer backed by the segment file at `file_path`.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened, synchronized, or queried for
    /// metadata, or if the on-disk length does not match the seeded size counter.
    pub async fn new(
        file_path: &str,
        messages_size_bytes: Rc<AtomicU64>,
        fsync: bool,
        write_io: SegmentIoMode,
        file_exists: bool,
        preallocate_size: Option<IggyByteSize>,
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

        if let Some(preallocate_size) = preallocate_size {
            preallocate_file(&file, file_path, preallocate_size.as_bytes_u64());
        }

        if file_exists {
            file.sync_all()
                .await
                .map_err(|_| IggyError::CannotWriteToFile)?;

            let actual_messages_size = file
                .metadata()
                .await
                .map_err(|_| IggyError::CannotReadFileMetadata)?
                .len();

            // Refusal rationale documented on `IggyError::SegmentSizeMismatchAtOpen`.
            let expected_messages_size = messages_size_bytes.load(Ordering::Relaxed);
            if actual_messages_size != expected_messages_size {
                error!(
                    target: "iggy.partitions.storage",
                    file = file_path,
                    on_disk_size = actual_messages_size,
                    expected_size = expected_messages_size,
                    "segment messages file size does not match the seeded size at open"
                );
                return Err(IggyError::SegmentSizeMismatchAtOpen(
                    actual_messages_size,
                    expected_messages_size,
                ));
            }
        }

        Ok(Self {
            file_path: file_path.to_string(),
            file,
            messages_size_bytes,
            fsync,
            #[cfg(target_os = "linux")]
            write_io,
        })
    }

    /// Appends a batch of frozen message buffers at the current write cursor
    /// and returns how many bytes landed. The cursor is left where it was: the
    /// caller advances it with `advance` once the companion index save has
    /// also succeeded.
    ///
    /// # Errors
    ///
    /// Returns an error if any chunk cannot be written or synced to disk.
    pub(crate) async fn save_frozen_batches<const ALIGN: usize>(
        &self,
        buffers: &[Frozen<ALIGN>],
    ) -> Result<IggyByteSize, IggyError> {
        let messages_size: u64 = buffers.iter().map(|buffer| buffer.len() as u64).sum();

        if messages_size == 0 {
            return Ok(IggyByteSize::from(0));
        }

        let position = self.messages_size_bytes.load(Ordering::Relaxed);
        self.write_frozen_chunked(position, buffers).await?;

        if self.fsync {
            self.fsync().await?;
        }

        Ok(IggyByteSize::from(messages_size))
    }

    /// Move the write cursor forward over `bytes` that are now durable. Split
    /// out of the save so the segment and index cursors advance together, only
    /// once both halves have succeeded.
    pub(crate) fn advance(&self, bytes: u64) {
        self.messages_size_bytes.fetch_add(bytes, Ordering::Release);
    }

    #[must_use]
    pub fn path(&self) -> String {
        self.file_path.clone()
    }

    /// Flushes buffered segment file contents to disk.
    ///
    /// Uses `fdatasync` (data only): segment files are append-only and the
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

    async fn write_frozen_chunked<const ALIGN: usize>(
        &self,
        mut position: u64,
        buffers: &[Frozen<ALIGN>],
    ) -> Result<(), IggyError> {
        for chunk in buffers.chunks(IOV_MAX) {
            let chunk_size: usize = chunk.iter().map(Frozen::len).sum();
            let chunk_vec: Vec<_> = chunk.to_vec();

            self.write_vectored_all(chunk_vec, position)
                .await
                .map_err(|err| {
                    #[cfg(target_os = "linux")]
                    report_uncached_write_unsupported(self.write_io, &err, self.file_path.as_str());
                    error!(
                        target: "iggy.partitions.storage",
                        file = self.file_path.as_str(),
                        write_position = position,
                        %err,
                        "failed to write frozen messages to segment file"
                    );
                    IggyError::CannotWriteToFile
                })?;

            position += chunk_size as u64;
        }

        Ok(())
    }

    /// Uncached writes take our own `io_uring` op: compio's carries no
    /// `rw_flags`, and `RWF_DONTCACHE` is a per-write flag, not an open flag.
    async fn write_vectored_all<const ALIGN: usize>(
        &self,
        chunk: Vec<Frozen<ALIGN>>,
        position: u64,
    ) -> std::io::Result<()> {
        #[cfg(target_os = "linux")]
        if self.write_io == SegmentIoMode::Uncached {
            return write_vectored_all_at_uncached(&self.file.to_shared_fd(), chunk, position)
                .await
                .0;
        }
        (&self.file).write_vectored_all_at(chunk, position).await.0
    }
}

#[cfg(target_os = "linux")]
fn preallocate_file(file: &File, file_path: &str, len: u64) {
    let Ok(len) = i64::try_from(len) else {
        warn!(
            target: "iggy.partitions.storage",
            file = file_path,
            preallocate_len = len,
            "file preallocation size is unsupported, using buffered allocation"
        );
        return;
    };

    // Runs INLINE on the shard thread, deliberately. `server_common::executor`
    // sets `thread_pool_limit(0)` on the shard proactor, so `spawn_blocking`
    // has no worker to park a task on and compio panics the shard outright with
    // "the thread pool is needed but no worker thread is running". (That limit
    // is skipped on macOS, whose polling driver routes fs through the pool, so
    // the panic is Linux-and-most-targets, not universal. This arm is
    // Linux-only regardless.)
    //
    // The cost is acceptable only because of what this call is: a metadata-only
    // extent reservation, microseconds on the local filesystems this option
    // exists for, and an immediate `EOPNOTSUPP` where the filesystem cannot do
    // it. Where it can genuinely block -- NFSv4.2 `ALLOCATE`, FUSE, a badly
    // fragmented extent tree forcing a journal commit -- it stalls the whole
    // core, not one partition, because nothing here yields. Preallocation is
    // opt-in per topic at creation for that reason; on such a deployment,
    // create topics without `preallocate_segments` rather than reintroducing a
    // pool the shard runtime does not have.
    if let Err(error) = fallocate(file, FallocateFlags::FALLOC_FL_KEEP_SIZE, 0, len) {
        warn!(
            target: "iggy.partitions.storage",
            file = file_path,
            preallocate_len = len,
            %error,
            "file preallocation failed, using buffered allocation"
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn preallocate_file(_file: &File, file_path: &str, _len: u64) {
    warn!(
        target: "iggy.partitions.storage",
        file = file_path,
        "file preallocation is unavailable on this platform, using buffered allocation"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "linux")]
    use crate::uncached_test_support::{tmpfs_scratch_dir, uncached_scratch_dir};
    #[cfg(target_os = "linux")]
    use server_common::iobuf::Owned;

    #[compio::test]
    async fn preallocated_file_keeps_logical_length() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("segment.log");
        let writer = MessagesWriter::new(
            path.to_str().unwrap(),
            Rc::new(AtomicU64::new(0)),
            false,
            SegmentIoMode::Buffered,
            false,
            Some(IggyByteSize::from(1024 * 1024_u64)),
        )
        .await
        .unwrap();

        assert_eq!(writer.file.metadata().await.unwrap().len(), 0);
    }

    #[compio::test]
    async fn given_seeded_size_diverging_from_disk_when_opening_existing_file_should_return_size_mismatch_error()
     {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("segment.log");
        std::fs::write(&path, [7u8; 128]).unwrap();

        let result = MessagesWriter::new(
            path.to_str().unwrap(),
            Rc::new(AtomicU64::new(129)),
            false,
            SegmentIoMode::Buffered,
            true,
            None,
        )
        .await;

        assert!(matches!(
            result,
            Err(IggyError::SegmentSizeMismatchAtOpen(128, 129))
        ));
    }

    /// Unaligned lengths with per-offset content, so a misplaced or repeated
    /// write cannot pass a byte-exact comparison.
    #[cfg(target_os = "linux")]
    fn frozen_batch(len: usize, seed: usize) -> Frozen<4096> {
        let bytes: Vec<u8> = (1u8..=251).cycle().skip(seed).take(len).collect();
        Frozen::from(Owned::<4096>::copy_from_slice(&bytes))
    }

    #[cfg(target_os = "linux")]
    async fn open_writer(
        directory: &tempfile::TempDir,
        name: &str,
        write_io: SegmentIoMode,
        size: &Rc<AtomicU64>,
    ) -> MessagesWriter {
        let path = directory.path().join(name);
        MessagesWriter::new(
            path.to_str().expect("utf-8 path"),
            Rc::clone(size),
            false,
            write_io,
            false,
            None,
        )
        .await
        .expect("open messages writer")
    }

    #[cfg(target_os = "linux")]
    #[compio::test]
    async fn given_uncached_mode_when_saving_batches_should_persist_bytes_and_advance_cursor() {
        let Some(directory) = uncached_scratch_dir().await else {
            return;
        };
        let size = Rc::new(AtomicU64::new(0));
        let writer = open_writer(&directory, "segment.log", SegmentIoMode::Uncached, &size).await;

        let batches: Vec<Frozen<4096>> = [300usize, 4113, 70_000]
            .iter()
            .enumerate()
            .map(|(seed, len)| frozen_batch(*len, seed))
            .collect();
        let expected: Vec<u8> = batches
            .iter()
            .flat_map(|batch| batch.as_slice().to_vec())
            .collect();

        let saved = writer.save_frozen_batches(&batches).await.unwrap();
        writer.advance(saved.as_bytes_u64());

        assert_eq!(saved.as_bytes_u64(), expected.len() as u64);
        assert_eq!(size.load(Ordering::Relaxed), expected.len() as u64);
        assert_eq!(
            std::fs::read(directory.path().join("segment.log")).unwrap(),
            expected
        );
    }

    /// The negative control: tmpfs refuses `RWF_DONTCACHE` while taking the
    /// identical buffered write, so only a submission that really carries the
    /// flag can fail here. Without this, deleting the uncached branch of
    /// `write_vectored_all` leaves every other test in this file green.
    #[cfg(target_os = "linux")]
    #[compio::test]
    async fn given_tmpfs_when_saving_batches_should_fail_uncached_but_succeed_buffered() {
        let Some(directory) = tmpfs_scratch_dir() else {
            return;
        };
        let batches = vec![frozen_batch(300, 0), frozen_batch(4113, 1)];
        let total: u64 = batches.iter().map(|batch| batch.len() as u64).sum();

        let uncached_size = Rc::new(AtomicU64::new(0));
        let uncached = open_writer(
            &directory,
            "uncached.log",
            SegmentIoMode::Uncached,
            &uncached_size,
        )
        .await;
        let error = uncached.save_frozen_batches(&batches).await.expect_err(
            "tmpfs must reject RWF_DONTCACHE; a write that succeeds here never carried the flag",
        );
        assert!(matches!(error, IggyError::CannotWriteToFile), "{error}");

        let buffered_size = Rc::new(AtomicU64::new(0));
        let buffered = open_writer(
            &directory,
            "buffered.log",
            SegmentIoMode::Buffered,
            &buffered_size,
        )
        .await;
        let saved = buffered
            .save_frozen_batches(&batches)
            .await
            .expect("the same directory takes buffered writes");
        assert_eq!(saved.as_bytes_u64(), total);
    }
}
