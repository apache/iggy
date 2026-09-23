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

use crate::Storage;
use compio::buf::IoBuf;
use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use std::cell::{Cell, UnsafeCell};
use std::fs;
use std::io;
use std::os::fd::AsFd;
use std::path::{Path, PathBuf};

/// File-backed storage implementing the `Storage` trait.
pub struct FileStorage {
    file: UnsafeCell<compio::fs::File>,
    write_offset: Cell<u64>,
    path: PathBuf,
}

#[allow(clippy::future_not_send)]
impl FileStorage {
    /// Open or create the file at `path` in read-write mode, setting
    /// `write_offset` to current file length.
    ///
    /// # Errors
    /// Returns an I/O error if the file cannot be opened or created.
    pub async fn open(path: &Path) -> io::Result<Self> {
        let file = compio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .await?;
        let len = file.metadata().await?.len();
        Ok(Self {
            file: UnsafeCell::new(file),
            write_offset: Cell::new(len),
            path: path.to_path_buf(),
        })
    }

    /// Current file size (tracks append position).
    pub const fn file_len(&self) -> u64 {
        self.write_offset.get()
    }

    /// Truncate the file to `len` bytes and make the new length durable.
    ///
    /// Synchronous `std::fs` on a duplicate of the open descriptor, not compio:
    /// compio's `set_len` submits `IORING_OP_FTRUNCATE`, which landed in
    /// mainline Linux 6.9. When the opcode is unavailable, the driver falls
    /// back to its blocking pool, and shard proactors run with
    /// `thread_pool_limit(0)`, so the fallback panics the shard instead of
    /// repairing the WAL. `std::fs` needs neither the opcode nor the pool. The
    /// sole caller is boot-time torn-tail repair, so blocking the shard thread
    /// here costs nothing.
    ///
    /// `sync_all` makes the durable-truncation contract explicit and matches
    /// segment recovery. Its additional metadata synchronization is acceptable
    /// because this runs only during boot-time repair.
    ///
    /// # Errors
    /// Returns an I/O error if the descriptor cannot be cloned, truncated, or synced.
    pub(crate) fn truncate(&self, len: u64) -> io::Result<()> {
        // SAFETY: single-threaded compio runtime, no concurrent access to the file.
        let file = unsafe { &*self.file.get() };
        let file = fs::File::from(file.as_fd().try_clone_to_owned()?);
        file.set_len(len)?;
        self.write_offset.set(len);
        file.sync_all()
    }

    /// Fsync the file to disk.
    ///
    /// # Errors
    /// Returns an I/O error if sync fails.
    pub async fn fsync(&self) -> io::Result<()> {
        // SAFETY: single-threaded compio runtime, no concurrent access to the file.
        unsafe { &*self.file.get() }.sync_data().await
    }

    /// Positional read into `buf`. Returns the buffer with data filled in.
    ///
    /// # Errors
    /// Returns an I/O error if the read fails.
    pub async fn read_at(&self, offset: u64, buf: Vec<u8>) -> io::Result<Vec<u8>> {
        // SAFETY: single-threaded compio runtime, no concurrent access to the file.
        let file = unsafe { &*self.file.get() };
        let (result, buf) = file.read_exact_at(buf, offset).await.into();
        result?;
        Ok(buf)
    }

    /// Append write at the next free offset; returns the offset written to.
    ///
    /// Reserves the region by advancing `write_offset` **synchronously,
    /// before** the write `.await`. On the single-threaded compio runtime
    /// two `write_append` calls can interleave at the await; reserving first
    /// hands each a distinct, non-overlapping offset. The previous code read
    /// the offset and advanced the cursor *after* the await, so two in-flight
    /// appends both saw the same offset and wrote over each other (and the
    /// journal recorded the same index position for both) -- the corruption
    /// seen under interleaved `on_replicate` calls (a queued op drained while
    /// the next op is freshly submitted).
    ///
    /// On write error the reservation is rolled back **only when no later
    /// append has reserved space after us** (`write_offset == offset + len`).
    /// Leaving the gap is unsafe: a subsequent append (e.g. a VSR retransmit
    /// of this op) would write past the zero hole, fsync and ack, and on
    /// reopen the recovery scan would hit the hole and truncate from there --
    /// silently dropping the committed op that now sits above the gap. Rolling
    /// back lets the next append reuse the slot instead. In the rare
    /// interleave where another append already reserved after us, rolling back
    /// would clobber its slot, so the gap is left for recovery in that case.
    ///
    /// # Errors
    /// Returns an I/O error if the write fails.
    pub async fn write_append<B: IoBuf>(&self, buf: B) -> io::Result<u64> {
        let len = buf.buf_len() as u64;
        let offset = self.write_offset.get();
        self.write_offset.set(offset + len);
        // SAFETY: single-threaded compio runtime, no concurrent access to the file.
        let file = unsafe { &mut *self.file.get() };
        let (result, _buf) = file.write_all_at(buf, offset).await.into();
        if let Err(error) = result {
            if self.write_offset.get() == offset + len {
                self.write_offset.set(offset);
            }
            return Err(error);
        }
        Ok(offset)
    }

    /// The file path this storage was opened with.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Reopen the underlying file descriptor at the stored path.
    ///
    /// Used after an atomic rename replaces the file on disk.
    ///
    /// # Errors
    /// Returns an I/O error if the file cannot be reopened.
    pub async fn reopen(&self) -> io::Result<()> {
        let file = compio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
            .await?;
        let len = file.metadata().await?.len();
        // SAFETY: single-threaded compio runtime, no concurrent access to the file.
        unsafe { *self.file.get() = file };
        self.write_offset.set(len);
        Ok(())
    }
}

#[allow(clippy::future_not_send)]
impl Storage for FileStorage {
    type Buffer = Vec<u8>;

    async fn write_at(&self, offset: usize, buf: Self::Buffer) -> io::Result<usize> {
        let len = buf.buf_len();
        let file = unsafe { &mut *self.file.get() };
        let (result, _buf) = file.write_all_at(buf, offset as u64).await.into();
        result?;
        Ok(len)
    }

    async fn read_at(&self, offset: usize, buffer: Self::Buffer) -> io::Result<Self::Buffer> {
        let file = unsafe { &*self.file.get() };
        let (result, buffer) = file.read_exact_at(buffer, offset as u64).await.into();
        result?;
        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::FileStorage;
    use server_common::executor::create_shard_executor;
    use tempfile::tempdir;

    /// Pins the synchronous truncate signature and verifies it works inside a
    /// shard executor with no blocking pool. A modern test kernel supports
    /// `IORING_OP_FTRUNCATE`, so this does not reproduce compio's fallback.
    #[test]
    fn given_a_shard_executor_with_no_blocking_pool_when_truncating_should_repair_the_file() {
        let runtime = create_shard_executor().unwrap();
        runtime.block_on(async {
            let dir = tempdir().unwrap();
            let path = dir.path().join("journal.wal");
            let storage = FileStorage::open(&path).await.unwrap();
            storage.write_append(vec![0xAB_u8; 128]).await.unwrap();

            storage.truncate(64).unwrap();

            assert_eq!(storage.file_len(), 64);
            assert_eq!(std::fs::metadata(&path).unwrap().len(), 64);
        });
    }

    #[test]
    fn given_a_replaced_path_when_truncating_should_truncate_the_open_file() {
        let runtime = create_shard_executor().unwrap();
        runtime.block_on(async {
            let dir = tempdir().unwrap();
            let path = dir.path().join("journal.wal");
            let renamed_path = dir.path().join("journal.renamed.wal");
            let storage = FileStorage::open(&path).await.unwrap();
            storage.write_append(vec![0xAB_u8; 128]).await.unwrap();
            std::fs::rename(&path, &renamed_path).unwrap();
            std::fs::write(&path, vec![0xCD_u8; 256]).unwrap();

            storage.truncate(64).unwrap();

            assert_eq!(storage.file_len(), 64);
            assert_eq!(std::fs::metadata(&renamed_path).unwrap().len(), 64);
            assert_eq!(std::fs::metadata(&path).unwrap().len(), 256);
        });
    }
}
