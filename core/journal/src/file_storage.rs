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
use std::io;
use std::path::{Path, PathBuf};

/// File-backed storage implementing the `Storage` trait.
pub struct FileStorage {
    file: UnsafeCell<compio::fs::File>,
    write_offset: Cell<u64>,
    path: PathBuf,
}

#[allow(clippy::future_not_send)]
impl FileStorage {
    /// Open or create the file at `path`, setting `write_offset` to current file length.
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

    /// Truncate the file to `len` bytes.
    ///
    /// # Errors
    /// Returns an I/O error if truncation fails.
    pub async fn truncate(&self, len: u64) -> io::Result<()> {
        let file = unsafe { &*self.file.get() };
        file.set_len(len).await?;
        self.write_offset.set(len);
        Ok(())
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

    /// Append write, returns bytes written.
    ///
    /// # Errors
    /// Returns an I/O error if the write fails.
    #[allow(clippy::cast_possible_truncation)]
    pub async fn write_append<B: IoBuf>(&self, buf: B) -> io::Result<usize> {
        let len = buf.buf_len();
        // SAFETY: single-threaded compio runtime, no concurrent access to the file.
        let file = unsafe { &mut *self.file.get() };
        let (result, _buf) = file.write_all_at(buf, self.write_offset.get()).await.into();
        result?;
        self.write_offset.set(self.write_offset.get() + len as u64);
        Ok(len)
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
