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
use std::cell::{Cell, RefCell};
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

/// File-backed storage implementing the `Storage` trait.
pub struct FileStorage {
    file: RefCell<File>,
    write_offset: Cell<u64>,
    path: PathBuf,
}

impl FileStorage {
    /// Open or create the file at `path`, setting `write_offset` to current file length.
    ///
    /// # Errors
    /// Returns an I/O error if the file cannot be opened or created.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let len = file.metadata()?.len();
        Ok(Self {
            file: RefCell::new(file),
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
    pub fn truncate(&self, len: u64) -> io::Result<()> {
        self.file.borrow().set_len(len)?;
        self.write_offset.set(len);
        Ok(())
    }

    /// Fsync the file to disk.
    ///
    /// # Errors
    /// Returns an I/O error if sync fails.
    pub fn fsync(&self) -> io::Result<()> {
        self.file.borrow().sync_data()
    }

    /// Positional read into `buf`.
    ///
    /// # Errors
    /// Returns an I/O error if the read fails.
    pub fn read_sync(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        self.file.borrow().read_exact_at(buf, offset)
    }

    /// Append write, returns bytes written.
    ///
    /// # Errors
    /// Returns an I/O error if the write fails.
    #[allow(clippy::cast_possible_truncation)]
    pub fn write_sync(&self, buf: &[u8]) -> io::Result<usize> {
        self.file
            .borrow()
            .write_all_at(buf, self.write_offset.get())?;
        let len = buf.len();
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
    pub fn reopen(&self) -> io::Result<()> {
        let file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let len = file.metadata()?.len();
        *self.file.borrow_mut() = file;
        self.write_offset.set(len);
        Ok(())
    }
}

#[allow(clippy::future_not_send)]
impl Storage for FileStorage {
    type Buffer = Vec<u8>;

    async fn write(&self, buf: Self::Buffer) -> io::Result<usize> {
        self.write_sync(&buf)
    }

    async fn read(&self, offset: usize, mut buffer: Self::Buffer) -> io::Result<Self::Buffer> {
        self.read_sync(offset as u64, &mut buffer)?;
        Ok(buffer)
    }
}
