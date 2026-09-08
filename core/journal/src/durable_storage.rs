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

use compio::fs::{File, OpenOptions};
use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use std::ffi::OsString;
use std::io;
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenMode {
    Read,
    ReadWrite,
    Create,
}

pub struct StorageEntry {
    pub name: OsString,
    pub directory: bool,
}

/// Filesystem operations whose completion and persistence order affect recovery.
/// Implementations must preserve open file identity across rename and unlink.
pub trait DurableStorage {
    type File: DurableFile;

    /// # Errors
    /// Returns the underlying filesystem error.
    fn open(&self, path: &Path, mode: OpenMode) -> impl Future<Output = io::Result<Self::File>>;
    /// # Errors
    /// Returns the underlying filesystem error.
    fn create_directories(&self, path: &Path) -> impl Future<Output = io::Result<()>>;
    /// # Errors
    /// Returns the underlying filesystem error.
    fn sync_directory(&self, path: &Path) -> impl Future<Output = io::Result<()>>;
    /// # Errors
    /// Returns the underlying filesystem error.
    fn rename(&self, source: &Path, target: &Path) -> impl Future<Output = io::Result<()>>;
    /// # Errors
    /// Returns the underlying filesystem error.
    fn remove_file(&self, path: &Path) -> impl Future<Output = io::Result<()>>;
    /// # Errors
    /// Returns the underlying filesystem error.
    fn hard_link(&self, source: &Path, target: &Path) -> impl Future<Output = io::Result<()>>;
    /// # Errors
    /// Returns the underlying filesystem error.
    fn exists(&self, path: &Path) -> io::Result<bool>;
    /// # Errors
    /// Returns an error for unreadable directories or unsupported file types.
    fn entries(&self, path: &Path) -> io::Result<Vec<StorageEntry>>;
    /// # Errors
    /// Returns the underlying filesystem error. Missing paths are accepted.
    fn remove_tree(&self, path: &Path) -> io::Result<()>;
}

pub trait DurableFile {
    /// # Errors
    /// Returns an error if the complete range cannot be read.
    fn read(&self, offset: u64, length: usize) -> impl Future<Output = io::Result<Vec<u8>>>;
    /// # Errors
    /// Returns an error if any part of the write fails.
    fn write(&mut self, offset: u64, bytes: Vec<u8>) -> impl Future<Output = io::Result<()>>;
    /// # Errors
    /// Returns the underlying filesystem error.
    fn length(&self) -> impl Future<Output = io::Result<u64>>;
    /// # Errors
    /// Returns the underlying filesystem error.
    fn truncate(&self, length: u64) -> impl Future<Output = io::Result<()>>;
    /// # Errors
    /// Returns an error unless prior writes and the file length are durable.
    fn sync(&self) -> impl Future<Output = io::Result<()>>;
}

#[derive(Clone, Copy, Default)]
pub struct DiskStorage;

impl DurableStorage for DiskStorage {
    type File = File;

    async fn open(&self, path: &Path, mode: OpenMode) -> io::Result<File> {
        let mut options = OpenOptions::new();
        options.read(true).write(mode != OpenMode::Read);
        if mode == OpenMode::Create {
            options.create(true).truncate(true);
        }
        options.open(path).await
    }

    async fn create_directories(&self, path: &Path) -> io::Result<()> {
        compio::fs::create_dir_all(path).await
    }

    async fn sync_directory(&self, path: &Path) -> io::Result<()> {
        File::open(path).await?.sync_all().await
    }

    async fn rename(&self, source: &Path, target: &Path) -> io::Result<()> {
        compio::fs::rename(source, target).await
    }

    async fn remove_file(&self, path: &Path) -> io::Result<()> {
        compio::fs::remove_file(path).await
    }

    async fn hard_link(&self, source: &Path, target: &Path) -> io::Result<()> {
        compio::fs::hard_link(source, target).await
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        path.try_exists()
    }

    fn entries(&self, path: &Path) -> io::Result<Vec<StorageEntry>> {
        std::fs::read_dir(path)?
            .map(|entry| {
                let entry = entry?;
                let kind = entry.file_type()?;
                if !(kind.is_file() || kind.is_dir()) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected storage file type",
                    ));
                }
                Ok(StorageEntry {
                    name: entry.file_name(),
                    directory: kind.is_dir(),
                })
            })
            .collect()
    }

    fn remove_tree(&self, path: &Path) -> io::Result<()> {
        let metadata = match path.symlink_metadata() {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        if metadata.is_dir() {
            std::fs::remove_dir_all(path)
        } else {
            std::fs::remove_file(path)
        }
    }
}

impl DurableFile for File {
    async fn read(&self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        let (result, bytes) = self.read_exact_at(vec![0; length], offset).await.into();
        result?;
        Ok(bytes)
    }

    async fn write(&mut self, offset: u64, bytes: Vec<u8>) -> io::Result<()> {
        self.write_all_at(bytes, offset).await.0
    }

    async fn length(&self) -> io::Result<u64> {
        Ok(self.metadata().await?.len())
    }

    fn truncate(&self, length: u64) -> impl Future<Output = io::Result<()>> {
        // The running kernel may predate IORING_OP_FTRUNCATE. This is a
        // recovery operation and must not enter a disabled blocking pool.
        std::future::ready(
            std::os::fd::AsFd::as_fd(self)
                .try_clone_to_owned()
                .and_then(|descriptor| std::fs::File::from(descriptor).set_len(length)),
        )
    }

    async fn sync(&self) -> io::Result<()> {
        self.sync_data().await
    }
}
