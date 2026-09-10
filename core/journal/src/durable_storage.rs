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

use compio::buf::{IntoInner, IoBuf};
use compio::fs::{File, OpenOptions};
use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use futures::channel::oneshot;
use futures::lock::Mutex;
use server_common::iobuf::{Frozen, Owned};
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

    /// Process-local identity for exclusive writer ownership.
    ///
    /// # Errors
    /// Returns an error if the absolute identity cannot be resolved.
    fn writer_identity(&self, _path: &Path) -> io::Result<Option<std::path::PathBuf>> {
        Ok(None)
    }

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
    fn exists(&self, path: &Path) -> impl Future<Output = io::Result<bool>>;
    /// # Errors
    /// Returns an error for unreadable directories or unsupported file types.
    fn entries(&self, path: &Path) -> impl Future<Output = io::Result<Vec<StorageEntry>>>;
    /// # Errors
    /// Returns the underlying filesystem error. Missing paths are accepted.
    fn remove_tree(&self, path: &Path) -> impl Future<Output = io::Result<()>>;
}

pub trait DurableFile {
    /// Best-effort reservation that must not change bytes or logical length.
    /// Backends without physical allocation may ignore this hint.
    fn preallocate(&self, _path: &Path, _length: u64) {}

    /// # Errors
    /// Returns an error if the complete range cannot be read.
    fn read(&self, offset: u64, length: usize) -> impl Future<Output = io::Result<Vec<u8>>>;
    /// # Errors
    /// Returns an error if any part of the write fails.
    fn write(&mut self, offset: u64, bytes: Vec<u8>) -> impl Future<Output = io::Result<()>>;
    /// # Errors
    /// Returns an error if the immutable extent cannot be written completely.
    fn write_frozen(
        &mut self,
        offset: u64,
        bytes: Frozen<4096>,
    ) -> impl Future<Output = io::Result<()>> {
        async move { self.write(offset, bytes.as_slice().to_vec()).await }
    }
    /// Write adjacent immutable extents in order.
    /// Callers must limit the buffer count to [`server_common::iobuf::IOV_MAX`].
    ///
    /// # Errors
    /// Returns an error if any extent cannot be written completely.
    fn write_frozen_vectored(
        &mut self,
        offset: u64,
        buffers: Vec<Frozen<4096>>,
    ) -> impl Future<Output = io::Result<()>> {
        async move {
            let mut bytes = Vec::with_capacity(buffers.iter().map(Frozen::len).sum());
            for buffer in buffers {
                bytes.extend_from_slice(buffer.as_slice());
            }
            self.write(offset, bytes).await
        }
    }
    /// # Errors
    /// Returns an error if the aligned extent cannot be written completely.
    fn write_aligned(
        &mut self,
        offset: u64,
        bytes: Owned<4096>,
    ) -> impl Future<Output = io::Result<()>> {
        async move { self.write(offset, bytes.as_slice().to_vec()).await }
    }
    /// # Errors
    /// Returns an error if the requested range cannot be read completely.
    fn read_aligned(
        &self,
        offset: u64,
        length: usize,
    ) -> impl Future<Output = io::Result<Owned<4096>>> {
        async move { Ok(Owned::copy_from_slice(&self.read(offset, length).await?)) }
    }
    /// # Errors
    /// Returns an error if the remaining aligned range cannot be read completely.
    fn read_aligned_tail(
        &self,
        offset: u64,
        mut bytes: Owned<4096>,
        start: usize,
    ) -> impl Future<Output = io::Result<Owned<4096>>> {
        async move {
            let tail = self.read(offset, bytes.as_slice().len() - start).await?;
            bytes.as_mut_slice()[start..].copy_from_slice(&tail);
            Ok(bytes)
        }
    }
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

    fn writer_identity(&self, path: &Path) -> io::Result<Option<std::path::PathBuf>> {
        let absolute = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };
        let mut normalized = std::path::PathBuf::new();
        for component in absolute.components() {
            match component {
                std::path::Component::CurDir => {}
                std::path::Component::ParentDir => {
                    normalized.pop();
                }
                component => normalized.push(component.as_os_str()),
            }
        }
        Ok(Some(normalized))
    }

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

    async fn exists(&self, path: &Path) -> io::Result<bool> {
        match compio::fs::symlink_metadata(path).await {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn entries(&self, path: &Path) -> io::Result<Vec<StorageEntry>> {
        // getdents has no io_uring operation, and shard fallback pools are disabled.
        // The permit lives on the worker so caller cancellation cannot spawn
        // an unbounded number of directory scanners.
        static SCANNER: Mutex<()> = Mutex::new(());
        let permit = SCANNER.lock().await;
        let path = path.to_path_buf();
        let (sender, receiver) = oneshot::channel();
        std::thread::Builder::new()
            .name("iggy-directory-scan".to_owned())
            .spawn(move || {
                let _permit = permit;
                let result = directory_entries(&path);
                let _ = sender.send(result);
            })?;
        receiver
            .await
            .map_err(|_| io::Error::other("directory scanner stopped"))?
    }

    async fn remove_tree(&self, path: &Path) -> io::Result<()> {
        let mut pending = vec![(path.to_path_buf(), false)];
        while let Some((path, visited)) = pending.pop() {
            let metadata = match compio::fs::symlink_metadata(&path).await {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            let result = if !metadata.is_dir() {
                compio::fs::remove_file(&path).await
            } else if visited {
                compio::fs::remove_dir(&path).await
            } else {
                let entries = self.entries(&path).await?;
                pending.push((path.clone(), true));
                pending.extend(
                    entries
                        .into_iter()
                        .map(|entry| (path.join(entry.name), false)),
                );
                continue;
            };
            if let Err(error) = result
                && error.kind() != io::ErrorKind::NotFound
            {
                return Err(error);
            }
        }
        Ok(())
    }
}

impl DurableFile for File {
    fn preallocate(&self, path: &Path, length: u64) {
        server_common::fs_utils::preallocate_file(self, path, length);
    }

    async fn read(&self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        let (result, bytes) = self.read_exact_at(vec![0; length], offset).await.into();
        result?;
        Ok(bytes)
    }

    async fn write(&mut self, offset: u64, bytes: Vec<u8>) -> io::Result<()> {
        self.write_all_at(bytes, offset).await.0
    }

    async fn write_frozen(&mut self, offset: u64, bytes: Frozen<4096>) -> io::Result<()> {
        self.write_all_at(bytes, offset).await.0
    }

    async fn write_frozen_vectored(
        &mut self,
        offset: u64,
        buffers: Vec<Frozen<4096>>,
    ) -> io::Result<()> {
        self.write_vectored_all_at(buffers, offset).await.0
    }

    async fn write_aligned(&mut self, offset: u64, bytes: Owned<4096>) -> io::Result<()> {
        self.write_all_at(bytes, offset).await.0
    }

    async fn read_aligned(&self, offset: u64, length: usize) -> io::Result<Owned<4096>> {
        let (result, bytes) = self
            .read_exact_at(Owned::with_capacity(length), offset)
            .await
            .into();
        result?;
        Ok(bytes)
    }

    async fn read_aligned_tail(
        &self,
        offset: u64,
        bytes: Owned<4096>,
        start: usize,
    ) -> io::Result<Owned<4096>> {
        let (result, slice) = self
            .read_exact_at(bytes.slice(start..), offset)
            .await
            .into();
        result?;
        Ok(slice.into_inner())
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

fn directory_entries(path: &Path) -> io::Result<Vec<StorageEntry>> {
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
