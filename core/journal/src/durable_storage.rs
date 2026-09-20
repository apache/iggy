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
use futures::{Stream, stream};
use server_common::iobuf::{Frozen, Owned};
use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use tokio::sync::{Semaphore, mpsc};
use tracing::warn;

const FILE_DIRECTORY_BUFFER: usize = 64;
static FILE_DIRECTORY_READERS: Semaphore = Semaphore::const_new(4);

/// Paths to regular files from one directory scan, followed by any scan error.
/// Dropping the stream may cancel enumeration without waiting for its worker.
pub type RegularFiles = Pin<Box<dyn Stream<Item = io::Result<PathBuf>>>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenMode {
    Read,
    ReadWrite,
    Create,
    /// Create or truncate a file without requiring read permission.
    CreateWriteOnly,
    /// Create a file if absent, preserving the inode and bytes if it exists.
    CreateOrOpen,
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
    /// Check whether a path resolves to an existing target, following symbolic links.
    /// Backends without symbolic links can use the default existence check.
    ///
    /// # Errors
    /// Returns the underlying filesystem error. Missing targets return `false`.
    fn exists_following_links(&self, path: &Path) -> impl Future<Output = io::Result<bool>> {
        self.exists(path)
    }
    /// # Errors
    /// Returns an error for unreadable directories or unsupported file types.
    fn entries(&self, path: &Path) -> impl Future<Output = io::Result<Vec<StorageEntry>>>;
    /// Enumerate regular files without including directories.
    ///
    /// Disk scans skip unreadable entries and unsupported file types, and bound
    /// both worker concurrency and buffered paths. Other backends may use their
    /// existing directory enumeration through the default implementation.
    ///
    /// # Errors
    /// Returns an error, or yields one in the stream, if the directory cannot be
    /// enumerated. Dropping a stream does not make any filesystem changes durable.
    fn regular_files(&self, path: &Path) -> impl Future<Output = io::Result<RegularFiles>> {
        async move {
            let entries = self.entries(path).await?;
            let path = path.to_path_buf();
            Ok(Box::pin(stream::iter(
                entries
                    .into_iter()
                    .filter(|entry| !entry.directory)
                    .map(move |entry| Ok(path.join(entry.name))),
            )) as RegularFiles)
        }
    }
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
        options
            .read(mode != OpenMode::CreateWriteOnly)
            .write(mode != OpenMode::Read);
        if matches!(
            mode,
            OpenMode::Create | OpenMode::CreateWriteOnly | OpenMode::CreateOrOpen
        ) {
            options
                .create(true)
                .truncate(matches!(mode, OpenMode::Create | OpenMode::CreateWriteOnly));
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

    async fn exists_following_links(&self, path: &Path) -> io::Result<bool> {
        match compio::fs::metadata(path).await {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn entries(&self, path: &Path) -> io::Result<Vec<StorageEntry>> {
        // getdents has no io_uring operation, and shard fallback pools are disabled.
        let path = path.to_path_buf();
        run_blocking("iggy-directory-scan", move || directory_entries(&path)).await
    }

    async fn regular_files(&self, path: &Path) -> io::Result<RegularFiles> {
        // Compio has no asynchronous directory iterator and the shard's blocking
        // pool is disabled. The worker owns the permit so cancellation cannot
        // exceed the concurrency bound.
        let permit = FILE_DIRECTORY_READERS
            .acquire()
            .await
            .map_err(io::Error::other)?;
        let (sender, receiver) = mpsc::channel(FILE_DIRECTORY_BUFFER);
        let directory = path.to_path_buf();
        std::thread::Builder::new()
            .name("iggy-offset-recovery".to_owned())
            .spawn(move || {
                let _permit = permit;
                let result = (|| {
                    // A single bad entry must not keep offset recovery from
                    // booting a partition. Only opening the directory is fatal.
                    for entry in std::fs::read_dir(&directory)? {
                        let entry = match entry {
                            Ok(entry) => entry,
                            Err(error) => {
                                warn!(path = %directory.display(), %error, "failed to read directory entry");
                                continue;
                            }
                        };
                        let is_file = match entry.file_type() {
                            Ok(file_type) => file_type.is_file(),
                            Err(error) => {
                                warn!(path = %directory.display(), %error, "failed to read entry type");
                                continue;
                            }
                        };
                        if is_file && sender.blocking_send(Ok(Some(entry.path()))).is_err() {
                            return Ok(());
                        }
                    }
                    Ok(())
                })();
                // Explicit completion distinguishes an empty directory from an
                // interrupted worker. Closed receivers abandon enumeration.
                let _ = sender.blocking_send(result.map(|()| None));
            })?;
        Ok(Box::pin(stream::unfold(
            Some(receiver),
            |receiver| async move {
                let mut receiver = receiver?;
                match receiver.recv().await {
                    Some(Ok(Some(path))) => Some((Ok(path), Some(receiver))),
                    Some(Ok(None)) => None,
                    Some(Err(error)) => Some((Err(error), None)),
                    None => Some((Err(io::Error::other("directory reader stopped")), None)),
                }
            },
        )))
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

    async fn truncate(&self, length: u64) -> io::Result<()> {
        // Older kernels lack IORING_OP_FTRUNCATE and shard fallback pools are
        // disabled. Own the inode until the worker completes, even on cancellation.
        let descriptor = std::os::fd::AsFd::as_fd(self).try_clone_to_owned()?;
        run_blocking("iggy-file-truncate", move || {
            std::fs::File::from(descriptor).set_len(length)
        })
        .await
    }

    async fn sync(&self) -> io::Result<()> {
        self.sync_data().await
    }
}

async fn run_blocking<T: Send + 'static>(
    name: &'static str,
    operation: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    // Keep the permit on the worker: cancelling its caller must not admit
    // another blocking operation while this one still owns filesystem state.
    static WORKER: Mutex<()> = Mutex::new(());
    let permit = WORKER.lock().await;
    let (sender, receiver) = oneshot::channel();
    std::thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || {
            let _permit = permit;
            let _ = sender.send(operation());
        })?;
    receiver
        .await
        .map_err(|_| io::Error::other(format!("{name} stopped")))?
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

#[cfg(test)]
mod tests {
    use super::{
        DiskStorage, DurableFile, DurableStorage, FILE_DIRECTORY_BUFFER, OpenMode, run_blocking,
    };
    use futures::TryStreamExt;
    use futures::channel::oneshot;
    use futures::future::{Either, select};
    use std::io;
    use std::sync::mpsc;
    use std::time::Duration;

    const WORKER_TIMEOUT: Duration = Duration::from_secs(5);

    #[compio::test]
    async fn given_symlink_when_target_disappears_should_report_target_absent() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("target");
        let link = directory.path().join("link");
        std::fs::write(&target, []).unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        assert!(DiskStorage.exists_following_links(&target).await.unwrap());
        assert!(DiskStorage.exists_following_links(&link).await.unwrap());

        std::fs::remove_file(&target).unwrap();

        assert!(DiskStorage.exists(&link).await.unwrap());
        assert!(!DiskStorage.exists_following_links(&link).await.unwrap());
    }

    #[compio::test]
    async fn given_buffered_file_scan_when_cancelled_should_release_worker_capacity() {
        let directory = tempfile::tempdir().unwrap();
        let entry_count = FILE_DIRECTORY_BUFFER * 2;
        for consumer_id in 0..entry_count {
            std::fs::write(directory.path().join(consumer_id.to_string()), []).unwrap();
        }

        // More cancellations than available workers expose permits retained by
        // a worker whose receiver has gone away.
        for _ in 0..8 {
            let entries = DiskStorage.regular_files(directory.path()).await.unwrap();
            drop(entries);
        }

        let paths: Vec<_> = DiskStorage
            .regular_files(directory.path())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(paths.len(), entry_count);
    }

    #[compio::test]
    async fn given_non_regular_entries_when_scanning_files_should_skip_them() {
        let directory = tempfile::tempdir().unwrap();
        let regular_file = directory.path().join("1");
        std::fs::write(&regular_file, []).unwrap();
        std::fs::create_dir(directory.path().join("2")).unwrap();
        std::os::unix::fs::symlink(&regular_file, directory.path().join("3")).unwrap();

        let paths: Vec<_> = DiskStorage
            .regular_files(directory.path())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        assert_eq!(paths, [regular_file]);
    }

    #[compio::test]
    async fn cancelled_blocking_operation_keeps_its_permit_until_completion() {
        let executor_thread = std::thread::current().id();
        let (started, started_rx) = oneshot::channel();
        let (release, release_rx) = mpsc::channel();
        let mut blocked = Box::pin(run_blocking("iggy-test-blocked", move || {
            assert_ne!(std::thread::current().id(), executor_thread);
            let _ = started.send(());
            release_rx
                .recv_timeout(WORKER_TIMEOUT)
                .map_err(io::Error::other)
        }));
        assert!(futures::poll!(&mut blocked).is_pending());
        assert!(matches!(
            select(started_rx, blocked.as_mut()).await,
            Either::Left((Ok(()), _))
        ));
        drop(blocked);

        let mut successor = Box::pin(run_blocking("iggy-test-successor", || Ok(())));
        assert!(futures::poll!(&mut successor).is_pending());
        compio::time::sleep(Duration::from_millis(1)).await;
        assert!(futures::poll!(&mut successor).is_pending());
        release.send(()).unwrap();
        successor.await.unwrap();
    }

    #[compio::test]
    async fn queued_truncate_preserves_the_open_inode_and_completes_before_sync() {
        let directory = tempfile::tempdir().unwrap();
        let original = directory.path().join("original");
        let renamed = directory.path().join("renamed");
        std::fs::write(&original, b"original contents").unwrap();
        let file = DiskStorage
            .open(&original, OpenMode::ReadWrite)
            .await
            .unwrap();
        let (started, started_rx) = oneshot::channel();
        let (release, release_rx) = mpsc::channel();
        let mut blocked = Box::pin(run_blocking("iggy-test-blocked", move || {
            let _ = started.send(());
            release_rx
                .recv_timeout(WORKER_TIMEOUT)
                .map_err(io::Error::other)
        }));
        assert!(futures::poll!(&mut blocked).is_pending());
        assert!(matches!(
            select(started_rx, blocked.as_mut()).await,
            Either::Left((Ok(()), _))
        ));
        let mut truncate = Box::pin(file.truncate(3));
        assert!(futures::poll!(&mut truncate).is_pending());
        std::fs::rename(&original, &renamed).unwrap();
        std::fs::write(&original, b"replacement contents").unwrap();
        release.send(()).unwrap();
        blocked.await.unwrap();
        truncate.await.unwrap();
        file.sync().await.unwrap();

        assert_eq!(std::fs::read(&renamed).unwrap(), b"ori");
        assert_eq!(std::fs::read(&original).unwrap(), b"replacement contents");
    }
}
