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

use compio::fs::File;
use futures::future::LocalBoxFuture;
use journal::durable_storage::{
    DiskStorage, DurableFile, DurableStorage, OpenMode, RegularFiles, StorageEntry,
};
use server_common::iobuf::{Frozen, Owned};
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;

/// The storage under a partition WAL. A partition names one storage type, so
/// the simulator injects its filesystem model behind a trait object.
#[derive(Clone, Default)]
pub enum WalStorage {
    #[default]
    Disk,
    Injected(Rc<dyn InjectedStorage>),
}

/// A file opened through a [`WalStorage`].
pub enum WalFile {
    Disk(File),
    Injected(Box<dyn InjectedFile>),
}

/// [`DurableStorage`] as a trait object.
pub trait InjectedStorage {
    /// # Errors
    /// As [`DurableStorage::writer_identity`].
    fn writer_identity(&self, path: &Path) -> io::Result<Option<PathBuf>>;
    fn open<'a>(
        &'a self,
        path: &'a Path,
        mode: OpenMode,
    ) -> LocalBoxFuture<'a, io::Result<Box<dyn InjectedFile>>>;
    fn create_directories<'a>(&'a self, path: &'a Path) -> LocalBoxFuture<'a, io::Result<()>>;
    fn sync_directory<'a>(&'a self, path: &'a Path) -> LocalBoxFuture<'a, io::Result<()>>;
    fn rename<'a>(
        &'a self,
        source: &'a Path,
        target: &'a Path,
    ) -> LocalBoxFuture<'a, io::Result<()>>;
    fn remove_file<'a>(&'a self, path: &'a Path) -> LocalBoxFuture<'a, io::Result<()>>;
    fn hard_link<'a>(
        &'a self,
        source: &'a Path,
        target: &'a Path,
    ) -> LocalBoxFuture<'a, io::Result<()>>;
    fn exists<'a>(&'a self, path: &'a Path) -> LocalBoxFuture<'a, io::Result<bool>>;
    fn exists_following_links<'a>(&'a self, path: &'a Path)
    -> LocalBoxFuture<'a, io::Result<bool>>;
    fn entries<'a>(&'a self, path: &'a Path) -> LocalBoxFuture<'a, io::Result<Vec<StorageEntry>>>;
    fn regular_files<'a>(&'a self, path: &'a Path) -> LocalBoxFuture<'a, io::Result<RegularFiles>>;
    fn remove_tree<'a>(&'a self, path: &'a Path) -> LocalBoxFuture<'a, io::Result<()>>;
}

/// [`DurableFile`] as a trait object.
pub trait InjectedFile {
    fn preallocate<'a>(&'a self, path: &'a Path, length: u64) -> LocalBoxFuture<'a, ()>;
    fn read(&self, offset: u64, length: usize) -> LocalBoxFuture<'_, io::Result<Vec<u8>>>;
    fn write(&mut self, offset: u64, bytes: Vec<u8>) -> LocalBoxFuture<'_, io::Result<()>>;
    fn write_frozen(
        &mut self,
        offset: u64,
        bytes: Frozen<4096>,
    ) -> LocalBoxFuture<'_, io::Result<()>>;
    fn write_frozen_vectored(
        &mut self,
        offset: u64,
        buffers: Vec<Frozen<4096>>,
    ) -> LocalBoxFuture<'_, io::Result<()>>;
    fn write_aligned(
        &mut self,
        offset: u64,
        bytes: Owned<4096>,
    ) -> LocalBoxFuture<'_, io::Result<()>>;
    fn read_aligned(
        &self,
        offset: u64,
        length: usize,
    ) -> LocalBoxFuture<'_, io::Result<Owned<4096>>>;
    fn read_aligned_tail(
        &self,
        offset: u64,
        bytes: Owned<4096>,
        start: usize,
    ) -> LocalBoxFuture<'_, io::Result<Owned<4096>>>;
    fn length(&self) -> LocalBoxFuture<'_, io::Result<u64>>;
    fn truncate(&self, length: u64) -> LocalBoxFuture<'_, io::Result<()>>;
    fn sync(&self) -> LocalBoxFuture<'_, io::Result<()>>;
}

impl DurableStorage for WalStorage {
    type File = WalFile;

    fn writer_identity(&self, path: &Path) -> io::Result<Option<PathBuf>> {
        match self {
            Self::Disk => DurableStorage::writer_identity(&DiskStorage, path),
            Self::Injected(storage) => storage.writer_identity(path),
        }
    }

    async fn open(&self, path: &Path, mode: OpenMode) -> io::Result<WalFile> {
        match self {
            Self::Disk => DurableStorage::open(&DiskStorage, path, mode)
                .await
                .map(WalFile::Disk),
            Self::Injected(storage) => storage.open(path, mode).await.map(WalFile::Injected),
        }
    }

    async fn create_directories(&self, path: &Path) -> io::Result<()> {
        match self {
            Self::Disk => DurableStorage::create_directories(&DiskStorage, path).await,
            Self::Injected(storage) => storage.create_directories(path).await,
        }
    }

    async fn sync_directory(&self, path: &Path) -> io::Result<()> {
        match self {
            Self::Disk => DurableStorage::sync_directory(&DiskStorage, path).await,
            Self::Injected(storage) => storage.sync_directory(path).await,
        }
    }

    async fn rename(&self, source: &Path, target: &Path) -> io::Result<()> {
        match self {
            Self::Disk => DurableStorage::rename(&DiskStorage, source, target).await,
            Self::Injected(storage) => storage.rename(source, target).await,
        }
    }

    async fn remove_file(&self, path: &Path) -> io::Result<()> {
        match self {
            Self::Disk => DurableStorage::remove_file(&DiskStorage, path).await,
            Self::Injected(storage) => storage.remove_file(path).await,
        }
    }

    async fn hard_link(&self, source: &Path, target: &Path) -> io::Result<()> {
        match self {
            Self::Disk => DurableStorage::hard_link(&DiskStorage, source, target).await,
            Self::Injected(storage) => storage.hard_link(source, target).await,
        }
    }

    async fn exists(&self, path: &Path) -> io::Result<bool> {
        match self {
            Self::Disk => DurableStorage::exists(&DiskStorage, path).await,
            Self::Injected(storage) => storage.exists(path).await,
        }
    }

    async fn exists_following_links(&self, path: &Path) -> io::Result<bool> {
        match self {
            Self::Disk => DurableStorage::exists_following_links(&DiskStorage, path).await,
            Self::Injected(storage) => storage.exists_following_links(path).await,
        }
    }

    async fn entries(&self, path: &Path) -> io::Result<Vec<StorageEntry>> {
        match self {
            Self::Disk => DurableStorage::entries(&DiskStorage, path).await,
            Self::Injected(storage) => storage.entries(path).await,
        }
    }

    async fn regular_files(&self, path: &Path) -> io::Result<RegularFiles> {
        match self {
            Self::Disk => DurableStorage::regular_files(&DiskStorage, path).await,
            Self::Injected(storage) => storage.regular_files(path).await,
        }
    }

    async fn remove_tree(&self, path: &Path) -> io::Result<()> {
        match self {
            Self::Disk => DurableStorage::remove_tree(&DiskStorage, path).await,
            Self::Injected(storage) => storage.remove_tree(path).await,
        }
    }
}

impl DurableFile for WalFile {
    async fn preallocate(&self, path: &Path, length: u64) {
        match self {
            Self::Disk(file) => DurableFile::preallocate(file, path, length).await,
            Self::Injected(file) => file.preallocate(path, length).await,
        }
    }

    async fn read(&self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        match self {
            Self::Disk(file) => DurableFile::read(file, offset, length).await,
            Self::Injected(file) => file.read(offset, length).await,
        }
    }

    async fn write(&mut self, offset: u64, bytes: Vec<u8>) -> io::Result<()> {
        match self {
            Self::Disk(file) => DurableFile::write(file, offset, bytes).await,
            Self::Injected(file) => file.write(offset, bytes).await,
        }
    }

    async fn write_frozen(&mut self, offset: u64, bytes: Frozen<4096>) -> io::Result<()> {
        match self {
            Self::Disk(file) => DurableFile::write_frozen(file, offset, bytes).await,
            Self::Injected(file) => file.write_frozen(offset, bytes).await,
        }
    }

    async fn write_frozen_vectored(
        &mut self,
        offset: u64,
        buffers: Vec<Frozen<4096>>,
    ) -> io::Result<()> {
        match self {
            Self::Disk(file) => DurableFile::write_frozen_vectored(file, offset, buffers).await,
            Self::Injected(file) => file.write_frozen_vectored(offset, buffers).await,
        }
    }

    async fn write_aligned(&mut self, offset: u64, bytes: Owned<4096>) -> io::Result<()> {
        match self {
            Self::Disk(file) => DurableFile::write_aligned(file, offset, bytes).await,
            Self::Injected(file) => file.write_aligned(offset, bytes).await,
        }
    }

    async fn read_aligned(&self, offset: u64, length: usize) -> io::Result<Owned<4096>> {
        match self {
            Self::Disk(file) => DurableFile::read_aligned(file, offset, length).await,
            Self::Injected(file) => file.read_aligned(offset, length).await,
        }
    }

    async fn read_aligned_tail(
        &self,
        offset: u64,
        bytes: Owned<4096>,
        start: usize,
    ) -> io::Result<Owned<4096>> {
        match self {
            Self::Disk(file) => DurableFile::read_aligned_tail(file, offset, bytes, start).await,
            Self::Injected(file) => file.read_aligned_tail(offset, bytes, start).await,
        }
    }

    async fn length(&self) -> io::Result<u64> {
        match self {
            Self::Disk(file) => DurableFile::length(file).await,
            Self::Injected(file) => file.length().await,
        }
    }

    async fn truncate(&self, length: u64) -> io::Result<()> {
        match self {
            Self::Disk(file) => DurableFile::truncate(file, length).await,
            Self::Injected(file) => file.truncate(length).await,
        }
    }

    async fn sync(&self) -> io::Result<()> {
        match self {
            Self::Disk(file) => DurableFile::sync(file).await,
            Self::Injected(file) => file.sync().await,
        }
    }
}

impl From<File> for WalFile {
    fn from(file: File) -> Self {
        Self::Disk(file)
    }
}

impl TryFrom<WalFile> for File {
    type Error = WalFile;

    fn try_from(file: WalFile) -> Result<Self, WalFile> {
        match file {
            WalFile::Disk(file) => Ok(file),
            injected @ WalFile::Injected(_) => Err(injected),
        }
    }
}

impl<S> InjectedStorage for S
where
    S: DurableStorage,
    S::File: 'static,
{
    fn writer_identity(&self, path: &Path) -> io::Result<Option<PathBuf>> {
        DurableStorage::writer_identity(self, path)
    }

    fn open<'a>(
        &'a self,
        path: &'a Path,
        mode: OpenMode,
    ) -> LocalBoxFuture<'a, io::Result<Box<dyn InjectedFile>>> {
        Box::pin(async move {
            let file = DurableStorage::open(self, path, mode).await?;
            Ok(Box::new(file) as Box<dyn InjectedFile>)
        })
    }

    fn create_directories<'a>(&'a self, path: &'a Path) -> LocalBoxFuture<'a, io::Result<()>> {
        Box::pin(DurableStorage::create_directories(self, path))
    }

    fn sync_directory<'a>(&'a self, path: &'a Path) -> LocalBoxFuture<'a, io::Result<()>> {
        Box::pin(DurableStorage::sync_directory(self, path))
    }

    fn rename<'a>(
        &'a self,
        source: &'a Path,
        target: &'a Path,
    ) -> LocalBoxFuture<'a, io::Result<()>> {
        Box::pin(DurableStorage::rename(self, source, target))
    }

    fn remove_file<'a>(&'a self, path: &'a Path) -> LocalBoxFuture<'a, io::Result<()>> {
        Box::pin(DurableStorage::remove_file(self, path))
    }

    fn hard_link<'a>(
        &'a self,
        source: &'a Path,
        target: &'a Path,
    ) -> LocalBoxFuture<'a, io::Result<()>> {
        Box::pin(DurableStorage::hard_link(self, source, target))
    }

    fn exists<'a>(&'a self, path: &'a Path) -> LocalBoxFuture<'a, io::Result<bool>> {
        Box::pin(DurableStorage::exists(self, path))
    }

    fn exists_following_links<'a>(
        &'a self,
        path: &'a Path,
    ) -> LocalBoxFuture<'a, io::Result<bool>> {
        Box::pin(DurableStorage::exists_following_links(self, path))
    }

    fn entries<'a>(&'a self, path: &'a Path) -> LocalBoxFuture<'a, io::Result<Vec<StorageEntry>>> {
        Box::pin(DurableStorage::entries(self, path))
    }

    fn regular_files<'a>(&'a self, path: &'a Path) -> LocalBoxFuture<'a, io::Result<RegularFiles>> {
        Box::pin(DurableStorage::regular_files(self, path))
    }

    fn remove_tree<'a>(&'a self, path: &'a Path) -> LocalBoxFuture<'a, io::Result<()>> {
        Box::pin(DurableStorage::remove_tree(self, path))
    }
}

impl<F: DurableFile> InjectedFile for F {
    fn preallocate<'a>(&'a self, path: &'a Path, length: u64) -> LocalBoxFuture<'a, ()> {
        Box::pin(DurableFile::preallocate(self, path, length))
    }

    fn read(&self, offset: u64, length: usize) -> LocalBoxFuture<'_, io::Result<Vec<u8>>> {
        Box::pin(DurableFile::read(self, offset, length))
    }

    fn write(&mut self, offset: u64, bytes: Vec<u8>) -> LocalBoxFuture<'_, io::Result<()>> {
        Box::pin(DurableFile::write(self, offset, bytes))
    }

    fn write_frozen(
        &mut self,
        offset: u64,
        bytes: Frozen<4096>,
    ) -> LocalBoxFuture<'_, io::Result<()>> {
        Box::pin(DurableFile::write_frozen(self, offset, bytes))
    }

    fn write_frozen_vectored(
        &mut self,
        offset: u64,
        buffers: Vec<Frozen<4096>>,
    ) -> LocalBoxFuture<'_, io::Result<()>> {
        Box::pin(DurableFile::write_frozen_vectored(self, offset, buffers))
    }

    fn write_aligned(
        &mut self,
        offset: u64,
        bytes: Owned<4096>,
    ) -> LocalBoxFuture<'_, io::Result<()>> {
        Box::pin(DurableFile::write_aligned(self, offset, bytes))
    }

    fn read_aligned(
        &self,
        offset: u64,
        length: usize,
    ) -> LocalBoxFuture<'_, io::Result<Owned<4096>>> {
        Box::pin(DurableFile::read_aligned(self, offset, length))
    }

    fn read_aligned_tail(
        &self,
        offset: u64,
        bytes: Owned<4096>,
        start: usize,
    ) -> LocalBoxFuture<'_, io::Result<Owned<4096>>> {
        Box::pin(DurableFile::read_aligned_tail(self, offset, bytes, start))
    }

    fn length(&self) -> LocalBoxFuture<'_, io::Result<u64>> {
        Box::pin(DurableFile::length(self))
    }

    fn truncate(&self, length: u64) -> LocalBoxFuture<'_, io::Result<()>> {
        Box::pin(DurableFile::truncate(self, length))
    }

    fn sync(&self) -> LocalBoxFuture<'_, io::Result<()>> {
        Box::pin(DurableFile::sync(self))
    }
}
