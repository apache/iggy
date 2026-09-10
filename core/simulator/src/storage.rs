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

//! Deterministic filesystem model for persistence ordering and power-loss tests.

#![allow(clippy::future_not_send)]

use journal::durable_storage::{DurableFile, DurableStorage, OpenMode, StorageEntry};
use server_common::iobuf::Frozen;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io;
use std::path::{Component, Path};
use std::rc::Rc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Crash {
    Process,
    PowerLoss,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultMode {
    Before,
    After,
    TornWrite,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageOperation {
    Open,
    Create,
    Read,
    Write,
    Length,
    Truncate,
    FileSync,
    CreateDirectory,
    DirectorySync,
    Rename,
    Unlink,
    Link,
    Exists,
    List,
    RemoveTree,
}

#[derive(Clone, Default)]
pub struct SimStorage {
    state: Rc<RefCell<State>>,
}

pub struct SimFile {
    storage: SimStorage,
    inode: usize,
    epoch: u64,
}

#[derive(Clone)]
enum Inode {
    File {
        buffered: Vec<u8>,
        stable: Vec<u8>,
    },
    Directory {
        entries: BTreeMap<OsString, usize>,
        stable: BTreeMap<OsString, usize>,
    },
}

#[derive(Clone)]
struct State {
    inodes: Vec<Inode>,
    epoch: u64,
    trace: Vec<StorageOperation>,
    written_bytes: BTreeMap<usize, usize>,
    fault: Option<(usize, FaultMode)>,
    paused: Option<StorageOperation>,
    waiters: Vec<std::task::Waker>,
}

impl SimStorage {
    #[must_use]
    pub fn trace(&self) -> Vec<StorageOperation> {
        self.state.borrow().trace.clone()
    }

    pub fn clear_trace(&self) {
        let mut state = self.state.borrow_mut();
        state.trace.clear();
        state.fault = None;
    }

    pub fn fail_at(&self, operation: usize, mode: FaultMode) {
        let mut state = self.state.borrow_mut();
        state.trace.clear();
        state.fault = Some((operation, mode));
    }

    /// A process restart preserves the OS cache. Power loss discards it.
    /// Every restart invalidates open handles owned by the old process.
    pub fn crash(&self, crash: Crash) {
        let mut state = self.state.borrow_mut();
        state.epoch += 1;
        if crash == Crash::PowerLoss {
            for inode in &mut state.inodes {
                match inode {
                    Inode::File { buffered, stable } => buffered.clone_from(stable),
                    Inode::Directory { entries, stable } => entries.clone_from(stable),
                }
            }
        }
        state.trace.clear();
        state.fault = None;
    }

    /// Model background writeback without attributing a durability barrier to it.
    pub fn writeback(&self) {
        for inode in &mut self.state.borrow_mut().inodes {
            match inode {
                Inode::File { buffered, stable } => stable.clone_from(buffered),
                Inode::Directory { entries, stable } => stable.clone_from(entries),
            }
        }
    }

    pub fn pause_writes(&self) {
        self.state.borrow_mut().paused = Some(StorageOperation::Write);
    }

    pub fn pause_file_syncs(&self) {
        self.state.borrow_mut().paused = Some(StorageOperation::FileSync);
    }

    pub fn resume(&self) {
        let waiters = {
            let mut state = self.state.borrow_mut();
            state.paused = None;
            std::mem::take(&mut state.waiters)
        };
        for waiter in waiters {
            waiter.wake();
        }
    }

    async fn wait_for(&self, operation: StorageOperation) {
        futures::future::poll_fn(|context| {
            let mut state = self.state.borrow_mut();
            if state.paused != Some(operation) {
                return std::task::Poll::Ready(());
            }
            if !state
                .waiters
                .iter()
                .any(|waiter| waiter.will_wake(context.waker()))
            {
                state.waiters.push(context.waker().clone());
            }
            std::task::Poll::Pending
        })
        .await;
    }

    fn perform<T>(
        &self,
        operation: StorageOperation,
        action: impl FnOnce(&mut State, bool) -> io::Result<T>,
    ) -> io::Result<T> {
        let mut state = self.state.borrow_mut();
        let index = state.trace.len();
        state.trace.push(operation);
        let mode = state
            .fault
            .filter(|(at, _)| *at == index)
            .map(|(_, mode)| mode);
        if mode.is_some() {
            state.fault = None;
        }
        if mode == Some(FaultMode::Before) {
            return Err(io::Error::other("injected storage failure"));
        }
        let result = action(&mut state, mode == Some(FaultMode::TornWrite))?;
        if mode.is_some() {
            return Err(io::Error::other("injected failure after storage effect"));
        }
        Ok(result)
    }
}

impl DurableStorage for SimStorage {
    type File = SimFile;

    async fn open(&self, path: &Path, mode: OpenMode) -> io::Result<SimFile> {
        let operation = if mode == OpenMode::Create {
            StorageOperation::Create
        } else {
            StorageOperation::Open
        };
        let (inode, epoch) = self.perform(operation, |state, _| {
            let inode = if mode == OpenMode::Create {
                let (parent, name) = state.parent(path)?;
                if let Some(&inode) = state.directory(parent)?.get(&name) {
                    match &mut state.inodes[inode] {
                        Inode::File { buffered, .. } => buffered.clear(),
                        Inode::Directory { .. } => {
                            return Err(invalid("cannot truncate directory"));
                        }
                    }
                    inode
                } else {
                    let inode = state.inodes.len();
                    state.inodes.push(Inode::File {
                        buffered: Vec::new(),
                        stable: Vec::new(),
                    });
                    state.directory_mut(parent)?.insert(name, inode);
                    inode
                }
            } else {
                state.lookup(path)?
            };
            Ok((inode, state.epoch))
        })?;
        Ok(SimFile {
            storage: self.clone(),
            inode,
            epoch,
        })
    }

    async fn create_directories(&self, path: &Path) -> io::Result<()> {
        self.perform(StorageOperation::CreateDirectory, |state, _| {
            let mut parent = 0;
            for name in components(path)? {
                parent = if let Some(&inode) = state.directory(parent)?.get(&name) {
                    inode
                } else {
                    let inode = state.inodes.len();
                    state.inodes.push(Inode::directory());
                    state.directory_mut(parent)?.insert(name, inode);
                    inode
                };
                state.directory(parent)?;
            }
            Ok(())
        })
    }

    async fn sync_directory(&self, path: &Path) -> io::Result<()> {
        self.perform(StorageOperation::DirectorySync, |state, _| {
            let inode = state.lookup(path)?;
            match &mut state.inodes[inode] {
                Inode::Directory { entries, stable } => stable.clone_from(entries),
                Inode::File { .. } => return Err(invalid("directory sync on a file")),
            }
            Ok(())
        })
    }

    async fn rename(&self, source: &Path, target: &Path) -> io::Result<()> {
        self.perform(StorageOperation::Rename, |state, _| {
            let (parent, name) = state.parent(source)?;
            let (target_parent, target_name) = state.parent(target)?;
            let inode = *state.directory(parent)?.get(&name).ok_or_else(missing)?;
            if let Some(&target_inode) = state.directory(target_parent)?.get(&target_name) {
                if inode == target_inode {
                    return Ok(());
                }
                match (&state.inodes[inode], &state.inodes[target_inode]) {
                    (Inode::Directory { .. }, Inode::Directory { entries, .. })
                        if !entries.is_empty() =>
                    {
                        return Err(io::Error::from(io::ErrorKind::DirectoryNotEmpty));
                    }
                    (Inode::File { .. }, Inode::Directory { .. }) => {
                        return Err(io::Error::from(io::ErrorKind::IsADirectory));
                    }
                    (Inode::Directory { .. }, Inode::File { .. }) => {
                        return Err(io::Error::from(io::ErrorKind::NotADirectory));
                    }
                    _ => {}
                }
            }
            state.directory_mut(parent)?.remove(&name);
            state
                .directory_mut(target_parent)?
                .insert(target_name, inode);
            Ok(())
        })
    }

    async fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.perform(StorageOperation::Unlink, |state, _| {
            let (parent, name) = state.parent(path)?;
            state
                .directory_mut(parent)?
                .remove(&name)
                .ok_or_else(missing)?;
            Ok(())
        })
    }

    async fn hard_link(&self, source: &Path, target: &Path) -> io::Result<()> {
        self.perform(StorageOperation::Link, |state, _| {
            let inode = state.lookup(source)?;
            let (parent, name) = state.parent(target)?;
            if state.directory(parent)?.contains_key(&name) {
                return Err(io::Error::from(io::ErrorKind::AlreadyExists));
            }
            state.directory_mut(parent)?.insert(name, inode);
            Ok(())
        })
    }

    async fn exists(&self, path: &Path) -> io::Result<bool> {
        self.perform(StorageOperation::Exists, |state, _| {
            match state.lookup(path) {
                Ok(_) => Ok(true),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
                Err(error) => Err(error),
            }
        })
    }

    async fn entries(&self, path: &Path) -> io::Result<Vec<StorageEntry>> {
        self.perform(StorageOperation::List, |state, _| {
            let inode = state.lookup(path)?;
            Ok(state
                .directory(inode)?
                .iter()
                .map(|(name, &child)| StorageEntry {
                    name: name.clone(),
                    directory: matches!(state.inodes[child], Inode::Directory { .. }),
                })
                .collect())
        })
    }

    async fn remove_tree(&self, path: &Path) -> io::Result<()> {
        let mut pending = vec![(path.to_path_buf(), false)];
        while let Some((path, visited)) = pending.pop() {
            let children = self.perform(StorageOperation::RemoveTree, |state, _| {
                let inode = match state.lookup(&path) {
                    Ok(inode) => inode,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
                    Err(error) => return Err(error),
                };
                if !visited
                    && let Inode::Directory { entries, .. } = &state.inodes[inode]
                    && !entries.is_empty()
                {
                    return Ok(entries.keys().map(|name| path.join(name)).collect());
                }
                let (parent, name) = state.parent(&path)?;
                state.directory_mut(parent)?.remove(&name);
                Ok(Vec::new())
            })?;
            if !children.is_empty() {
                pending.push((path, true));
                pending.extend(children.into_iter().map(|path| (path, false)));
            }
        }
        Ok(())
    }
}

impl DurableFile for SimFile {
    async fn read(&self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        self.storage.perform(StorageOperation::Read, |state, _| {
            let buffered = state.file(self.inode, self.epoch)?;
            let offset = usize::try_from(offset).map_err(|_| invalid("offset overflow"))?;
            let end = offset
                .checked_add(length)
                .ok_or_else(|| invalid("read overflow"))?;
            buffered
                .get(offset..end)
                .map(<[u8]>::to_vec)
                .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))
        })
    }

    async fn write(&mut self, offset: u64, bytes: Vec<u8>) -> io::Result<()> {
        self.write_chunks(offset, std::iter::once(bytes.as_slice()))
            .await
    }

    async fn write_frozen(&mut self, offset: u64, bytes: Frozen<4096>) -> io::Result<()> {
        self.write_chunks(offset, std::iter::once(bytes.as_slice()))
            .await
    }

    async fn write_frozen_vectored(
        &mut self,
        offset: u64,
        buffers: Vec<Frozen<4096>>,
    ) -> io::Result<()> {
        self.write_chunks(offset, buffers.iter().map(Frozen::as_slice))
            .await
    }

    async fn length(&self) -> io::Result<u64> {
        self.storage.perform(StorageOperation::Length, |state, _| {
            Ok(state.file(self.inode, self.epoch)?.len() as u64)
        })
    }

    async fn truncate(&self, length: u64) -> io::Result<()> {
        self.storage
            .perform(StorageOperation::Truncate, |state, _| {
                let length = usize::try_from(length).map_err(|_| invalid("length overflow"))?;
                state.file_mut(self.inode, self.epoch)?.resize(length, 0);
                Ok(())
            })
    }

    async fn sync(&self) -> io::Result<()> {
        self.storage.wait_for(StorageOperation::FileSync).await;
        self.storage
            .perform(StorageOperation::FileSync, |state, _| {
                state.file(self.inode, self.epoch)?;
                if let Inode::File { buffered, stable } = &mut state.inodes[self.inode] {
                    stable.clone_from(buffered);
                }
                Ok(())
            })
    }
}

impl SimFile {
    async fn write_chunks<'a>(
        &self,
        offset: u64,
        chunks: impl Iterator<Item = &'a [u8]> + Clone,
    ) -> io::Result<()> {
        let length = chunks.clone().try_fold(0usize, |length, chunk| {
            length
                .checked_add(chunk.len())
                .ok_or_else(|| invalid("write overflow"))
        })?;
        self.storage.wait_for(StorageOperation::Write).await;
        self.storage
            .perform(StorageOperation::Write, |state, torn| {
                let buffered = state.file_mut(self.inode, self.epoch)?;
                let offset = usize::try_from(offset).map_err(|_| invalid("offset overflow"))?;
                let length = if torn { length / 2 } else { length };
                let end = offset
                    .checked_add(length)
                    .ok_or_else(|| invalid("write overflow"))?;
                if end > buffered.len() {
                    buffered.resize(end, 0);
                }
                let mut position = offset;
                for chunk in chunks {
                    let written = chunk.len().min(end - position);
                    buffered[position..position + written].copy_from_slice(&chunk[..written]);
                    position += written;
                    if position == end {
                        break;
                    }
                }
                *state.written_bytes.entry(self.inode).or_default() += length;
                Ok(())
            })
    }
}

impl Default for State {
    fn default() -> Self {
        Self {
            inodes: vec![Inode::directory()],
            epoch: 0,
            trace: Vec::new(),
            written_bytes: BTreeMap::new(),
            fault: None,
            paused: None,
            waiters: Vec::new(),
        }
    }
}

impl State {
    fn lookup(&self, path: &Path) -> io::Result<usize> {
        let mut inode = 0;
        for name in components(path)? {
            inode = *self.directory(inode)?.get(&name).ok_or_else(missing)?;
        }
        Ok(inode)
    }

    fn parent(&self, path: &Path) -> io::Result<(usize, OsString)> {
        let mut names = components(path)?;
        let name = names.pop().ok_or_else(|| invalid("root has no parent"))?;
        let mut inode = 0;
        for name in names {
            inode = *self.directory(inode)?.get(&name).ok_or_else(missing)?;
        }
        Ok((inode, name))
    }

    fn directory(&self, inode: usize) -> io::Result<&BTreeMap<OsString, usize>> {
        match &self.inodes[inode] {
            Inode::Directory { entries, .. } => Ok(entries),
            Inode::File { .. } => Err(invalid("not a directory")),
        }
    }

    fn directory_mut(&mut self, inode: usize) -> io::Result<&mut BTreeMap<OsString, usize>> {
        match &mut self.inodes[inode] {
            Inode::Directory { entries, .. } => Ok(entries),
            Inode::File { .. } => Err(invalid("not a directory")),
        }
    }

    fn file(&self, inode: usize, epoch: u64) -> io::Result<&Vec<u8>> {
        if epoch != self.epoch {
            return Err(invalid("stale process file handle"));
        }
        match &self.inodes[inode] {
            Inode::File { buffered, .. } => Ok(buffered),
            Inode::Directory { .. } => Err(invalid("not a file")),
        }
    }

    fn file_mut(&mut self, inode: usize, epoch: u64) -> io::Result<&mut Vec<u8>> {
        if epoch != self.epoch {
            return Err(invalid("stale process file handle"));
        }
        match &mut self.inodes[inode] {
            Inode::File { buffered, .. } => Ok(buffered),
            Inode::Directory { .. } => Err(invalid("not a file")),
        }
    }
}

impl Inode {
    const fn directory() -> Self {
        Self::Directory {
            entries: BTreeMap::new(),
            stable: BTreeMap::new(),
        }
    }
}

fn components(path: &Path) -> io::Result<Vec<OsString>> {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(name) => Some(Ok(name.to_os_string())),
            Component::RootDir | Component::CurDir => None,
            _ => Some(Err(invalid("unsupported modeled path"))),
        })
        .collect()
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn missing() -> io::Error {
    io::Error::from(io::ErrorKind::NotFound)
}

#[cfg(test)]
mod tests;
