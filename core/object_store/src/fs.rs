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

mod header;
mod list;
#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet};
use std::io;
use std::mem;
use std::ops::{Deref, Range};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;
use compio::fs::{File, OpenOptions};
use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use futures::StreamExt;
use futures::channel::oneshot;
use futures::lock::Mutex;

use crate::error::ObjectStoreError;
use crate::store::{
    DeleteFailure, DeleteOutcome, ListPage, ListRequest, MAX_KEY_SEGMENT_LEN, ObjectMeta,
    ObjectRange, ObjectStore, PutBody, PutMode, PutOptions, validate_delete, validate_key,
    validate_metadata, validate_prefix, validate_range,
};

use header::{Etag, Header, HeaderError, MAX_HEADER_LEN, PendingHeader};
use list::{ListCache, Walk};

/// The most directories whose sync a store remembers. Past it, the store
/// forgets them all and syncs each one again.
const MAX_DURABLE_DIRECTORIES: usize = 65_536;

/// A put syncs its scratch file each time it wrote this many body bytes
/// since the last sync.
const SYNC_INTERVAL: u64 = 8 * 1024 * 1024;

// A scratch name adds two dots and the 32 hex digits of an etag to the last
// segment of a key, and a file name holds 255 bytes.
const _: () = assert!(MAX_KEY_SEGMENT_LEN + 34 <= 255);

/// An [`ObjectStore`] in a directory, for tests and for archives on a local
/// or shared filesystem.
///
/// - Each object is the file `root/key`: a header with the metadata, the
///   CRC32C and the etag, then the body.
/// - A put writes a scratch file, a dot-name next to the object, and syncs
///   it. Then it renames it over the object, or links it for
///   [`PutMode::IfAbsent`]. A crash never leaves a torn object.
/// - A head or a read of a file without a valid header fails with
///   [`ObjectStoreError::Malformed`]. Reads do not check the body, so a
///   damaged body reads back as it is. A head gives the CRC32C of the put.
/// - A listing skips dot-names and shows every other file, so a delete can
///   find a damaged one. A file without a valid header shows its file length
///   and an empty etag. A listing removes the scratch files of crashed puts
///   an hour after their last write.
/// - Directories stay after their objects go.
/// - `root` must exist. Without it, calls fail with
///   [`ObjectStoreError::BucketNotFound`].
/// - `root` must be the mounted store itself. A failed mount leaves an empty
///   mountpoint that looks like an empty bucket: puts land on the local
///   disk, and reads find nothing.
/// - Calls go through `io_uring` and never block the shard. A listing reads
///   directories on a thread of its own, one listing at a time.
/// - Calls run on a compio runtime, and panic outside one.
#[derive(Debug)]
pub struct FsStore {
    root: PathBuf,
    /// Held by the listing that runs, with what the last page read.
    listing: Arc<Mutex<ListCache>>,
    /// Keys of the directories whose entries a sync made durable.
    durable: RefCell<HashSet<String>>,
    leftovers: RefCell<Leftovers>,
}

/// Files that dropped calls left open, and scratch files that they left on
/// disk. `Drop` cannot wait, and it never spawns, because it can run while
/// the runtime shuts down, so the next call hands them to a task.
#[derive(Debug, Default)]
struct Leftovers {
    files: Vec<File>,
    scratch: Vec<PathBuf>,
}

/// An object's open file, with its header and first bytes.
struct OpenObject<'a> {
    file: ReadFile<'a>,
    header: Header,
    /// The first bytes of the file: the header, then the start of the body.
    start: Bytes,
    last_modified: SystemTime,
}

/// A file opened for a read. Dropped, it joins the leftovers, whose task
/// closes it through `io_uring`, because a plain close on NFS can wait for
/// the server.
struct ReadFile<'a> {
    file: File,
    leftovers: &'a RefCell<Leftovers>,
}

/// A put's scratch file. Dropped while armed, it joins the leftovers, as a
/// dropped put cannot wait. Closes go through `io_uring`, because a plain
/// close of a written file on NFS flushes it and blocks.
struct Scratch<'a> {
    path: PathBuf,
    file: Option<File>,
    /// Whether the scratch file can still be on disk.
    armed: bool,
    leftovers: &'a RefCell<Leftovers>,
}

impl FsStore {
    /// A store in `root`, an existing directory.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            listing: Arc::default(),
            durable: RefCell::default(),
            leftovers: RefCell::default(),
        }
    }

    /// Hands what dropped calls left to a task of its own.
    fn clean_up(&self) {
        let leftovers = mem::take(&mut *self.leftovers.borrow_mut());
        if !leftovers.is_empty() {
            compio::runtime::spawn(leftovers.remove()).detach();
        }
    }

    /// Writes the object into its scratch file and closes it, after making
    /// the directories of `key` durable, so only the commit is left.
    async fn write_scratch(
        &self,
        key: &str,
        scratch: &mut Scratch<'_>,
        body: PutBody,
        options: &PutOptions,
        etag: Etag,
    ) -> Result<(), ObjectStoreError> {
        let header = PendingHeader::new(&options.metadata);
        let file = self.create_scratch(key, &scratch.path).await?;
        let file = scratch.file.insert(file);
        self.sync_directories(key).await?;
        write_object(key, file, body, &header, options.crc32c, etag).await?;
        // A failed close fails the put before it publishes anything.
        scratch
            .close()
            .await
            .map_err(|error| io_error(key, "closing", &error))
    }

    /// Opens a new scratch file, after creating the directories that `key`
    /// needs.
    async fn create_scratch(&self, key: &str, scratch: &Path) -> Result<File, ObjectStoreError> {
        let opened = match open_new(scratch).await {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.create_directories(key).await?;
                open_new(scratch).await
            }
            opened => opened,
        };
        match opened {
            Ok(file) => Ok(file),
            Err(error) => Err(self.creation_error(key, &error).await),
        }
    }

    /// Tells a missing root and a key that clashes with another from other
    /// failures to create a scratch file.
    async fn creation_error(&self, key: &str, error: &io::Error) -> ObjectStoreError {
        if is_missing(error) && self.root_is_missing().await {
            bucket_not_found(&self.root)
        } else if error.kind() == io::ErrorKind::NotADirectory {
            clash(key)
        } else {
            io_error(key, "creating", error)
        }
    }

    /// Creates the directories between the root and `key`.
    async fn create_directories(&self, key: &str) -> Result<(), ObjectStoreError> {
        for (end, _) in key.match_indices('/') {
            let directory = &key[..end];
            match compio::fs::create_dir(self.root.join(directory)).await {
                // A new entry needs a sync, even where an older directory
                // of the same name had one.
                Ok(()) => {
                    self.durable.borrow_mut().remove(directory);
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                // The next open fails too, and tells a missing root from a clash.
                Err(error) if is_missing(&error) => return Ok(()),
                Err(error) => return Err(io_error(key, "creating a directory of", &error)),
            }
        }
        Ok(())
    }

    /// Syncs the parent of each directory of `key`, once per store. A put
    /// that created a directory can stop before its sync, so a directory in
    /// place is not yet durable.
    async fn sync_directories(&self, key: &str) -> Result<(), ObjectStoreError> {
        for (end, _) in key.match_indices('/') {
            let directory = &key[..end];
            if self.durable.borrow().contains(directory) {
                continue;
            }
            let parent = match directory.rsplit_once('/') {
                Some((parent, _)) => self.root.join(parent),
                None => self.root.clone(),
            };
            sync_dir(&parent)
                .await
                .map_err(|error| io_error(key, "syncing a directory of", &error))?;
            let mut durable = self.durable.borrow_mut();
            if durable.len() >= MAX_DURABLE_DIRECTORIES {
                durable.clear();
            }
            durable.insert(directory.to_owned());
        }
        Ok(())
    }

    /// Links the scratch file as the object, unless something is under the
    /// key.
    async fn link(&self, key: &str, scratch: &Path, target: &Path) -> Result<(), ObjectStoreError> {
        let Err(error) = compio::fs::hard_link(scratch, target).await else {
            note(|| format!("link to {key}"));
            return Ok(());
        };
        match error.kind() {
            io::ErrorKind::AlreadyExists => Err(self.taken(key, target).await),
            // The scratch file is ours, so a refused link means a filesystem
            // without hard links.
            io::ErrorKind::PermissionDenied | io::ErrorKind::Unsupported => {
                Err(ObjectStoreError::Unsupported(format!(
                    "IfAbsent in {}, whose filesystem refuses hard links: {error}",
                    self.root.display()
                )))
            }
            _ => Err(io_error(key, "linking", &error)),
        }
    }

    /// Why an `IfAbsent` put cannot take `target`, which exists.
    async fn taken(&self, key: &str, target: &Path) -> ObjectStoreError {
        // As for an overwrite, a directory under the key is a clash.
        if compio::fs::metadata(target)
            .await
            .is_ok_and(|metadata| metadata.is_dir())
        {
            return clash(key);
        }
        // The put that linked the object can have stopped before its sync,
        // and the caller can take the object for its own, so the object must
        // be durable first.
        match sync_dir(self.directory_of(target)).await {
            Ok(()) => ObjectStoreError::PreconditionFailed {
                key: key.to_owned(),
            },
            Err(error) => unavailable(key, "syncing the directory of", &error),
        }
    }

    async fn open_object(&self, key: &str) -> Result<OpenObject<'_>, ObjectStoreError> {
        let file = match File::open(self.root.join(key)).await {
            Ok(file) => ReadFile {
                file,
                leftovers: &self.leftovers,
            },
            Err(error) if is_missing(&error) => return Err(self.not_found(key).await),
            Err(error) => return Err(io_error(key, "opening", &error)),
        };
        let metadata = file
            .metadata()
            .await
            .map_err(|error| io_error(key, "reading", &error))?;
        // A directory under a key holds no object.
        if !metadata.is_file() {
            return Err(self.not_found(key).await);
        }
        let file_len = metadata.len();
        let start_len =
            usize::try_from(file_len).map_or(MAX_HEADER_LEN, |len| len.min(MAX_HEADER_LEN));
        let (read, start) = file
            .read_exact_at(Vec::with_capacity(start_len), 0)
            .await
            .into();
        read.map_err(|error| io_error(key, "reading", &error))?;
        let header = Header::try_from(start.as_slice())
            .and_then(|header| header.check_file_len(file_len).map(|()| header))
            .map_err(|error| malformed(key, &error))?;
        let last_modified = metadata
            .modified()
            .map_err(|error| io_error(key, "reading", &error))?;
        Ok(OpenObject {
            file,
            header,
            start: Bytes::from(start),
            last_modified,
        })
    }

    /// `NotFound` for `key`, or `BucketNotFound` when the root is gone.
    async fn not_found(&self, key: &str) -> ObjectStoreError {
        if self.root_is_missing().await {
            bucket_not_found(&self.root)
        } else {
            ObjectStoreError::NotFound {
                key: key.to_owned(),
            }
        }
    }

    async fn root_is_missing(&self) -> bool {
        match compio::fs::metadata(&self.root).await {
            Ok(metadata) => !metadata.is_dir(),
            Err(error) => is_missing(&error),
        }
    }

    /// The directory that holds `path`, a path under the root.
    fn directory_of<'a>(&'a self, path: &'a Path) -> &'a Path {
        path.parent().unwrap_or(&self.root)
    }

    async fn read_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<ObjectRange, ObjectStoreError> {
        let object = self.open_object(key).await?;
        let size = object.header.body_len;
        if range.start >= size {
            return Err(ObjectStoreError::InvalidRange {
                key: key.to_owned(),
                start: range.start,
                end: range.end,
            });
        }
        let bytes = object.read(key, range.start..range.end.min(size)).await?;
        Ok(ObjectRange {
            bytes,
            etag: object.header.etag.to_string(),
            size,
        })
    }
}

impl ObjectStore for FsStore {
    async fn put(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        validate_key(key)?;
        validate_metadata(&options.metadata)?;
        self.clean_up();
        let target = self.root.join(key);
        let etag = Etag::random();
        let mut scratch = Scratch::new(&self.leftovers, &target, key, etag);
        if let Err(error) = self
            .write_scratch(key, &mut scratch, body, options, etag)
            .await
        {
            scratch.remove().await;
            return Err(error);
        }
        let published = match options.mode {
            PutMode::Overwrite => replace(key, &scratch.path, &target).await,
            PutMode::IfAbsent => self.link(key, &scratch.path, &target).await,
        };
        // A rename takes the scratch name, and a link leaves it.
        if published.is_ok() && options.mode == PutMode::Overwrite {
            scratch.disarm();
        } else {
            scratch.remove().await;
        }
        published?;
        // The object is in place, so a retry can meet it, and no error from
        // here on is permanent.
        sync_dir(self.directory_of(&target))
            .await
            .map_err(|error| unavailable(key, "syncing the directory of", &error))
    }

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<ObjectRange, ObjectStoreError> {
        validate_key(key)?;
        validate_range(&range)?;
        let read = self.read_range(key, range).await;
        // The read left its file for the cleanup to close.
        self.clean_up();
        read
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
        validate_key(key)?;
        let head = self.open_object(key).await.map(
            |OpenObject {
                 header,
                 last_modified,
                 ..
             }| ObjectMeta {
                size: header.body_len,
                etag: header.etag.to_string(),
                last_modified,
                metadata: header.metadata,
                crc32c: Some(header.crc32c),
            },
        );
        // The read left its file for the cleanup to close.
        self.clean_up();
        head
    }

    async fn delete(&self, keys: &[String]) -> Result<DeleteOutcome, ObjectStoreError> {
        validate_delete(keys)?;
        self.clean_up();
        let mut outcome = DeleteOutcome::default();
        // The keys gone from each directory, which a sync makes durable. A
        // key already absent can wait for the sync of an earlier unlink that
        // lost its answer, so its directory gets one too.
        let mut gone: BTreeMap<PathBuf, Vec<&str>> = BTreeMap::new();
        let mut absent = false;
        for key in keys {
            if let Err(error) = validate_key(key) {
                outcome.failed.push(DeleteFailure {
                    key: key.clone(),
                    error,
                });
                continue;
            }
            let path = self.root.join(key);
            match remove_object(&path).await {
                Ok(removed) => {
                    absent |= !removed;
                    let directory = self.directory_of(&path).to_path_buf();
                    gone.entry(directory).or_default().push(key);
                }
                Err(error) => outcome.failed.push(DeleteFailure {
                    key: key.clone(),
                    error: io_error(key, "deleting", &error),
                }),
            }
        }
        if absent && self.root_is_missing().await {
            return Err(bucket_not_found(&self.root));
        }
        for (directory, keys) in gone {
            // A key without its directory is gone with it.
            if let Err(error) = sync_dir(&directory).await
                && !is_missing(&error)
            {
                outcome
                    .failed
                    .extend(keys.into_iter().map(|key| DeleteFailure {
                        key: key.to_owned(),
                        error: io_error(key, "syncing the directory of", &error),
                    }));
            }
        }
        Ok(outcome)
    }

    async fn list(&self, request: &ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
        validate_prefix(request.prefix)?;
        self.clean_up();
        let walk = Walk::new(&self.root, request);
        let mut cache = Arc::clone(&self.listing).lock_owned().await;
        on_thread(move || walk.page(&mut cache)).await
    }
}

impl OpenObject<'_> {
    /// The body's bytes in `range`, which lies inside the body.
    async fn read(&self, key: &str, range: Range<u64>) -> Result<Bytes, ObjectStoreError> {
        let start = self.header.len + range.start;
        let end = self.header.len + range.end;
        // A small object is whole in the bytes read with its header.
        if let (Ok(start), Ok(end)) = (usize::try_from(start), usize::try_from(end))
            && end <= self.start.len()
        {
            return Ok(self.start.slice(start..end));
        }
        let len = usize::try_from(end - start).map_err(|_| {
            ObjectStoreError::InvalidRequest(format!(
                "a range of {} bytes does not fit in memory",
                end - start
            ))
        })?;
        let (read, bytes) = self
            .file
            .read_exact_at(Vec::with_capacity(len), start)
            .await
            .into();
        read.map_err(|error| io_error(key, "reading", &error))?;
        Ok(Bytes::from(bytes))
    }
}

impl Deref for ReadFile<'_> {
    type Target = File;

    fn deref(&self) -> &File {
        &self.file
    }
}

impl Drop for ReadFile<'_> {
    fn drop(&mut self) {
        // The clone keeps the file open for the task that closes it.
        self.leftovers.borrow_mut().files.push(self.file.clone());
    }
}

impl<'a> Scratch<'a> {
    fn new(leftovers: &'a RefCell<Leftovers>, target: &Path, key: &str, etag: Etag) -> Self {
        let name = key.rsplit_once('/').map_or(key, |(_, name)| name);
        Self {
            path: target.with_file_name(format!(".{name}.{etag}")),
            file: None,
            armed: true,
            leftovers,
        }
    }

    async fn close(&mut self) -> io::Result<()> {
        match self.file.take() {
            Some(file) => file.close().await,
            None => Ok(()),
        }
    }

    fn disarm(mut self) {
        self.armed = false;
    }

    async fn remove(mut self) {
        // A leftover scratch file is never listed, so nothing waits on this.
        let _ = compio::fs::remove_file(&self.path).await;
        let _ = self.close().await;
        // Dropped before this line, it is still armed and joins the leftovers.
        self.armed = false;
    }
}

impl Drop for Scratch<'_> {
    fn drop(&mut self) {
        let mut leftovers = self.leftovers.borrow_mut();
        leftovers.files.extend(self.file.take());
        if self.armed {
            leftovers.scratch.push(mem::take(&mut self.path));
        }
    }
}

impl Leftovers {
    const fn is_empty(&self) -> bool {
        self.files.is_empty() && self.scratch.is_empty()
    }

    /// Removes the paths before it closes the files, as a close can wait for
    /// an operation that still holds its file.
    async fn remove(self) {
        for path in self.scratch {
            let _ = compio::fs::remove_file(path).await;
        }
        for file in self.files {
            let _ = file.close().await;
        }
    }
}

/// Writes the body after the header's room, then the header, and syncs the
/// file.
///
/// The body is synced every [`SYNC_INTERVAL`] bytes as it arrives, so the
/// last sync flushes little more than that. A caller that times each piece,
/// as the tier service does, then sees the pace of the disk, and a large
/// body never ends in one long sync after its last piece.
async fn write_object(
    key: &str,
    file: &File,
    body: PutBody,
    header: &PendingHeader,
    expected_crc32c: Option<u32>,
    etag: Etag,
) -> Result<(), ObjectStoreError> {
    let mut writer = file;
    let mut position = header.len();
    let mut checksum = 0;
    let mut unsynced = 0;
    let mut pieces = body.into_pieces();
    while let Some(piece) = pieces.next().await {
        let piece = piece.map_err(ObjectStoreError::Body)?;
        checksum = crc32c::crc32c_append(checksum, &piece);
        let len = piece.len() as u64;
        writer
            .write_all_at(piece, position)
            .await
            .0
            .map_err(|error| io_error(key, "writing", &error))?;
        position += len;
        unsynced += len;
        if unsynced >= SYNC_INTERVAL {
            file.sync_data()
                .await
                .map_err(|error| io_error(key, "syncing", &error))?;
            unsynced = 0;
        }
    }
    if expected_crc32c.is_some_and(|expected| expected != checksum) {
        return Err(ObjectStoreError::ChecksumMismatch {
            key: key.to_owned(),
        });
    }
    let body_len = position - header.len();
    writer
        .write_all_at(header.finish(body_len, checksum, etag), 0)
        .await
        .0
        .map_err(|error| io_error(key, "writing", &error))?;
    file.sync_all()
        .await
        .map_err(|error| io_error(key, "syncing", &error))?;
    note(|| format!("sync scratch of {key}"));
    Ok(())
}

/// Renames the scratch file over the object.
async fn replace(key: &str, scratch: &Path, target: &Path) -> Result<(), ObjectStoreError> {
    compio::fs::rename(scratch, target)
        .await
        .map_err(|error| match error.kind() {
            io::ErrorKind::IsADirectory | io::ErrorKind::DirectoryNotEmpty => clash(key),
            _ => io_error(key, "renaming", &error),
        })?;
    note(|| format!("rename to {key}"));
    Ok(())
}

/// Unlinks the object file at `path`. Returns `false` when nothing, or only a
/// directory, is under the key.
async fn remove_object(path: &Path) -> io::Result<bool> {
    match compio::fs::remove_file(path).await {
        Ok(()) => {
            note(|| format!("remove {}", path.display()));
            Ok(true)
        }
        Err(error) if is_missing(&error) || error.kind() == io::ErrorKind::IsADirectory => {
            Ok(false)
        }
        // macOS refuses to unlink a directory with EPERM.
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            match compio::fs::symlink_metadata(path).await {
                Ok(metadata) if metadata.is_dir() => Ok(false),
                _ => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

async fn open_new(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
}

/// Makes the entries of `directory` durable.
async fn sync_dir(directory: &Path) -> io::Result<()> {
    let handle = File::open(directory).await?;
    let synced = handle.sync_all().await;
    if synced.is_ok() {
        note(|| format!("sync dir {}", directory.display()));
    }
    synced.and(handle.close().await)
}

/// Notes a step that a put or a delete needs to be durable, so tests can
/// check that each step happens and in which order.
#[cfg(test)]
fn note(step: impl FnOnce() -> String) {
    tests::NOTED_STEPS.with_borrow_mut(|steps| steps.push(step()));
}

#[cfg(not(test))]
fn note(_step: impl FnOnce() -> String) {}

/// Runs `work` on a thread of its own, as shards have no blocking pool.
async fn on_thread<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, ObjectStoreError> + Send + 'static,
) -> Result<T, ObjectStoreError> {
    let (sender, receiver) = oneshot::channel();
    std::thread::Builder::new()
        .name("iggy-fs-list".to_owned())
        .spawn(move || {
            let _ = sender.send(work());
        })
        .map_err(|error| {
            ObjectStoreError::Unavailable(format!("starting a listing thread: {error}"))
        })?;
    receiver
        .await
        .map_err(|_| ObjectStoreError::Unavailable("the listing thread stopped".to_owned()))?
}

/// Whether `name` is a scratch name, `.{name}.{etag}`, as a put makes one.
fn is_scratch(name: &str) -> bool {
    name.strip_prefix('.')
        .and_then(|rest| rest.rsplit_once('.'))
        .is_some_and(|(object, etag)| {
            !object.is_empty()
                && etag.len() == 32
                && etag
                    .bytes()
                    .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        })
}

fn is_missing(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
    )
}

fn io_error(key: &str, action: &str, error: &io::Error) -> ObjectStoreError {
    let message = format!("{action} {key:?}: {error}");
    match error.kind() {
        // Both refuse every write until an operator steps in.
        io::ErrorKind::PermissionDenied | io::ErrorKind::ReadOnlyFilesystem => {
            ObjectStoreError::Auth(message)
        }
        io::ErrorKind::InvalidFilename | io::ErrorKind::FileTooLarge => {
            ObjectStoreError::InvalidRequest(message)
        }
        // The rest can pass on a retry. That includes a full disk or quota,
        // because deletes free space.
        _ => ObjectStoreError::Unavailable(message),
    }
}

/// An error that a retry can get past, whatever the I/O error.
fn unavailable(key: &str, action: &str, error: &io::Error) -> ObjectStoreError {
    ObjectStoreError::Unavailable(format!("{action} {key:?}: {error}"))
}

fn bucket_not_found(root: &Path) -> ObjectStoreError {
    ObjectStoreError::BucketNotFound(root.display().to_string())
}

fn clash(key: &str) -> ObjectStoreError {
    ObjectStoreError::InvalidRequest(format!(
        "{key:?} clashes with another key: no key can be the directory of another"
    ))
}

fn malformed(key: &str, error: &HeaderError) -> ObjectStoreError {
    ObjectStoreError::Malformed(format!("{key:?}: {error}"))
}
