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

//! The disk cache of the embed endpoints. It keeps every PNG and every embed page that the server
//! built, up to a size limit. A store past the limit deletes the files that readers used least
//! recently, until the cache is a tenth below the limit, so the stores after it don't each delete
//! a file. A file system can fill before the cache reaches its limit, so a store that finds the
//! disk full deletes the least recently used files too, and writes once more.
//!
//! The stamp of the server binary starts the name of every file. Another binary can draw other
//! charts or write other pages, so it never reads the files of an earlier one. Nothing reads those
//! files again, so they are the least recently used, and the next eviction deletes them first.

use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File};
use std::future::Future;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::SystemTime;

use actix_web::{rt, web};
use tempfile::NamedTempFile;
use tokio::sync::Mutex as AsyncMutex;
use tracing::{info, warn};

/// The directory of the embed cache in `--cache-dir`.
pub const EMBED_CACHE_DIR: &str = "embed";
pub const DEFAULT_MAX_GIB: u64 = 10;
pub const BYTES_PER_GIB: u64 = 1 << 30;
/// The name start of the temp files of a store. A crash during a store leaves one behind.
const TEMP_FILE_PREFIX: &str = ".tmp";

/// Writes `file` under the name in the directory.
type WriteFile = fn(&Path, &str, &[u8]) -> io::Result<()>;

/// The stamp of the running server binary, for [`EmbedCache::open`].
pub fn build_stamp() -> io::Result<FileStamp> {
    FileStamp::of_file(&std::env::current_exe()?)
}

/// A stamp that only this start of the server has, for a server that can't read its binary. It
/// never serves a file that another binary built, but a restart builds every file again.
pub fn start_stamp() -> FileStamp {
    let since_epoch = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let mut hasher = blake3::Hasher::new();
    hasher.update(&since_epoch.as_nanos().to_le_bytes());
    hasher.update(&std::process::id().to_le_bytes());
    FileStamp::from(hasher.finalize())
}

/// The first 16 hexadecimal digits of a BLAKE3 hash, for the names of the cached files. The stamp of
/// a file changes with its bytes, so the cache never serves a file that it built from the old bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileStamp(u64);

impl FileStamp {
    pub fn of_bytes(bytes: &[u8]) -> Self {
        Self::from(blake3::hash(bytes))
    }

    pub fn of_file(path: &Path) -> io::Result<Self> {
        let mut hasher = blake3::Hasher::new();
        hasher.update_reader(File::open(path)?)?;
        Ok(Self::from(hasher.finalize()))
    }
}

impl From<blake3::Hash> for FileStamp {
    fn from(hash: blake3::Hash) -> Self {
        let mut first = [0; 8];
        first.copy_from_slice(&hash.as_bytes()[..8]);
        Self(u64::from_be_bytes(first))
    }
}

impl fmt::Display for FileStamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// The name on the disk of the cached file `name` of the binary with the stamp `build`.
pub fn stamped_name(build: FileStamp, name: &str) -> String {
    format!("{build}_{name}")
}

pub struct EmbedCache {
    dir: PathBuf,
    max_bytes: u64,
    /// The stamp of the server binary. See [`stamped_name`].
    build: FileStamp,
    index: Mutex<Index>,
    /// The build turn of each file name that a request holds or waits for. See
    /// [`Self::get_or_build`].
    turns: Mutex<HashMap<String, Weak<AsyncMutex<()>>>>,
    /// [`store_file`], except in a test that fills the disk.
    write_file: WriteFile,
    #[cfg(test)]
    _temp_dir: Option<tempfile::TempDir>,
}

impl EmbedCache {
    /// The cache in `dir`, with the files that an earlier server left there in the order of their
    /// modification times. It deletes the temp files of stores that did not finish, and the files
    /// past `max_bytes`. `build` is the stamp of the server binary.
    pub fn open(dir: PathBuf, max_bytes: u64, build: FileStamp) -> io::Result<Self> {
        Self::open_with_writer(dir, max_bytes, build, store_file)
    }

    fn open_with_writer(
        dir: PathBuf,
        max_bytes: u64,
        build: FileStamp,
        write_file: WriteFile,
    ) -> io::Result<Self> {
        let index = Index::scan(&dir)?;
        let cache = Self {
            dir,
            max_bytes,
            build,
            index: Mutex::new(index),
            turns: Mutex::new(HashMap::new()),
            write_file,
            #[cfg(test)]
            _temp_dir: None,
        };
        remove_files(&cache.dir, &cache.evict());
        let index = cache.index();
        info!(
            "Embed cache: {} files, {} of {} bytes in {}, build stamp {}",
            index.files.len(),
            index.total_bytes,
            cache.max_bytes,
            cache.dir.display(),
            cache.build
        );
        drop(index);
        Ok(cache)
    }

    /// The cached file `name`, else the bytes of `build`, which the cache then keeps.
    ///
    /// Requests that miss the same file take turns to build it, and each turn starts with a second
    /// read. A request that missed the file while another request built it reads the stored file,
    /// so the one render thread does not draw the same chart again. Requests for other files don't
    /// wait.
    ///
    /// The build and the store run in a task that holds the turn. actix drops the request after a
    /// reset of its connection, and the task goes on, so the next request for the file reads it.
    ///
    /// The file on the disk has the name of [`stamped_name`].
    pub async fn get_or_build<E>(
        self: &Arc<Self>,
        name: &str,
        build: impl Future<Output = Result<web::Bytes, E>> + 'static,
    ) -> Result<web::Bytes, E>
    where
        E: From<io::Error> + 'static,
    {
        let name = stamped_name(self.build, name);
        if let Some(file) = self.read(&name).await {
            return Ok(file);
        }
        let turn = self.turn(&name).lock_owned().await;
        if let Some(file) = self.read(&name).await {
            return Ok(file);
        }
        let cache = Arc::clone(self);
        rt::spawn(async move {
            let _turn = turn;
            let file = build.await?;
            cache.store(&name, file.clone()).await;
            Ok::<_, E>(file)
        })
        .await
        .unwrap_or_else(|e| Err(io::Error::other(e).into()))
    }

    /// The build turn of `name`, which every request that holds or waits for it shares. The map
    /// keeps only the turns that a request still has.
    fn turn(&self, name: &str) -> Arc<AsyncMutex<()>> {
        let mut turns = self.turns.lock().unwrap_or_else(PoisonError::into_inner);
        turns.retain(|_, turn| turn.strong_count() > 0);
        if let Some(turn) = turns.get(name).and_then(Weak::upgrade) {
            return turn;
        }
        let turn = Arc::new(AsyncMutex::new(()));
        turns.insert(name.to_owned(), Arc::downgrade(&turn));
        turn
    }

    /// `None` for a file that the cache doesn't have, and for one that can't be read, so the caller
    /// builds it again.
    async fn read(&self, name: &str) -> Option<web::Bytes> {
        let path = self.dir.join(name);
        let read = web::block(move || fs::read(path))
            .await
            .unwrap_or_else(|e| Err(io::Error::other(e)));
        match read {
            Ok(file) => {
                self.index().touch(name, file.len() as u64);
                Some(web::Bytes::from(file))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                self.index().forget(name);
                None
            }
            Err(e) => {
                warn!(
                    "Failed to read cached embed file {}: {}",
                    self.dir.join(name).display(),
                    e
                );
                None
            }
        }
    }

    /// A failed store is only logged, so the next request builds the file again. A store that finds
    /// the disk full deletes the least recently used files, and writes once more.
    async fn store(&self, name: &str, file: web::Bytes) {
        let bytes = file.len() as u64;
        let mut stored = self.write(name, file.clone()).await;
        if stored.as_ref().is_err_and(is_disk_full) {
            let evicted = self.make_room(bytes);
            if !evicted.is_empty() {
                info!(
                    "The disk of the embed cache is full, deleting the {} least recently used files",
                    evicted.len()
                );
                self.remove(evicted).await;
                stored = self.write(name, file).await;
            }
        }
        if let Err(e) = stored {
            warn!(
                "Failed to cache embed file {}: {}",
                self.dir.join(name).display(),
                e
            );
            return;
        }
        self.index().touch(name, bytes);
        let evicted = self.evict();
        if evicted.is_empty() {
            return;
        }
        info!(
            "Embed cache past {} bytes, deleting the {} least recently used files",
            self.max_bytes,
            evicted.len()
        );
        self.remove(evicted).await;
    }

    async fn write(&self, name: &str, file: web::Bytes) -> io::Result<()> {
        let dir = self.dir.clone();
        let name = name.to_owned();
        let write_file = self.write_file;
        web::block(move || write_file(&dir, &name, &file))
            .await
            .unwrap_or_else(|e| Err(io::Error::other(e)))
    }

    async fn remove(&self, names: Vec<String>) {
        let dir = self.dir.clone();
        if let Err(e) = web::block(move || remove_files(&dir, &names)).await {
            warn!("Failed to delete files of the embed cache: {}", e);
        }
    }

    /// The names that leave the index, empty while the cache is within its limit.
    fn evict(&self) -> Vec<String> {
        let mut index = self.index();
        if index.total_bytes <= self.max_bytes {
            return Vec::new();
        }
        index.evict_to(self.max_bytes - self.max_bytes / 10)
    }

    /// The names that leave the index to free a tenth of the cache, or room for a file of `bytes`
    /// if that is more. Empty for an empty cache.
    fn make_room(&self, bytes: u64) -> Vec<String> {
        let mut index = self.index();
        let target = index
            .total_bytes
            .saturating_sub((index.total_bytes / 10).max(bytes));
        index.evict_to(target)
    }

    /// The index holds no invariant that a panic during an update could break, so a poisoned lock
    /// is still usable.
    fn index(&self) -> MutexGuard<'_, Index> {
        self.index.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The size and the last use of each cached file. A counter of uses orders them, so the order
/// needs no clock. A file that an older server left, or that a reader asked for after the index
/// lost it, joins at its next read.
#[derive(Debug, Default)]
struct Index {
    files: HashMap<String, Entry>,
    total_bytes: u64,
    uses: u64,
}

#[derive(Debug, Clone, Copy)]
struct Entry {
    bytes: u64,
    last_use: u64,
}

impl Index {
    /// Only a directory that can't be listed fails the scan. An entry that fails, such as a file
    /// that another process deletes during the scan, stays out of the index until its next read.
    fn scan(dir: &Path) -> io::Result<Self> {
        let entries = match fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(e) => return Err(e),
        };
        let mut files = Vec::new();
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    warn!(
                        "Failed to read an entry of the embed cache {}: {}",
                        dir.display(),
                        e
                    );
                    continue;
                }
            };
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(e) => {
                    warn!(
                        "Failed to read the metadata of cached embed file {}: {}",
                        entry.path().display(),
                        e
                    );
                    continue;
                }
            };
            if !metadata.is_file() {
                continue;
            }
            if name.starts_with(TEMP_FILE_PREFIX) {
                if let Err(e) = fs::remove_file(entry.path()) {
                    warn!(
                        "Failed to delete the temp file {}: {}",
                        entry.path().display(),
                        e
                    );
                }
                continue;
            }
            let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            files.push((modified, name, metadata.len()));
        }
        files.sort_unstable();
        let mut index = Self::default();
        for (_, name, bytes) in files {
            index.touch(&name, bytes);
        }
        Ok(index)
    }

    fn touch(&mut self, name: &str, bytes: u64) {
        self.uses += 1;
        let entry = Entry {
            bytes,
            last_use: self.uses,
        };
        if let Some(old) = self.files.insert(name.to_owned(), entry) {
            self.total_bytes = self.total_bytes.saturating_sub(old.bytes);
        }
        self.total_bytes += bytes;
    }

    fn forget(&mut self, name: &str) {
        if let Some(old) = self.files.remove(name) {
            self.total_bytes = self.total_bytes.saturating_sub(old.bytes);
        }
    }

    /// Takes the least recently used files out of the index until it holds `target` bytes at most,
    /// and gives their names.
    fn evict_to(&mut self, target: u64) -> Vec<String> {
        let mut by_use: Vec<(u64, String)> = self
            .files
            .iter()
            .map(|(name, entry)| (entry.last_use, name.clone()))
            .collect();
        by_use.sort_unstable();
        let mut evicted = Vec::new();
        for (_, name) in by_use {
            if self.total_bytes <= target {
                break;
            }
            self.forget(&name);
            evicted.push(name);
        }
        evicted
    }
}

/// Writes through a temp file renamed into place, so a parallel reader or a failed write never
/// leaves a partial file under `name` for later requests. The temp file is in `dir`, because a
/// rename can't move a file to another file system.
fn store_file(dir: &Path, name: &str, file: &[u8]) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let mut temp_file = NamedTempFile::new_in(dir)?;
    temp_file.write_all(file)?;
    temp_file.as_file().sync_all()?;
    temp_file.persist(dir.join(name))?;
    Ok(())
}

/// Full for this store, whether the file system or the user's quota ran out.
fn is_disk_full(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::StorageFull | io::ErrorKind::QuotaExceeded
    )
}

/// A file that is already gone counts as deleted.
fn remove_files(dir: &Path, names: &[String]) {
    for name in names {
        let path = dir.join(name);
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => warn!(
                "Failed to delete cached embed file {}: {}",
                path.display(),
                e
            ),
        }
    }
}

/// The build stamp of the caches in tests.
#[cfg(test)]
pub const TEST_BUILD: FileStamp = FileStamp(0x7e57);

#[cfg(test)]
impl EmbedCache {
    /// A cache in a temporary directory of its own, which lives as long as the cache.
    pub fn temporary() -> Self {
        let temp_dir = tempfile::tempdir().expect("a temporary directory is created");
        let cache = Self::open(
            temp_dir.path().join(EMBED_CACHE_DIR),
            DEFAULT_MAX_GIB * BYTES_PER_GIB,
            TEST_BUILD,
        )
        .expect("an empty cache opens");
        Self {
            _temp_dir: Some(temp_dir),
            ..cache
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::sync::oneshot;
    use tokio::time;

    use super::*;

    const FILE: &[u8] = b"0123456789";
    /// Two files fit, and a third one goes past the limit.
    const LIMIT: u64 = 25;
    /// Far longer than a build in these tests, so a build that runs out of it waits for another.
    const BUILD_TIMEOUT: Duration = Duration::from_secs(5);

    async fn build_counted(
        cache: &Arc<EmbedCache>,
        name: &str,
        builds: &Arc<AtomicUsize>,
    ) -> Result<web::Bytes, io::Error> {
        let builds = Arc::clone(builds);
        cache
            .get_or_build(name, async move {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok(web::Bytes::from_static(FILE))
            })
            .await
    }

    fn stamped(name: &str) -> String {
        stamped_name(TEST_BUILD, name)
    }

    pub(crate) fn names_in(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(dir)
            .expect("the cache directory is readable")
            .map(|entry| {
                entry
                    .expect("the entry is readable")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        names.sort();
        names
    }

    /// Stores as [`store_file`] does, on a disk of `LIMIT` bytes.
    fn store_on_small_disk(dir: &Path, name: &str, file: &[u8]) -> io::Result<()> {
        let used: u64 = fs::read_dir(dir)
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.ok()?.metadata().ok())
            .map(|metadata| metadata.len())
            .sum();
        if used + file.len() as u64 > LIMIT {
            return Err(io::ErrorKind::StorageFull.into());
        }
        store_file(dir, name, file)
    }

    #[actix_web::test]
    async fn given_missing_file_when_getting_twice_should_build_it_once_and_store_it() {
        let temp_dir = tempfile::tempdir().expect("a temporary directory is created");
        let dir = temp_dir.path().join(EMBED_CACHE_DIR);
        let cache =
            Arc::new(EmbedCache::open(dir.clone(), LIMIT, TEST_BUILD).expect("the cache opens"));
        let builds = Arc::new(AtomicUsize::new(0));

        for _ in 0..2 {
            let file = build_counted(&cache, "a.png", &builds)
                .await
                .expect("the file is built");
            assert_eq!(file, FILE);
        }

        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert_eq!(names_in(&dir), [stamped("a.png")]);
    }

    #[actix_web::test]
    async fn given_concurrent_misses_of_one_file_when_getting_should_build_it_once() {
        let cache = Arc::new(EmbedCache::temporary());
        let builds = Arc::new(AtomicUsize::new(0));
        let get = || {
            let builds = Arc::clone(&builds);
            cache.get_or_build("a.png", async move {
                builds.fetch_add(1, Ordering::SeqCst);
                // Both requests miss the file before this build stores it.
                time::sleep(Duration::from_millis(50)).await;
                Ok::<_, io::Error>(web::Bytes::from_static(FILE))
            })
        };

        let (first, second) = tokio::join!(get(), get());

        assert_eq!(first.expect("the file is built"), FILE);
        assert_eq!(second.expect("the file is read"), FILE);
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[actix_web::test]
    async fn given_build_of_one_file_running_when_getting_another_file_should_build_it_meanwhile() {
        let cache = Arc::new(EmbedCache::temporary());
        let (first_started, first_running) = oneshot::channel();
        let (second_built, second_done) = oneshot::channel::<()>();
        // The first build ends only after the second one ran, so one turn for every file would
        // make each build wait for the other.
        let first_build = async move {
            let _ = first_started.send(());
            second_done.await.map_err(io::Error::other)?;
            Ok::<_, io::Error>(web::Bytes::from_static(FILE))
        };
        let first = rt::spawn({
            let cache = Arc::clone(&cache);
            async move { cache.get_or_build("a.png", first_build).await }
        });
        first_running.await.expect("the first build starts");

        let second = cache.get_or_build("b.png", async move {
            let _ = second_built.send(());
            Ok::<_, io::Error>(web::Bytes::from_static(FILE))
        });
        let second = time::timeout(BUILD_TIMEOUT, second)
            .await
            .expect("the second build doesn't wait for the first one");
        let first = time::timeout(BUILD_TIMEOUT, first)
            .await
            .expect("the first build ends")
            .expect("the first request ends");

        assert_eq!(first.expect("the first file is built"), FILE);
        assert_eq!(second.expect("the second file is built"), FILE);
    }

    #[actix_web::test]
    async fn given_request_dropped_during_build_when_getting_again_should_read_file_it_stored() {
        let temp_dir = tempfile::tempdir().expect("a temporary directory is created");
        let dir = temp_dir.path().join(EMBED_CACHE_DIR);
        let cache =
            Arc::new(EmbedCache::open(dir.clone(), LIMIT, TEST_BUILD).expect("the cache opens"));
        let (started, running) = oneshot::channel();
        let (release, released) = oneshot::channel::<()>();
        let dropped = cache.get_or_build("a.png", async move {
            let _ = started.send(());
            released.await.map_err(io::Error::other)?;
            Ok::<_, io::Error>(web::Bytes::from_static(FILE))
        });

        // actix drops the request after a reset of its connection.
        tokio::select! {
            _ = dropped => panic!("the build waits for its release"),
            _ = running => {}
        }
        let _ = release.send(());
        let builds = Arc::new(AtomicUsize::new(0));
        let file = build_counted(&cache, "a.png", &builds)
            .await
            .expect("the file is read");

        assert_eq!(file, FILE);
        assert_eq!(builds.load(Ordering::SeqCst), 0);
        assert_eq!(names_in(&dir), [stamped("a.png")]);
        assert_eq!(cache.index().total_bytes, FILE.len() as u64);
    }

    #[actix_web::test]
    async fn given_failed_build_when_getting_should_store_nothing_and_build_again() {
        let cache = Arc::new(EmbedCache::temporary());

        let failed = cache
            .get_or_build("a.png", async { Err(io::Error::other("no chart")) })
            .await;
        let builds = Arc::new(AtomicUsize::new(0));
        build_counted(&cache, "a.png", &builds)
            .await
            .expect("the second build works");

        assert!(failed.is_err());
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[actix_web::test]
    async fn given_store_past_limit_when_storing_should_delete_least_recently_used_file() {
        let temp_dir = tempfile::tempdir().expect("a temporary directory is created");
        let dir = temp_dir.path().join(EMBED_CACHE_DIR);
        let cache =
            Arc::new(EmbedCache::open(dir.clone(), LIMIT, TEST_BUILD).expect("the cache opens"));
        let builds = Arc::new(AtomicUsize::new(0));
        for name in ["a.png", "b.png"] {
            build_counted(&cache, name, &builds)
                .await
                .expect("the file is built");
        }

        // A read makes `a.png` the most recently used file, so `b.png` goes.
        build_counted(&cache, "a.png", &builds)
            .await
            .expect("the file is read");
        build_counted(&cache, "c.html", &builds)
            .await
            .expect("the file is built");

        assert_eq!(builds.load(Ordering::SeqCst), 3);
        assert_eq!(names_in(&dir), [stamped("a.png"), stamped("c.html")]);
        assert_eq!(cache.index().total_bytes, 2 * FILE.len() as u64);
    }

    #[actix_web::test]
    async fn given_full_disk_below_limit_when_storing_should_delete_least_recently_used_file_and_retry()
     {
        let temp_dir = tempfile::tempdir().expect("a temporary directory is created");
        let dir = temp_dir.path().join(EMBED_CACHE_DIR);
        let cache = Arc::new(
            EmbedCache::open_with_writer(
                dir.clone(),
                DEFAULT_MAX_GIB * BYTES_PER_GIB,
                TEST_BUILD,
                store_on_small_disk,
            )
            .expect("the cache opens"),
        );
        let builds = Arc::new(AtomicUsize::new(0));
        for name in ["a.png", "b.png"] {
            build_counted(&cache, name, &builds)
                .await
                .expect("the file is built");
        }

        // A read makes `a.png` the most recently used file, so `b.png` goes.
        build_counted(&cache, "a.png", &builds)
            .await
            .expect("the file is read");
        build_counted(&cache, "c.html", &builds)
            .await
            .expect("the file is built");

        assert_eq!(builds.load(Ordering::SeqCst), 3);
        assert_eq!(names_in(&dir), [stamped("a.png"), stamped("c.html")]);
        assert_eq!(cache.index().total_bytes, 2 * FILE.len() as u64);
    }

    #[actix_web::test]
    async fn given_files_of_other_build_when_getting_should_build_again_and_evict_them_first() {
        let temp_dir = tempfile::tempdir().expect("a temporary directory is created");
        let dir = temp_dir.path().join(EMBED_CACHE_DIR);
        let old_build = FileStamp::of_bytes(b"old server binary");
        let builds = Arc::new(AtomicUsize::new(0));
        let old_cache =
            Arc::new(EmbedCache::open(dir.clone(), LIMIT, old_build).expect("the cache opens"));
        build_counted(&old_cache, "a.png", &builds)
            .await
            .expect("the file is built");
        drop(old_cache);
        let cache = Arc::new(EmbedCache::open(dir.clone(), LIMIT, TEST_BUILD).expect("it opens"));

        build_counted(&cache, "a.png", &builds)
            .await
            .expect("the file is built again");
        let mut both_builds = vec![stamped_name(old_build, "a.png"), stamped("a.png")];
        both_builds.sort();
        assert_eq!(names_in(&dir), both_builds);
        // The third file takes the cache past its limit.
        build_counted(&cache, "b.png", &builds)
            .await
            .expect("the file is built");

        assert_eq!(builds.load(Ordering::SeqCst), 3);
        assert_eq!(names_in(&dir), [stamped("a.png"), stamped("b.png")]);
        assert_eq!(cache.index().total_bytes, 2 * FILE.len() as u64);
    }

    #[test]
    fn given_file_when_stamping_should_take_hash_of_its_bytes() {
        let temp_dir = tempfile::tempdir().expect("a temporary directory is created");
        let path = temp_dir.path().join("server");
        fs::write(&path, FILE).expect("the file is written");

        let stamp = FileStamp::of_file(&path).expect("the file is readable");
        fs::write(&path, b"0123456780").expect("the file is rewritten");
        let rewritten = FileStamp::of_file(&path).expect("the file is readable");

        assert_eq!(stamp, FileStamp::of_bytes(FILE));
        assert_eq!(stamp.to_string(), &blake3::hash(FILE).to_hex()[..16]);
        assert_ne!(rewritten, stamp);
    }

    /// A restart of the same binary keeps the cache. A server that can't read its binary keeps it
    /// for one start only.
    #[test]
    fn given_same_binary_when_stamping_build_twice_should_give_same_stamp() {
        let first = build_stamp().expect("the test binary is readable");
        let second = build_stamp().expect("the test binary is readable");

        assert_eq!(first, second);
        assert_ne!(start_stamp(), first);
    }

    #[actix_web::test]
    async fn given_file_deleted_outside_cache_when_getting_should_build_it_again() {
        let temp_dir = tempfile::tempdir().expect("a temporary directory is created");
        let dir = temp_dir.path().join(EMBED_CACHE_DIR);
        let cache =
            Arc::new(EmbedCache::open(dir.clone(), LIMIT, TEST_BUILD).expect("the cache opens"));
        let builds = Arc::new(AtomicUsize::new(0));
        build_counted(&cache, "a.png", &builds)
            .await
            .expect("the file is built");
        fs::remove_file(dir.join(stamped("a.png"))).expect("the file is deleted");

        build_counted(&cache, "a.png", &builds)
            .await
            .expect("the file is built again");

        assert_eq!(builds.load(Ordering::SeqCst), 2);
        assert_eq!(cache.index().total_bytes, FILE.len() as u64);
    }

    #[test]
    fn given_partial_file_when_storing_should_replace_it_without_leaving_temp_files() {
        let temp_dir = tempfile::tempdir().expect("a temporary directory is created");
        let dir = temp_dir.path().join(EMBED_CACHE_DIR);
        fs::create_dir(&dir).expect("the cache directory is created");
        fs::write(dir.join("a.png"), &FILE[..FILE.len() / 2]).expect("the partial file is written");

        store_file(&dir, "a.png", FILE).expect("the file is stored");

        assert_eq!(
            fs::read(dir.join("a.png")).expect("the file is readable"),
            FILE
        );
        assert_eq!(names_in(&dir), ["a.png"]);
    }

    #[actix_web::test]
    async fn given_files_of_earlier_server_when_opening_should_drop_temp_files_and_oldest_past_limit()
     {
        let temp_dir = tempfile::tempdir().expect("a temporary directory is created");
        let dir = temp_dir.path().join(EMBED_CACHE_DIR);
        fs::create_dir(&dir).expect("the cache directory is created");
        let now = SystemTime::now();
        for (name, age_secs) in [("old.png", 300), ("middle.png", 200), ("new.png", 100)] {
            let file = File::create(dir.join(name)).expect("the file is created");
            (&file).write_all(FILE).expect("the file is written");
            file.set_modified(now - Duration::from_secs(age_secs))
                .expect("the modification time is set");
        }
        fs::write(dir.join(".tmpA1b2C3"), FILE).expect("the temp file is written");
        fs::create_dir(dir.join("not-a-file")).expect("the directory is created");

        let cache = EmbedCache::open(dir.clone(), LIMIT, TEST_BUILD).expect("the cache opens");

        assert_eq!(names_in(&dir), ["middle.png", "new.png", "not-a-file"]);
        assert_eq!(cache.index().total_bytes, 2 * FILE.len() as u64);
    }

    #[cfg(unix)]
    #[actix_web::test]
    async fn given_entry_whose_metadata_fails_when_opening_should_skip_it() {
        let temp_dir = tempfile::tempdir().expect("a temporary directory is created");
        let dir = temp_dir.path().join(EMBED_CACHE_DIR);
        fs::create_dir(&dir).expect("the cache directory is created");
        fs::write(dir.join("a.png"), FILE).expect("the file is written");
        // Without the search bit, the directory lists its names, but the metadata of each entry
        // fails, as for a file that another process deletes during the scan.
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o600)).expect("the mode is set");
        let metadata_fails = fs::metadata(dir.join("a.png")).is_err();

        let opened = EmbedCache::open(dir.clone(), LIMIT, TEST_BUILD);
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).expect("the mode is restored");

        let cache = opened.expect("the cache opens");
        // Root reads the metadata anyway, and then the file joins the index.
        let indexed_bytes = if metadata_fails { 0 } else { FILE.len() as u64 };
        assert_eq!(cache.index().total_bytes, indexed_bytes);
    }
}
