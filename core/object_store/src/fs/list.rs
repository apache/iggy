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

//! One page of a listing: a walk of the directory tree in key order, with
//! blocking calls, so it runs on a thread of its own.

use std::fs::{self, DirEntry, File};
use std::io::Read;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::error::ObjectStoreError;
use crate::store::{ListPage, ListRequest, ListedObject};

use super::header::{Header, MAX_HEADER_LEN};
use super::{bucket_not_found, io_error, is_missing, is_scratch};

/// How long a scratch file stays untouched before a listing takes it for the
/// leftover of a crash and removes it. A put that still writes it then fails
/// at its commit.
const SCRATCH_GRACE: Duration = Duration::from_hours(1);

#[derive(Debug)]
pub(super) struct Walk {
    root: PathBuf,
    prefix: String,
    /// The page starts strictly after this key.
    after: Option<String>,
    page_len: usize,
    now: SystemTime,
}

/// The directories that the last page read, for the page that resumes it.
#[derive(Debug, Default)]
pub(super) struct ListCache {
    /// The continuation that the last page returned.
    next: Option<String>,
    directories: Vec<Directory>,
}

/// A directory's children in key order, as of its modification time.
#[derive(Debug)]
struct Directory {
    key: String,
    modified: SystemTime,
    children: Arc<[Child]>,
}

/// The directories of one page: those the page before it read, and those
/// it reads.
#[derive(Debug)]
struct Directories {
    cached: Vec<Directory>,
    read: Vec<Directory>,
}

/// A directory entry that can hold objects.
#[derive(Debug)]
enum Child {
    /// Its key ends with `/`, so it sorts where the keys below it sort.
    Directory(String),
    File(String),
}

impl Walk {
    pub(super) fn new(root: &Path, request: &ListRequest<'_>) -> Self {
        // A continuation from another listing can sort before the prefix, and
        // the walk must still start inside it.
        let after = request
            .continuation
            .filter(|after| *after >= request.prefix);
        Self {
            root: root.to_path_buf(),
            prefix: request.prefix.to_owned(),
            after: after.map(str::to_owned),
            page_len: request.page_len(),
            now: SystemTime::now(),
        }
    }

    pub(super) fn page(&self, cache: &mut ListCache) -> Result<ListPage, ObjectStoreError> {
        match fs::metadata(&self.root) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => return Err(bucket_not_found(&self.root)),
            Err(error) if is_missing(&error) => return Err(bucket_not_found(&self.root)),
            Err(error) => return Err(io_error(&self.prefix, "listing", &error)),
        }
        // The prefix can end inside a name, so the walk starts in the
        // directory before it and keeps the names that start with the rest.
        let (directory_key, partial) = match self.prefix.rsplit_once('/') {
            Some((directory, partial)) => (format!("{directory}/"), partial),
            None => (String::new(), self.prefix.as_str()),
        };
        // Only the page that resumes the last one reuses what it read, so the
        // cache can miss only an object put while the listing runs.
        let cached = if self.after.is_some() && self.after == cache.next {
            mem::take(&mut cache.directories)
        } else {
            Vec::new()
        };
        let mut directories = Directories {
            cached,
            read: Vec::new(),
        };
        let mut objects = Vec::new();
        let more = self.visit(&mut directories, &mut objects, &directory_key, partial)?;
        let next = if more {
            objects.last().map(|last| last.key.clone())
        } else {
            None
        };
        // The next page enters only the directories that hold keys after
        // `next`.
        cache.directories = match next.as_deref() {
            Some(next) => directories
                .read
                .into_iter()
                .filter(|directory| !passed(next, &directory.key))
                .collect(),
            None => Vec::new(),
        };
        cache.next.clone_from(&next);
        Ok(ListPage { objects, next })
    }

    /// Adds the objects below `directory_key` whose next name starts with
    /// `partial`, in key order. Returns whether the page is full and another
    /// object follows.
    fn visit(
        &self,
        directories: &mut Directories,
        objects: &mut Vec<ListedObject>,
        directory_key: &str,
        partial: &str,
    ) -> Result<bool, ObjectStoreError> {
        let children = self.children(directories, directory_key)?;
        let lower = format!("{directory_key}{partial}");
        // The children that sort before the partial name, or whose keys all
        // sort before the page, come first.
        let start = children
            .partition_point(|child| child.key() < lower.as_str() || self.before_page(child));
        for child in children[start..]
            .iter()
            .take_while(|child| child.key().starts_with(&lower))
        {
            match child {
                Child::Directory(key) => {
                    if self.visit(directories, objects, key, "")? {
                        return Ok(true);
                    }
                }
                Child::File(key) => {
                    // The page reads only the files it lists, so a file
                    // after a full page never fails it.
                    if objects.len() == self.page_len {
                        return Ok(true);
                    }
                    if let Some(object) = self.object(key)? {
                        objects.push(object);
                    }
                }
            }
        }
        Ok(false)
    }

    /// Whether every key at or below `child` sorts at or before the page's
    /// start.
    fn before_page(&self, child: &Child) -> bool {
        self.after.as_deref().is_some_and(|after| match child {
            Child::File(key) => key.as_str() <= after,
            Child::Directory(key) => passed(after, key),
        })
    }

    /// The children of `directory_key` in key order, from the cache when the
    /// directory has not changed since the last page read it.
    fn children(
        &self,
        directories: &mut Directories,
        directory_key: &str,
    ) -> Result<Arc<[Child]>, ObjectStoreError> {
        let path = self.root.join(directory_key);
        // The time comes before the entries, so an entry added meanwhile
        // changes it.
        let modified = match fs::metadata(&path).and_then(|metadata| metadata.modified()) {
            Ok(modified) => modified,
            Err(error) if is_missing(&error) => return Ok(Arc::new([])),
            Err(error) => return Err(io_error(directory_key, "listing", &error)),
        };
        let cached = directories
            .cached
            .iter()
            .find(|directory| directory.key == directory_key && directory.modified == modified);
        let children = match cached {
            Some(directory) => Arc::clone(&directory.children),
            None => self.read_children(directory_key, &path)?.into(),
        };
        directories.read.push(Directory {
            key: directory_key.to_owned(),
            modified,
            children: Arc::clone(&children),
        });
        Ok(children)
    }

    /// Reads the files and directories under `directory_key`, in key order.
    /// Dot-names hold scratch files, and a name that is not UTF-8 is no key,
    /// so both stay out.
    fn read_children(
        &self,
        directory_key: &str,
        path: &Path,
    ) -> Result<Vec<Child>, ObjectStoreError> {
        let entries = match fs::read_dir(path) {
            Ok(entries) => entries,
            Err(error) if is_missing(&error) => return Ok(Vec::new()),
            Err(error) => return Err(io_error(directory_key, "listing", &error)),
        };
        let mut children = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| io_error(directory_key, "listing", &error))?;
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            if name.starts_with('.') {
                if is_scratch(&name) {
                    self.remove_if_stale(&entry);
                }
                continue;
            }
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(error) if is_missing(&error) => continue,
                Err(error) => return Err(io_error(directory_key, "listing", &error)),
            };
            if file_type.is_dir() {
                children.push(Child::Directory(format!("{directory_key}{name}/")));
            } else if file_type.is_file() {
                children.push(Child::File(format!("{directory_key}{name}")));
            }
        }
        children.sort_unstable_by(|left, right| left.key().cmp(right.key()));
        Ok(children)
    }

    /// Removes a scratch file that no put wrote for [`SCRATCH_GRACE`].
    fn remove_if_stale(&self, entry: &DirEntry) {
        let stale = entry.metadata().is_ok_and(|metadata| {
            metadata.is_file()
                && metadata.modified().is_ok_and(|modified| {
                    self.now
                        .duration_since(modified)
                        .is_ok_and(|age| age >= SCRATCH_GRACE)
                })
        });
        if stale {
            let _ = fs::remove_file(entry.path());
        }
    }

    /// The object in the file under `key`, or `None` when the file is gone. A
    /// file without a valid header shows its length and an empty etag, so a
    /// delete can still find it.
    fn object(&self, key: &str) -> Result<Option<ListedObject>, ObjectStoreError> {
        let mut file = match File::open(self.root.join(key)) {
            Ok(file) => file,
            Err(error) if is_missing(&error) => return Ok(None),
            Err(error) => return Err(io_error(key, "reading", &error)),
        };
        let metadata = file
            .metadata()
            .map_err(|error| io_error(key, "reading", &error))?;
        let mut start = Vec::with_capacity(MAX_HEADER_LEN);
        file.by_ref()
            .take(MAX_HEADER_LEN as u64)
            .read_to_end(&mut start)
            .map_err(|error| io_error(key, "reading", &error))?;
        let header = Header::try_from(start.as_slice())
            .and_then(|header| header.check_file_len(metadata.len()).map(|()| header));
        let (size, etag) = match header {
            Ok(header) => (header.body_len, header.etag.to_string()),
            Err(_) => (metadata.len(), String::new()),
        };
        let last_modified = metadata
            .modified()
            .map_err(|error| io_error(key, "reading", &error))?;
        Ok(Some(ListedObject {
            key: key.to_owned(),
            size,
            etag,
            last_modified,
        }))
    }
}

impl Child {
    fn key(&self) -> &str {
        match self {
            Self::Directory(key) | Self::File(key) => key,
        }
    }
}

/// Whether every key below the directory `key` sorts before `after`.
fn passed(after: &str, key: &str) -> bool {
    key < after && !after.starts_with(key)
}
