/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Pluggable object storage abstraction.
//!
//! `ObjectStorage` is a small async trait that backs the milestone's "S3 as
//! optional primary storage" feature. Phase 2+ persistence sites consume an
//! `Rc<dyn ObjectStorage>` (per-shard, not cross-shard); the choice is made
//! at boot from `[system.storage]`.
//!
//! Backends:
//!
//! | Backend            | Where it lives                                   |
//! |--------------------|--------------------------------------------------|
//! | `CompioFsStorage`  | this file — passthrough to the local filesystem  |
//! | `InMemoryStorage`  | this file (cfg(test)) — HashMap-backed for tests |
//! | `S3Storage`        | added in Phase 1b (rusty-s3 + cyper)             |

use std::ops::Range;
use std::rc::Rc;

use async_trait::async_trait;
use bytes::Bytes;
use compio::fs::OpenOptions;
use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use err_trail::ErrContext;
use iggy_common::IggyError;

const COMPONENT: &str = "STREAMING_OBJECT_STORAGE";

/// Lightweight metadata returned by [`ObjectStorage::head`] and
/// [`ObjectStorage::list_prefix`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
    /// Backend-supplied entity tag, when available. The fs backend leaves
    /// this `None`; S3 returns the server-assigned ETag.
    pub etag: Option<String>,
}

/// Pluggable byte-addressable storage.
///
/// Designed to be cheap to share across tasks within a compio shard:
/// implementations are stateless or hold internal handles behind their own
/// synchronization, and callers pass `Rc<dyn ObjectStorage>` around freely.
/// The trait is `?Send` because compio's per-thread io_uring driver yields
/// non-`Send` futures; `Arc<dyn ObjectStorage>` across shards isn't a
/// supported pattern (each shard owns its own instance).
#[async_trait(?Send)]
pub trait ObjectStorage: std::fmt::Debug {
    /// Write `bytes` at `key`, replacing any existing object atomically from
    /// the reader's perspective.
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), IggyError>;

    /// Conditional create: succeeds only when no object exists at `key`.
    /// Returns `IggyError::CannotOverwriteFile` if the object already exists.
    /// Used by Phase 8 fencing.
    async fn put_if_absent(&self, key: &str, bytes: Bytes) -> Result<(), IggyError>;

    /// Open a streaming write. The handle accumulates bytes via `upload_part`
    /// and seals via `complete` (or discards via `abort`).
    async fn put_multipart(&self, key: &str) -> Result<Box<dyn MultipartHandle>, IggyError>;

    /// Read the byte range `[range.start, range.end)`. Returns exactly
    /// `range.end - range.start` bytes on success.
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, IggyError>;

    /// Object metadata. Errors with `CannotReadFileMetadata` if absent.
    async fn head(&self, key: &str) -> Result<ObjectMeta, IggyError>;

    /// Enumerate every object whose key begins with `prefix`. Order is
    /// lexicographic on the key. The fs backend treats `prefix` as a
    /// directory path and walks it recursively.
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<ObjectMeta>, IggyError>;

    /// Delete a single object. Idempotent — deleting a missing key succeeds.
    async fn delete(&self, key: &str) -> Result<(), IggyError>;

    /// Delete many objects. Backends with batch APIs (S3 `DeleteObjects`)
    /// override this; the default is a per-key loop.
    async fn delete_many(&self, keys: &[&str]) -> Result<(), IggyError> {
        for k in keys {
            self.delete(k).await?;
        }
        Ok(())
    }
}

/// In-progress streaming write returned by [`ObjectStorage::put_multipart`].
///
/// The handle is consumed by either `complete` (seal) or `abort` (discard);
/// the `self: Box<Self>` bounds make the consumption explicit at the call
/// site. Dropping the handle without calling either is a programming error
/// — backends best-effort abort on drop where possible, but callers should
/// not rely on it. Like [`ObjectStorage`], the trait is `?Send` because the
/// compio io_uring driver yields non-`Send` futures.
#[async_trait(?Send)]
pub trait MultipartHandle {
    /// Append `bytes` to the in-progress upload.
    async fn upload_part(&mut self, bytes: Bytes) -> Result<(), IggyError>;

    /// Seal the upload. Returns the backend-assigned final identifier (ETag
    /// for S3; empty string for fs).
    async fn complete(self: Box<Self>) -> Result<String, IggyError>;

    /// Discard the upload and release any backend-side resources.
    async fn abort(self: Box<Self>) -> Result<(), IggyError>;
}

// =====================================================================
// CompioFsStorage — local filesystem
// =====================================================================

/// Filesystem-backed `ObjectStorage`. Keys are filesystem paths.
///
/// This is a thin wrapper over the same compio fs APIs the legacy
/// `PersisterKind::File` already uses; it does not introduce new I/O
/// behavior and is byte-compatible with existing fs-mode deployments.
#[derive(Debug, Default, Clone)]
pub struct CompioFsStorage;

#[async_trait(?Send)]
impl ObjectStorage for CompioFsStorage {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), IggyError> {
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(key)
            .await
            .error(|e: &std::io::Error| format!("{COMPONENT} (error: {e}) - put open: {key}"))
            .map_err(|_| IggyError::CannotOverwriteFile)?;
        file.write_all_at(bytes.to_vec(), 0)
            .await
            .0
            .error(|e: &std::io::Error| format!("{COMPONENT} (error: {e}) - put write: {key}"))
            .map_err(|_| IggyError::CannotWriteToFile)?;
        Ok(())
    }

    async fn put_if_absent(&self, key: &str, bytes: Bytes) -> Result<(), IggyError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(key)
            .await
            .error(|e: &std::io::Error| {
                format!("{COMPONENT} (error: {e}) - put_if_absent open: {key}")
            })
            .map_err(|_| IggyError::CannotOverwriteFile)?;
        file.write_all_at(bytes.to_vec(), 0)
            .await
            .0
            .error(|e: &std::io::Error| {
                format!("{COMPONENT} (error: {e}) - put_if_absent write: {key}")
            })
            .map_err(|_| IggyError::CannotWriteToFile)?;
        Ok(())
    }

    async fn put_multipart(&self, key: &str) -> Result<Box<dyn MultipartHandle>, IggyError> {
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(key)
            .await
            .error(|e: &std::io::Error| {
                format!("{COMPONENT} (error: {e}) - put_multipart open: {key}")
            })
            .map_err(|_| IggyError::CannotOverwriteFile)?;
        Ok(Box::new(FsMultipart {
            key: key.to_owned(),
            file: Some(file),
            offset: 0,
        }))
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, IggyError> {
        let len = range
            .end
            .checked_sub(range.start)
            .ok_or(IggyError::CannotReadFile)? as usize;
        let file = OpenOptions::new()
            .read(true)
            .open(key)
            .await
            .error(|e: &std::io::Error| format!("{COMPONENT} (error: {e}) - get_range open: {key}"))
            .map_err(|_| IggyError::CannotReadFile)?;
        let (result, buf): (std::io::Result<()>, Vec<u8>) =
            file.read_exact_at(vec![0u8; len], range.start).await.into();
        result
            .error(|e: &std::io::Error| format!("{COMPONENT} (error: {e}) - get_range read: {key}"))
            .map_err(|_| IggyError::CannotReadFile)?;
        Ok(Bytes::from(buf))
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, IggyError> {
        let meta = std::fs::metadata(key)
            .error(|e: &std::io::Error| format!("{COMPONENT} (error: {e}) - head: {key}"))
            .map_err(|_| IggyError::CannotReadFileMetadata)?;
        Ok(ObjectMeta {
            key: key.to_owned(),
            size: meta.len(),
            etag: None,
        })
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<ObjectMeta>, IggyError> {
        // NOTE: blocking std::fs walk. Phase 4 replaces production callers
        // with fs-mode-aware async paths; for the fs backend itself this is
        // unchanged from current iggy behavior.
        let mut out = Vec::new();
        walk_fs(prefix, &mut out)?;
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    async fn delete(&self, key: &str) -> Result<(), IggyError> {
        match compio::fs::remove_file(key).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(IggyError::CannotDeleteFile)
                .error(|_: &IggyError| format!("{COMPONENT} (error: {e}) - delete: {key}")),
        }
    }
}

fn walk_fs(prefix: &str, out: &mut Vec<ObjectMeta>) -> Result<(), IggyError> {
    let entries = match std::fs::read_dir(prefix) {
        Ok(it) => it,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(IggyError::CannotReadFile).error(|_: &IggyError| {
                format!("{COMPONENT} (error: {e}) - list_prefix: {prefix}")
            });
        }
    };
    for entry in entries {
        let entry = entry
            .error(|e: &std::io::Error| format!("{COMPONENT} (error: {e}) - read_dir: {prefix}"))
            .map_err(|_| IggyError::CannotReadFile)?;
        let path = entry.path();
        let path_str = path.to_string_lossy().into_owned();
        let ftype = entry
            .file_type()
            .error(|e: &std::io::Error| format!("{COMPONENT} (error: {e}) - file_type: {path_str}"))
            .map_err(|_| IggyError::CannotReadFileMetadata)?;
        if ftype.is_dir() {
            walk_fs(&path_str, out)?;
        } else if ftype.is_file() {
            let meta = entry
                .metadata()
                .error(|e: &std::io::Error| format!("{COMPONENT} (error: {e}) - meta: {path_str}"))
                .map_err(|_| IggyError::CannotReadFileMetadata)?;
            out.push(ObjectMeta {
                key: path_str,
                size: meta.len(),
                etag: None,
            });
        }
    }
    Ok(())
}

struct FsMultipart {
    key: String,
    file: Option<compio::fs::File>,
    offset: u64,
}

#[async_trait(?Send)]
impl MultipartHandle for FsMultipart {
    async fn upload_part(&mut self, bytes: Bytes) -> Result<(), IggyError> {
        let file = self.file.as_mut().ok_or_else(|| {
            tracing::warn!(
                "{COMPONENT} - upload_part on already-finished handle: {}",
                self.key,
            );
            IggyError::CannotWriteToFile
        })?;
        let len = bytes.len() as u64;
        let offset = self.offset;
        let result = file.write_all_at(bytes.to_vec(), offset).await.0;
        if let Err(e) = result {
            tracing::warn!("{COMPONENT} (error: {e}) - upload_part write: {}", self.key,);
            return Err(IggyError::CannotWriteToFile);
        }
        self.offset += len;
        Ok(())
    }

    async fn complete(mut self: Box<Self>) -> Result<String, IggyError> {
        // Drop the file handle; bytes already on disk.
        let _ = self.file.take();
        Ok(String::new())
    }

    async fn abort(mut self: Box<Self>) -> Result<(), IggyError> {
        let _ = self.file.take();
        match compio::fs::remove_file(&self.key).await {
            Ok(()) | Err(_) => Ok(()),
        }
    }
}

// =====================================================================
// InMemoryStorage — test-only, HashMap-backed
// =====================================================================

#[cfg(test)]
mod in_memory {
    use super::*;
    use std::collections::{BTreeMap, HashMap};
    use std::sync::{Arc, Mutex};

    /// Test-only `ObjectStorage` backed by a sorted in-memory map.
    /// Replaces what `opendal::services::Memory` would have given us.
    #[derive(Debug, Default, Clone)]
    pub(super) struct InMemoryStorage {
        state: Arc<Mutex<InMemoryState>>,
    }

    #[derive(Debug, Default)]
    struct InMemoryState {
        objects: BTreeMap<String, Bytes>,
        in_progress: HashMap<u64, InProgress>,
        next_upload_id: u64,
        next_etag: u64,
    }

    #[derive(Debug)]
    struct InProgress {
        key: String,
        parts: Vec<Bytes>,
    }

    impl InMemoryStorage {
        pub(super) fn new() -> Self {
            Self::default()
        }

        fn lock(&self) -> std::sync::MutexGuard<'_, InMemoryState> {
            // Test-only; poisoning is fatal anyway.
            self.state.lock().expect("InMemoryStorage state poisoned")
        }
    }

    #[async_trait(?Send)]
    impl ObjectStorage for InMemoryStorage {
        async fn put(&self, key: &str, bytes: Bytes) -> Result<(), IggyError> {
            self.lock().objects.insert(key.to_owned(), bytes);
            Ok(())
        }

        async fn put_if_absent(&self, key: &str, bytes: Bytes) -> Result<(), IggyError> {
            let mut s = self.lock();
            if s.objects.contains_key(key) {
                return Err(IggyError::CannotOverwriteFile);
            }
            s.objects.insert(key.to_owned(), bytes);
            Ok(())
        }

        async fn put_multipart(&self, key: &str) -> Result<Box<dyn MultipartHandle>, IggyError> {
            let mut s = self.lock();
            let upload_id = s.next_upload_id;
            s.next_upload_id += 1;
            s.in_progress.insert(
                upload_id,
                InProgress {
                    key: key.to_owned(),
                    parts: Vec::new(),
                },
            );
            Ok(Box::new(InMemoryMultipart {
                state: self.state.clone(),
                upload_id,
            }))
        }

        async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, IggyError> {
            let s = self.lock();
            let obj = s.objects.get(key).ok_or(IggyError::CannotReadFile)?;
            let start = range.start as usize;
            let end = (range.end as usize).min(obj.len());
            if start > obj.len() {
                return Err(IggyError::CannotReadFile);
            }
            Ok(obj.slice(start..end))
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, IggyError> {
            let s = self.lock();
            let obj = s
                .objects
                .get(key)
                .ok_or(IggyError::CannotReadFileMetadata)?;
            Ok(ObjectMeta {
                key: key.to_owned(),
                size: obj.len() as u64,
                etag: None,
            })
        }

        async fn list_prefix(&self, prefix: &str) -> Result<Vec<ObjectMeta>, IggyError> {
            let s = self.lock();
            Ok(s.objects
                .range(prefix.to_owned()..)
                .take_while(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| ObjectMeta {
                    key: k.clone(),
                    size: v.len() as u64,
                    etag: None,
                })
                .collect())
        }

        async fn delete(&self, key: &str) -> Result<(), IggyError> {
            self.lock().objects.remove(key);
            Ok(())
        }
    }

    struct InMemoryMultipart {
        state: Arc<Mutex<InMemoryState>>,
        upload_id: u64,
    }

    #[async_trait(?Send)]
    impl MultipartHandle for InMemoryMultipart {
        async fn upload_part(&mut self, bytes: Bytes) -> Result<(), IggyError> {
            let mut s = self.state.lock().expect("InMemoryStorage state poisoned");
            let in_progress = s
                .in_progress
                .get_mut(&self.upload_id)
                .ok_or(IggyError::CannotAppendToFile)?;
            in_progress.parts.push(bytes);
            Ok(())
        }

        async fn complete(self: Box<Self>) -> Result<String, IggyError> {
            let mut s = self.state.lock().expect("InMemoryStorage state poisoned");
            let in_progress = s
                .in_progress
                .remove(&self.upload_id)
                .ok_or(IggyError::CannotAppendToFile)?;
            let total: usize = in_progress.parts.iter().map(|b| b.len()).sum();
            let mut buf = bytes::BytesMut::with_capacity(total);
            for part in in_progress.parts {
                buf.extend_from_slice(&part);
            }
            s.objects.insert(in_progress.key, buf.freeze());
            let etag_id = s.next_etag;
            s.next_etag += 1;
            Ok(format!("im-etag-{etag_id}"))
        }

        async fn abort(self: Box<Self>) -> Result<(), IggyError> {
            let mut s = self.state.lock().expect("InMemoryStorage state poisoned");
            s.in_progress.remove(&self.upload_id);
            Ok(())
        }
    }
}

// =====================================================================
// Tests
// =====================================================================

#[cfg(test)]
mod tests {
    use super::in_memory::InMemoryStorage;
    use super::*;
    use bytes::Bytes;

    fn store() -> InMemoryStorage {
        InMemoryStorage::new()
    }

    #[compio::test]
    async fn put_and_head_round_trip() {
        let s = store();
        s.put("a/b", Bytes::from_static(b"hello")).await.unwrap();
        let meta = s.head("a/b").await.unwrap();
        assert_eq!(meta.size, 5);
    }

    #[compio::test]
    async fn get_range_returns_slice() {
        let s = store();
        s.put("k", Bytes::from_static(b"abcdef")).await.unwrap();
        let got = s.get_range("k", 1..4).await.unwrap();
        assert_eq!(got.as_ref(), b"bcd");
    }

    #[compio::test]
    async fn put_if_absent_first_wins() {
        let s = store();
        s.put_if_absent("k", Bytes::from_static(b"first"))
            .await
            .unwrap();
        let err = s
            .put_if_absent("k", Bytes::from_static(b"second"))
            .await
            .unwrap_err();
        assert!(matches!(err, IggyError::CannotOverwriteFile));
    }

    #[compio::test]
    async fn list_prefix_returns_sorted_matches() {
        let s = store();
        s.put("a/1", Bytes::from_static(b"_")).await.unwrap();
        s.put("a/2", Bytes::from_static(b"_")).await.unwrap();
        s.put("b/1", Bytes::from_static(b"_")).await.unwrap();
        let listed = s.list_prefix("a/").await.unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].key, "a/1");
        assert_eq!(listed[1].key, "a/2");
    }

    #[compio::test]
    async fn delete_is_idempotent() {
        let s = store();
        s.put("k", Bytes::from_static(b"_")).await.unwrap();
        s.delete("k").await.unwrap();
        s.delete("k").await.unwrap(); // second time also OK
        assert!(s.head("k").await.is_err());
    }

    #[compio::test]
    async fn delete_many_calls_default_path() {
        let s = store();
        s.put("a", Bytes::from_static(b"_")).await.unwrap();
        s.put("b", Bytes::from_static(b"_")).await.unwrap();
        s.delete_many(&["a", "b"]).await.unwrap();
        assert!(s.list_prefix("").await.unwrap().is_empty());
    }

    #[compio::test]
    async fn multipart_complete_assembles_object() {
        let s = store();
        let mut h = s.put_multipart("k").await.unwrap();
        h.upload_part(Bytes::from_static(b"hel")).await.unwrap();
        h.upload_part(Bytes::from_static(b"lo")).await.unwrap();
        let etag = h.complete().await.unwrap();
        assert!(etag.starts_with("im-etag-"));

        let got = s.get_range("k", 0..5).await.unwrap();
        assert_eq!(got.as_ref(), b"hello");
    }

    #[compio::test]
    async fn multipart_abort_does_not_publish() {
        let s = store();
        let mut h = s.put_multipart("k").await.unwrap();
        h.upload_part(Bytes::from_static(b"hello")).await.unwrap();
        h.abort().await.unwrap();
        assert!(s.head("k").await.is_err());
    }

    // ---- CompioFsStorage round-trips ----
    //
    // These use a tempdir and exercise the same trait surface. They run on
    // the compio runtime exactly like the in-memory tests above.

    #[compio::test]
    async fn fs_put_get_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let key = tmp.path().join("obj").to_string_lossy().into_owned();
        let s = CompioFsStorage;
        s.put(&key, Bytes::from_static(b"hello")).await.unwrap();
        let got = s.get_range(&key, 0..5).await.unwrap();
        assert_eq!(got.as_ref(), b"hello");
    }

    #[compio::test]
    async fn fs_put_if_absent_rejects_existing() {
        let tmp = tempfile::tempdir().unwrap();
        let key = tmp.path().join("obj").to_string_lossy().into_owned();
        let s = CompioFsStorage;
        s.put_if_absent(&key, Bytes::from_static(b"a"))
            .await
            .unwrap();
        let err = s
            .put_if_absent(&key, Bytes::from_static(b"b"))
            .await
            .unwrap_err();
        assert!(matches!(err, IggyError::CannotOverwriteFile));
    }

    #[compio::test]
    async fn fs_multipart_writes_in_order() {
        let tmp = tempfile::tempdir().unwrap();
        let key = tmp.path().join("obj").to_string_lossy().into_owned();
        let s = CompioFsStorage;
        let mut h = s.put_multipart(&key).await.unwrap();
        h.upload_part(Bytes::from_static(b"part1-")).await.unwrap();
        h.upload_part(Bytes::from_static(b"part2")).await.unwrap();
        h.complete().await.unwrap();

        let got = s.get_range(&key, 0..11).await.unwrap();
        assert_eq!(got.as_ref(), b"part1-part2");
    }
}
