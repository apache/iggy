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

//! [`ObjectStore`] trait used to copy finished segments to object store and read them back. This is
//! used for background copies only, not live writes.

use std::ops::Range;
use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;
use compio::fs::File;
use compio::io::AsyncReadAtExt;

#[cfg(test)]
pub(crate) mod in_memory;
#[cfg(any(test, feature = "testing"))]
pub mod testing;

/// S3 requires every multipart part except the last to be at least 5 MiB.
pub const S3_MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Object metadata returned from [`ObjectStore::head`] and [`ObjectStore::list_prefix`]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
    /// Backend-assigned entity tag when available (S3 ETag). The in-memory
    /// backend leaves this `None`.
    pub etag: Option<String>,
}

/// Used to track if a backend enforces conditional creates. This is determined once per backend.
/// Checked before using [`ObjectStore::put_if_absent`] for cross-replica dedup or fencing:
/// only [`ConditionalWriteSupport::Atomic`] guarantees the "first writer wins, others observe a
/// conflict" contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionalWriteSupport {
    /// `If-None-Match`/`O_EXCL` is genuinely enforced
    Atomic,
    /// The backend ignores conditional headers; `put_if_absent` may overwrite.
    BestEffort,
}

/// Result of a completed write
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutOutcome {
    pub etag: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ObjectStorageError {
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("object already exists: {0}")]
    AlreadyExists(String),
    #[error("conditional writes are not supported by this backend")]
    ConditionalWriteUnsupported,
    #[error("invalid part size {got} (minimum {min})")]
    PartSizeTooSmall { got: u64, min: u64 },
    #[error("requested range {start}..{end} is out of bounds for object of size {size}")]
    RangeOutOfBounds { start: u64, end: u64, size: u64 },
    #[error("backend unavailable: {0}")]
    Unavailable(String),
    #[error("backend error: {0}")]
    Backend(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Object store trait
#[async_trait(?Send)]
pub trait ObjectStore: std::fmt::Debug {
    /// Write `bytes` at `key`, replacing any existing object.
    async fn put(&self, key: &str, bytes: Bytes) -> Result<PutOutcome, ObjectStorageError>;

    /// Conditional create: succeeds only when no object exists at `key`, otherwise returns
    /// [`ObjectStorageError::AlreadyExists`].
    /// Only atomic if [`ObjectStore::conditional_write_support`] returns
    /// [`ConditionalWriteSupport::Atomic`]
    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<PutOutcome, ObjectStorageError>;

    /// Begin a streaming multipart upload. `part_size` must be at least [`S3_MIN_PART_SIZE`],
    /// else returns [`ObjectStorageError::PartSizeTooSmall`].
    async fn put_multipart(
        &self,
        key: &str,
        part_size: u64,
    ) -> Result<Box<dyn MultipartUpload>, ObjectStorageError>;

    /// Read the byte range `[range.start, range.end)` and returns `range.end - range.start`
    /// on success. Returns [`ObjectStorageError::RangeOutOfBounds`] if the range exceeds the object
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, ObjectStorageError>;

    /// Object metadata, else [`ObjectStorageError::NotFound`]
    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStorageError>;

    /// Lists every object whose key begins with the `prefix`. Backends paginate internally and
    /// should not truncate silently.
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<ObjectMeta>, ObjectStorageError>;

    /// Delete a single object. This is idempotent, so deleting a missing key should succeed.
    async fn delete(&self, key: &str) -> Result<(), ObjectStorageError>;

    /// Delete many objects. Must be overridden for backends with a batch API,
    async fn delete_many(&self, keys: &[&str]) -> Result<(), ObjectStorageError> {
        for key in keys {
            self.delete(key).await?;
        }
        Ok(())
    }

    /// Whether conditional creates are genuinely atomic on this backend. This should return a
    /// cached values, not make live calls on each invocation.
    fn conditional_write_support(&self) -> ConditionalWriteSupport;
}

/// A streaming multipart upload. Accumulate bytes with [`MultipartUpload::upload_part`]. Close
/// with [`MultipartUpload::complete`] or discard with [`MultipartUpload::abort`].
#[async_trait(?Send)]
pub trait MultipartUpload {
    async fn upload_part(&mut self, bytes: Bytes) -> Result<(), ObjectStorageError>;
    async fn complete(self: Box<Self>) -> Result<PutOutcome, ObjectStorageError>;
    async fn abort(self: Box<Self>) -> Result<(), ObjectStorageError>;
}

/// Called by the tiered uploader for each sealed segment to upload a local file to `key`:
/// uses a single [`ObjectStore::put`] when it fits in one part or a [`ObjectStore::put_multipart`]
/// to stream in `part_size` chunks.
/// This is backend-agnostic and reads via `compio::fs`.
///
/// Propagates local read/backend errors or returns [`ObjectStorageError::PartSizeTooSmall`] if a
/// multipart upload's `part_size` is below [`S3_MIN_PART_SIZE`].
pub async fn upload_file(
    store: &dyn ObjectStore,
    key: &str,
    path: &Path,
    part_size: u64,
) -> Result<PutOutcome, ObjectStorageError> {
    let file = File::open(path).await?;
    let size = file.metadata().await?.len();

    if size <= part_size {
        let buffer = Vec::with_capacity(size as usize);
        let (result, buffer) = file.read_to_end_at(buffer, 0).await.into();
        result?;
        return store.put(key, Bytes::from(buffer)).await;
    }

    if part_size < S3_MIN_PART_SIZE {
        return Err(ObjectStorageError::PartSizeTooSmall {
            got: part_size,
            min: S3_MIN_PART_SIZE,
        });
    }

    let mut upload = store.put_multipart(key, part_size).await?;
    let mut offset = 0u64;
    while offset < size {
        let this = part_size.min(size - offset);
        let buffer = Vec::with_capacity(this as usize);
        let (result, buffer) = file.read_exact_at(buffer, offset).await.into();
        result?;
        upload.upload_part(Bytes::from(buffer)).await?;
        offset += this;
    }
    upload.complete().await
}

#[cfg(test)]
mod contract {
    use crate::in_memory::InMemoryStorage;

    crate::object_store_contract_tests!(
        mod atomic,
        attr = compio::test,
        fixture = async { InMemoryStorage::new() }
    );

    crate::object_store_contract_tests!(
        mod best_effort,
        attr = compio::test,
        fixture = async { InMemoryStorage::best_effort() }
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::in_memory::InMemoryStorage;

    #[compio::test]
    async fn upload_file_uses_single_put_below_part_size() {
        let dir = std::env::temp_dir().join("iggy_object_storage_upload_small");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("small.bin");
        std::fs::write(&path, testing::pattern(1024)).unwrap();

        let store = InMemoryStorage::new();
        upload_file(&store, "small", &path, S3_MIN_PART_SIZE)
            .await
            .unwrap();

        assert_eq!(store.head("small").await.unwrap().size, 1024);
        let _ = std::fs::remove_file(&path);
    }

    #[compio::test]
    async fn upload_file_streams_multipart_above_part_size() {
        let dir = std::env::temp_dir().join("iggy_object_storage_upload_large");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("large.bin");
        let len = (S3_MIN_PART_SIZE * 2 + 123) as usize;
        std::fs::write(&path, testing::pattern(len)).unwrap();

        let store = InMemoryStorage::new();
        upload_file(&store, "large", &path, S3_MIN_PART_SIZE)
            .await
            .unwrap();

        assert_eq!(store.head("large").await.unwrap().size, len as u64);
        let got = store.get_range("large", 0..len as u64).await.unwrap();
        assert_eq!(got, testing::pattern(len));
        let _ = std::fs::remove_file(&path);
    }

    #[compio::test]
    async fn upload_file_rejects_undersized_part_size_for_multipart() {
        let dir = std::env::temp_dir().join("iggy_object_storage_upload_undersized");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("oversized.bin");
        // larger than part_size, so upload_file must go through the multipart path and hit the
        // floor check before ever calling store.put_multipart
        std::fs::write(&path, testing::pattern(200)).unwrap();

        let store = InMemoryStorage::new();
        let result = upload_file(&store, "oversized", &path, 100).await;
        let is_expected_error = matches!(
            result,
            Err(ObjectStorageError::PartSizeTooSmall { got: 100, .. })
        );
        assert!(
            is_expected_error,
            "expected PartSizeTooSmall, got {result:?}"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[compio::test]
    async fn delete_many_uses_default_per_key_loop() {
        let store = InMemoryStorage::new();
        store.put("a", testing::pattern(1)).await.unwrap();
        store.put("b", testing::pattern(1)).await.unwrap();

        store.delete_many(&["a", "b", "missing"]).await.unwrap();

        assert!(matches!(
            store.head("a").await,
            Err(ObjectStorageError::NotFound(_))
        ));
        assert!(matches!(
            store.head("b").await,
            Err(ObjectStorageError::NotFound(_))
        ));
    }

    #[compio::test]
    async fn default_matches_new() {
        let store = InMemoryStorage::default();
        assert_eq!(
            store.conditional_write_support(),
            ConditionalWriteSupport::Atomic
        );
    }
}
