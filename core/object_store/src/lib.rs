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

//! The object store behind tiered storage: one trait with five operations and one provider per
//! kind of store. Providers run on the shard that owns the partition, as compio tasks, and never
//! block the shard thread. Retries and timeouts belong to the caller.

mod backend;
mod body;
mod error;
pub mod fs;
mod offload;
mod resolver;
pub mod s3;

pub use backend::Backend;
pub use body::{PutBody, UploadPacer, file_crc32c};
pub use error::ObjectStoreError;

use bytes::Bytes;
use std::collections::BTreeSet;
use std::ops::Range;

/// Most keys one `list` page returns and one `delete` call accepts, S3's limit for both.
pub const MAX_KEYS_PER_REQUEST: usize = 1000;

/// Longest key accepted, in bytes, S3's limit.
pub const MAX_KEY_LEN: usize = 1024;

/// Most bytes of user metadata one object carries, keys and values together, S3's limit.
pub const MAX_METADATA_LEN: usize = 2048;

#[allow(async_fn_in_trait)]
pub trait ObjectStore {
    /// Stores exactly `body.len()` bytes under `key`. With `if_absent` an existing key fails
    /// with [`ObjectStoreError::PreconditionFailed`] and is left untouched, otherwise it is
    /// replaced. A failed put never leaves a partial object under `key`.
    async fn put(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError>;

    /// Reads `range` of the object. A range running past the end returns the bytes up to the
    /// end, one starting at or past the end fails with [`ObjectStoreError::InvalidRange`].
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, ObjectStoreError>;

    /// Returns the object's size, entity tag and user metadata without reading its body.
    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError>;

    /// Deletes up to [`MAX_KEYS_PER_REQUEST`] keys. A missing key counts as deleted, so a
    /// failed call can be retried whole.
    async fn delete(&self, keys: &[String]) -> Result<(), ObjectStoreError>;

    /// Lists up to [`MAX_KEYS_PER_REQUEST`] keys starting with `prefix`, in byte order. Pass
    /// the previous page's `next` to continue.
    async fn list(
        &self,
        prefix: &str,
        continuation: Option<&str>,
    ) -> Result<ListPage, ObjectStoreError>;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutOptions {
    /// Create the object only if no object exists under the key.
    pub if_absent: bool,
    /// User metadata. Keys are lowercase ASCII letters, digits and `-`, values printable ASCII
    /// without leading or trailing spaces, as every provider's headers carry them.
    pub metadata: Vec<(String, String)>,
    /// CRC32C of the body. The store verifies it before publishing the object and rejects the
    /// put with [`ObjectStoreError::ChecksumMismatch`] when the received bytes differ.
    pub crc32c: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub size: u64,
    pub etag: String,
    /// User metadata, sorted by key.
    pub metadata: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedObject {
    pub key: String,
    pub size: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListPage {
    pub objects: Vec<ListedObject>,
    /// Opaque continuation, `None` on the last page.
    pub next: Option<String>,
}

/// Keys are `/`-separated segments. A segment is never empty and never starts with `.`, which
/// keeps `.` and `..` out of filesystem paths and leaves dot-names free for provider scratch.
pub(crate) fn validate_key(key: &str) -> Result<(), ObjectStoreError> {
    let valid = !key.is_empty()
        && key.len() <= MAX_KEY_LEN
        && has_valid_characters(key)
        && key.split('/').all(is_valid_segment);
    if valid {
        Ok(())
    } else {
        Err(ObjectStoreError::InvalidKey(key.to_owned()))
    }
}

/// A prefix is a key whose last segment may be partial or empty.
pub(crate) fn validate_prefix(prefix: &str) -> Result<(), ObjectStoreError> {
    if prefix.is_empty() {
        return Ok(());
    }
    let mut segments: Vec<&str> = prefix.split('/').collect();
    let last = segments.pop().unwrap_or_default();
    let valid = prefix.len() <= MAX_KEY_LEN
        && has_valid_characters(prefix)
        && segments.into_iter().all(is_valid_segment)
        && !last.starts_with('.');
    if valid {
        Ok(())
    } else {
        Err(ObjectStoreError::InvalidKey(prefix.to_owned()))
    }
}

pub(crate) fn validate_metadata(metadata: &[(String, String)]) -> Result<(), ObjectStoreError> {
    let mut keys = BTreeSet::new();
    let mut total = 0;
    for (key, value) in metadata {
        let valid_key = !key.is_empty()
            && key
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
        let valid_value =
            value.trim() == value && value.bytes().all(|byte| (0x20..0x7f).contains(&byte));
        if !valid_key || !valid_value {
            return Err(ObjectStoreError::InvalidRequest(format!(
                "metadata entry {key:?} is not portable across providers"
            )));
        }
        if !keys.insert(key.as_str()) {
            return Err(ObjectStoreError::InvalidRequest(format!(
                "metadata key {key:?} repeats"
            )));
        }
        total += key.len() + value.len();
    }
    if total > MAX_METADATA_LEN {
        return Err(ObjectStoreError::InvalidRequest(format!(
            "metadata takes {total} bytes, more than {MAX_METADATA_LEN}"
        )));
    }
    Ok(())
}

fn has_valid_characters(value: &str) -> bool {
    !value.starts_with('/')
        && value
            .chars()
            .all(|character| !character.is_control() && character != '\\')
}

fn is_valid_segment(segment: &str) -> bool {
    !segment.is_empty() && !segment.starts_with('.')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_traversal_or_hidden_segments_when_validating_key_should_reject() {
        for key in [
            "", "/a", "a/", "a//b", "../a", "a/..", "a/.tmp", ".a", "a\\b", "a\nb",
        ] {
            assert!(validate_key(key).is_err(), "{key:?} should be rejected");
        }
        for key in ["a", "a/b", "1/2/3/00000000000000000000.log", "a-b_c.d"] {
            assert!(validate_key(key).is_ok(), "{key:?} should be accepted");
        }
    }

    #[test]
    fn given_partial_last_segment_when_validating_prefix_should_accept() {
        for prefix in ["", "a/", "a/b", "a/b/0000"] {
            assert!(
                validate_prefix(prefix).is_ok(),
                "{prefix:?} should be accepted"
            );
        }
        for prefix in ["/", "a//", "../", "a/.", "a/../b"] {
            assert!(
                validate_prefix(prefix).is_err(),
                "{prefix:?} should be rejected"
            );
        }
    }

    #[test]
    fn given_non_portable_metadata_when_validating_should_reject() {
        let entry = |key: &str, value: &str| vec![(key.to_owned(), value.to_owned())];
        assert!(validate_metadata(&entry("start-offset", "42")).is_ok());
        assert!(validate_metadata(&entry("Start", "42")).is_err());
        assert!(validate_metadata(&entry("start_offset", "42")).is_err());
        assert!(validate_metadata(&entry("start", " 42")).is_err());
        assert!(validate_metadata(&entry("start", "\u{e9}")).is_err());
        let repeated = [entry("a", "1"), entry("a", "2")].concat();
        assert!(validate_metadata(&repeated).is_err());
        assert!(validate_metadata(&entry("a", &"x".repeat(MAX_METADATA_LEN))).is_err());
    }
}
