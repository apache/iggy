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

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::io;
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::SystemTime;

use bytes::Bytes;
use futures::stream::{self, LocalBoxStream, Stream, StreamExt};

use crate::error::ObjectStoreError;

/// Most keys one `list` page returns and one `delete` call takes: S3's limit
/// for both.
pub const MAX_KEYS_PER_REQUEST: usize = 1000;

/// Longest key, in bytes: S3's limit.
pub const MAX_KEY_LEN: usize = 1024;

/// Most bytes of user metadata on one object, names and values together:
/// S3's limit.
pub const MAX_METADATA_LEN: usize = 2048;

/// An object's user metadata, by name.
///
/// [`validate_metadata`] keeps every entry portable: a name is lowercase ASCII
/// letters, digits and `-`, starting with a letter, and a value is printable
/// ASCII with no space at either end. A provider whose store takes no `-` in a
/// name, such as Azure Blob, maps it to `_` both ways.
pub type UserMetadata = BTreeMap<String, String>;

/// The five operations tiered storage needs from an object store.
///
/// Every provider refuses a key that [`validate_key`] refuses before any I/O,
/// so a key valid on one store is valid on all. Callers never use one key as
/// the directory of another, such as `a/b` and `a/b/c`, because a filesystem
/// store cannot hold both.
///
/// Retries and timeouts belong to the caller, which branches on
/// [`ObjectStoreError::class`]. A provider runs on the shard that owns it and
/// never blocks the shard thread. Dropping a call's future cancels the call.
pub trait ObjectStore {
    /// Stores `body` under `key` with the user metadata of `options`.
    ///
    /// The object appears whole or not at all. A failed or cancelled put
    /// leaves no part of the body under `key`, and an object that was there
    /// stays as it was. [`PutMode::IfAbsent`] refuses a key that holds an
    /// object. With a CRC32C in `options`, the store checks the bytes it
    /// received against it before it stores them.
    ///
    /// # Errors
    /// [`ObjectStoreError::PreconditionFailed`] when `IfAbsent` finds an
    /// object, [`ObjectStoreError::ChecksumMismatch`] when the CRC32C differs,
    /// [`ObjectStoreError::Body`] when the body fails or yields a length other
    /// than the one it declared, and any error the store returns.
    fn put(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> impl Future<Output = Result<(), ObjectStoreError>>;

    /// Reads `range` of the object under `key`.
    ///
    /// A range that runs past the end returns the bytes up to the end, so
    /// `0..u64::MAX` reads a whole object of unknown size. An empty object has
    /// no readable range. The bytes are held in memory, so the caller bounds
    /// the range.
    ///
    /// # Errors
    /// [`ObjectStoreError::NotFound`] without an object,
    /// [`ObjectStoreError::InvalidRange`] when `range` starts at or past the
    /// end, [`ObjectStoreError::InvalidRequest`] when `range` is empty, and
    /// any error the store returns.
    fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> impl Future<Output = Result<Bytes, ObjectStoreError>>;

    /// Returns what the store knows about the object under `key`, without
    /// its bytes.
    ///
    /// # Errors
    /// [`ObjectStoreError::NotFound`] without an object, and any error the
    /// store returns.
    fn head(&self, key: &str) -> impl Future<Output = Result<ObjectMeta, ObjectStoreError>>;

    /// Deletes the objects under up to [`MAX_KEYS_PER_REQUEST`] keys.
    ///
    /// A missing key counts as deleted, so a caller can send a failed batch
    /// again whole. A key that the store refuses alone goes to
    /// [`DeleteOutcome::failed`], and the other keys are still deleted.
    ///
    /// # Errors
    /// [`ObjectStoreError::InvalidRequest`] for too many keys, and an error
    /// that kept the store from taking the batch at all.
    fn delete(
        &self,
        keys: &[String],
    ) -> impl Future<Output = Result<DeleteOutcome, ObjectStoreError>>;

    /// Lists one page of the objects whose keys start with the request's
    /// prefix, in the byte order of their keys.
    ///
    /// A page holds at most [`ListRequest::page_len`] objects, and it can hold
    /// fewer when more follow. Following [`ListPage::next`] until it is `None`
    /// visits once every object that existed for the whole listing. An object
    /// put or deleted during the listing may or may not appear.
    ///
    /// # Errors
    /// [`ObjectStoreError::InvalidKey`] for a prefix that [`validate_prefix`]
    /// refuses, and any error the store returns.
    fn list(
        &self,
        request: &ListRequest<'_>,
    ) -> impl Future<Output = Result<ListPage, ObjectStoreError>>;
}

/// The bytes of one put: a declared length and the pieces that make it up.
///
/// A provider sends each piece as it comes, so a large body never sits in
/// memory whole.
pub struct PutBody {
    len: u64,
    pieces: LocalBoxStream<'static, io::Result<Bytes>>,
}

impl PutBody {
    /// A body held in memory.
    #[must_use]
    pub fn from_bytes(bytes: impl Into<Bytes>) -> Self {
        let bytes = bytes.into();
        Self {
            len: bytes.len() as u64,
            pieces: stream::iter([Ok(bytes)]).boxed_local(),
        }
    }

    /// A body of exactly `len` bytes that `pieces` yields in order.
    ///
    /// A stream that yields an error, or a total other than `len`, fails the
    /// put with [`ObjectStoreError::Body`], and the put stores nothing.
    #[must_use]
    pub fn from_stream(len: u64, pieces: impl Stream<Item = io::Result<Bytes>> + 'static) -> Self {
        Self {
            len,
            pieces: pieces.boxed_local(),
        }
    }

    #[must_use]
    pub const fn len(&self) -> u64 {
        self.len
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The pieces for a provider to send, checked against the declared
    /// length: the stream ends with an error instead of ending short or
    /// running past the length. Any other error is the body's own.
    pub fn into_pieces(self) -> LocalBoxStream<'static, io::Result<Bytes>> {
        ExactLen {
            pieces: self.pieces,
            remaining: self.len,
            done: false,
        }
        .boxed_local()
    }
}

impl fmt::Debug for PutBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PutBody")
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

struct ExactLen {
    pieces: LocalBoxStream<'static, io::Result<Bytes>>,
    remaining: u64,
    done: bool,
}

impl Stream for ExactLen {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        let failure = match ready!(self.pieces.poll_next_unpin(context)) {
            Some(Ok(piece)) => match self.remaining.checked_sub(piece.len() as u64) {
                Some(remaining) => {
                    self.remaining = remaining;
                    return Poll::Ready(Some(Ok(piece)));
                }
                None => io::Error::new(
                    io::ErrorKind::InvalidData,
                    "the body yields more bytes than its declared length",
                ),
            },
            Some(Err(error)) => error,
            None if self.remaining == 0 => {
                self.done = true;
                return Poll::Ready(None);
            }
            None => io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "the body ends {} bytes short of its declared length",
                    self.remaining
                ),
            ),
        };
        self.done = true;
        Poll::Ready(Some(Err(failure)))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PutMode {
    /// Replace any object under the key.
    #[default]
    Overwrite,
    /// Create the object only when no object exists under the key.
    IfAbsent,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutOptions {
    pub mode: PutMode,
    pub metadata: UserMetadata,
    /// CRC32C of the whole body. The store refuses the put when the bytes it
    /// received hash to another value.
    pub crc32c: Option<u32>,
}

/// What [`ObjectStore::head`] returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub size: u64,
    /// Changes when the object is replaced with other bytes. Compare it,
    /// never parse it.
    pub etag: String,
    pub last_modified: SystemTime,
    pub metadata: UserMetadata,
}

/// One page of a listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListRequest<'a> {
    /// Lists the keys that start with it. An empty prefix lists every key.
    pub prefix: &'a str,
    /// [`ListPage::next`] of the previous page, `None` for the first page.
    pub continuation: Option<&'a str>,
    /// The most objects the page holds, clamped as [`Self::page_len`] says.
    pub max_keys: usize,
}

impl<'a> ListRequest<'a> {
    /// The first page of the objects under `prefix`, as long as a page can be.
    #[must_use]
    pub const fn new(prefix: &'a str) -> Self {
        Self {
            prefix,
            continuation: None,
            max_keys: MAX_KEYS_PER_REQUEST,
        }
    }

    /// The page that follows the one whose [`ListPage::next`] is `continuation`.
    #[must_use]
    pub const fn after(self, continuation: &'a str) -> Self {
        Self {
            continuation: Some(continuation),
            ..self
        }
    }

    #[must_use]
    pub const fn with_max_keys(self, max_keys: usize) -> Self {
        Self { max_keys, ..self }
    }

    /// `max_keys` clamped to `1..=MAX_KEYS_PER_REQUEST`: the most objects a
    /// provider returns for this request.
    #[must_use]
    pub const fn page_len(&self) -> usize {
        if self.max_keys == 0 {
            1
        } else if self.max_keys > MAX_KEYS_PER_REQUEST {
            MAX_KEYS_PER_REQUEST
        } else {
            self.max_keys
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListPage {
    pub objects: Vec<ListedObject>,
    /// Opaque continuation, `None` on the last page.
    pub next: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedObject {
    pub key: String,
    pub size: u64,
    /// The same value [`ObjectStore::head`] reports.
    pub etag: String,
    pub last_modified: SystemTime,
}

/// What a batch delete left undone.
#[derive(Debug, Default)]
pub struct DeleteOutcome {
    /// The keys the store refused, each once, with the store's reason.
    pub failed: Vec<DeleteFailure>,
}

impl DeleteOutcome {
    /// Whether the store deleted every key of the batch.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.failed.is_empty()
    }
}

#[derive(Debug)]
pub struct DeleteFailure {
    pub key: String,
    pub error: ObjectStoreError,
}

/// Checks `key` against the rules every provider enforces.
///
/// A key is at most [`MAX_KEY_LEN`] bytes of `/`-separated segments. No
/// segment is empty or starts with `.`, which keeps `.` and `..` out of
/// filesystem paths and leaves dot-names free for a provider's scratch files.
/// No key holds a control character or `\`.
///
/// # Errors
/// [`ObjectStoreError::InvalidKey`] that names the broken rule.
pub fn validate_key(key: &str) -> Result<(), ObjectStoreError> {
    check_key(key, false).map_err(|reason| invalid_key(key, reason))
}

/// Checks a listing prefix: a key whose last segment can be partial or empty.
/// An empty prefix lists every key.
///
/// # Errors
/// [`ObjectStoreError::InvalidKey`] that names the broken rule.
pub fn validate_prefix(prefix: &str) -> Result<(), ObjectStoreError> {
    if prefix.is_empty() {
        return Ok(());
    }
    check_key(prefix, true).map_err(|reason| invalid_key(prefix, reason))
}

/// Checks that every entry is portable across providers and that all of them
/// fit in [`MAX_METADATA_LEN`] bytes.
///
/// # Errors
/// [`ObjectStoreError::InvalidRequest`] that names the broken rule.
pub fn validate_metadata(metadata: &UserMetadata) -> Result<(), ObjectStoreError> {
    let mut total = 0;
    for (name, value) in metadata {
        if !is_portable_name(name) {
            return Err(ObjectStoreError::InvalidRequest(format!(
                "metadata name {name:?} is not lowercase ASCII letters, digits and '-' \
                 starting with a letter"
            )));
        }
        if !is_portable_value(value) {
            return Err(ObjectStoreError::InvalidRequest(format!(
                "metadata value of {name:?} is not printable ASCII without a space at \
                 either end"
            )));
        }
        total += name.len() + value.len();
    }
    if total > MAX_METADATA_LEN {
        return Err(ObjectStoreError::InvalidRequest(format!(
            "metadata takes {total} bytes, more than {MAX_METADATA_LEN}"
        )));
    }
    Ok(())
}

/// Checks a range before a provider sends it.
///
/// # Errors
/// [`ObjectStoreError::InvalidRequest`] for an empty range.
pub fn validate_range(range: &Range<u64>) -> Result<(), ObjectStoreError> {
    if range.is_empty() {
        return Err(ObjectStoreError::InvalidRequest(format!(
            "range {}..{} is empty",
            range.start, range.end
        )));
    }
    Ok(())
}

/// Checks a delete batch: at most [`MAX_KEYS_PER_REQUEST`] keys, each valid.
///
/// # Errors
/// [`ObjectStoreError::InvalidRequest`] for too many keys and
/// [`ObjectStoreError::InvalidKey`] for the first invalid one.
pub fn validate_delete(keys: &[String]) -> Result<(), ObjectStoreError> {
    if keys.len() > MAX_KEYS_PER_REQUEST {
        return Err(ObjectStoreError::InvalidRequest(format!(
            "{} keys in one delete, at most {MAX_KEYS_PER_REQUEST}",
            keys.len()
        )));
    }
    keys.iter().try_for_each(|key| validate_key(key))
}

/// The rule `key` breaks. In a prefix, the last segment can be empty.
fn check_key(key: &str, prefix: bool) -> Result<(), &'static str> {
    if key.is_empty() {
        return Err("is empty");
    }
    if key.len() > MAX_KEY_LEN {
        return Err("is longer than 1024 bytes");
    }
    if key
        .chars()
        .any(|character| character.is_control() || character == '\\')
    {
        return Err("holds a control character or a backslash");
    }
    let mut segments = key.split('/').peekable();
    while let Some(segment) = segments.next() {
        if segment.starts_with('.') {
            return Err("has a segment that starts with '.'");
        }
        let last = segments.peek().is_none();
        if segment.is_empty() && !(prefix && last) {
            return Err("has an empty segment");
        }
    }
    Ok(())
}

fn invalid_key(key: &str, reason: &'static str) -> ObjectStoreError {
    ObjectStoreError::InvalidKey {
        key: key.to_owned(),
        reason,
    }
}

fn is_portable_name(name: &str) -> bool {
    name.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn is_portable_value(value: &str) -> bool {
    value.bytes().all(|byte| (b' '..=b'~').contains(&byte))
        && !value.starts_with(' ')
        && !value.ends_with(' ')
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use futures::stream::TryStreamExt;

    use super::*;

    fn drain(body: PutBody) -> io::Result<Vec<u8>> {
        block_on(
            body.into_pieces()
                .try_fold(Vec::new(), |mut collected, piece| async move {
                    collected.extend_from_slice(&piece);
                    Ok(collected)
                }),
        )
    }

    fn pieces(parts: &'static [&'static [u8]]) -> impl Stream<Item = io::Result<Bytes>> + 'static {
        stream::iter(parts.iter().map(|part| Ok(Bytes::from_static(part))))
    }

    #[test]
    fn given_pieces_matching_declared_length_when_draining_should_yield_them_in_order() {
        let body = PutBody::from_stream(6, pieces(&[b"ab", b"", b"cdef"]));

        assert_eq!(drain(body).unwrap(), b"abcdef");
    }

    #[test]
    fn given_short_stream_when_draining_should_end_with_unexpected_eof() {
        let body = PutBody::from_stream(7, pieces(&[b"ab", b"cdef"]));

        let error = drain(body).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn given_long_stream_when_draining_should_end_with_invalid_data() {
        let body = PutBody::from_stream(5, pieces(&[b"ab", b"cdef"]));

        let error = drain(body).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn given_failing_stream_when_draining_should_pass_its_error_through() {
        let failing = stream::iter([
            Ok(Bytes::from_static(b"ab")),
            Err(io::Error::other("disk read failed")),
        ]);
        let body = PutBody::from_stream(4, failing);

        let error = drain(body).unwrap_err();

        assert_eq!(error.to_string(), "disk read failed");
    }

    #[test]
    fn given_bytes_when_building_body_should_declare_their_length() {
        let body = PutBody::from_bytes(Bytes::from_static(b"segment"));

        assert_eq!(body.len(), 7);
        assert!(!body.is_empty());
        assert!(PutBody::from_bytes(Bytes::new()).is_empty());
        assert_eq!(drain(body).unwrap(), b"segment");
    }

    #[test]
    fn given_traversal_hidden_or_empty_segments_when_validating_key_should_refuse() {
        let too_long = "a".repeat(MAX_KEY_LEN + 1);
        for key in [
            "", "/a", "a/", "a//b", "../a", "a/..", "a/.tmp", ".a", "a\\b", "a\nb", &too_long,
        ] {
            assert!(validate_key(key).is_err(), "{key:?} should be refused");
        }
        let longest = "a".repeat(MAX_KEY_LEN);
        for key in [
            "a",
            "a/b",
            "iggy/1/2/3/4/00000000000000000000.log",
            "a-b_c.d",
            &longest,
        ] {
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
                "{prefix:?} should be refused"
            );
        }
    }

    #[test]
    fn given_non_portable_entries_when_validating_metadata_should_refuse() {
        let entry =
            |name: &str, value: &str| UserMetadata::from([(name.to_owned(), value.to_owned())]);
        for (name, value) in [("start-offset", "42"), ("a1", ""), ("a", "~ !")] {
            assert!(validate_metadata(&entry(name, value)).is_ok(), "{name:?}");
        }
        for (name, value) in [
            ("Start", "42"),
            ("start_offset", "42"),
            ("1start", "42"),
            ("", "42"),
            ("start", " 42"),
            ("start", "42 "),
            ("start", "\u{e9}"),
            ("start", "4\t2"),
        ] {
            assert!(
                validate_metadata(&entry(name, value)).is_err(),
                "{name:?} = {value:?}"
            );
        }
        let at_limit = entry("a", &"x".repeat(MAX_METADATA_LEN - 1));
        assert!(validate_metadata(&at_limit).is_ok());
        let over_limit = entry("a", &"x".repeat(MAX_METADATA_LEN));
        assert!(validate_metadata(&over_limit).is_err());
    }

    #[test]
    fn given_max_keys_out_of_bounds_when_paging_should_clamp() {
        assert_eq!(ListRequest::new("a/").with_max_keys(0).page_len(), 1);
        assert_eq!(ListRequest::new("a/").with_max_keys(7).page_len(), 7);
        assert_eq!(
            ListRequest::new("a/").with_max_keys(5000).page_len(),
            MAX_KEYS_PER_REQUEST
        );
    }

    #[test]
    fn given_too_many_keys_when_validating_delete_should_refuse() {
        let keys: Vec<String> = (0..=MAX_KEYS_PER_REQUEST)
            .map(|index| format!("a/{index}"))
            .collect();

        assert!(matches!(
            validate_delete(&keys),
            Err(ObjectStoreError::InvalidRequest(_))
        ));
        assert!(validate_delete(&keys[..MAX_KEYS_PER_REQUEST]).is_ok());
    }
}
