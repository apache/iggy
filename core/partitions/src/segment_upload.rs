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

//! The upload of one sealed segment to the object store.
//!
//! The job reads the segment through its own descriptors, off the partition's
//! I/O lane. Before it sends a byte it reads the `.log` once, checks every
//! batch against its checksum and the whole file against the hashes the chain
//! records, so a media fault on the primary fails the upload instead of
//! reaching the store. The store then checks the bytes it receives against
//! the recorded CRC32C, which covers the second read.

use std::io;

use bytes::Bytes;
use compio::io::AsyncReadAtExt;
use futures::future::LocalBoxFuture;
use futures::{FutureExt, Stream};
use iggy_binary_protocol::batch::{BATCH_HEADER_SIZE, BatchHeader};
use iggy_object_store::{
    ErrorClass, KeyLayout, ObjectPrefix, ObjectStore, ObjectStoreError, PutBody, PutMode,
    PutOptions, SegmentKey, SegmentMetadata, SegmentObject, TierExecutor, TierJobCancelled,
    TierJobClass, TierJobRequest, TierScopeToken, UserMetadata,
};
use server_common::send_messages::decode_batch_slice;

use crate::iggy_index::IGGY_INDEX_SIZE;
use crate::remote_segments::IndexCheck;
use crate::segment_chain::{SegmentHasher, SegmentRecord};
use crate::segment_recovery::MAX_RECOVERABLE_BATCH_BYTES;
use crate::state_transfer::{ChunkHasher, hash_segment_range};

/// Bytes of a file that one piece of a put body holds.
const UPLOAD_PIECE_LEN: usize = 1 << 20;

/// A sealed segment to upload, as the partition's owner captured it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentUpload {
    pub prefix: ObjectPrefix,
    /// The segment's chain record: its bounds, its size and the hashes its
    /// bytes must have.
    pub record: SegmentRecord,
    pub log_path: String,
    pub index_path: String,
}

/// One start of an upload by a partition. A segment whose upload was
/// cancelled is started again under another attempt, so the attempt, and not
/// the segment, says which upload a result belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentUploadAttempt(pub(crate) u64);

/// An upload a partition started: what to upload, the scope to run it under,
/// and the attempt its result is handed back with.
pub type StartedSegmentUpload = (SegmentUpload, TierScopeToken, SegmentUploadAttempt);

#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    #[error("cannot read the segment: {0}")]
    Io(#[from] io::Error),
    /// The primary's own copy is not what was committed, so nothing was sent.
    #[error("the segment at offset {start_offset} does not match its record: {reason}")]
    MediaFault {
        start_offset: u64,
        reason: &'static str,
    },
    /// The store holds another object under the segment's key.
    #[error("the store holds a different object under {key:?}")]
    Diverged { key: String },
    #[error(transparent)]
    Store(ObjectStoreError),
}

impl UploadError {
    /// Whether the same upload can succeed later without anyone repairing the
    /// segment or the store.
    #[must_use]
    pub const fn is_transient(&self) -> bool {
        match self {
            Self::Store(error) => matches!(error.class(), ErrorClass::Transient),
            Self::Io(_) | Self::MediaFault { .. } | Self::Diverged { .. } => false,
        }
    }
}

/// What an upload ended as: the job's own result, or a cancellation before it
/// finished.
pub type UploadOutcome = Result<Result<(), UploadError>, TierJobCancelled>;

/// Runs the uploads of a shard, behind a pointer, so the shard does not carry
/// the executor's type.
pub trait SegmentUploader {
    /// Upload `upload` under `scope`. The upload stops when the scope is
    /// cancelled, as when its partition loses primacy.
    fn upload(
        &self,
        upload: SegmentUpload,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, UploadOutcome>;
}

/// Uploads through a tier executor, in its upload class, so its caps bound
/// how many segments and bytes are in flight to the store.
pub struct ExecutorUploader<E> {
    executor: E,
    layout: KeyLayout,
}

impl<E> ExecutorUploader<E> {
    #[must_use]
    pub const fn new(executor: E, layout: KeyLayout) -> Self {
        Self { executor, layout }
    }

    /// The executor the uploads go through.
    #[must_use]
    pub const fn executor(&self) -> &E {
        &self.executor
    }
}

impl<E: TierExecutor> SegmentUploader for ExecutorUploader<E> {
    fn upload(
        &self,
        upload: SegmentUpload,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, UploadOutcome> {
        let layout = self.layout.clone();
        let request =
            TierJobRequest::new(TierJobClass::Upload, scope).with_bytes(upload.record.size);
        self.executor
            .submit(request, move |store| async move {
                upload_segment(&*store, &layout, &upload).await
            })
            .boxed_local()
    }
}

/// Store the `.log` and then the `.index` of `upload`.
///
/// An object that is there already counts as stored when it is the same one:
/// a `.log` of the recorded size and checksum, and an `.index` that holds
/// whole entries. A retried upload and one that a new primary repeats both
/// meet their own objects this way.
///
/// # Errors
/// [`UploadError::MediaFault`] when the local files are not what the chain
/// records, [`UploadError::Diverged`] when the store holds something else
/// under a key, and the store's own error otherwise.
pub async fn upload_segment<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    upload: &SegmentUpload,
) -> Result<(), UploadError> {
    let record = &upload.record;
    let fault = |reason| UploadError::MediaFault {
        start_offset: record.start_offset,
        reason,
    };
    let mut check = LogCheck::new(record.size);
    hash_segment_range(&upload.log_path, 0, record.size, &mut check, None).await?;
    check.finish().map_err(fault)?;
    let hashes = check.hasher.finalize();
    if (hashes.blake3, hashes.crc32c) != (record.hash, record.crc32c) {
        return Err(fault("its bytes hash to something else"));
    }
    let index_len = compio::fs::metadata(&upload.index_path).await?.len();
    let mut index = IndexFileCheck::new(record);
    hash_segment_range(&upload.index_path, 0, index_len, &mut index, None).await?;
    if index_len == 0 || !index.is_whole() {
        return Err(fault("its index does not describe it"));
    }

    let key = |object| {
        layout.segment_key(&SegmentKey {
            prefix: upload.prefix,
            start_offset: record.start_offset,
            object,
        })
    };
    let log_key = key(SegmentObject::Log);
    let metadata = UserMetadata::from(&SegmentMetadata {
        start_offset: record.start_offset,
        end_offset: record.end_offset,
        start_timestamp: record.start_timestamp,
        end_timestamp: record.end_timestamp,
        max_timestamp: record.max_timestamp,
        size: record.size,
        hash: record.hash,
    });
    let stored = put_file(
        store,
        &log_key,
        &upload.log_path,
        record.size,
        metadata,
        record.crc32c,
    )
    .await;
    match stored {
        Ok(()) => {}
        Err(ObjectStoreError::PreconditionFailed { .. }) => {
            let held = store.head(&log_key).await.map_err(UploadError::Store)?;
            let same_bytes = held.crc32c.map_or_else(
                || {
                    SegmentMetadata::try_from(&held.metadata)
                        .is_ok_and(|metadata| metadata.hash == record.hash)
                },
                |crc32c| crc32c == record.crc32c,
            );
            if held.size != record.size || !same_bytes {
                return Err(UploadError::Diverged { key: log_key });
            }
        }
        // The bytes changed between the check and the send.
        Err(ObjectStoreError::ChecksumMismatch { .. }) => {
            return Err(fault("its bytes changed while they were sent"));
        }
        Err(error) => return Err(UploadError::Store(error)),
    }

    let index_key = key(SegmentObject::Index);
    let stored = put_file(
        store,
        &index_key,
        &upload.index_path,
        index_len,
        UserMetadata::new(),
        index.crc32c,
    )
    .await;
    match stored {
        Ok(()) => Ok(()),
        // Sparse indexes differ between replicas, so an index another primary
        // stored only has to be one.
        Err(ObjectStoreError::PreconditionFailed { .. }) => {
            let held = store.head(&index_key).await.map_err(UploadError::Store)?;
            if held.size == 0 || held.size % IGGY_INDEX_SIZE as u64 != 0 {
                return Err(UploadError::Diverged { key: index_key });
            }
            Ok(())
        }
        Err(error) => Err(UploadError::Store(error)),
    }
}

/// Put the first `len` bytes of the file at `path` under `key`, unless an
/// object is there already.
async fn put_file<S: ObjectStore>(
    store: &S,
    key: &str,
    path: &str,
    len: u64,
    metadata: UserMetadata,
    crc32c: u32,
) -> Result<(), ObjectStoreError> {
    let file = compio::fs::File::open(path)
        .await
        .map_err(ObjectStoreError::Body)?;
    let options = PutOptions {
        mode: PutMode::IfAbsent,
        metadata,
        crc32c: Some(crc32c),
    };
    store
        .put(key, PutBody::from_stream(len, pieces(file, len)), &options)
        .await
}

/// The first `len` bytes of `file`, a piece at a time.
fn pieces(file: compio::fs::File, len: u64) -> impl Stream<Item = io::Result<Bytes>> + 'static {
    futures::stream::try_unfold((file, 0_u64), move |(file, position)| async move {
        if position >= len {
            return Ok(None);
        }
        #[allow(clippy::cast_possible_truncation)]
        let want = UPLOAD_PIECE_LEN.min((len - position) as usize);
        let compio::BufResult(read, piece) =
            file.read_exact_at(Vec::with_capacity(want), position).await;
        read?;
        Ok(Some((Bytes::from(piece), (file, position + want as u64))))
    })
}

/// Takes a `.log` in order: its hashes, and every batch against its own
/// checksum.
struct LogCheck {
    hasher: SegmentHasher,
    /// The bytes of the batch that the chunks so far have not completed.
    partial: Vec<u8>,
    /// The bytes of the segment after the batches checked so far.
    unchecked: u64,
    fault: Option<&'static str>,
}

impl LogCheck {
    /// A check of a segment of `size` bytes.
    fn new(size: u64) -> Self {
        Self {
            hasher: SegmentHasher::default(),
            partial: Vec::new(),
            unchecked: size,
            fault: None,
        }
    }

    /// Fails when a batch is not a whole, checksummed batch of the segment,
    /// or the file ends inside one.
    const fn finish(&self) -> Result<(), &'static str> {
        match self.fault {
            Some(fault) => Err(fault),
            None if self.partial.is_empty() => Ok(()),
            None => Err("it ends inside a batch"),
        }
    }

    fn fail(&mut self, fault: &'static str) {
        self.fault = Some(fault);
        self.partial = Vec::new();
    }
}

impl ChunkHasher for LogCheck {
    fn update(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
        if self.fault.is_some() {
            return;
        }
        self.partial.extend_from_slice(bytes);
        let mut at = 0;
        while let Some(header) = self.partial.get(at..at + BATCH_HEADER_SIZE) {
            let Ok(header) = BatchHeader::decode(header) else {
                return self.fail("a batch does not match its checksum");
            };
            // Checked before the batch is waited for: a length that no batch
            // has would otherwise keep every byte up to it in memory.
            if header.batch_length > self.unchecked.min(MAX_RECOVERABLE_BATCH_BYTES) {
                return self.fail("a batch is longer than a batch can be");
            }
            let Some(batch) = self.partial.get(at..at + header.total_size()) else {
                break;
            };
            if decode_batch_slice(batch).is_err() {
                return self.fail("a batch does not match its checksum");
            }
            at += header.total_size();
            self.unchecked -= header.batch_length;
        }
        self.partial.drain(..at);
    }
}

/// Takes an `.index` in order: its CRC32C, and whether its entries ascend
/// inside the segment, as a reader of the object checks them.
struct IndexFileCheck {
    check: IndexCheck,
    crc32c: u32,
    partial: Vec<u8>,
    valid: bool,
}

impl IndexFileCheck {
    const fn new(record: &SegmentRecord) -> Self {
        Self {
            check: IndexCheck::new(record.start_offset, record.size),
            crc32c: 0,
            partial: Vec::new(),
            valid: true,
        }
    }

    const fn is_whole(&self) -> bool {
        self.valid && self.partial.is_empty()
    }
}

impl ChunkHasher for IndexFileCheck {
    fn update(&mut self, bytes: &[u8]) {
        self.crc32c = crc32c::crc32c_append(self.crc32c, bytes);
        self.partial.extend_from_slice(bytes);
        let whole = self.partial.len() - self.partial.len() % IGGY_INDEX_SIZE;
        self.valid &= self.check.accept(&self.partial[..whole]);
        self.partial.drain(..whole);
    }
}
