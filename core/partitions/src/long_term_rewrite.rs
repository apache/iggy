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

//! The rewrite of a placing partition's committed batches into a long-term
//! segment, by its primary.
//!
//! A rewrite reads the batches above the long-term floor the way a poll
//! does, so it gets them whole, with the messages their placements name. It
//! writes them back to back into a `.log`, with an `.index` beside it, in the
//! layout of a tiered segment, and stores both under the partition's own
//! prefix. A reader of the bucket then needs no placement and no fresh
//! object to read the partition.
//!
//! The segment ends where the rewrite stops: at the commit point, or once
//! it holds [`LONG_TERM_SEGMENT_BYTES`]. Another primary can have stored a
//! segment from the same offset that ends elsewhere, just before it lost
//! primacy. The store refuses to replace it, and the rewrite then reports
//! where that one ends, so the next attempt ends there too and writes the
//! same bytes.

use std::io;
use std::rc::Rc;

use compio::io::AsyncWriteAtExt;
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use iggy_binary_protocol::PrepareHeader;
use iggy_object_store::{
    KeyLayout, ManifestSegment, ObjectPrefix, ObjectStore, SegmentKey, SegmentMetadata,
    SegmentObject, TierExecutor, TierJobCancelled, TierJobClass, TierJobRequest, TierScope,
    TierScopeToken,
};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use server_common::iobuf::Frozen;
use server_common::send_messages::decode_prepare_batch_trusted;
use tracing::{debug, warn};

use crate::IggyPartition;
use crate::Partition;
use crate::iggy_index::IGGY_INDEX_SIZE;
use crate::journal::MessageLookup;
use crate::placed_read::PlacedReadPlan;
use crate::poll_plan::DiskFault;
use crate::segment_chain::{HashSource, SegmentRecord, hash_segment_file};
use crate::segment_upload::{SegmentUpload, UploadError, upload_segment};
use crate::state_transfer::STAGING_SUFFIX;

/// Bytes of batches at which a rewrite ends its segment. The rewrite holds
/// the batches of one segment in memory while it writes them.
pub const LONG_TERM_SEGMENT_BYTES: usize = 64 * 1024 * 1024;

/// Bytes of a segment between two entries of its index.
const INDEX_STRIDE_BYTES: u64 = 64 * 1024;

/// One rewrite, as the partition's owner captured it.
pub struct SegmentRewrite {
    pub prefix: ObjectPrefix,
    /// The first offset of the segment: the long-term floor plus one.
    pub start_offset: u64,
    /// The last offset the segment may hold. The commit point, or where a
    /// segment another primary stored from `start_offset` ends.
    pub up_to: u64,
    /// Whether the segment must end exactly at `up_to`.
    pub ends_at_up_to: bool,
    pub partition_dir: String,
    pub(crate) read: PlacedReadPlan,
}

#[derive(Debug, thiserror::Error)]
pub enum RewriteError {
    /// A batch could not be read, from the disk or from the object store.
    #[error("cannot read the batches to rewrite: {0:?}")]
    Read(DiskFault),
    /// The batches read are not the run of offsets the segment needs.
    #[error("the batches read from offset {expected} continue at {found:?}")]
    Gap { expected: u64, found: Option<u64> },
    #[error("cannot write the segment: {0}")]
    Io(#[from] io::Error),
    /// The store holds a segment from the same offset that ends at
    /// `end_offset`, which another primary stored.
    #[error("the store holds a segment from the same offset that ends at {end_offset}")]
    EndsElsewhere { end_offset: u64 },
    #[error(transparent)]
    Upload(UploadError),
}

/// What a rewrite ended as: the segment it stored, or why it stored none,
/// or a cancellation before it finished.
pub type RewriteOutcome = Result<Result<ManifestSegment, RewriteError>, TierJobCancelled>;

/// Runs the rewrites of a shard, behind a pointer, so the shard does not
/// carry the executor's type.
pub trait LongTermWriter {
    /// Run `rewrite` under `scope`. The rewrite stops when the scope is
    /// cancelled, as when its partition loses primacy.
    fn write(
        &self,
        rewrite: SegmentRewrite,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, RewriteOutcome>;
}

/// Rewrites through a tier executor, in its upload class.
pub struct ExecutorLongTermWriter<E> {
    executor: E,
    layout: KeyLayout,
}

impl<E> ExecutorLongTermWriter<E> {
    #[must_use]
    pub const fn new(executor: E, layout: KeyLayout) -> Self {
        Self { executor, layout }
    }

    /// The executor the rewrites go through.
    #[must_use]
    pub const fn executor(&self) -> &E {
        &self.executor
    }
}

impl<E: TierExecutor> LongTermWriter for ExecutorLongTermWriter<E> {
    fn write(
        &self,
        rewrite: SegmentRewrite,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, RewriteOutcome> {
        let layout = self.layout.clone();
        let request = TierJobRequest::new(TierJobClass::Upload, scope)
            .with_bytes(LONG_TERM_SEGMENT_BYTES as u64);
        self.executor
            .submit(request, move |store: Rc<E::Store>| async move {
                rewrite.run(&*store, &layout).await
            })
            .boxed_local()
    }
}

impl SegmentRewrite {
    /// Read the batches, write the segment's two files beside the
    /// partition's own, store them, and remove the files again.
    async fn run<S: ObjectStore>(
        self,
        store: &S,
        layout: &KeyLayout,
    ) -> Result<ManifestSegment, RewriteError> {
        let query = MessageLookup::Offset {
            offset: self.start_offset,
            count: u32::MAX,
            ceiling: self.up_to,
        };
        let batches = self
            .read
            .read(query, LONG_TERM_SEGMENT_BYTES)
            .await
            .map_err(RewriteError::Read)?;
        let built = BuiltSegment::of(self.start_offset, &batches)?;
        if self.ends_at_up_to && built.end_offset != self.up_to {
            return Err(RewriteError::Gap {
                expected: self.up_to.saturating_add(1),
                found: Some(built.end_offset.saturating_add(1)),
            });
        }
        let stem = format!("{}/{:0>20}.rewrite", self.partition_dir, self.start_offset);
        let log_path = format!("{stem}.log{STAGING_SUFFIX}");
        let index_path = format!("{stem}.index{STAGING_SUFFIX}");
        let stored = self
            .store(store, layout, &built, &batches, &log_path, &index_path)
            .await;
        // Left behind, the files are swept at the next boot.
        let _ = compio::fs::remove_file(&log_path).await;
        let _ = compio::fs::remove_file(&index_path).await;
        stored
    }

    async fn store<S: ObjectStore>(
        &self,
        store: &S,
        layout: &KeyLayout,
        built: &BuiltSegment,
        batches: &[Frozen<4096>],
        log_path: &str,
        index_path: &str,
    ) -> Result<ManifestSegment, RewriteError> {
        write_log(log_path, batches).await?;
        write_file(index_path, built.index.clone()).await?;
        let hashes = hash_segment_file(log_path, built.size).await?;
        let record = SegmentRecord {
            start_offset: self.start_offset,
            end_offset: built.end_offset,
            start_timestamp: built.start_timestamp,
            end_timestamp: built.end_timestamp,
            max_timestamp: built.max_timestamp,
            size: built.size,
            incarnation: 0,
            hash: hashes.blake3,
            crc32c: hashes.crc32c,
            source: HashSource::Disk,
        };
        let upload = SegmentUpload {
            prefix: self.prefix,
            record,
            log_path: log_path.to_owned(),
            index_path: index_path.to_owned(),
        };
        match upload_segment(store, layout, &upload).await {
            Ok(()) => Ok(ManifestSegment {
                start_offset: record.start_offset,
                end_offset: record.end_offset,
                start_timestamp: record.start_timestamp,
                end_timestamp: record.end_timestamp,
                max_timestamp: record.max_timestamp,
                size: record.size,
                crc32c: record.crc32c,
                hash: record.hash,
            }),
            Err(UploadError::Diverged { key }) => {
                Err(self.ends_elsewhere(store, layout, key).await)
            }
            Err(error) => Err(RewriteError::Upload(error)),
        }
    }

    /// Why the store refused the segment: where the one it holds ends, when
    /// its metadata says so, and the refusal itself otherwise.
    async fn ends_elsewhere<S: ObjectStore>(
        &self,
        store: &S,
        layout: &KeyLayout,
        key: String,
    ) -> RewriteError {
        let log_key = layout.segment_key(&SegmentKey {
            prefix: self.prefix,
            start_offset: self.start_offset,
            object: SegmentObject::Log,
        });
        let stored = store
            .head(&log_key)
            .await
            .ok()
            .and_then(|meta| SegmentMetadata::try_from(&meta.metadata).ok())
            .filter(|stored| stored.start_offset == self.start_offset);
        stored.map_or(
            RewriteError::Upload(UploadError::Diverged { key }),
            |stored| RewriteError::EndsElsewhere {
                end_offset: stored.end_offset,
            },
        )
    }
}

/// What a run of whole batches makes as one segment.
struct BuiltSegment {
    end_offset: u64,
    start_timestamp: u64,
    end_timestamp: u64,
    max_timestamp: u64,
    size: u64,
    index: Vec<u8>,
}

impl BuiltSegment {
    /// The segment of `batches`, which must hold every offset from
    /// `start_offset` on without a gap.
    fn of(start_offset: u64, batches: &[Frozen<4096>]) -> Result<Self, RewriteError> {
        let mut next_offset = start_offset;
        let mut built: Option<Self> = None;
        let mut position = 0u64;
        let mut indexed_at: Option<u64> = None;
        let mut index = Vec::new();
        for batch in batches {
            let header = decode_prepare_batch_trusted(batch.as_slice())
                .map_err(|_| RewriteError::Read(DiskFault::Unreadable))?
                .header;
            if header.base_offset != next_offset || header.message_count == 0 {
                return Err(RewriteError::Gap {
                    expected: next_offset,
                    found: Some(header.base_offset),
                });
            }
            if indexed_at.is_none_or(|indexed| position - indexed >= INDEX_STRIDE_BYTES) {
                indexed_at = Some(position);
                for field in [header.base_offset, header.base_timestamp, position] {
                    index.extend_from_slice(&field.to_le_bytes());
                }
            }
            let end_offset = header.base_offset + u64::from(header.message_count) - 1;
            let timestamp = header.base_timestamp;
            position += header.batch_length;
            next_offset = end_offset + 1;
            built = Some(built.map_or(
                Self {
                    end_offset,
                    start_timestamp: timestamp,
                    end_timestamp: timestamp,
                    max_timestamp: timestamp,
                    size: position,
                    index: Vec::new(),
                },
                |previous| Self {
                    end_offset,
                    end_timestamp: timestamp,
                    max_timestamp: previous.max_timestamp.max(timestamp),
                    size: position,
                    ..previous
                },
            ));
        }
        debug_assert_eq!(index.len() % IGGY_INDEX_SIZE, 0);
        built
            .map(|built| Self { index, ..built })
            .ok_or(RewriteError::Gap {
                expected: start_offset,
                found: None,
            })
    }
}

/// Write the batches of `batches` back to back into a new file at `path`,
/// without the prepare header each one carries, and sync it.
async fn write_log(path: &str, batches: &[Frozen<4096>]) -> io::Result<()> {
    let mut file = compio::fs::File::create(path).await?;
    let mut position = 0u64;
    for batch in batches {
        let body = batch.as_slice()[size_of::<PrepareHeader>()..].to_vec();
        let len = body.len() as u64;
        let compio::BufResult(written, _) = file.write_all_at(body, position).await;
        written?;
        position += len;
    }
    file.sync_all().await
}

async fn write_file(path: &str, bytes: Vec<u8>) -> io::Result<()> {
    let mut file = compio::fs::File::create(path).await?;
    let compio::BufResult(written, _) = file.write_all_at(bytes, 0).await;
    written?;
    file.sync_all().await
}

/// What a replica keeps of its rewrites while it is the primary of a
/// partition that places its batches. Nothing here is persisted.
#[derive(Default)]
pub struct LongTermRewrites {
    /// The view this state belongs to, `None` until the replica acts as the
    /// primary.
    view: Option<u32>,
    /// The rewrite in flight: its number and the scope whose end stops it.
    running: Option<(u64, TierScope)>,
    /// A segment the store holds that the long-term log does not yet. Its
    /// command is sent again until the log ends with it.
    stored: Option<ManifestSegment>,
    /// Where the segment from the floor on must end: where the one another
    /// primary stored ends.
    ends_at: Option<u64>,
    /// Number of the last rewrite this partition started.
    jobs: u64,
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// Start the rewrite of the committed batches above the long-term
    /// floor, when this replica is the primary of a partition that places
    /// its batches, none is in flight, and a batch is there to rewrite. The
    /// caller runs it under the returned scope and hands the result to
    /// [`Self::finish_segment_rewrite`] with the returned number.
    ///
    /// A replica that is not the primary cancels the rewrite it started, so
    /// a pass over a demoted replica is what stops it.
    pub fn begin_segment_rewrite(&mut self) -> Option<(u64, SegmentRewrite, TierScopeToken)> {
        if !self.places_batches() || !self.state_machines_current() {
            self.long_term_rewrites = LongTermRewrites::default();
            return None;
        }
        let floor = self.long_term_floor();
        let offsets = self.offsets();
        let committed = self
            .holds_committed_messages()
            .then_some(offsets.commit_offset);
        let partition_dir = self.partition_dir()?;
        let rewrites = self.long_term_rewrites_of_this_view();
        if rewrites
            .stored
            .is_some_and(|stored| floor >= Some(stored.end_offset))
        {
            rewrites.stored = None;
        }
        if rewrites.running.is_some() || rewrites.stored.is_some() {
            return None;
        }
        let start_offset = floor.map_or(0, |floor| floor.saturating_add(1));
        let committed = committed.filter(|committed| *committed >= start_offset)?;
        let (up_to, ends_at_up_to) = match rewrites.ends_at {
            // The other primary rewrote batches this replica has yet to
            // commit, which it will.
            Some(end) if end > committed => return None,
            Some(end) if end >= start_offset => (end, true),
            // The log moved past that segment since.
            Some(_) | None => (committed, false),
        };
        let scope = TierScope::new();
        let token = scope.token();
        rewrites.jobs += 1;
        let id = rewrites.jobs;
        rewrites.running = Some((id, scope));
        let rewrite = SegmentRewrite {
            prefix: crate::object_prefix(self.namespace(), self.created_revision()),
            start_offset,
            up_to,
            ends_at_up_to,
            partition_dir,
            read: self.placed_read_plan(None),
        };
        Some((id, rewrite, token))
    }

    /// Take the result of the rewrite numbered `id`, and return the segment
    /// it stored, which the caller commits to the long-term log. A result of
    /// a rewrite this partition does not wait for, or one that arrives after
    /// primacy moved, changes nothing. A rewrite that failed is repeated by
    /// a later pass.
    pub fn finish_segment_rewrite(
        &mut self,
        id: u64,
        outcome: RewriteOutcome,
    ) -> Option<ManifestSegment> {
        let awaited = self
            .long_term_rewrites
            .running
            .as_ref()
            .is_some_and(|(running, _)| *running == id);
        if !awaited {
            return None;
        }
        if !self.state_machines_current()
            || self.long_term_rewrites.view != Some(self.consensus().view())
        {
            self.long_term_rewrites = LongTermRewrites::default();
            return None;
        }
        let namespace_raw = self.namespace().inner();
        let rewrites = &mut self.long_term_rewrites;
        rewrites.running = None;
        match outcome {
            Ok(Ok(segment)) => {
                rewrites.stored = Some(segment);
                rewrites.ends_at = None;
                debug!(
                    namespace_raw,
                    start_offset = segment.start_offset,
                    end_offset = segment.end_offset,
                    size = segment.size,
                    "stored a long-term segment of the partition"
                );
                Some(segment)
            }
            Ok(Err(RewriteError::EndsElsewhere { end_offset })) => {
                rewrites.ends_at = Some(end_offset);
                None
            }
            Ok(Err(error)) => {
                warn!(
                    namespace_raw,
                    %error,
                    "rewriting the partition's batches into a long-term segment failed and is tried again"
                );
                None
            }
            Err(TierJobCancelled) => None,
        }
    }

    /// The segment the store holds that the long-term log does not end with
    /// yet, whose command the primary sends until it does.
    #[must_use]
    pub fn unpublished_segment(&self) -> Option<ManifestSegment> {
        let rewrites = &self.long_term_rewrites;
        rewrites.stored.filter(|stored| {
            rewrites.view == Some(self.consensus().view())
                && self.state_machines_current()
                && self.long_term_floor() < Some(stored.end_offset)
        })
    }

    /// The rewrite state of the current view. What an earlier view left is
    /// dropped, and its rewrite cancelled.
    fn long_term_rewrites_of_this_view(&mut self) -> &mut LongTermRewrites {
        let view = self.consensus().view();
        if self.long_term_rewrites.view != Some(view) {
            self.long_term_rewrites = LongTermRewrites {
                view: Some(view),
                ..LongTermRewrites::default()
            };
        }
        &mut self.long_term_rewrites
    }
}
