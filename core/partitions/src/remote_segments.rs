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

//! Reads of segments whose `.log` left the disk.
//!
//! A poll plan names a remote segment by an object and a byte range in it,
//! and the walk reads that range with ranged GETs straight from the store.
//! The segment's sparse index is the local `.index` while one is usable, and
//! otherwise the `.index` object, fetched once and kept on disk.
//!
//! No partition is handed a store yet, so nothing in production reads one.

use std::cell::{Cell, RefCell};
use std::ops::Range;
use std::rc::Rc;

use compio::io::AsyncWriteAtExt;
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use iggy_object_store::{
    ErrorClass, KeyLayout, ObjectRange, ObjectStore, ObjectStoreError, SegmentKey, SegmentObject,
};
use server_common::fatal::NoteDescriptorExhaustion;
use server_common::iobuf::{Frozen, Owned};
use server_common::poll::PollHistoryId;
use tracing::{error, warn};

use crate::iggy_index::IGGY_INDEX_SIZE;
use crate::io::PartitionIoQuiescence;
use crate::poll_plan::DiskFault;
use crate::state_transfer::{STAGING_SUFFIX, fsync_dir};

/// Bytes of an index object that one GET asks for: a whole number of entries,
/// so every response can be checked on its own.
const INDEX_FETCH_CHUNK: u64 = (IGGY_INDEX_SIZE as u64) * 32 * 1024;

/// The ranged read of an [`ObjectStore`], behind a pointer, so a partition
/// does not carry the provider's type.
pub trait RemoteObjects {
    /// See [`ObjectStore::get_range`].
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
    ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>>;
}

impl<S: ObjectStore> RemoteObjects for S {
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
    ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>> {
        ObjectStore::get_range(self, key, range).boxed_local()
    }
}

/// The object that holds a segment's bytes and where they sit in it.
///
/// A whole-segment object holds them at `0..size`. The range is part of the
/// source so that data stored in objects other than whole segments reads
/// through the same plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteRange {
    pub key: SegmentKey,
    pub range: Range<u64>,
}

impl RemoteRange {
    /// Bytes of segment data the range holds.
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.range.end - self.range.start
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.range.end == self.range.start
    }
}

/// Where the shard reads remote segments from.
pub struct RemoteSegments {
    objects: Rc<dyn RemoteObjects>,
    layout: KeyLayout,
}

impl RemoteSegments {
    #[must_use]
    pub fn new(objects: Rc<dyn RemoteObjects>, layout: KeyLayout) -> Self {
        Self { objects, layout }
    }

    /// Exactly `len` bytes of the segment in `source`, from byte `position`
    /// of the segment.
    ///
    /// An error the store may not repeat is retryable. Anything else is not:
    /// a missing object, a refusal, or fewer bytes than the segment holds.
    pub(crate) async fn read(
        &self,
        source: &RemoteRange,
        position: u64,
        len: usize,
    ) -> Result<Frozen<4096>, DiskFault> {
        let key = self.layout.segment_key(&source.key);
        let start = source.range.start + position;
        let end = start + len as u64;
        if end > source.range.end {
            return Err(DiskFault::Unreadable);
        }
        let object = self
            .objects
            .get_range(&key, start..end)
            .await
            .map_err(|error| fault(&key, &error))?;
        if object.bytes.len() != len {
            error!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                key,
                start,
                asked = len,
                returned = object.bytes.len(),
                "remote poll: the object is shorter than its segment"
            );
            return Err(DiskFault::Unreadable);
        }
        Ok(Frozen::from(Owned::<4096>::copy_from_slice(&object.bytes)))
    }

    /// Fetch the `.index` object next to the `.log` in `log` and put it at
    /// `index_path`, whole or not at all.
    ///
    /// The object's own size says how long the index is. Every entry has to
    /// sit inside the segment and none may step back, so an index that would
    /// send a walk outside the segment's bytes is refused and nothing is
    /// written.
    ///
    /// Nothing is kept either once `fetch` no longer stands for the segment
    /// the plan saw: the path then belongs to another segment or to nothing.
    pub(crate) async fn fetch_index(
        &self,
        log: &RemoteRange,
        start_offset: u64,
        partition_dir: &str,
        index_path: &str,
        fetch: &IndexFetch<'_>,
    ) -> Result<(), DiskFault> {
        let key = self.layout.segment_key(&SegmentKey {
            object: SegmentObject::Index,
            ..log.key
        });
        let staging = format!("{index_path}.fetch{STAGING_SUFFIX}");
        let written = self
            .stage_index(&key, log.len(), start_offset, &staging)
            .await;
        if written.is_err() {
            // Best effort: the staging sweeps remove what this leaves behind.
            let _ = compio::fs::remove_file(&staging).await;
            return written;
        }
        if !fetch.may_publish() {
            let _ = compio::fs::remove_file(&staging).await;
            return Err(DiskFault::Retryable);
        }
        let renamed = compio::fs::rename(&staging, index_path).await;
        if renamed.is_ok() && fetch.segment_retired() {
            // Retention unlinked the segment's files while the rename was in
            // flight, and may have looked for this one before it existed.
            let _ = compio::fs::remove_file(index_path).await;
            return Err(DiskFault::Retryable);
        }
        let renamed = match renamed {
            Ok(()) => fsync_dir(partition_dir).await,
            Err(error) => Err(error),
        };
        renamed.map_err(|error| {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                index_path,
                %error,
                "remote poll: could not keep the fetched index"
            );
            DiskFault::Retryable
        })
    }

    async fn stage_index(
        &self,
        key: &str,
        segment_size: u64,
        start_offset: u64,
        staging: &str,
    ) -> Result<(), DiskFault> {
        let mut file = compio::fs::File::create(staging)
            .await
            .note_descriptor_exhaustion(|| format!("creating {staging}"))
            .map_err(|error| local_fault(staging, &error))?;
        let mut check = IndexCheck::new(start_offset, segment_size);
        let mut at = 0;
        let mut whole: Option<(String, u64)> = None;
        loop {
            let object = self
                .objects
                .get_range(key, at..at + INDEX_FETCH_CHUNK)
                .await
                .map_err(|error| fault(key, &error))?;
            let (etag, size) = whole.get_or_insert_with(|| (object.etag.clone(), object.size));
            if *etag != object.etag || *size != object.size {
                // Another uploader replaced the object between two GETs. The
                // next poll fetches the new one whole.
                return Err(DiskFault::Retryable);
            }
            let size = *size;
            let expected = (size - at).min(INDEX_FETCH_CHUNK);
            if object.bytes.len() as u64 != expected || !check.accept(&object.bytes) {
                error!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    key,
                    size,
                    at,
                    "remote poll: refusing an index object that does not describe its segment"
                );
                return Err(DiskFault::Unreadable);
            }
            let compio::BufResult(result, _) = file.write_all_at(object.bytes, at).await;
            result.map_err(|error| local_fault(staging, &error))?;
            at += expected;
            if at == size {
                break;
            }
        }
        file.sync_all()
            .await
            .map_err(|error| local_fault(staging, &error))
    }
}

/// Checks index entries as they arrive: offsets and positions never step
/// back, no offset is before the segment and no position is past its bytes.
struct IndexCheck {
    segment_size: u64,
    last_offset: u64,
    last_position: u64,
}

impl IndexCheck {
    const fn new(start_offset: u64, segment_size: u64) -> Self {
        Self {
            segment_size,
            last_offset: start_offset,
            last_position: 0,
        }
    }

    fn accept(&mut self, entries: &[u8]) -> bool {
        let (entries, rest) = entries.as_chunks::<IGGY_INDEX_SIZE>();
        if !rest.is_empty() {
            return false;
        }
        entries.iter().all(|entry| {
            let (offset, position) = (field(entry, 0), field(entry, 2));
            let ascends = offset >= self.last_offset
                && position >= self.last_position
                && position < self.segment_size;
            self.last_offset = offset;
            self.last_position = position;
            ascends
        })
    }
}

/// The `slot`th little-endian `u64` of an index entry.
fn field(entry: &[u8; IGGY_INDEX_SIZE], slot: usize) -> u64 {
    let at = slot * size_of::<u64>();
    let mut bytes = [0; size_of::<u64>()];
    bytes.copy_from_slice(&entry[at..at + size_of::<u64>()]);
    u64::from_le_bytes(bytes)
}

fn fault(key: &str, error: &ObjectStoreError) -> DiskFault {
    warn!(
        target: "iggy.partitions.diag",
        plane = "partitions",
        key,
        %error,
        "remote poll: the store failed a read"
    );
    match error.class() {
        ErrorClass::Transient => DiskFault::Retryable,
        ErrorClass::NotFound | ErrorClass::PreconditionFailed | ErrorClass::Permanent => {
            DiskFault::Unreadable
        }
    }
}

fn local_fault(path: &str, error: &std::io::Error) -> DiskFault {
    warn!(
        target: "iggy.partitions.diag",
        plane = "partitions",
        path,
        %error,
        "remote poll: could not write the fetched index"
    );
    DiskFault::Retryable
}

/// The index fetches of one partition incarnation.
///
/// A fetch runs without the partition and ends by putting a file in its
/// directory. While the fetch is suspended the directory can pass to another
/// incarnation, the history can be replaced and the segment can retire, and
/// each of them reuses or frees the path the fetch writes. So teardown waits
/// for the fetches in flight, and a fetch starts and publishes only while its
/// segment is still the one its plan saw.
pub struct IndexFetches {
    io: Rc<PartitionIoQuiescence>,
    history: Cell<PollHistoryId>,
    /// End offset of the newest segment that retention retired from
    /// `history`.
    retired_through: Cell<Option<u64>>,
    /// Start offsets of the segments whose index is being fetched, so a
    /// second poll of a segment does not write the same file under the first.
    fetching: RefCell<Vec<u64>>,
}

impl IndexFetches {
    pub(crate) fn new(io: Rc<PartitionIoQuiescence>, history: PollHistoryId) -> Self {
        Self {
            io,
            history: Cell::new(history),
            retired_through: Cell::new(None),
            fetching: RefCell::default(),
        }
    }

    /// What a plan built now holds of the fetches.
    pub(crate) fn fence(self: &Rc<Self>) -> IndexFetchFence {
        IndexFetchFence {
            fetches: Rc::clone(self),
            history: self.history.get(),
        }
    }

    /// The partition serves another history from here on, whose segments can
    /// reuse the paths of the ones before it.
    pub(crate) fn replace_history(&self, history: PollHistoryId) {
        self.history.set(history);
        self.retired_through.set(None);
    }

    /// Retention retired the segments that end at or below `end_offset`.
    pub(crate) fn retire_through(&self, end_offset: u64) {
        self.retired_through
            .set(self.retired_through.get().max(Some(end_offset)));
    }
}

/// The index fetches of a plan's partition, with the history the plan was
/// built in.
#[derive(Clone)]
pub struct IndexFetchFence {
    fetches: Rc<IndexFetches>,
    history: PollHistoryId,
}

impl IndexFetchFence {
    /// Start fetching the index of the segment at `start_offset`.
    ///
    /// `None` while another poll fetches it, and once the plan's partition is
    /// retiring or no longer serves the segment.
    pub(crate) fn begin(&self, start_offset: u64) -> Option<IndexFetch<'_>> {
        let fetches = &*self.fetches;
        if !self.serves(start_offset)
            || fetches.fetching.borrow().contains(&start_offset)
            || !fetches.io.begin_local_work()
        {
            return None;
        }
        fetches.fetching.borrow_mut().push(start_offset);
        Some(IndexFetch {
            fence: self,
            start_offset,
        })
    }

    fn serves(&self, start_offset: u64) -> bool {
        !self.fetches.io.is_retiring() && !self.retired(start_offset)
    }

    /// The plan's history is gone, or retention took the segment from it.
    fn retired(&self, start_offset: u64) -> bool {
        self.fetches.history.get() != self.history
            || self
                .fetches
                .retired_through
                .get()
                .is_some_and(|through| start_offset <= through)
    }

    /// A fence no partition holds, for a plan built by hand.
    #[cfg(test)]
    pub(crate) fn detached() -> Self {
        Rc::new(IndexFetches::new(Rc::default(), PollHistoryId::default())).fence()
    }
}

/// One fetch of a segment's index, which its partition's teardown waits for.
pub struct IndexFetch<'a> {
    fence: &'a IndexFetchFence,
    start_offset: u64,
}

impl IndexFetch<'_> {
    /// Whether the fetched index may still take its place in the partition
    /// directory.
    fn may_publish(&self) -> bool {
        self.fence.serves(self.start_offset)
    }

    /// Whether retention retired the segment from the history the plan saw.
    /// A replaced history says nothing about who owns the path now.
    fn segment_retired(&self) -> bool {
        self.fence.fetches.history.get() == self.fence.history
            && self.fence.retired(self.start_offset)
    }
}

impl Drop for IndexFetch<'_> {
    fn drop(&mut self) {
        let fetches = &self.fence.fetches;
        fetches
            .fetching
            .borrow_mut()
            .retain(|start_offset| *start_offset != self.start_offset);
        fetches.io.settle_local_work();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use iggy_object_store::ObjectPrefix;
    use std::cell::RefCell;

    const START_OFFSET: u64 = 100;
    /// Enough entries that the index takes three GETs.
    const ENTRIES: u64 = 2 * INDEX_FETCH_CHUNK / IGGY_INDEX_SIZE as u64 + 7;
    const SEGMENT_SIZE: u64 = ENTRIES * 10;

    /// Serves one index object and stamps each answer with the next etag.
    struct Scripted {
        index: Bytes,
        etags: RefCell<Vec<&'static str>>,
        asked: RefCell<Vec<Range<u64>>>,
    }

    impl RemoteObjects for Scripted {
        fn get_range<'a>(
            &'a self,
            _key: &'a str,
            range: Range<u64>,
        ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>> {
            self.asked.borrow_mut().push(range.clone());
            let size = self.index.len() as u64;
            let end = range.end.min(size);
            let object = ObjectRange {
                bytes: self
                    .index
                    .slice(usize::try_from(range.start).unwrap()..usize::try_from(end).unwrap()),
                etag: self.etags.borrow_mut().remove(0).to_owned(),
                size,
            };
            async move { Ok(object) }.boxed_local()
        }
    }

    fn index() -> Bytes {
        (0..ENTRIES)
            .flat_map(|entry| [START_OFFSET + entry, 1000 + entry, entry * 10])
            .flat_map(u64::to_le_bytes)
            .collect::<Vec<u8>>()
            .into()
    }

    fn log() -> RemoteRange {
        RemoteRange {
            key: SegmentKey {
                prefix: ObjectPrefix {
                    stream_id: 1,
                    topic_id: 2,
                    partition_id: 3,
                    created_revision: 4,
                },
                start_offset: START_OFFSET,
                object: SegmentObject::Log,
            },
            range: 0..SEGMENT_SIZE,
        }
    }

    async fn fetch(
        etags: [&'static str; 3],
    ) -> (Result<(), DiskFault>, Option<Vec<u8>>, Vec<Range<u64>>) {
        let directory = tempfile::tempdir().unwrap();
        let partition_dir = directory.path().to_string_lossy().into_owned();
        let index_path = format!("{partition_dir}/{START_OFFSET:020}.index");
        let objects = Rc::new(Scripted {
            index: index(),
            etags: RefCell::new(etags.to_vec()),
            asked: RefCell::default(),
        });
        let remote = RemoteSegments::new(objects.clone(), KeyLayout::new("iggy").unwrap());
        let fence = IndexFetchFence::detached();
        let fetch = fence.begin(START_OFFSET).unwrap();
        let fetched = remote
            .fetch_index(&log(), START_OFFSET, &partition_dir, &index_path, &fetch)
            .await;
        let left = std::fs::read_dir(directory.path()).unwrap().count();
        let written = std::fs::read(&index_path).ok();
        assert_eq!(left, usize::from(written.is_some()));
        (fetched, written, objects.asked.take())
    }

    #[compio::test]
    async fn given_an_index_wider_than_one_get_when_fetching_should_write_every_entry() {
        let (fetched, written, asked) = fetch(["a", "a", "a"]).await;

        assert_eq!(fetched, Ok(()));
        assert_eq!(written.unwrap(), index());
        assert_eq!(
            asked,
            [0, 1, 2].map(|chunk| chunk * INDEX_FETCH_CHUNK..(chunk + 1) * INDEX_FETCH_CHUNK)
        );
    }

    #[compio::test]
    async fn given_an_index_replaced_between_gets_when_fetching_should_write_nothing() {
        let (fetched, written, _asked) = fetch(["a", "a", "b"]).await;

        assert_eq!(fetched, Err(DiskFault::Retryable));
        assert_eq!(written, None);
    }

    #[compio::test]
    async fn given_a_segment_retired_under_the_rename_when_fetching_should_remove_the_index() {
        let directory = tempfile::tempdir().unwrap();
        let partition_dir = directory.path().to_string_lossy().into_owned();
        let index_path = format!("{partition_dir}/{START_OFFSET:020}.index");
        let objects = Rc::new(Scripted {
            index: index(),
            etags: RefCell::new(vec!["a"; 3]),
            asked: RefCell::default(),
        });
        let remote = RemoteSegments::new(objects, KeyLayout::new("iggy").unwrap());
        let fetches = Rc::new(IndexFetches::new(Rc::default(), PollHistoryId::default()));
        let fence = fetches.fence();
        let fetch = fence.begin(START_OFFSET).unwrap();
        let log = log();
        let mut fetching =
            Box::pin(remote.fetch_index(&log, START_OFFSET, &partition_dir, &index_path, &fetch));

        // The file is in place before the fetch learns that its rename ended,
        // which is where retention can find nothing to unlink and move on.
        while !std::fs::exists(&index_path).unwrap() {
            assert!(futures::poll!(fetching.as_mut()).is_pending());
            compio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        fetches.retire_through(START_OFFSET);

        assert_eq!(fetching.await, Err(DiskFault::Retryable));
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    fn entries(entries: &[[u64; 3]]) -> Vec<u8> {
        entries
            .iter()
            .flatten()
            .flat_map(|field| field.to_le_bytes())
            .collect()
    }

    #[test]
    fn given_index_entries_when_checking_should_accept_only_ones_that_ascend_inside_the_segment() {
        let accepts = |index: &[[u64; 3]]| IndexCheck::new(100, 50).accept(&entries(index));

        assert!(accepts(&[[100, 7, 0], [100, 7, 0], [105, 3, 49]]));
        assert!(!accepts(&[[99, 7, 0]]));
        assert!(!accepts(&[[100, 7, 0], [104, 8, 10], [103, 9, 20]]));
        assert!(!accepts(&[[100, 7, 10], [104, 8, 9]]));
        assert!(!accepts(&[[100, 7, 50]]));
        assert!(!IndexCheck::new(100, 50).accept(&entries(&[[100, 7, 0]])[1..]));
        // The check carries over from one GET's entries to the next.
        let mut check = IndexCheck::new(100, 50);
        assert!(check.accept(&entries(&[[104, 7, 10]])));
        assert!(!check.accept(&entries(&[[103, 8, 20]])));
    }

    #[test]
    fn given_a_fetch_in_flight_when_a_second_begins_should_wait_until_the_first_ends() {
        let fence = IndexFetchFence::detached();
        let first = fence.begin(START_OFFSET).unwrap();
        assert!(fence.begin(START_OFFSET).is_none());
        assert!(fence.begin(START_OFFSET).is_none());
        // Another segment's index is another file.
        assert!(fence.begin(START_OFFSET + 1).is_some());
        drop(first);
        assert!(fence.begin(START_OFFSET).is_some());
    }

    #[test]
    fn given_a_plan_whose_segment_is_gone_when_fetching_should_neither_begin_nor_publish() {
        let io = Rc::<PartitionIoQuiescence>::default();
        let history = PollHistoryId::default();
        let fetches = Rc::new(IndexFetches::new(Rc::clone(&io), history));
        let fence = fetches.fence();

        let fetch = fence.begin(START_OFFSET).unwrap();
        assert!(fetch.may_publish());
        fetches.retire_through(START_OFFSET - 1);
        assert!(fetch.may_publish() && !fetch.segment_retired());
        fetches.retire_through(START_OFFSET);
        assert!(!fetch.may_publish() && fetch.segment_retired());
        drop(fetch);
        assert!(fence.begin(START_OFFSET).is_none());
        assert!(fence.begin(START_OFFSET + 1).is_some());

        let fetch = fence.begin(START_OFFSET + 1).unwrap();
        fetches.replace_history(PollHistoryId::default());
        // The new history may hold another segment at the path, so the fetch
        // must not unlink it either.
        assert!(!fetch.may_publish() && !fetch.segment_retired());
        drop(fetch);
        assert!(fence.begin(START_OFFSET + 1).is_none());
        assert!(fetches.fence().begin(START_OFFSET).is_some());

        let fence = fetches.fence();
        let fetch = fence.begin(START_OFFSET).unwrap();
        io.retire();
        assert!(!fetch.may_publish() && !fetch.segment_retired());
        drop(fetch);
        assert!(fence.begin(START_OFFSET).is_none());
    }
}
