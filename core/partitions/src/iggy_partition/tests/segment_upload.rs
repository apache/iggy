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

use super::eviction::KEEP_NOTHING;
use super::floor_eviction::apply;
use super::remote_poll::{attach, layout, polled_all};
use super::segment_chain::{PREFIX, tiered_partition};
use super::*;
use crate::iggy_index::IGGY_INDEX_SIZE;
use crate::tiered_floor::AdvanceTieredFloor;
use crate::{SegmentLocation, SegmentUpload, SegmentUploader, UploadError, upload_segment};
use iggy_object_store::{
    InlineTierExecutor, MemoryStore, ObjectStore, ObjectStoreError, PutBody, PutOptions,
    SegmentKey, SegmentMetadata, SegmentObject,
};

fn key(start_offset: u64, object: SegmentObject) -> String {
    layout().segment_key(&SegmentKey {
        prefix: PREFIX,
        start_offset,
        object,
    })
}

async fn object(store: &MemoryStore, key: &str) -> Vec<u8> {
    store
        .get_range(key, 0..u64::MAX)
        .await
        .unwrap()
        .bytes
        .to_vec()
}

async fn put(store: &MemoryStore, key: &str, bytes: Vec<u8>) {
    store
        .put(key, PutBody::from_bytes(bytes), &PutOptions::default())
        .await
        .unwrap();
}

async fn upload(store: &MemoryStore, upload: &SegmentUpload) -> Result<(), UploadError> {
    upload_segment(store, &layout(), upload).await
}

fn flip_last_byte(path: &str) {
    let mut bytes = std::fs::read(path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    std::fs::write(path, bytes).unwrap();
}

#[compio::test]
async fn given_sealed_segments_when_the_primary_uploads_should_store_each_oldest_first() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let store = MemoryStore::new();
    let mut uploaded = Vec::new();

    while let Some(next) = partition.next_segment_upload() {
        upload(&store, &next).await.unwrap();
        let log_key = key(next.record.start_offset, SegmentObject::Log);
        assert_eq!(
            object(&store, &log_key).await,
            std::fs::read(&next.log_path).unwrap()
        );
        assert_eq!(
            object(&store, &key(next.record.start_offset, SegmentObject::Index)).await,
            std::fs::read(&next.index_path).unwrap()
        );
        let held = store.head(&log_key).await.unwrap();
        assert_eq!(held.crc32c, Some(next.record.crc32c));
        let metadata = SegmentMetadata::try_from(&held.metadata).unwrap();
        assert_eq!(
            (
                metadata.start_offset,
                metadata.end_offset,
                metadata.size,
                metadata.hash
            ),
            (
                next.record.start_offset,
                next.record.end_offset,
                next.record.size,
                next.record.hash
            )
        );
        uploaded.push(partition.note_segment_uploaded(&next.record));
    }

    // The active segment is not sealed, so it stays out. Both advances start
    // at zero, because the floor has not moved between them.
    let starts: Vec<_> = uploaded
        .iter()
        .map(|advance| (advance.start, advance.up_to))
        .collect();
    assert_eq!(starts, [(0, 1), (0, 3)]);
    assert_eq!(store.len(), 4);
    assert_eq!(
        partition
            .log
            .segments()
            .iter()
            .map(|segment| segment.location)
            .collect::<Vec<_>>(),
        [
            SegmentLocation::Uploaded,
            SegmentLocation::Uploaded,
            SegmentLocation::Local
        ]
    );
}

#[compio::test]
async fn given_an_upload_and_its_advance_when_the_segment_is_evicted_should_poll_the_same_bytes() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let local = Box::pin(polled_all(&mut partition)).await;
    let store = Rc::new(MemoryStore::new());
    attach(&mut partition, store.clone());
    partition.set_local_envelope(Some(KEEP_NOTHING));

    let mut op = 100;
    while let Some(next) = partition.next_segment_upload() {
        upload(&store, &next).await.unwrap();
        let advance = partition.note_segment_uploaded(&next.record);
        Box::pin(apply(&mut partition, op, advance)).await;
        op += 1;
    }

    assert_eq!(partition.tiered_floor(), Some(3));
    assert_eq!(
        partition
            .log
            .segments()
            .iter()
            .filter(|segment| segment.location == SegmentLocation::Remote)
            .count(),
        2
    );
    assert_eq!(Box::pin(polled_all(&mut partition)).await, local);
}

#[compio::test]
async fn given_a_damaged_log_when_uploading_should_fail_before_anything_reaches_the_store() {
    let (_directory, partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let store = MemoryStore::new();
    let next = partition.next_segment_upload().unwrap();
    flip_last_byte(&next.log_path);

    assert!(matches!(
        upload(&store, &next).await,
        Err(UploadError::MediaFault {
            start_offset: 0,
            ..
        })
    ));
    assert!(store.is_empty());
}

/// Batches that each match their checksum can still be other batches than
/// the ones consensus committed, which only the recorded hash shows.
#[compio::test]
async fn given_a_log_of_valid_batches_that_is_not_the_recorded_one_when_uploading_should_fail() {
    let (_directory, partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let store = MemoryStore::new();
    let recorded = partition.next_segment_upload().unwrap();

    for other in [
        SegmentUpload {
            record: crate::segment_chain::SegmentRecord {
                hash: [1; 32],
                ..recorded.record
            },
            ..recorded.clone()
        },
        SegmentUpload {
            record: crate::segment_chain::SegmentRecord {
                crc32c: !recorded.record.crc32c,
                ..recorded.record
            },
            ..recorded.clone()
        },
    ] {
        let failure = upload(&store, &other).await.unwrap_err();
        assert!(
            matches!(
                failure,
                UploadError::MediaFault {
                    reason: "its bytes hash to something else",
                    ..
                }
            ),
            "{failure}"
        );
    }
    assert!(store.is_empty());
}

/// A record hashed from disk matches whatever the disk held, so only the
/// batch checksums can tell that the bytes are not what was committed.
#[compio::test]
async fn given_a_log_damaged_before_its_hash_from_disk_when_uploading_should_fail_on_its_batch() {
    let config = repair_config();
    let (directory, mut partition) = Box::pin(disk_poll_partition_with_replicas(&config, 3)).await;
    let batch_len = build_segment_record(partition.namespace(), 0).len() as u64;
    let segment_size = IggyByteSize::from(BATCHES_PER_SEGMENT * batch_len);
    partition.runtime_options.segment_size = Some(segment_size);
    partition.log.active_segment_mut().max_size = segment_size;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    Box::pin(commit_records(
        &mut partition,
        1,
        &segment_records(RECEIVER_OPS),
    ))
    .await;
    flip_last_byte(
        &directory
            .path()
            .join(format!("{:020}.log", 0))
            .to_string_lossy(),
    );
    partition.enable_tiering(PREFIX);
    Box::pin(settle_partition_io(&mut partition, &config)).await;
    let store = MemoryStore::new();
    let next = partition.next_segment_upload().unwrap();

    let failure = upload(&store, &next).await.unwrap_err();

    assert!(
        matches!(
            failure,
            UploadError::MediaFault {
                reason: "a batch does not match its checksum",
                ..
            }
        ),
        "{failure}"
    );
    assert!(!failure.is_transient());
    assert!(store.is_empty());
}

#[compio::test]
async fn given_an_index_that_does_not_describe_the_segment_when_uploading_should_send_nothing() {
    let (_directory, partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let next = partition.next_segment_upload().unwrap();
    let index = std::fs::read(&next.index_path).unwrap();
    let mut past_the_segment = index.clone();
    let position = past_the_segment.len() - 8;
    past_the_segment[position..].copy_from_slice(&next.record.size.to_le_bytes());

    for damaged in [
        index[..index.len() - 1].to_vec(),
        Vec::new(),
        past_the_segment,
    ] {
        std::fs::write(&next.index_path, damaged).unwrap();
        let store = MemoryStore::new();
        assert!(matches!(
            upload(&store, &next).await,
            Err(UploadError::MediaFault { .. })
        ));
        assert!(store.is_empty());
    }
}

#[compio::test]
async fn given_objects_in_the_store_when_uploading_should_accept_its_own_and_refuse_others() {
    let (_directory, partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let next = partition.next_segment_upload().unwrap();
    let log_key = key(0, SegmentObject::Log);
    let index_key = key(0, SegmentObject::Index);

    // A retry, or a new primary repeating the upload, meets its own objects.
    let store = MemoryStore::new();
    upload(&store, &next).await.unwrap();
    upload(&store, &next).await.unwrap();

    // Another uploader's index only has to hold whole entries.
    let store = MemoryStore::new();
    put(&store, &index_key, vec![0; IGGY_INDEX_SIZE * 3]).await;
    upload(&store, &next).await.unwrap();
    assert_eq!(
        object(&store, &index_key).await,
        vec![0; IGGY_INDEX_SIZE * 3]
    );

    let store = MemoryStore::new();
    put(&store, &index_key, vec![0; IGGY_INDEX_SIZE + 1]).await;
    assert!(matches!(
        upload(&store, &next).await,
        Err(UploadError::Diverged { key }) if key == index_key
    ));

    // Other bytes under the log key, of the same size or not, are a divergence.
    let log = std::fs::read(&next.log_path).unwrap();
    let mut same_size = log.clone();
    same_size[0] ^= 0xff;
    for other in [same_size, log[..log.len() - 1].to_vec()] {
        let store = MemoryStore::new();
        put(&store, &log_key, other.clone()).await;
        let failure = upload(&store, &next).await.unwrap_err();
        assert!(
            matches!(&failure, UploadError::Diverged { key } if *key == log_key),
            "{failure}"
        );
        assert!(!failure.is_transient());
        assert_eq!(object(&store, &log_key).await, other);
        assert_eq!(store.len(), 1, "the index did not follow a refused log");
    }
}

#[compio::test]
async fn given_a_segment_that_cannot_upload_when_asking_for_the_next_should_not_skip_past_it() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let contradicting = AdvanceTieredFloor {
        start: 0,
        up_to: 1,
        hash: [9; 32],
    };
    Box::pin(apply(&mut partition, 100, contradicting)).await;
    // The floor passed the first segment, so the second is next.
    assert_eq!(
        partition.next_segment_upload().unwrap().record.start_offset,
        2
    );

    // A segment above the floor that an advance contradicted blocks the rest.
    let contradicting = AdvanceTieredFloor {
        start: 0,
        up_to: 2,
        hash: [9; 32],
    };
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let first = partition.next_segment_upload().unwrap();
    assert_eq!(first.record.start_offset, 0);
    partition.note_segment_uploaded(&first.record);
    assert_eq!(
        partition.next_segment_upload().unwrap().record.start_offset,
        2
    );
    Box::pin(apply(&mut partition, 100, contradicting)).await;
    assert_eq!(partition.tiered_floor(), Some(2));
    assert_eq!(partition.next_segment_upload(), None);
}

/// A store that refuses every put, and keeps the CRC32C the last one came
/// with.
#[derive(Default)]
struct Refusing {
    crc32c: std::cell::Cell<Option<u32>>,
}

impl ObjectStore for Refusing {
    async fn put(&self, _: &str, _: PutBody, options: &PutOptions) -> Result<(), ObjectStoreError> {
        self.crc32c.set(options.crc32c);
        Err(ObjectStoreError::Unavailable("503".to_owned()))
    }

    async fn get_range(
        &self,
        key: &str,
        _: std::ops::Range<u64>,
    ) -> Result<iggy_object_store::ObjectRange, ObjectStoreError> {
        Err(ObjectStoreError::NotFound {
            key: key.to_owned(),
        })
    }

    async fn head(&self, key: &str) -> Result<iggy_object_store::ObjectMeta, ObjectStoreError> {
        Err(ObjectStoreError::NotFound {
            key: key.to_owned(),
        })
    }

    async fn delete(
        &self,
        _: &[String],
    ) -> Result<iggy_object_store::DeleteOutcome, ObjectStoreError> {
        Err(ObjectStoreError::Unavailable("503".to_owned()))
    }

    async fn list(
        &self,
        _: &iggy_object_store::ListRequest<'_>,
    ) -> Result<iggy_object_store::ListPage, ObjectStoreError> {
        Err(ObjectStoreError::Unavailable("503".to_owned()))
    }
}

#[compio::test]
async fn given_a_store_outage_when_uploading_should_report_a_failure_worth_a_retry() {
    let (_directory, partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let next = partition.next_segment_upload().unwrap();

    let store = Refusing::default();
    let failure = upload_segment(&store, &layout(), &next).await.unwrap_err();

    assert!(matches!(failure, UploadError::Store(_)), "{failure}");
    assert!(failure.is_transient());
    // The store is told what the bytes must hash to, so bytes that changed
    // after the check are refused there.
    assert_eq!(store.crc32c.get(), Some(next.record.crc32c));
}

fn uploader<S: ObjectStore + 'static>(store: S) -> crate::ExecutorUploader<InlineTierExecutor<S>> {
    crate::ExecutorUploader::new(InlineTierExecutor::new(store), layout())
}

fn local_starts(partition: &IggyPartition<IggyMessageBus>) -> Vec<u64> {
    partition
        .log
        .segments()
        .iter()
        .filter(|segment| segment.sealed && segment.location == SegmentLocation::Local)
        .map(|segment| segment.start_offset)
        .collect()
}

#[compio::test]
async fn given_a_primary_when_uploads_run_should_keep_one_in_flight_and_yield_each_advance() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    assert!(partition.consensus.is_primary(), "the fixture is a primary");
    let uploader = uploader(MemoryStore::new());

    let (first, scope) = partition.begin_segment_upload().unwrap();
    assert_eq!(first.record.start_offset, 0);
    assert!(
        partition.begin_segment_upload().is_none(),
        "one upload at a time"
    );
    let outcome = uploader.upload(first.clone(), scope).await;
    assert!(matches!(outcome, Ok(Ok(()))));

    let advance = partition
        .finish_segment_upload(&first.record, &outcome)
        .unwrap();
    assert_eq!((advance.start, advance.up_to), (0, 1));
    assert_eq!(advance.hash, first.record.hash);
    // A result that arrives twice is not an upload this partition waits for.
    assert_eq!(
        partition.finish_segment_upload(&first.record, &outcome),
        None
    );

    let (second, _scope) = partition.begin_segment_upload().unwrap();
    assert_eq!(second.record.start_offset, 2);
}

#[compio::test]
async fn given_an_upload_that_fails_when_it_finishes_should_offer_the_segment_again() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let uploader = uploader(Refusing::default());

    let (first, scope) = partition.begin_segment_upload().unwrap();
    let outcome = uploader.upload(first.clone(), scope).await;
    assert!(matches!(&outcome, Ok(Err(failure)) if failure.is_transient()));

    assert_eq!(
        partition.finish_segment_upload(&first.record, &outcome),
        None
    );
    assert_eq!(local_starts(&partition), [0, 2]);
    let (again, _scope) = partition.begin_segment_upload().unwrap();
    assert_eq!(again, first);
}

#[compio::test]
async fn given_a_cancelled_upload_when_its_result_arrives_should_not_advance() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let (first, scope) = partition.begin_segment_upload().unwrap();

    partition.cancel_segment_upload();

    assert!(scope.is_cancelled());
    assert_eq!(
        partition.finish_segment_upload(&first.record, &Ok(Ok(()))),
        None
    );
    assert_eq!(local_starts(&partition), [0, 2]);

    // A job submitted under the cancelled scope does not end as an upload.
    let outcome = uploader(MemoryStore::new()).upload(first, scope).await;
    assert!(outcome.is_err());
}

#[compio::test]
async fn given_a_replica_that_lost_primacy_when_scanned_should_cancel_its_upload_and_start_none() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let (first, scope) = partition.begin_segment_upload().unwrap();

    // The next view's primary is another replica.
    let view = partition.consensus.view() + 1;
    partition.consensus.set_view(view);
    assert!(!partition.consensus.is_primary());

    assert!(partition.begin_segment_upload().is_none());
    assert!(
        scope.is_cancelled(),
        "the scan stops the demoted replica's job"
    );
    assert_eq!(
        partition.finish_segment_upload(&first.record, &Ok(Ok(()))),
        None
    );
    assert_eq!(local_starts(&partition), [0, 2]);
}

#[compio::test]
async fn given_an_upload_that_ends_after_primacy_moved_when_it_finishes_should_not_advance() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let (first, _scope) = partition.begin_segment_upload().unwrap();
    let view = partition.consensus.view() + 1;
    partition.consensus.set_view(view);

    // No scan ran in between, so the result is still the awaited one.
    assert_eq!(
        partition.finish_segment_upload(&first.record, &Ok(Ok(()))),
        None
    );
    assert_eq!(local_starts(&partition), [0, 2]);
}

/// An executor that runs each job at once and keeps what it was asked for.
struct Recording {
    store: Rc<MemoryStore>,
    requests: std::cell::RefCell<Vec<(iggy_object_store::TierJobClass, u64)>>,
}

impl iggy_object_store::TierExecutor for Recording {
    type Store = MemoryStore;

    fn submit<T, F, Fut>(
        &self,
        request: iggy_object_store::TierJobRequest,
        job: F,
    ) -> iggy_object_store::TierJobHandle<T>
    where
        T: 'static,
        F: FnOnce(Rc<Self::Store>) -> Fut + 'static,
        Fut: Future<Output = T> + 'static,
    {
        self.requests
            .borrow_mut()
            .push((request.class(), request.bytes()));
        let job = job(Rc::clone(&self.store));
        iggy_object_store::TierJobHandle::new(async move { Ok(job.await) })
    }
}

/// The executor caps uploads by their class and their bytes, so an upload has
/// to say both.
#[compio::test]
async fn given_an_upload_when_submitted_should_name_the_upload_class_and_the_segment_size() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let (first, scope) = partition.begin_segment_upload().unwrap();
    let size = first.record.size;
    let uploader = crate::ExecutorUploader::new(
        Recording {
            store: Rc::new(MemoryStore::new()),
            requests: std::cell::RefCell::default(),
        },
        layout(),
    );

    let outcome = uploader.upload(first, scope).await;

    assert!(matches!(outcome, Ok(Ok(()))));
    assert_eq!(
        uploader.executor().requests.take(),
        [(iggy_object_store::TierJobClass::Upload, size)]
    );
}
