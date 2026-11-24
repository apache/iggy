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

use super::eviction::tiered_partition_on_disk;
use super::floor_eviction::apply;
use super::manifest_publish::{SEALED, partition_with_stored, regain_primacy};
use super::remote_poll::layout;
use super::segment_chain::PREFIX;
use super::*;
use crate::segment_chain::SegmentRecord;
use crate::{
    ExecutorSegmentVerifier, ObjectHead, SegmentHeads, SegmentVerifier, SegmentVerifyJob,
    SegmentVerifyReport, VerifyBegin, head_segments, upload_segment,
};
use iggy_object_store::{
    DeleteOutcome, InlineTierExecutor, ListPage, ListRequest, MemoryStore, ObjectMeta, ObjectRange,
    ObjectStore, ObjectStoreError, PutBody, PutOptions, RecordingStore, SegmentKey, SegmentObject,
    StoreCall, UserMetadata,
};
use std::time::SystemTime;

type Verifier = ExecutorSegmentVerifier<InlineTierExecutor<RecordingStore<MemoryStore>>>;

fn verifier() -> Verifier {
    ExecutorSegmentVerifier::new(
        InlineTierExecutor::new(RecordingStore::new(MemoryStore::new())),
        layout(),
    )
}

fn key(start_offset: u64, object: SegmentObject) -> String {
    layout().segment_key(&SegmentKey {
        prefix: PREFIX,
        start_offset,
        object,
    })
}

/// A tiered primary on disk whose sealed segments are all in `verifier`'s
/// store, with their advances committed.
async fn primary_with_every_segment_stored(
    root: &Path,
    verifier: &Verifier,
) -> IggyPartition<IggyMessageBus> {
    let (_config, mut partition) = Box::pin(tiered_partition_on_disk(root)).await;
    for op in 100..100 + SEALED.len() as u64 {
        let upload = partition.next_segment_upload().unwrap();
        upload_segment(verifier.executor().store(), &layout(), &upload)
            .await
            .unwrap();
        let advance = partition.note_segment_uploaded(&upload.record).unwrap();
        Box::pin(apply(&mut partition, op, advance)).await;
    }
    partition
}

/// One job of the pass, run through `verifier`. Returns how many segments it
/// checked.
async fn verify_job(
    partition: &mut IggyPartition<IggyMessageBus>,
    verifier: &Verifier,
) -> Option<usize> {
    let VerifyBegin::Job { id, job, scope } = partition.begin_segment_verify(IggyTimestamp::now())
    else {
        return None;
    };
    let outcome = verifier.verify(job, scope).await;
    partition.finish_segment_verify(id, &outcome)
}

fn heads_of_stored(record: SegmentRecord) -> SegmentHeads {
    let meta = |size, crc32c| ObjectMeta {
        size,
        etag: String::new(),
        last_modified: SystemTime::UNIX_EPOCH,
        metadata: UserMetadata::new(),
        crc32c: Some(crc32c),
    };
    SegmentHeads {
        log: ObjectHead::Found(meta(record.size, record.crc32c)),
        index: ObjectHead::Found(meta(crate::iggy_index::IGGY_INDEX_SIZE as u64, 0)),
        record,
    }
}

#[compio::test]
async fn given_a_primary_whose_store_holds_its_segments_when_its_first_pass_runs_should_head_each_object_once_and_find_nothing()
 {
    let root = tempfile::tempdir().unwrap();
    let verifier = verifier();
    let mut partition = Box::pin(primary_with_every_segment_stored(root.path(), &verifier)).await;
    let calls = verifier.executor().store().calls();
    let before = calls.all().len();

    let checked = verify_job(&mut partition, &verifier).await;

    assert_eq!(checked, Some(SEALED.len()));
    let heads: Vec<StoreCall> = SEALED
        .iter()
        .flat_map(|(start, _)| {
            [SegmentObject::Log, SegmentObject::Index].map(|object| (*start, object))
        })
        .map(|(start, object)| StoreCall::Head {
            key: key(start, object),
        })
        .collect();
    assert_eq!(calls.all()[before..], heads);
    assert_eq!(partition.stored_segment_findings(), (0, 0));
    assert!(matches!(
        partition.begin_segment_verify(IggyTimestamp::now()),
        VerifyBegin::PassEnded
    ));
    assert!(matches!(
        partition.begin_segment_verify(IggyTimestamp::now()),
        VerifyBegin::Idle
    ));
    assert_eq!(calls.all().len(), before + heads.len());
}

#[compio::test]
async fn given_a_stored_log_gone_from_the_store_when_a_new_primary_verifies_should_count_it_missing_until_it_loses_primacy()
 {
    let root = tempfile::tempdir().unwrap();
    let verifier = verifier();
    let mut partition = Box::pin(primary_with_every_segment_stored(root.path(), &verifier)).await;
    verify_job(&mut partition, &verifier).await.unwrap();
    let deleted = verifier
        .executor()
        .store()
        .delete(&[key(SEALED[0].0, SegmentObject::Log)])
        .await
        .unwrap();
    assert!(deleted.failed.is_empty());

    regain_primacy(&mut partition);
    verify_job(&mut partition, &verifier).await.unwrap();

    assert_eq!(partition.stored_segment_findings(), (1, 0));
    assert_eq!(partition.gauge_share().stored_segments_missing, 1);
    let view = partition.consensus.view() + 1;
    partition.consensus.set_view(view);
    assert!(!partition.consensus.is_primary());
    assert_eq!(partition.stored_segment_findings(), (0, 0));
    assert_eq!(partition.gauge_share().stored_segments_missing, 0);
    assert!(matches!(
        partition.begin_segment_verify(IggyTimestamp::now()),
        VerifyBegin::Idle
    ));
    assert!(partition.segment_verifies.is_none());
}

#[compio::test]
async fn given_a_segment_retention_trims_while_its_job_runs_when_the_job_finds_it_missing_should_record_nothing()
 {
    let (_directory, mut partition) = Box::pin(partition_with_stored(2)).await;
    let VerifyBegin::Job { id, job, .. } = partition.begin_segment_verify(IggyTimestamp::now())
    else {
        panic!("no job");
    };
    assert_eq!(job.segments.len(), SEALED.len());

    Box::pin(partition.remove_sealed_segments_up_to(SEALED[0].1)).await;
    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;
    let trimmed = SegmentHeads {
        log: ObjectHead::NotFound,
        index: ObjectHead::NotFound,
        record: job.segments[0],
    };
    let report = SegmentVerifyReport {
        answered: vec![trimmed, heads_of_stored(job.segments[1])],
        unanswered: None,
    };
    let checked = partition.finish_segment_verify(id, &Ok(report));

    assert_eq!(checked, Some(1));
    assert_eq!(partition.stored_segment_findings(), (0, 0));
}

#[compio::test]
async fn given_a_replica_that_does_not_verify_when_asking_for_a_job_should_start_none() {
    let (_directory, mut backup) = Box::pin(partition_with_stored(2)).await;
    let view = backup.consensus.view() + 1;
    backup.consensus.set_view(view);
    let (_directory, mut placing) = Box::pin(partition_with_stored(2)).await;
    placing.set_places_batches(true);

    for partition in [&mut backup, &mut placing] {
        assert!(matches!(
            partition.begin_segment_verify(IggyTimestamp::now()),
            VerifyBegin::Idle
        ));
    }
}

/// A memory store whose HEADs of one key fail as a store under load does.
struct OneKeyUnavailable {
    inner: RecordingStore<MemoryStore>,
    key: String,
}

impl ObjectStore for OneKeyUnavailable {
    async fn put(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        self.inner.put(key, body, options).await
    }

    async fn get_range(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
    ) -> Result<ObjectRange, ObjectStoreError> {
        self.inner.get_range(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
        if key == self.key {
            return Err(ObjectStoreError::Unavailable("503".to_owned()));
        }
        self.inner.head(key).await
    }

    async fn delete(&self, keys: &[String]) -> Result<DeleteOutcome, ObjectStoreError> {
        self.inner.delete(keys).await
    }

    async fn list(&self, request: &ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
        self.inner.list(request).await
    }
}

#[compio::test]
async fn given_a_head_the_store_does_not_answer_when_a_job_runs_should_stop_there_and_report_the_segment_unanswered()
 {
    let (_directory, partition) = Box::pin(partition_with_stored(2)).await;
    let records: Vec<SegmentRecord> = SEALED
        .iter()
        .map(|(start, _)| partition.sealed_segment_record(*start).unwrap())
        .collect();
    let store = OneKeyUnavailable {
        inner: RecordingStore::new(MemoryStore::new()),
        key: key(SEALED[1].0, SegmentObject::Log),
    };
    let calls = store.inner.calls();
    let job = SegmentVerifyJob {
        prefix: PREFIX,
        segments: records.clone(),
    };

    let report = head_segments(&store, &layout(), &job).await;

    assert_eq!(
        report.answered,
        [SegmentHeads {
            record: records[0],
            log: ObjectHead::NotFound,
            index: ObjectHead::NotFound,
        }]
    );
    let unanswered = report.unanswered.unwrap();
    assert_eq!(unanswered.start_offset, SEALED[1].0);
    assert!(matches!(unanswered.error, ObjectStoreError::Unavailable(_)));
    assert_eq!(
        calls.all(),
        [
            StoreCall::Head {
                key: key(SEALED[0].0, SegmentObject::Log)
            },
            StoreCall::Head {
                key: key(SEALED[0].0, SegmentObject::Index)
            }
        ]
    );
}
