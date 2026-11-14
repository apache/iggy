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

//! Uploads of a tiered primary's sealed segments through the tier service,
//! against a store that stalls, crawls or never answers.

use super::segment_uploads::{floor, namespace, pumped, sealed_owner, tiered_owner, until};
use super::*;
use crate::FinishedSegmentUpload;
use crate::tier_service::test_store::TestStore;
use crate::tier_service::{ClassLoad, StoreCallPolicy, TierService, TierServiceConfig};
use crate::upload_pacing::UPLOAD_RETRY_BASE;
use iggy_object_store::{
    KeyLayout, ObjectStore, ObjectStoreError, SegmentKey, SegmentObject, TierJobCancelled,
    TierJobClass,
};
use partitions::{ExecutorUploader, UploadError};
use std::time::Instant;

type UploadService = TierService<TestStore, Rc<IggyMessageBus>>;

/// How long a put may go without a step before the service cuts it.
const PROGRESS_TIMEOUT: Duration = Duration::from_millis(100);

const MIB: usize = 1024 * 1024;

fn layout() -> KeyLayout {
    KeyLayout::new("segments").unwrap()
}

/// A tier service over a test store, given to `owner` for its uploads.
fn give_uploads(owner: &IoTestShard) -> UploadService {
    let config = TierServiceConfig {
        calls: StoreCallPolicy {
            progress_timeout: PROGRESS_TIMEOUT,
            ..StoreCallPolicy::default()
        },
        ..TierServiceConfig::default()
    };
    let service = TierService::new(
        TestStore::default(),
        Rc::new(IggyMessageBus::new(0)),
        &config,
    );
    owner.set_segment_uploader(Some(Rc::new(ExecutorUploader::new(
        service.clone(),
        layout(),
    ))));
    service
}

fn provider(service: &UploadService) -> &TestStore {
    service.store().provider()
}

/// The key of the `.log` object of the segment that starts at
/// `start_offset`.
fn log_key(start_offset: u64) -> String {
    layout().segment_key(&SegmentKey {
        prefix: partitions::object_prefix(namespace(), 0),
        start_offset,
        object: SegmentObject::Log,
    })
}

/// The result of the upload in flight, once its frame reaches the inbox.
#[allow(clippy::future_not_send)]
async fn finished_upload(owner: &IoTestShard) -> FinishedSegmentUpload {
    until(owner, |owner| owner.inbox_len() > 0).await;
    match owner.inbox.try_recv() {
        Ok(ShardFrame::Lifecycle(LifecycleFrame::SegmentUploadFinished(finished))) => *finished,
        _ => panic!("only the upload result is on its way"),
    }
}

/// Commit sends of `messages` messages of `payload_len` bytes each into an
/// empty partition, the last of which seals its active segment.
#[allow(clippy::future_not_send)]
async fn seal_segment(owner: &IoTestShard, messages: u64, payload_len: usize) {
    for request in 1..=messages {
        owner.senders[0]
            .try_send(ShardFrame::lifecycle(LifecycleFrame::PartitionSubmit {
                request: sized_send_request(namespace(), request, payload_len),
                reply: channel(1).0,
                attachment: None,
            }))
            .unwrap();
        until(owner, |owner| {
            let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
            partition.offset_frontier() >= request
        })
        .await;
    }
    until(owner, |owner| sealed_segments(owner) == 1).await;
}

/// The sealed segments whose chain record the commit path wrote.
fn sealed_segments(owner: &IoTestShard) -> usize {
    let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
    partition
        .log
        .segments()
        .iter()
        .filter(|segment| {
            segment.sealed
                && partition
                    .sealed_segment_record(segment.start_offset)
                    .is_some()
        })
        .count()
}

#[compio::test]
async fn given_a_store_that_stalls_a_put_when_the_backoff_passes_should_store_the_segment_on_the_retry()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;
    let service = give_uploads(&owner);
    provider(&service).stall_after_pieces.set(Some(0));
    let failed_at = Instant::now();

    owner.upload_segments_at(namespace(), true, failed_at).await;
    let finished = finished_upload(&owner).await;

    assert!(
        matches!(
            &finished.outcome,
            Ok(Err(error @ UploadError::Store(ObjectStoreError::Transport(_))))
                if error.is_transient()
        ),
        "{:?}",
        finished.outcome
    );
    owner.segment_upload_finished_at(finished, failed_at).await;
    assert!(provider(&service).inner.head(&log_key(0)).await.is_err());

    provider(&service).stall_after_pieces.set(None);
    owner
        .upload_segments_at(namespace(), true, failed_at + UPLOAD_RETRY_BASE)
        .await;
    let finished = finished_upload(&owner).await;

    assert!(
        matches!(finished.outcome, Ok(Ok(()))),
        "{:?}",
        finished.outcome
    );
    assert!(provider(&service).inner.head(&log_key(0)).await.is_ok());
    assert_eq!(
        provider(&service).puts.get(),
        3,
        "the stalled put, then the `.log` and the `.index`"
    );
}

#[compio::test]
async fn given_a_store_that_takes_each_piece_slowly_when_uploading_should_store_the_segment_without_a_restart()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = tiered_owner(directory.path()).await;
    // One message over three upload pieces, so the `.log` takes four.
    pumped(&owner, seal_segment(&owner, 1, 3 * MIB + MIB / 2)).await;
    let service = give_uploads(&owner);
    // Each step of a put stays inside the progress timeout, and the whole put
    // does not.
    provider(&service).piece_delay.set(PROGRESS_TIMEOUT * 2 / 3);
    let started_at = Instant::now();

    owner
        .upload_segments_at(namespace(), true, started_at)
        .await;
    let finished = finished_upload(&owner).await;

    assert!(
        matches!(finished.outcome, Ok(Ok(()))),
        "{:?}",
        finished.outcome
    );
    assert!(started_at.elapsed() > 3 * PROGRESS_TIMEOUT);
    assert_eq!(
        provider(&service).puts.get(),
        2,
        "one put for the `.log` and one for the `.index`"
    );
}

#[compio::test]
async fn given_an_upload_that_waits_on_the_store_when_its_partition_cancels_it_should_end_cancelled_for_good()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(sealed_owner(directory.path())).await;
    let service = give_uploads(&owner);
    provider(&service).hangs.set(true);

    owner
        .upload_segments_at(namespace(), true, Instant::now())
        .await;
    until(&owner, |_| provider(&service).puts.get() == 1).await;
    // What a partition does on its way out, and what a scan of a demoted
    // primary does to the upload it started.
    owner
        .plane
        .partitions()
        .get_mut_by_ns(&namespace())
        .unwrap()
        .cancel_segment_upload();
    let finished = finished_upload(&owner).await;

    assert!(
        matches!(finished.outcome, Err(TierJobCancelled)),
        "{:?}",
        finished.outcome
    );
    // Past the progress timeout, so a job that was still running would have
    // failed and could have been tried again.
    compio::time::sleep(2 * PROGRESS_TIMEOUT).await;
    assert_eq!(provider(&service).puts.get(), 1);
    assert_eq!(service.load(TierJobClass::Upload), ClassLoad::default());
    assert!(provider(&service).inner.head(&log_key(0)).await.is_err());
}

/// The most a pump tick may run late while a large segment uploads beside it.
const LATE_TICK: Duration = Duration::from_millis(10);

/// A 1 GiB segment of 1 MiB batches uploads while the pump commits a steady
/// stream of small sends, and no consensus tick runs more than [`LATE_TICK`]
/// late.
///
/// The upload's CPU work is its hash pass, which checks each batch whole, so
/// a segment of larger batches runs longer without an await. An optimized
/// build measures what a server does:
/// `cargo test -p shard --release --lib -- --ignored keep_every_pump_tick_on_time`
#[compio::test]
#[ignore = "measures the pump beside a 1 GiB upload; run it in a release build"]
async fn given_a_one_gib_segment_when_it_uploads_beside_a_busy_pump_should_keep_every_pump_tick_on_time()
 {
    const MESSAGE_LEN: usize = MIB;
    const MESSAGES: u64 = 1024;
    let directory = tempfile::tempdir().unwrap();
    let owner = tiered_owner(directory.path()).await;
    let segment_size = IggyByteSize::from(MESSAGES * MESSAGE_LEN as u64);
    {
        let partition = owner
            .plane
            .partitions()
            .get_mut_by_ns(&namespace())
            .unwrap();
        partition.set_runtime_options(TopicRuntimeOptions {
            segment_size: Some(segment_size),
            messages_required_to_save: Some(1),
            ..Default::default()
        });
        partition.log.active_segment_mut().max_size = segment_size;
    }

    // One pump run for all of it: a shard runs its pump once, and the floor
    // advance after the upload needs it.
    let window = pumped(&owner, async {
        seal_segment(&owner, MESSAGES, MESSAGE_LEN).await;
        let service = give_uploads(&owner);
        provider(&service).discard_bodies.set(true);
        let before = tick_lateness(&owner);
        owner.request_upload_segments(namespace());
        // Small sends keep the pump busy until the upload's floor advance
        // commits. A full inbox only skips a send.
        let mut request = MESSAGES;
        let deadline = Instant::now() + Duration::from_secs(120);
        while floor(&owner).is_none() {
            assert!(
                Instant::now() < deadline,
                "the upload's floor advance did not commit"
            );
            request += 1;
            let _ =
                owner.senders[0].try_send(ShardFrame::lifecycle(LifecycleFrame::PartitionSubmit {
                    request: sized_send_request(namespace(), request, 1024),
                    reply: channel(1).0,
                    attachment: None,
                }));
            compio::time::sleep(Duration::from_millis(1)).await;
        }
        let after = tick_lateness(&owner);
        after
            .iter()
            .zip(&before)
            .map(|(&(edge, after), &(_, before))| (edge, after - before))
            .collect::<Vec<_>>()
    })
    .await;

    let ticks = window.last().map_or(0, |&(_, count)| count);
    let on_time = window
        .iter()
        .find(|&&(edge, _)| edge >= LATE_TICK.as_secs_f64())
        .map_or(ticks, |&(_, count)| count);
    let latest = window
        .iter()
        .find(|&&(_, count)| count == ticks)
        .map(|&(edge, _)| edge);
    eprintln!(
        "pump ticks during the upload: {ticks}, the latest at most {latest:?} s late, \
         cumulative buckets {window:?}"
    );
    assert!(ticks > 0, "no tick ran during the upload");
    assert_eq!(
        ticks - on_time,
        0,
        "{} of {ticks} pump ticks ran more than {LATE_TICK:?} late",
        ticks - on_time
    );
}

/// The shard's pump tick lateness histogram: each bucket's upper edge in
/// seconds, with the ticks at or below it.
fn tick_lateness(owner: &IoTestShard) -> Vec<(f64, u64)> {
    owner
        .metrics
        .scrape()
        .lines()
        .filter_map(|line| line.strip_prefix("pump_tick_lateness_seconds_bucket{le=\""))
        .map(|rest| {
            let (edge, count) = rest.split_once("\"} ").unwrap();
            (
                edge.parse().unwrap_or(f64::INFINITY),
                count.trim().parse().unwrap(),
            )
        })
        .collect()
}
