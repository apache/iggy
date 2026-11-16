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

//! The upload-first produce path on a shard that hosts a primary which
//! places its batches.

use super::segment_uploads::{namespace, pumped, until};
use super::*;
use futures::future::LocalBoxFuture;
use iggy_binary_protocol::placement::{PLACED_BATCH_SIZE, PlacedBatch};
use iggy_object_store::{
    FreshObjectId, InlineTierExecutor, KeyLayout, MemoryStore, ObjectStore, ObjectStoreError,
    TierScopeToken, is_fresh_object,
};
use partitions::{ExecutorFreshUploader, FreshObjectUploader, FreshUploadOutcome};

fn layout() -> KeyLayout {
    KeyLayout::new("fresh-test").unwrap()
}

type MemoryUploader = ExecutorFreshUploader<InlineTierExecutor<MemoryStore>>;

/// Uploads into a memory store, unless told to refuse.
struct TestUploader {
    uploader: MemoryUploader,
    refuses: Cell<bool>,
}

impl TestUploader {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            uploader: ExecutorFreshUploader::new(
                InlineTierExecutor::new(MemoryStore::new()),
                layout(),
            ),
            refuses: Cell::new(false),
        })
    }

    fn store(&self) -> &MemoryStore {
        self.uploader.executor().store()
    }
}

impl FreshObjectUploader for TestUploader {
    fn upload(
        &self,
        object_id: FreshObjectId,
        bytes: bytes::Bytes,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, FreshUploadOutcome> {
        if self.refuses.get() {
            Box::pin(std::future::ready(Ok(Err(ObjectStoreError::Unavailable(
                "503".to_owned(),
            )))))
        } else {
            self.uploader.upload(object_id, bytes, scope)
        }
    }
}

/// A shard whose partition [`namespace`] is a single-replica primary on disk
/// that places its batches.
#[allow(clippy::future_not_send)]
async fn placing_owner(directory: &Path) -> IoTestShard {
    let bus = Rc::new(IggyMessageBus::new(0));
    let (owner, _sender) = test_owner(&bus, None);
    let partitions = owner.plane.partitions();
    partitions.set_io_notifier(
        owner.partition_io.notifier(),
        owner.partition_io.limits.bytes_max(),
    );
    let partition = partitions.get_mut_by_ns(&namespace()).unwrap();
    partition.set_runtime_options(TopicRuntimeOptions {
        messages_required_to_save: Some(1),
        places_batches: true,
        ..Default::default()
    });
    attach_segment_files(partition, directory, false).await;
    owner
}

/// Submit one send of a message of `payload_len` bytes, as a producer does.
/// Returns where its answer arrives.
fn submit_send(
    owner: &IoTestShard,
    request: u64,
    payload_len: usize,
) -> Receiver<Option<Message<GenericHeader>>> {
    let (reply, answer) = channel(1);
    owner.senders[0]
        .try_send(ShardFrame::lifecycle(LifecycleFrame::PartitionSubmit {
            request: sized_send_request(namespace(), request, payload_len),
            reply,
            attachment: None,
        }))
        .unwrap();
    answer
}

/// Ops the partition committed and wrote to its segment. Each send is one.
fn committed_sends(owner: &IoTestShard) -> u64 {
    let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
    let written = partition.log.active_segment().size.as_bytes_u64() / PLACED_BATCH_SIZE as u64;
    partition.consensus().commit_min().min(written)
}

/// The placed batches the partition's segment holds.
fn stored_placed_batches(directory: &Path) -> Vec<PlacedBatch> {
    std::fs::read(directory.join("00000000000000000000.log"))
        .unwrap()
        .chunks(PLACED_BATCH_SIZE)
        .map(|placed| PlacedBatch::decode(placed).unwrap())
        .collect()
}

#[compio::test]
async fn given_sends_to_a_placing_primary_when_the_window_closes_should_store_their_messages_and_commit_where_they_sit()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = TestUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));

    let answers = pumped(&owner, async {
        let answers = [submit_send(&owner, 1, 100), submit_send(&owner, 2, 300)];
        until(&owner, |owner| committed_sends(owner) == 2).await;
        answers
    })
    .await;

    for answer in answers {
        assert!(
            answer.try_recv().unwrap().is_some(),
            "each producer is answered once its request commits"
        );
    }
    let store = uploader.store();
    let keys = store.keys();
    assert_eq!(keys.len(), 1, "both sends came inside one window: {keys:?}");
    let object_id = layout().parse_fresh_key(&keys[0]).unwrap();
    let size = store.head(&keys[0]).await.unwrap().size;
    let object = store.get_range(&keys[0], 0..size).await.unwrap().bytes;
    assert!(is_fresh_object(&object));
    let placed = stored_placed_batches(directory.path());
    assert_eq!(placed.len(), 2);
    for (offset, placed) in (0..).zip(&placed) {
        assert_eq!(placed.header.base_offset, offset);
        assert_eq!(placed.placement.object_id, object_id);
        let start = usize::try_from(placed.placement.position).unwrap();
        let end = start + usize::try_from(placed.placement.length).unwrap();
        assert!(
            placed.placement.matches(&object[start..end]),
            "the placement at offset {offset} names its own messages"
        );
    }
    assert!(placed[0].placement.length < placed[1].placement.length);
    assert_eq!(
        placed[0].placement.position + placed[0].placement.length,
        placed[1].placement.position
    );
}

#[compio::test]
async fn given_a_store_that_refuses_when_the_window_closes_should_answer_its_producers_and_commit_nothing()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = TestUploader::new();
    uploader.refuses.set(true);
    owner.set_fresh_uploader(Some(uploader.clone()));

    let answer = pumped(&owner, async {
        let answer = submit_send(&owner, 1, 100);
        until(&owner, |_| !answer.is_empty()).await;
        answer
    })
    .await;

    assert!(answer.try_recv().unwrap().is_some());
    assert_eq!(committed_sends(&owner), 0);
    assert_eq!(uploader.store().keys(), Vec::<String>::new());
}

#[compio::test]
async fn given_no_uploader_when_a_send_arrives_should_answer_it_at_once_and_keep_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;

    let answer = pumped(&owner, async {
        let answer = submit_send(&owner, 1, 100);
        until(&owner, |_| !answer.is_empty()).await;
        answer
    })
    .await;

    assert!(answer.try_recv().unwrap().is_some());
    assert_eq!(committed_sends(&owner), 0);
}

#[compio::test]
async fn given_sends_in_two_windows_when_both_close_should_store_two_objects_and_keep_the_order() {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = TestUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));

    pumped(&owner, async {
        let _first = submit_send(&owner, 1, 100);
        until(&owner, |owner| committed_sends(owner) == 1).await;
        let _second = submit_send(&owner, 2, 100);
        until(&owner, |owner| committed_sends(owner) == 2).await;
    })
    .await;

    let mut keys = uploader.store().keys();
    keys.sort();
    assert_eq!(keys.len(), 2);
    let ids: Vec<_> = keys
        .iter()
        .map(|key| layout().parse_fresh_key(key).unwrap())
        .collect();
    let placed = stored_placed_batches(directory.path());
    assert_eq!(
        placed
            .iter()
            .map(|placed| placed.placement.object_id)
            .collect::<Vec<_>>(),
        ids,
        "the later window got the later id, and committed second"
    );
}

/// The shard holds the messages it has just stored, so a consumer at the
/// tail is served without a read of the store. The store the polls would
/// read here is empty, so only those kept bytes can answer.
#[compio::test]
async fn given_sends_this_shard_stored_when_polling_them_should_serve_them_without_reading_the_store()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = TestUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    owner
        .plane
        .partitions()
        .set_remote_segments(Rc::new(partitions::RemoteSegments::new(
            Rc::new(MemoryStore::new()),
            layout(),
        )));
    pumped(&owner, async {
        let _answers = [submit_send(&owner, 1, 100), submit_send(&owner, 2, 300)];
        until(&owner, |owner| committed_sends(owner) == 2).await;
    })
    .await;

    let plan = owner
        .plane
        .partitions()
        .build_poll_snapshot(
            &namespace(),
            partitions::PollingConsumer::Consumer(1, 0),
            &partitions::PollingArgs::new(iggy_common::PollingStrategy::offset(0), 10, false),
        )
        .unwrap();
    assert!(plan.needs_off_pump_io());
    let polled = plan.execute().await.checked().unwrap();

    assert_eq!(polled.message_count(), 2);
    assert_eq!(polled.last_matching_offset(), Some(1));
}

/// Stores the object, and then holds the answer back until told.
struct HeldUploader {
    uploader: MemoryUploader,
    release: RefCell<Option<futures::channel::oneshot::Receiver<()>>>,
}

impl FreshObjectUploader for HeldUploader {
    fn upload(
        &self,
        object_id: FreshObjectId,
        bytes: bytes::Bytes,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, FreshUploadOutcome> {
        let upload = self.uploader.upload(object_id, bytes, scope);
        let release = self.release.borrow_mut().take();
        Box::pin(async move {
            let outcome = upload.await;
            if let Some(release) = release {
                let _ = release.await;
            }
            outcome
        })
    }
}

/// A store that does not answer holds back the sends of its window and
/// nothing else: the pump keeps running, and a partition that keeps its
/// batches whole commits a send meanwhile. The producer of the held send is
/// answered only after the store answered and the placement committed.
#[compio::test]
async fn given_a_store_that_does_not_answer_when_sends_wait_for_it_should_keep_serving_other_partitions()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let (release, held) = futures::channel::oneshot::channel();
    let uploader = Rc::new(HeldUploader {
        uploader: ExecutorFreshUploader::new(InlineTierExecutor::new(MemoryStore::new()), layout()),
        release: RefCell::new(Some(held)),
    });
    owner.set_fresh_uploader(Some(uploader.clone()));
    let whole = IggyNamespace::new(0, 0, 1);
    let committed = |owner: &IoTestShard, namespace: IggyNamespace| {
        owner
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .unwrap()
            .consensus()
            .commit_min()
    };

    let answer = pumped(&owner, async {
        let answer = submit_send(&owner, 1, 100);
        until(&owner, |_| {
            uploader.uploader.executor().store().keys().len() == 1
        })
        .await;

        let (reply, other) = channel(1);
        owner.senders[0]
            .try_send(ShardFrame::lifecycle(LifecycleFrame::PartitionSubmit {
                request: send_request(whole, 2),
                reply,
                attachment: None,
            }))
            .unwrap();
        until(&owner, |owner| committed(owner, whole) == 1).await;
        until(&owner, |_| !other.is_empty()).await;
        assert!(
            answer.is_empty(),
            "the object is stored and the store has not answered: no answer yet"
        );
        assert_eq!(committed(&owner, namespace()), 0);

        release.send(()).unwrap();
        until(&owner, |owner| committed_sends(owner) == 1).await;
        until(&owner, |_| !answer.is_empty()).await;
        answer
    })
    .await;

    assert!(answer.try_recv().unwrap().is_some());
    let (stored, refused, bytes) = owner.metrics().fresh_uploads_value();
    assert_eq!((stored, refused), (1, 0));
    assert!(bytes > 100);
}

/// The sends of every placing partition of the shard go into the object of
/// the open window, so the uploads follow the windows and not the
/// partitions.
#[compio::test]
async fn given_sends_to_two_placing_partitions_in_one_window_when_it_closes_should_store_one_object()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let other = IggyNamespace::new(0, 0, 1);
    {
        let partition = owner.plane.partitions().get_mut_by_ns(&other).unwrap();
        partition.set_runtime_options(TopicRuntimeOptions {
            places_batches: true,
            ..Default::default()
        });
        partition.register_builtin_state_machines().unwrap();
    }
    let uploader = TestUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));

    pumped(&owner, async {
        let _first = submit_send(&owner, 1, 100);
        let (reply, _second) = channel(1);
        owner.senders[0]
            .try_send(ShardFrame::lifecycle(LifecycleFrame::PartitionSubmit {
                request: sized_send_request(other, 2, 100),
                reply,
                attachment: None,
            }))
            .unwrap();
        until(&owner, |owner| {
            committed_sends(owner) == 1
                && owner
                    .plane
                    .partitions()
                    .get_by_ns(&other)
                    .unwrap()
                    .consensus()
                    .commit_min()
                    == 1
        })
        .await;
    })
    .await;

    assert_eq!(uploader.store().keys().len(), 1);
    assert_eq!(owner.metrics().fresh_uploads_value().0, 1);
}
