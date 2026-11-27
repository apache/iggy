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

//! Manifest passes on a shard that hosts a tiered primary on disk.

use super::object_deletes::{
    Shared, TRUNCATED_UP_TO, assert_started, layout, sweep, truncated_owner,
};
use super::segment_uploads::{SEALED_SEGMENTS, namespace, staged_revision, until};
use super::*;
use iggy_object_store::{ObjectStore, PartitionManifest};
use partitions::ExecutorManifestPublisher;

/// The objects of the fixture's sealed segments, a `.log` and an `.index`
/// each.
const UPLOADED_OBJECTS: usize = 6;

fn give_publisher(owner: &IoTestShard, executor: &Shared) {
    owner.set_manifest_publisher(Some(Rc::new(ExecutorManifestPublisher::new(
        executor.clone(),
        layout(),
    ))));
}

/// Run one manifest pass and settle the publish it started.
#[allow(clippy::future_not_send)]
async fn settled_pass(owner: &IoTestShard) {
    owner.on_publish_manifest(namespace(), staged_revision(owner), IggyTimestamp::now());
    until(owner, |owner| owner.inbox_len() > 0).await;
    match owner.inbox.try_recv() {
        Ok(ShardFrame::Lifecycle(LifecycleFrame::ManifestPublishFinished(finished))) => {
            owner.on_manifest_publish_finished(&finished);
        }
        _ => panic!("only the result of the publish is on its way"),
    }
}

#[allow(clippy::future_not_send)]
async fn stored_manifest(owner: &IoTestShard, executor: &Shared) -> Option<PartitionManifest> {
    let prefix = owner
        .plane
        .partitions()
        .get_by_ns(&namespace())
        .unwrap()
        .object_prefix()
        .unwrap();
    let key = layout().manifest_key(&prefix);
    let store = executor.0.store();
    let size = store.head(&key).await.ok()?.size;
    let object = store.get_range(&key, 0..size).await.unwrap();
    Some(PartitionManifest::decode(&object.bytes).unwrap())
}

#[compio::test]
async fn given_uploaded_segments_and_a_truncation_when_a_manifest_pass_runs_should_store_what_is_left()
 {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor, _deleter) = Box::pin(truncated_owner(directory.path())).await;
    give_publisher(&owner, &executor);

    settled_pass(&owner).await;

    let manifest = stored_manifest(&owner, &executor).await.unwrap();
    let last_sealed = SEALED_SEGMENTS - 1;
    assert_eq!(manifest.trim_point, Some(TRUNCATED_UP_TO));
    assert_eq!(manifest.floor, Some(last_sealed));
    let listed: Vec<_> = manifest
        .segments
        .iter()
        .map(|segment| (segment.start_offset, segment.end_offset))
        .collect();
    assert_eq!(listed, [(last_sealed, last_sealed)]);

    // Nothing changed, so the next pass stores nothing.
    owner.on_publish_manifest(namespace(), staged_revision(&owner), IggyTimestamp::now());
    compio::time::sleep(TASK_POLL_INTERVAL * 10).await;
    assert_eq!(owner.inbox_len(), 0, "no result is on its way");
}

#[compio::test]
async fn given_no_publisher_when_a_manifest_pass_runs_should_store_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor, _deleter) = Box::pin(truncated_owner(directory.path())).await;

    owner.on_publish_manifest(namespace(), staged_revision(&owner), IggyTimestamp::now());
    compio::time::sleep(TASK_POLL_INTERVAL * 10).await;

    assert_eq!(owner.inbox_len(), 0, "no result is on its way");
    assert_eq!(stored_manifest(&owner, &executor).await, None);
    assert_eq!(executor.0.store().len(), UPLOADED_OBJECTS);
}

#[compio::test]
async fn given_a_pass_staged_for_another_incarnation_when_it_runs_should_store_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor, _deleter) = Box::pin(truncated_owner(directory.path())).await;
    give_publisher(&owner, &executor);

    owner.on_publish_manifest(
        namespace(),
        staged_revision(&owner) + 1,
        IggyTimestamp::now(),
    );
    compio::time::sleep(TASK_POLL_INTERVAL * 10).await;

    assert_eq!(owner.inbox_len(), 0, "no result is on its way");
    assert_eq!(stored_manifest(&owner, &executor).await, None);
}

/// A reader of the manifest must not find a segment it lists missing.
#[compio::test]
async fn given_a_trim_point_no_manifest_holds_when_a_delete_pass_runs_should_wait_for_the_manifest()
{
    let directory = tempfile::tempdir().unwrap();
    let (owner, executor, deleter) = Box::pin(truncated_owner(directory.path())).await;
    give_publisher(&owner, &executor);
    let now = IggyTimestamp::now();

    owner.on_delete_objects(namespace(), staged_revision(&owner), now, TRUNCATED_UP_TO);
    assert_started(&owner, &deleter, 0).await;

    settled_pass(&owner).await;
    owner.on_delete_objects(namespace(), staged_revision(&owner), now, TRUNCATED_UP_TO);
    until(&owner, |owner| owner.inbox_len() > 0).await;

    assert_eq!(deleter.started(), [sweep()]);
}
