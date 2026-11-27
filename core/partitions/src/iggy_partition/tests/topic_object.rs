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

use super::free_space::send_request;
use super::*;
use crate::{ExecutorTopicObjects, TOPIC_OBJECT_RETRY, TopicObjectOutcome, TopicObjects};
use iggy_common::IggyError;
use iggy_object_store::{
    InlineTierExecutor, KeyLayout, MemoryStore, ObjectStoreError, TierJobCancelled, TierScope,
    TopicCompression, TopicGeoMode, TopicKey, TopicObject, TopicObjectError, TopicRead, TopicState,
};

const START: u64 = 1_700_000_000_000_000;

/// The primary of a partition of a geo topic, in view 0.
fn geo_partition() -> IggyPartition<RecordingBus> {
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    partition.runtime_options.geo_topic = true;
    partition
}

fn at(micros: u64) -> IggyTimestamp {
    IggyTimestamp::from(micros)
}

fn retry() -> u64 {
    u64::try_from(TOPIC_OBJECT_RETRY.as_micros()).unwrap()
}

fn object(key: TopicKey) -> TopicObject {
    TopicObject {
        key,
        origin: "dc-a".to_owned(),
        cluster_uid: 7,
        stream_name: "orders".to_owned(),
        topic_name: "payments".to_owned(),
        partition_count: 1,
        compression: TopicCompression::None,
        max_message_size: 1024 * 1024,
        message_expiry: u64::MAX,
        max_topic_size: u64::MAX,
        geo_mode: TopicGeoMode::Geo,
        created_at: START,
        published_at: START,
        state: TopicState::Live,
        tombstone_grace: 0,
        read_cache_size: None,
        read_ahead: None,
    }
}

/// Send one message. Returns whether the primary admitted it, and checks
/// that a refusal is the transient one that the SDKs retry.
async fn admits_a_message(partition: &mut IggyPartition<RecordingBus>) -> bool {
    let before = partition.consensus.sequencer().current_sequence();
    let (sender, reply) = consensus::oneshot_channel();
    partition
        .on_request(send_request(partition), Some(sender))
        .await;
    if partition.consensus.sequencer().current_sequence() > before {
        return true;
    }
    assert_eq!(
        reply.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );
    false
}

/// Make the replica the primary of a later view, as after primacy moved away
/// and came back.
fn regain_primacy(partition: &mut IggyPartition<RecordingBus>) {
    loop {
        let view = partition.consensus.view() + 1;
        partition.consensus.set_view(view);
        if partition.consensus.is_primary() {
            break;
        }
    }
}

#[compio::test]
async fn given_a_geo_topic_partition_when_its_primary_has_not_read_the_topic_object_should_refuse_messages_until_it_does()
 {
    let mut partition = geo_partition();
    assert!(!admits_a_message(&mut partition).await);

    let (id, key, _scope) = partition.begin_topic_object_check(at(START)).unwrap();
    assert_eq!(key, partition.topic_key());
    assert_eq!(
        key,
        TopicKey {
            stream_id: partition.namespace().stream_id() as u64,
            topic_id: partition.namespace().topic_id() as u64,
            created_revision: partition.created_revision(),
        }
    );
    assert!(
        partition.begin_topic_object_check(at(START)).is_none(),
        "one read at a time"
    );
    assert!(!admits_a_message(&mut partition).await);

    partition.finish_topic_object_check(id, &Ok(Ok(TopicRead::Found(object(key)))), at(START));

    assert!(admits_a_message(&mut partition).await);
    assert!(
        partition
            .begin_topic_object_check(at(START + 10 * retry()))
            .is_none(),
        "the view keeps what it found"
    );
}

#[test]
fn given_a_geo_topic_partition_that_places_batches_when_its_primary_has_not_read_the_topic_object_should_take_no_placed_send_until_it_does()
 {
    let mut partition = geo_partition();
    partition.set_places_batches(true);
    assert!(
        !partition.takes_placed_sends(),
        "a send uploaded now would be refused at the partition"
    );

    let (id, key, _scope) = partition.begin_topic_object_check(at(START)).unwrap();
    partition.finish_topic_object_check(id, &Ok(Ok(TopicRead::Found(object(key)))), at(START));

    assert!(partition.takes_placed_sends());
}

#[compio::test]
async fn given_a_read_that_did_not_find_a_live_topic_when_the_next_pass_runs_should_wait_and_keep_refusing()
 {
    let misses: [fn(TopicKey) -> TopicObjectOutcome<TopicRead>; 4] = [
        |_| Ok(Ok(TopicRead::Missing)),
        |key| Ok(Ok(TopicRead::Found(object(key).into_tombstone(START)))),
        |_| Ok(Ok(TopicRead::Invalid(TopicObjectError::Truncated(0)))),
        |_| Ok(Err(ObjectStoreError::Unavailable("down".to_owned()))),
    ];
    for miss in misses {
        let mut partition = geo_partition();
        let (id, key, _scope) = partition.begin_topic_object_check(at(START)).unwrap();

        partition.finish_topic_object_check(id, &miss(key), at(START));

        assert!(!admits_a_message(&mut partition).await);
        assert!(
            partition
                .begin_topic_object_check(at(START + retry() - 1))
                .is_none()
        );
        let (id, key, _scope) = partition
            .begin_topic_object_check(at(START + retry()))
            .unwrap();
        partition.finish_topic_object_check(
            id,
            &Ok(Ok(TopicRead::Found(object(key)))),
            at(START + retry()),
        );
        assert!(admits_a_message(&mut partition).await);
    }
}

#[compio::test]
async fn given_a_cancelled_read_when_the_next_pass_runs_should_read_again_at_once() {
    let mut partition = geo_partition();
    let (id, _key, _scope) = partition.begin_topic_object_check(at(START)).unwrap();

    partition.finish_topic_object_check(id, &Err(TierJobCancelled), at(START));

    assert!(!admits_a_message(&mut partition).await);
    assert!(partition.begin_topic_object_check(at(START)).is_some());
}

#[compio::test]
async fn given_the_result_of_another_read_when_finishing_should_change_nothing() {
    let mut partition = geo_partition();
    let (id, key, _scope) = partition.begin_topic_object_check(at(START)).unwrap();

    partition.finish_topic_object_check(id + 1, &Ok(Ok(TopicRead::Found(object(key)))), at(START));

    assert!(!admits_a_message(&mut partition).await);
    assert!(
        partition.begin_topic_object_check(at(START)).is_none(),
        "the read in flight still holds its place"
    );
}

#[compio::test]
async fn given_a_new_view_when_the_replica_is_the_primary_again_should_read_the_object_again() {
    let mut partition = geo_partition();
    let (id, key, _scope) = partition.begin_topic_object_check(at(START)).unwrap();
    partition.finish_topic_object_check(id, &Ok(Ok(TopicRead::Found(object(key)))), at(START));
    assert!(partition.takes_messages_of_its_topic());

    regain_primacy(&mut partition);

    assert!(!partition.takes_messages_of_its_topic());
    let (next, _key, _scope) = partition.begin_topic_object_check(at(START)).unwrap();
    assert!(next > id, "a read of a new view never reuses a number");
    // A late result of the earlier view's read changes nothing.
    partition.finish_topic_object_check(id, &Ok(Ok(TopicRead::Found(object(key)))), at(START));
    assert!(!partition.takes_messages_of_its_topic());
    partition.finish_topic_object_check(next, &Ok(Ok(TopicRead::Found(object(key)))), at(START));
    assert!(partition.takes_messages_of_its_topic());
}

#[compio::test]
async fn given_a_backup_or_a_normal_topic_when_a_pass_runs_should_read_nothing() {
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.runtime_options.geo_topic = true;
    assert!(!backup.consensus.is_primary());
    assert!(backup.begin_topic_object_check(at(START)).is_none());

    let (mut normal, _) = recording_partition_at(0, 3);
    normal.runtime_options.durability = iggy_common::Durability::Replicated;
    assert!(normal.begin_topic_object_check(at(START)).is_none());
    assert!(normal.takes_messages_of_its_topic());
    assert!(admits_a_message(&mut normal).await);
}

#[compio::test]
async fn given_topic_objects_of_two_roots_when_listing_one_should_skip_the_keys_of_the_other() {
    let objects = ExecutorTopicObjects::new(
        InlineTierExecutor::new(MemoryStore::new()),
        KeyLayout::new("iggy").unwrap(),
    );
    let longer = KeyLayout::new("iggy/b").unwrap();
    let ours = TopicKey {
        stream_id: 1,
        topic_id: 2,
        created_revision: 3,
    };
    let theirs = TopicKey {
        stream_id: 4,
        ..ours
    };
    let scope = TierScope::new();
    objects
        .put(object(ours), scope.token())
        .await
        .unwrap()
        .unwrap();
    object(theirs)
        .put(objects.executor().store(), &longer)
        .await
        .unwrap();

    let listed = objects.list(scope.token()).await.unwrap().unwrap();

    assert_eq!(listed, [ours]);
    assert_eq!(
        objects.read(ours, scope.token()).await.unwrap().unwrap(),
        TopicRead::Found(object(ours))
    );
    assert_eq!(
        objects.read(theirs, scope.token()).await.unwrap().unwrap(),
        TopicRead::Missing
    );
}
