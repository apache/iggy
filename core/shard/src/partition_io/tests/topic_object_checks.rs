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

//! Passes over the partitions of geo topics, which read each topic's object
//! before the partition takes messages.

use super::object_deletes::layout;
use super::segment_uploads::{namespace, until};
use super::*;
use iggy_object_store::{
    InlineTierExecutor, MemoryStore, TopicCompression, TopicGeoMode, TopicKey, TopicObject,
    TopicState,
};
use partitions::{ExecutorTopicObjects, TopicObjects};

type Objects = ExecutorTopicObjects<InlineTierExecutor<MemoryStore>>;

/// A shard whose partition [`namespace`] is the primary of a geo topic's
/// partition, with the jobs on topic objects over a memory store when
/// `objects` holds.
fn geo_owner(objects: bool) -> (IoTestShard, Rc<Objects>) {
    let bus = Rc::new(IggyMessageBus::new(0));
    let (owner, _sender) = test_owner(&bus, None);
    owner
        .plane
        .partitions()
        .get_mut_by_ns(&namespace())
        .unwrap()
        .set_runtime_options(TopicRuntimeOptions {
            geo_topic: true,
            ..Default::default()
        });
    let topic_objects = Rc::new(ExecutorTopicObjects::new(
        InlineTierExecutor::new(MemoryStore::new()),
        layout(),
    ));
    if objects {
        owner.set_topic_objects(Some(Rc::clone(&topic_objects) as Rc<dyn TopicObjects>));
    }
    (owner, topic_objects)
}

fn takes_messages(owner: &IoTestShard) -> bool {
    owner
        .plane
        .partitions()
        .get_by_ns(&namespace())
        .unwrap()
        .takes_messages_of_its_topic()
}

/// Store the live object of the fixture's topic.
#[allow(clippy::future_not_send)]
async fn publish(owner: &IoTestShard, objects: &Objects) {
    let key: TopicKey = owner
        .plane
        .partitions()
        .get_by_ns(&namespace())
        .unwrap()
        .topic_key();
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
        created_at: 1,
        published_at: 1,
        state: TopicState::Live,
        tombstone_grace: 0,
    }
    .put(objects.executor().store(), &layout())
    .await
    .unwrap();
}

/// Run one pass and settle the read it started.
#[allow(clippy::future_not_send)]
async fn settled_pass(owner: &IoTestShard) {
    owner.on_check_topic_objects(IggyTimestamp::now());
    until(owner, |owner| owner.inbox_len() > 0).await;
    match owner.inbox.try_recv() {
        Ok(ShardFrame::Lifecycle(LifecycleFrame::TopicObjectCheckFinished(finished))) => {
            owner.on_topic_object_check_finished(&finished);
        }
        _ => panic!("only the result of the read is on its way"),
    }
}

#[compio::test]
async fn given_the_object_of_its_geo_topic_in_the_store_when_a_pass_runs_should_let_the_partition_take_messages()
 {
    let (owner, objects) = geo_owner(true);
    assert!(!takes_messages(&owner));

    // The metadata primary has not stored the object yet.
    settled_pass(&owner).await;
    assert!(!takes_messages(&owner));

    publish(&owner, &objects).await;
    // A read that found nothing waits its retry before the next one.
    owner.on_check_topic_objects(IggyTimestamp::now());
    compio::time::sleep(TASK_POLL_INTERVAL * 10).await;
    assert_eq!(owner.inbox_len(), 0, "no read is on its way");
    compio::time::sleep(partitions::TOPIC_OBJECT_RETRY).await;
    settled_pass(&owner).await;

    assert!(takes_messages(&owner));
    // The other partitions of the shard are of a normal topic, so the pass
    // read for this one alone.
    assert_eq!(owner.inbox_len(), 0);
}

#[compio::test]
async fn given_no_topic_objects_when_a_pass_runs_should_read_nothing() {
    let (owner, objects) = geo_owner(false);
    publish(&owner, &objects).await;

    owner.on_check_topic_objects(IggyTimestamp::now());
    compio::time::sleep(TASK_POLL_INTERVAL * 10).await;

    assert_eq!(owner.inbox_len(), 0, "no read is on its way");
    assert!(!takes_messages(&owner));
}
