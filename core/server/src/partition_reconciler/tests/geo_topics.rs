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

//! The objects of geo topics that the metadata primary keeps in its store,
//! driven through the passes of the partition reconciler.

use super::*;
use crate::geo_topics::SWEEP_INTERVAL;
use iggy_binary_protocol::requests::streams::UpdateStreamResolvedRequest;
use iggy_common::{
    CompressionAlgorithm, GeoMode, GeoTopicDurations, IggyByteSize, MaxTopicSize,
    TopicCreateOptions, TopicType,
};
use iggy_object_store::{
    TopicCompression, TopicGeoMode, TopicKey, TopicObject, TopicRead, TopicState,
};
use partitions::{ExecutorTopicObjects, TopicObjects};

type MemoryTopicObjects = ExecutorTopicObjects<InlineTierExecutor<MemoryStore>>;

const OWN_CLUSTER_UID: u128 = 0x5eed_0000_0000_0000_0000_0000_0000_0001;
const GRACE: Duration = Duration::from_hours(1);
const MESSAGE_EXPIRY: Duration = Duration::from_hours(6);
const MAX_TOPIC_SIZE: u64 = 10 * 1024 * 1024 * 1024;

fn geo_config(tmp: &TempDir) -> ServerConfig {
    let mut config = test_config(tmp);
    config.geo.origin = "dc-a".to_owned();
    config.geo.tombstone_grace = IggyDuration::new(GRACE);
    config
}

/// Drive a `CreateTopicWithAssignments` of a geo topic through the state
/// machine, past the admission that refuses one from a client.
fn seed_geo_topic(
    mux: &TestMux,
    op: u64,
    stream_id: u32,
    name: &str,
    assignments: Vec<CreatedPartitionAssignment>,
) {
    seed_geo_topic_with(
        mux,
        op,
        (stream_id, name),
        assignments,
        GeoTopicDurations::default(),
    );
}

/// [`seed_geo_topic`] of a topic that sets `durations` for itself.
fn seed_geo_topic_with(
    mux: &TestMux,
    op: u64,
    (stream_id, name): (u32, &str),
    assignments: Vec<CreatedPartitionAssignment>,
    durations: GeoTopicDurations,
) {
    let options = TopicCreateOptions {
        topic_type: Some(TopicType::Geo),
        geo_mode: Some(GeoMode::Geo),
        compression_algorithm: Some(CompressionAlgorithm::Gzip),
        message_expiry: Some(IggyExpiry::ExpireDuration(IggyDuration::new(
            MESSAGE_EXPIRY,
        ))),
        max_topic_size: Some(MaxTopicSize::Custom(IggyByteSize::from(MAX_TOPIC_SIZE))),
        geo_durations: durations,
        ..TopicCreateOptions::default()
    };
    let req = CreateTopicWithAssignmentsRequest {
        request: CreateTopicRequest {
            stream_id: WireIdentifier::numeric(stream_id),
            partitions_count: u32::try_from(assignments.len()).unwrap(),
            name: WireName::new(name).expect("test topic name fits WireName"),
            options: options.to_wire().expect("geo topic options encode"),
        },
        derived_options: WireOptions::empty(),
        partitions: assignments,
    };
    mux.update(build_prepare(
        op,
        Operation::CreateTopicWithAssignments,
        &req,
    ))
    .expect("CreateTopicWithAssignments apply succeeds");
}

/// Make the change log name `cluster_uid`, as the first committed operation
/// of a cluster does.
fn seed_cluster_uid(shard: &TestShard, cluster_uid: u128) {
    let header = PrepareHeader {
        op: 1,
        checksum: cluster_uid,
        ..PrepareHeader::default()
    };
    shard
        .plane
        .metadata()
        .change_log()
        .append(&header, Vec::new())
        .expect("the change log of the metadata shard takes the first operation");
}

/// Give `shard` the jobs on topic objects over a memory store, and return
/// them for the test to reach the store.
fn install_memory_topic_objects(shard: &TestShard) -> Rc<MemoryTopicObjects> {
    let objects = Rc::new(ExecutorTopicObjects::new(
        InlineTierExecutor::new(MemoryStore::new()),
        object_layout(),
    ));
    shard.set_topic_objects(Some(Rc::clone(&objects) as Rc<dyn TopicObjects>));
    objects
}

/// The incarnation of the topic of `ns`, which names its object.
fn topic_key(shard: &TestShard, ns: IggyNamespace) -> TopicKey {
    let prefix = committed_prefix(shard, ns);
    TopicKey {
        stream_id: prefix.stream_id,
        topic_id: prefix.topic_id,
        created_revision: prefix.created_revision,
    }
}

fn read_topic(objects: &MemoryTopicObjects, key: TopicKey) -> TopicRead {
    futures::executor::block_on(TopicObject::read(
        objects.executor().store(),
        &object_layout(),
        &key,
    ))
    .expect("a memory store answers")
}

fn stored_object(objects: &MemoryTopicObjects, key: TopicKey) -> TopicObject {
    match read_topic(objects, key) {
        TopicRead::Found(object) => object,
        read => panic!("no topic object under {key:?}: {read:?}"),
    }
}

/// Run the pass of the geo topics as of `now` and let its task finish.
async fn geo_topics_pass(ctx: &ReconcilerCtx, now: IggyTimestamp) {
    ctx.geo_topics.pass(&ctx.shard, now);
    for _ in 0..256 {
        if !ctx.geo_topics.running() {
            return;
        }
        super::super::yield_now().await;
    }
    panic!("the task of the geo topics pass did not finish");
}

fn after(start: IggyTimestamp, wait: Duration) -> IggyTimestamp {
    IggyTimestamp::from(start.as_micros() + u64::try_from(wait.as_micros()).unwrap())
}

/// A metadata primary with one geo topic of two partitions and one normal
/// topic of one, in the stream `orders`.
fn geo_primary(config: &ServerConfig) -> (Rc<TestShard>, Rc<MemoryTopicObjects>, TopicKey) {
    geo_primary_with(config, GeoTopicDurations::default())
}

/// [`geo_primary`] whose geo topic sets `durations` for itself.
fn geo_primary_with(
    config: &ServerConfig,
    durations: GeoTopicDurations,
) -> (Rc<TestShard>, Rc<MemoryTopicObjects>, TopicKey) {
    let mux = TestMux::default();
    seed_stream(&mux, 1, "orders");
    seed_geo_topic_with(
        &mux,
        2,
        (0, "payments"),
        vec![assignment(0, 1), assignment(1, 2)],
        durations,
    );
    seed_topic(&mux, 3, 0, "normal", vec![assignment(0, 3)]);
    let shard = build_metadata_primary_shard(config, mux);
    seed_cluster_uid(&shard, OWN_CLUSTER_UID);
    let objects = install_memory_topic_objects(&shard);
    let key = topic_key(&shard, IggyNamespace::new(0, 0, 0));
    (shard, objects, key)
}

#[compio::test]
async fn given_a_committed_geo_topic_when_the_metadata_primary_reconciles_should_store_what_a_peer_needs()
 {
    let tmp = TempDir::new().unwrap();
    let config = geo_config(&tmp);
    let (shard, objects, key) = geo_primary(&config);
    let ctx = make_ctx(Rc::clone(&shard), Rc::new(config));
    let now = IggyTimestamp::now();

    geo_topics_pass(&ctx, now).await;

    let object = stored_object(&objects, key);
    let created_at = shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .read(|inner| inner.items[0].topics[0].created_at.as_micros());
    assert_eq!(
        object,
        TopicObject {
            key,
            origin: "dc-a".to_owned(),
            cluster_uid: OWN_CLUSTER_UID,
            stream_name: "orders".to_owned(),
            topic_name: "payments".to_owned(),
            partition_count: 2,
            compression: TopicCompression::Gzip,
            max_message_size: u64::try_from(shard.bus_max_message_size()).unwrap(),
            message_expiry: u64::try_from(MESSAGE_EXPIRY.as_micros()).unwrap(),
            max_topic_size: MAX_TOPIC_SIZE,
            geo_mode: TopicGeoMode::Geo,
            created_at,
            published_at: now.as_micros(),
            state: TopicState::Live,
            tombstone_grace: 0,
        }
    );
    // The normal topic has no object.
    assert_eq!(
        objects.executor().store().keys(),
        [object_layout().topic_key(&key)]
    );
}

#[compio::test]
async fn given_a_node_that_cannot_publish_when_reconciling_should_store_nothing() {
    let tmp = TempDir::new().unwrap();
    // A node without an origin publishes no geo topic.
    let (shard, objects, _) = geo_primary(&test_config(&tmp));
    let ctx = make_ctx(Rc::clone(&shard), Rc::new(test_config(&tmp)));
    geo_topics_pass(&ctx, IggyTimestamp::now()).await;
    assert!(objects.executor().store().is_empty());

    // Nor does a node that is not the metadata primary.
    let config = geo_config(&tmp);
    let mux = TestMux::default();
    seed_stream(&mux, 1, "orders");
    seed_geo_topic(&mux, 2, 0, "payments", vec![assignment(0, 1)]);
    let backup = build_test_shard(0, &config, mux);
    seed_cluster_uid(&backup, OWN_CLUSTER_UID);
    let objects = install_memory_topic_objects(&backup);
    let ctx = make_ctx(backup, Rc::new(config));
    geo_topics_pass(&ctx, IggyTimestamp::now()).await;
    assert!(objects.executor().store().is_empty());
}

#[compio::test]
async fn given_a_deleted_geo_topic_when_the_sweep_runs_should_keep_its_tombstone_and_objects_for_the_grace()
 {
    let tmp = TempDir::new().unwrap();
    let config = geo_config(&tmp);
    let (shard, objects, key) = geo_primary(&config);
    // The deletes of removed incarnations run beside the sweep, and must
    // leave the geo topic to it.
    let deleter = install_memory_deleter(&shard);
    let ctx = make_ctx(Rc::clone(&shard), Rc::new(config));
    reconcile_pass(&ctx).await;
    let store = objects.executor().store();
    let normal = committed_prefix(&shard, IggyNamespace::new(0, 1, 0));
    let mut kept = store_objects(store, normal).await;
    for partition_id in 0..2 {
        store_objects(store, key.partition_prefix(partition_id)).await;
    }
    let start = IggyTimestamp::now();
    geo_topics_pass(&ctx, start).await;
    let data_keys = store.keys();
    assert_eq!(data_keys.len(), 10, "{data_keys:?}");

    seed_delete_topic(&shard.plane.metadata().mux_stm, 4, 0, 0);
    reconcile_pass(&ctx).await;
    assert_eq!(ctx.incarnation_deletes.queued(), 0);
    assert_eq!(stored_keys(&deleter), Vec::<String>::new());

    // The next sweep turns the object into a tombstone.
    let swept = after(start, SWEEP_INTERVAL);
    geo_topics_pass(&ctx, swept).await;
    let tombstone = stored_object(&objects, key);
    assert_eq!(
        tombstone.state,
        TopicState::Tombstone {
            deleted_at: swept.as_micros()
        }
    );
    assert_eq!(tombstone.partition_count, 2);
    assert_eq!(store.keys(), data_keys);

    // Within the grace, the tombstone and the topic's objects stay.
    let within = GRACE.checked_sub(2 * SWEEP_INTERVAL).unwrap();
    geo_topics_pass(&ctx, after(swept, within)).await;
    assert_eq!(stored_object(&objects, key), tombstone);
    assert_eq!(store.keys(), data_keys);

    // Once the grace ends, the topic's objects go, and the tombstone last.
    geo_topics_pass(&ctx, after(swept, GRACE)).await;
    assert_eq!(read_topic(&objects, key), TopicRead::Missing);
    kept.sort();
    assert_eq!(store.keys(), kept);
}

/// The topic is no longer in metadata when its tombstone is written, so its
/// own grace lives in its object.
#[compio::test]
async fn given_a_deleted_geo_topic_with_its_own_grace_when_the_sweep_runs_should_delete_it_once_that_grace_ends()
 {
    const OWN_GRACE: Duration = Duration::from_mins(10);
    let tmp = TempDir::new().unwrap();
    let config = geo_config(&tmp);
    let (shard, objects, key) = geo_primary_with(
        &config,
        GeoTopicDurations {
            tombstone_grace: Some(IggyDuration::new(OWN_GRACE)),
            ..GeoTopicDurations::default()
        },
    );
    let ctx = make_ctx(Rc::clone(&shard), Rc::new(config));
    reconcile_pass(&ctx).await;
    let start = IggyTimestamp::now();
    geo_topics_pass(&ctx, start).await;
    let own_grace = u64::try_from(OWN_GRACE.as_micros()).unwrap();
    assert_eq!(stored_object(&objects, key).tombstone_grace, own_grace);

    seed_delete_topic(&shard.plane.metadata().mux_stm, 4, 0, 0);
    reconcile_pass(&ctx).await;
    let swept = after(start, SWEEP_INTERVAL);
    geo_topics_pass(&ctx, swept).await;
    let tombstone = stored_object(&objects, key);
    assert!(!tombstone.is_live());
    assert_eq!(tombstone.tombstone_grace, own_grace);

    geo_topics_pass(
        &ctx,
        after(swept, OWN_GRACE.checked_sub(SWEEP_INTERVAL).unwrap()),
    )
    .await;
    assert_eq!(stored_object(&objects, key), tombstone);
    // Long before the node's grace of an hour.
    geo_topics_pass(&ctx, after(swept, OWN_GRACE)).await;
    assert_eq!(read_topic(&objects, key), TopicRead::Missing);
}

#[compio::test]
async fn given_an_object_that_the_store_lost_or_a_deposed_primary_tombstoned_when_the_sweep_runs_should_store_it_again()
 {
    let tmp = TempDir::new().unwrap();
    let config = geo_config(&tmp);
    let (shard, objects, key) = geo_primary(&config);
    let ctx = make_ctx(shard, Rc::new(config));
    let start = IggyTimestamp::now();
    geo_topics_pass(&ctx, start).await;
    let live = stored_object(&objects, key);
    let store = objects.executor().store();

    live.clone()
        .into_tombstone(start.as_micros())
        .put(store, &object_layout())
        .await
        .unwrap();
    // A pass between sweeps trusts what it stored.
    geo_topics_pass(&ctx, after(start, Duration::from_secs(2))).await;
    assert!(!stored_object(&objects, key).is_live());
    geo_topics_pass(&ctx, after(start, SWEEP_INTERVAL)).await;
    assert!(stored_object(&objects, key).is_live());

    let deleted = store
        .delete(&[object_layout().topic_key(&key)])
        .await
        .unwrap();
    assert!(deleted.failed.is_empty());
    geo_topics_pass(&ctx, after(start, 2 * SWEEP_INTERVAL)).await;
    assert_eq!(
        stored_object(&objects, key),
        TopicObject {
            published_at: after(start, 2 * SWEEP_INTERVAL).as_micros(),
            ..live
        }
    );
}

#[compio::test]
async fn given_a_renamed_stream_when_the_metadata_primary_reconciles_should_store_the_new_name() {
    let tmp = TempDir::new().unwrap();
    let config = geo_config(&tmp);
    let (shard, objects, key) = geo_primary(&config);
    let ctx = make_ctx(Rc::clone(&shard), Rc::new(config));
    let start = IggyTimestamp::now();
    geo_topics_pass(&ctx, start).await;

    let rename = UpdateStreamResolvedRequest {
        stream_id: WireIdentifier::numeric(0),
        name: Some(WireName::new("orders-eu").unwrap()),
        explicit_options: WireOptions::empty(),
        derived_options: WireOptions::empty(),
    };
    shard
        .plane
        .metadata()
        .mux_stm
        .update(build_prepare(4, Operation::UpdateStreamResolved, &rename))
        .expect("UpdateStreamResolved apply succeeds");
    geo_topics_pass(&ctx, after(start, Duration::from_secs(2))).await;

    assert_eq!(stored_object(&objects, key).stream_name, "orders-eu");
}

/// Partitions added to a geo topic come at another revision, so the node
/// can no longer describe the topic in one object. The sweep must not take
/// that for a delete.
#[compio::test]
async fn given_a_geo_topic_the_node_cannot_describe_when_the_sweep_runs_should_leave_its_object() {
    let tmp = TempDir::new().unwrap();
    let config = geo_config(&tmp);
    let (shard, objects, key) = geo_primary(&config);
    let ctx = make_ctx(Rc::clone(&shard), Rc::new(config));
    let start = IggyTimestamp::now();
    geo_topics_pass(&ctx, start).await;
    let stored = stored_object(&objects, key);

    shard
        .plane
        .metadata()
        .mux_stm
        .update(build_prepare(
            4,
            Operation::CreatePartitionsWithAssignments,
            &CreatePartitionsWithAssignmentsRequest {
                request: CreatePartitionsRequest {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                    partitions_count: 1,
                },
                partitions: vec![assignment(0, 4)],
            },
        ))
        .expect("CreatePartitions apply succeeds");
    geo_topics_pass(&ctx, after(start, SWEEP_INTERVAL)).await;

    assert_eq!(stored_object(&objects, key), stored);
}
