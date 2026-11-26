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

//! The sweep of the store root for orphan incarnations, driven through the
//! passes of the metadata primary's reconciler over a memory store.

use super::geo_topics::seed_cluster_uid;
use super::*;
use configs::tiered_storage::TieredStorageBackend;
use iggy_object_store::{LongTermKey, StoreIdentity, TopicKey};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

const OWN_CLUSTER_UID: u128 = 0x5eed_0000_0000_0000_0000_0000_0000_0002;
const GRACE: Duration = Duration::from_secs(1);

fn sweep_config(tmp: &TempDir, grace: Duration) -> ServerConfig {
    let mut config = test_config(tmp);
    config.tiered_storage.backend = TieredStorageBackend::Fs;
    config.tiered_storage.orphan_grace = IggyDuration::new(grace);
    config
}

/// A metadata primary of one stream with one topic of one partition, that
/// holds a deleter over a memory store whose root `cluster_uid` claimed.
async fn sweeping_primary(
    config: &ServerConfig,
    cluster_uid: u128,
) -> (Rc<ReconcilerCtx>, Rc<MemoryDeleter>) {
    let mux = TestMux::default();
    seed_stream(&mux, 1, "objects");
    seed_topic(&mux, 2, 0, "kept", vec![assignment(0, 1)]);
    let shard = build_metadata_primary_shard(config, mux);
    seed_cluster_uid(&shard, OWN_CLUSTER_UID);
    let deleter = install_memory_deleter(&shard);
    StoreIdentity { cluster_uid }
        .claim(deleter.executor().store(), &object_layout())
        .await
        .unwrap();
    // The age checks count in whole seconds, so the store's clock must move
    // past the second of the claim before the test stores.
    let into_second = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_micros();
    compio::time::sleep(Duration::from_micros(
        u64::from(1_000_000 - into_second) + 5_000,
    ))
    .await;
    let ctx = make_ctx(shard, Rc::new(config.clone()));
    (ctx, deleter)
}

fn revision(ctx: &ReconcilerCtx) -> u64 {
    ctx.shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .read(|inner| inner.revision)
}

/// An incarnation of ids that committed metadata does not hold.
fn absent(stream_id: u64, created_revision: u64) -> ObjectPrefix {
    ObjectPrefix {
        stream_id,
        topic_id: 0,
        partition_id: 0,
        created_revision,
    }
}

async fn put_key(store: &MemoryStore, key: &str) {
    store
        .put(key, PutBody::from_bytes(vec![1]), &PutOptions::default())
        .await
        .unwrap();
}

/// The moment of the monotonic clock that the wall clock moment `now` maps
/// to, while the wall clock does not step.
fn monotonic(now: IggyTimestamp) -> Instant {
    static START: OnceLock<Instant> = OnceLock::new();
    *START.get_or_init(Instant::now) + Duration::from_micros(now.as_micros())
}

/// Run the sweep's pass as of `now` and let the sweep it starts finish.
async fn sweep_pass(ctx: &ReconcilerCtx, now: IggyTimestamp) {
    sweep_pass_at(ctx, now, monotonic(now)).await;
}

/// [`sweep_pass`] at the wall clock moment `now` and the monotonic moment
/// `at`.
async fn sweep_pass_at(ctx: &ReconcilerCtx, now: IggyTimestamp, at: Instant) {
    ctx.orphan_sweep
        .pass(&ctx.shard, &ctx.incarnation_deletes, now, at);
    wait_for_the_sweep(ctx).await;
}

async fn wait_for_the_sweep(ctx: &ReconcilerCtx) {
    for _ in 0..256 {
        if !ctx.orphan_sweep.running() {
            return;
        }
        super::super::yield_now().await;
    }
    panic!("the orphan sweep did not finish");
}

fn later(start: IggyTimestamp, wait: Duration) -> IggyTimestamp {
    IggyTimestamp::from(start.as_micros() + u64::try_from(wait.as_micros()).unwrap())
}

fn sorted(keys: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut keys: Vec<String> = keys.into_iter().collect();
    keys.sort();
    keys
}

#[compio::test]
async fn given_an_orphan_when_sweeps_found_it_unchanged_for_its_grace_should_delete_it_and_leave_everything_else()
 {
    let tmp = TempDir::new().unwrap();
    let config = sweep_config(&tmp, GRACE);
    let (ctx, deleter) = sweeping_primary(&config, OWN_CLUSTER_UID).await;
    let store = deleter.executor().store();
    let layout = object_layout();
    let committed = revision(&ctx);
    let orphan = absent(5, committed);
    let orphan_keys = store_objects(store, orphan).await;
    let live = store_objects(
        store,
        committed_prefix(&ctx.shard, IggyNamespace::new(0, 0, 0)),
    )
    .await;
    let after_snapshot = store_objects(store, absent(6, committed + 1)).await;
    let long_term = absent(7, committed);
    let mut geo = store_objects(store, long_term).await;
    let long_term_key = layout.long_term_key(&LongTermKey {
        prefix: long_term,
        start_offset: 0,
        end_offset: 9,
        object: SegmentObject::Log,
    });
    let topic_object = absent(8, committed);
    let topic_key = layout.topic_key(&TopicKey {
        stream_id: topic_object.stream_id,
        topic_id: topic_object.topic_id,
        created_revision: topic_object.created_revision,
    });
    geo.extend(store_objects(store, topic_object).await);
    let meta = [
        layout.fresh_key(1),
        "iggy/meta/notes".to_owned(),
        "iggy/README".to_owned(),
    ];
    for key in meta.iter().chain([&long_term_key, &topic_key]) {
        put_key(store, key).await;
    }
    let kept = sorted(
        [live, after_snapshot, geo, meta.to_vec()]
            .concat()
            .into_iter()
            .chain([long_term_key, topic_key, layout.identity_key()]),
    );
    let first = IggyTimestamp::now();

    sweep_pass(&ctx, first).await;
    sweep_pass(&ctx, later(first, GRACE / 2)).await;
    incarnation_delete_pass(&ctx, later(first, GRACE / 2)).await;

    assert_eq!(ctx.orphan_sweep.candidates(), 1);
    assert!(!ctx.incarnation_deletes.is_queued_orphan(&orphan));
    assert_eq!(
        stored_keys(&deleter),
        sorted([kept.clone(), orphan_keys].concat())
    );

    // The grace counts from the start of the second sweep.
    let second = later(first, GRACE);
    sweep_pass(&ctx, second).await;
    sweep_pass(&ctx, second).await;
    assert!(!ctx.incarnation_deletes.is_queued_orphan(&orphan));

    let third = later(first, GRACE * 2);
    sweep_pass(&ctx, third).await;
    assert!(stored_keys(&deleter).len() > kept.len(), "queued, not run");
    // The next pass queues what the sweep found due, then its turns delete
    // it, each in a pass of its own.
    sweep_pass(&ctx, third).await;
    assert!(ctx.incarnation_deletes.is_queued_orphan(&orphan));
    incarnation_delete_pass(&ctx, third).await;
    assert!(
        stored_keys(&deleter).len() > kept.len(),
        "checked, not deleted"
    );
    incarnation_delete_pass(&ctx, later(third, Duration::from_micros(1))).await;

    assert_eq!(stored_keys(&deleter), kept);
    assert_eq!(ctx.incarnation_deletes.queued(), 0);
    assert_eq!(
        ctx.shard.metrics().orphan_sweep_values(),
        (3, 1, 1, 0, 0),
        "sweeps, orphans of the last, deleted, objects before the claim, identity skips"
    );
}

#[compio::test]
async fn given_a_node_clock_ahead_of_the_store_when_an_orphan_gains_an_object_between_sweeps_should_wait_the_full_grace_again()
 {
    let tmp = TempDir::new().unwrap();
    let config = sweep_config(&tmp, GRACE);
    let (ctx, deleter) = sweeping_primary(&config, OWN_CLUSTER_UID).await;
    let store = deleter.executor().store();
    let orphan = absent(5, revision(&ctx));
    store_objects(store, orphan).await;
    let ahead = later(IggyTimestamp::now(), Duration::from_hours(24));
    for wait in [Duration::ZERO, GRACE, GRACE] {
        sweep_pass(&ctx, later(ahead, wait)).await;
    }
    assert!(!ctx.incarnation_deletes.is_queued_orphan(&orphan));

    compio::time::sleep(Duration::from_millis(2)).await;
    let gained = object_layout().segment_key(&SegmentKey {
        prefix: orphan,
        start_offset: 10,
        object: SegmentObject::Log,
    });
    put_key(store, &gained).await;
    let stored = stored_keys(&deleter);
    for wait in [GRACE * 2, GRACE * 3, GRACE * 3] {
        sweep_pass(&ctx, later(ahead, wait)).await;
    }
    incarnation_delete_pass(&ctx, later(ahead, GRACE * 3)).await;

    assert!(!ctx.incarnation_deletes.is_queued_orphan(&orphan));
    assert_eq!(stored_keys(&deleter), stored);

    let due = later(ahead, GRACE * 4);
    sweep_pass(&ctx, due).await;
    sweep_pass(&ctx, due).await;
    assert!(ctx.incarnation_deletes.is_queued_orphan(&orphan));
    incarnation_delete_pass(&ctx, due).await;
    incarnation_delete_pass(&ctx, later(due, Duration::from_micros(1))).await;
    assert_eq!(stored_keys(&deleter), [object_layout().identity_key()]);
}

#[compio::test]
async fn given_a_root_another_cluster_claimed_when_the_primary_sweeps_should_delete_nothing_and_count_the_skip()
 {
    let tmp = TempDir::new().unwrap();
    let config = sweep_config(&tmp, GRACE);
    let (ctx, deleter) = sweeping_primary(&config, OWN_CLUSTER_UID + 1).await;
    store_objects(deleter.executor().store(), absent(5, revision(&ctx))).await;
    let stored = stored_keys(&deleter);
    let first = IggyTimestamp::now();

    for wait in [Duration::ZERO, GRACE, GRACE * 2, GRACE * 2] {
        sweep_pass(&ctx, later(first, wait)).await;
    }
    incarnation_delete_pass(&ctx, later(first, GRACE * 2)).await;

    assert_eq!(stored_keys(&deleter), stored);
    assert_eq!(ctx.orphan_sweep.candidates(), 0);
    assert_eq!(ctx.shard.metrics().orphan_sweep_values(), (0, 0, 0, 0, 3));
}

#[compio::test]
async fn given_objects_stored_before_the_claim_when_two_sweeps_find_them_should_count_them_and_delete_nothing()
 {
    let tmp = TempDir::new().unwrap();
    let config = sweep_config(&tmp, GRACE);
    let mux = TestMux::default();
    seed_stream(&mux, 1, "objects");
    let shard = build_metadata_primary_shard(&config, mux);
    seed_cluster_uid(&shard, OWN_CLUSTER_UID);
    let deleter = install_memory_deleter(&shard);
    let ctx = make_ctx(shard, Rc::new(config.clone()));
    let store = deleter.executor().store();
    store_objects(store, absent(5, revision(&ctx))).await;
    compio::time::sleep(Duration::from_millis(2)).await;
    StoreIdentity {
        cluster_uid: OWN_CLUSTER_UID,
    }
    .replace(store, &object_layout())
    .await
    .unwrap();
    let stored = stored_keys(&deleter);
    let first = IggyTimestamp::now();

    for wait in [Duration::ZERO, GRACE, GRACE, GRACE * 2, GRACE * 2] {
        sweep_pass(&ctx, later(first, wait)).await;
    }
    incarnation_delete_pass(&ctx, later(first, GRACE * 2)).await;

    assert_eq!(stored_keys(&deleter), stored);
    assert_eq!(ctx.shard.metrics().orphan_sweep_values(), (3, 0, 0, 3, 0));
}

#[compio::test]
async fn given_orphan_grace_disabled_when_the_primary_reconciles_should_never_sweep() {
    let tmp = TempDir::new().unwrap();
    let config = sweep_config(&tmp, Duration::ZERO);
    let (ctx, deleter) = sweeping_primary(&config, OWN_CLUSTER_UID).await;
    store_objects(deleter.executor().store(), absent(5, revision(&ctx))).await;
    let stored = stored_keys(&deleter);
    let first = IggyTimestamp::now();

    for wait in [
        Duration::ZERO,
        Duration::from_hours(1),
        Duration::from_hours(2),
    ] {
        sweep_pass(&ctx, later(first, wait)).await;
        assert!(!ctx.orphan_sweep.running());
    }
    incarnation_delete_pass(&ctx, later(first, Duration::from_hours(2))).await;

    assert_eq!(stored_keys(&deleter), stored);
    assert_eq!(ctx.shard.metrics().orphan_sweep_values(), (0, 0, 0, 0, 0));
}

#[compio::test]
async fn given_a_primary_that_loses_its_deleter_when_it_passes_should_drop_what_its_sweeps_found() {
    let tmp = TempDir::new().unwrap();
    let config = sweep_config(&tmp, GRACE);
    let (ctx, deleter) = sweeping_primary(&config, OWN_CLUSTER_UID).await;
    store_objects(deleter.executor().store(), absent(5, revision(&ctx))).await;
    let stored = stored_keys(&deleter);
    let first = IggyTimestamp::now();
    sweep_pass(&ctx, first).await;
    assert_eq!(ctx.orphan_sweep.candidates(), 1);

    ctx.shard.set_object_deleter(None);
    sweep_pass(&ctx, later(first, GRACE)).await;
    assert_eq!(ctx.orphan_sweep.candidates(), 0);
    assert_eq!(ctx.shard.metrics().orphan_sweep_values().1, 0);

    // The deleter is back, and a single sweep is not enough again.
    ctx.shard.set_object_deleter(Some(
        Rc::clone(&deleter) as Rc<dyn partitions::ObjectDeleter>
    ));
    sweep_pass(&ctx, later(first, GRACE * 2)).await;
    sweep_pass(&ctx, later(first, GRACE * 2)).await;
    incarnation_delete_pass(&ctx, later(first, GRACE * 2)).await;
    assert_eq!(stored_keys(&deleter), stored);
}

#[compio::test]
async fn given_a_wall_clock_that_steps_ahead_when_the_monotonic_clock_has_not_reached_the_grace_should_neither_sweep_nor_delete()
 {
    let tmp = TempDir::new().unwrap();
    let config = sweep_config(&tmp, GRACE);
    let (ctx, deleter) = sweeping_primary(&config, OWN_CLUSTER_UID).await;
    let orphan = absent(5, revision(&ctx));
    store_objects(deleter.executor().store(), orphan).await;
    let stored = stored_keys(&deleter);
    let wall = IggyTimestamp::now();
    let start = Instant::now();

    for (step, since_start) in [
        (Duration::ZERO, Duration::ZERO),
        (Duration::from_hours(24), GRACE / 2),
        (Duration::from_hours(48), GRACE),
        (Duration::from_hours(72), GRACE + GRACE / 2),
        (Duration::from_hours(96), GRACE + GRACE / 2),
    ] {
        sweep_pass_at(&ctx, later(wall, step), start + since_start).await;
    }
    incarnation_delete_pass(&ctx, later(wall, Duration::from_hours(96))).await;

    assert!(!ctx.incarnation_deletes.is_queued_orphan(&orphan));
    assert_eq!(stored_keys(&deleter), stored);
    assert_eq!(
        ctx.shard.metrics().orphan_sweep_values().0,
        2,
        "a sweep at the start and one a grace later"
    );
}

#[compio::test]
async fn given_a_sweep_that_stops_early_when_the_next_one_lists_the_whole_root_should_count_the_grace_again()
 {
    let tmp = TempDir::new().unwrap();
    let config = sweep_config(&tmp, GRACE);
    let (ctx, deleter) = sweeping_primary(&config, OWN_CLUSTER_UID).await;
    let orphan = absent(5, revision(&ctx));
    store_objects(deleter.executor().store(), orphan).await;
    let stored = stored_keys(&deleter);
    let first = IggyTimestamp::now();
    sweep_pass(&ctx, first).await;
    assert_eq!(ctx.orphan_sweep.candidates(), 1);

    // The sweep starts, and finds no deleter once it runs.
    let cancelled = later(first, GRACE);
    ctx.orphan_sweep.pass(
        &ctx.shard,
        &ctx.incarnation_deletes,
        cancelled,
        monotonic(cancelled),
    );
    ctx.shard.set_object_deleter(None);
    wait_for_the_sweep(&ctx).await;
    ctx.shard.set_object_deleter(Some(
        Rc::clone(&deleter) as Rc<dyn partitions::ObjectDeleter>
    ));
    assert_eq!(ctx.orphan_sweep.candidates(), 0);

    for wait in [GRACE * 2, GRACE * 3, GRACE * 3] {
        sweep_pass(&ctx, later(first, wait)).await;
    }
    incarnation_delete_pass(&ctx, later(first, GRACE * 3)).await;
    assert!(!ctx.incarnation_deletes.is_queued_orphan(&orphan));
    assert_eq!(
        stored_keys(&deleter),
        stored,
        "two full sweeps are not enough"
    );
}
