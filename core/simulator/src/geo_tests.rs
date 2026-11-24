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

//! Geo topics in one cluster and in two.
//!
//! A send to a geo topic goes to the cluster's bucket before the partition
//! commits it, so every acknowledged send must be in the bucket (G8) and
//! must be read back once, in order (G1). A peer cluster serves the origin's
//! published log from the origin's bucket at the origin's offsets (G2), and
//! no cluster writes to a bucket that it does not own (G3).

use std::time::Duration;

use bytes::Bytes;
use server_common::sharding::IggyNamespace;

use crate::bucket::BucketFaults;
use crate::client::SimClient;
use crate::geo_world::{GeoWorld, GeoWorldOptions};

const REPLICAS: u8 = 3;
const CLIENT_ID: u128 = 1;
const STREAM: &str = "geo";
const TOPIC: &str = "orders";
/// The cluster index of the origin of the geo topic.
const ORIGIN: usize = 0;
/// The cluster index of its peer.
const PEER: usize = 1;
/// Steps one send gets: the wait for the topic object, its upload window,
/// the upload and the commit.
const SEND_STEPS: usize = 3_000;
/// Steps one attempt of a send waits for its answer.
const ANSWER_STEPS: usize = 300;
/// Steps a peer gets to serve what the origin published: a rewrite, a
/// manifest, the peer's listing of topic objects and of manifests.
const FOLLOW_STEPS: usize = 12_000;

/// How long a peer keeps the shadow of a topic that its origin deleted.
const TOMBSTONE_GRACE: Duration = Duration::from_secs(30);

/// `clusters` clusters of three replicas, each with one client logged in.
fn geo_world(seed: u64, clusters: u8, faults: BucketFaults) -> (GeoWorld, SimClient) {
    skewed_geo_world(seed, clusters, faults, [0, 0])
}

/// [`geo_world`] with each cluster's wall clock `skews_micros` from the
/// shared one.
fn skewed_geo_world(
    seed: u64,
    clusters: u8,
    faults: BucketFaults,
    skews_micros: [i64; 2],
) -> (GeoWorld, SimClient) {
    let mut options = GeoWorldOptions::new(seed, clusters, REPLICAS, &[CLIENT_ID]);
    options.faults = faults;
    options.tombstone_grace = TOMBSTONE_GRACE;
    options.clock_skews_micros = skews_micros;
    let mut world = GeoWorld::new(&options);
    let client = SimClient::new(CLIENT_ID);
    for cluster in 0..world.cluster_count() {
        world.sim_mut(cluster).shell_login(&client);
    }
    (world, client)
}

/// Create the geo topic on the origin and return its partition.
fn create_geo_topic(world: &mut GeoWorld, client: &SimClient) -> IggyNamespace {
    world.create_geo_topic(ORIGIN, client, STREAM, TOPIC, 1)[0]
}

/// The shadow of the geo topic on the peer, once the peer created it.
fn shadow_of(world: &GeoWorld) -> Option<IggyNamespace> {
    world.shadow_of(PEER, ORIGIN, STREAM, TOPIC, 0)
}

/// Send `payload` to the primary of `namespace` until the cluster commits it.
/// A primary refuses sends until it read the topic's object, and a send can
/// go unanswered when its primary stops. Each attempt sends the same request,
/// so the cluster commits it once whatever answers were lost. A node that the
/// client never logged in to evicts the request, and the client logs in there
/// and sends a new one.
fn send(
    world: &mut GeoWorld,
    cluster: usize,
    client: &SimClient,
    namespace: IggyNamespace,
    payload: &[u8],
) {
    let new_request = || client.send_messages(namespace, &[Bytes::copy_from_slice(payload)]);
    let mut request = new_request();
    let mut steps = 0;
    while steps < SEND_STEPS {
        let Some(target) = world.primary(cluster, namespace) else {
            world.step();
            steps += 1;
            continue;
        };
        let request_id = request.header().request;
        world.sim_mut(cluster).submit_request(
            client.client_id(),
            target,
            request.clone().into_generic(),
        );
        let mut status = None;
        for _ in 0..ANSWER_STEPS {
            status = world.step()[cluster]
                .iter()
                .find(|reply| reply.header().request == request_id)
                .map(|reply| reply.header().status);
            steps += 1;
            if status.is_some() {
                break;
            }
        }
        if status == Some(0) {
            return;
        }
        let evicted = world.sim_mut(cluster).take_evictions();
        if evicted
            .iter()
            .any(|(evicted, from)| *evicted == client.client_id() && *from == target)
        {
            world.sim_mut(cluster).shell_login_via(client, target);
            request = new_request();
        }
    }
    panic!(
        "a send of {:?} was not committed in {SEND_STEPS} steps",
        String::from_utf8_lossy(payload)
    );
}

/// Where each of `payloads` starts in `bytes`, or `None` when one is missing
/// or held twice.
fn positions(bytes: &[u8], payloads: &[Vec<u8>]) -> Option<Vec<usize>> {
    payloads
        .iter()
        .map(|payload| {
            let mut at = bytes
                .windows(payload.len())
                .enumerate()
                .filter(|(_, window)| window == payload)
                .map(|(at, _)| at);
            let first = at.next()?;
            at.next().is_none().then_some(first)
        })
        .collect()
}

/// Whether `bytes` holds every payload once, in order.
fn holds_once_in_order(bytes: &[u8], payloads: &[Vec<u8>]) -> bool {
    positions(bytes, payloads).is_some_and(|at| at.windows(2).all(|pair| pair[0] < pair[1]))
}

/// G8: every acknowledged payload is in an object of `cluster`'s bucket.
fn assert_proven_in_bucket(world: &GeoWorld, cluster: usize, payloads: &[Vec<u8>]) {
    let bucket = world.bucket(cluster);
    let objects: Vec<Bytes> = bucket
        .keys()
        .iter()
        .filter_map(|key| bucket.object(key))
        .collect();
    for payload in payloads {
        assert!(
            objects.iter().any(|object| object
                .windows(payload.len())
                .any(|window| window == payload)),
            "{:?} is in the bucket",
            String::from_utf8_lossy(payload)
        );
    }
}

/// G1: the primary serves every acknowledged payload once, in send order.
fn assert_read_once_in_order(
    world: &mut GeoWorld,
    cluster: usize,
    namespace: IggyNamespace,
    payloads: &[Vec<u8>],
) {
    let replica = world
        .primary(cluster, namespace)
        .expect("the partition has a primary");
    let bytes = world
        .poll(cluster, replica, namespace, 0, SEND_STEPS)
        .expect("the poll is served");
    assert!(
        holds_once_in_order(&bytes, payloads),
        "the primary serves each payload once, in send order: {:?}",
        positions(&bytes, payloads)
    );
}

/// G3: no cluster wrote to a bucket that it does not own.
fn assert_no_foreign_writes(world: &GeoWorld) {
    for cluster in 0..world.cluster_count() {
        assert_eq!(world.bucket(cluster).foreign_writes(), Vec::new());
    }
}

fn payload(index: usize) -> Vec<u8> {
    format!("geo-message-{index:04}-end").into_bytes()
}

fn faulty_bucket() -> BucketFaults {
    BucketFaults {
        latency_min: Duration::from_millis(1),
        latency_max: Duration::from_millis(40),
        error_probability: 0.1,
        lost_answer_probability: 0.1,
    }
}

#[test]
fn given_a_geo_topic_when_sends_are_acknowledged_should_hold_them_in_the_bucket_and_serve_them_in_order()
 {
    const SENDS: usize = 8;
    let (mut world, client) = geo_world(0x6745_4F31, 1, BucketFaults::default());
    let namespace = create_geo_topic(&mut world, &client);

    let payloads: Vec<Vec<u8>> = (0..SENDS).map(payload).collect();
    for payload in &payloads {
        send(&mut world, ORIGIN, &client, namespace, payload);
    }

    assert_proven_in_bucket(&world, ORIGIN, &payloads);
    assert_read_once_in_order(&mut world, ORIGIN, namespace, &payloads);
    assert_no_foreign_writes(&world);
}

#[test]
fn given_a_primary_that_stops_when_sends_go_on_should_keep_every_acknowledged_send() {
    const SENDS: usize = 12;
    const REJOIN_STEPS: usize = 1_000;
    let (mut world, client) = geo_world(0x6745_4F32, 1, BucketFaults::default());
    let namespace = create_geo_topic(&mut world, &client);
    let payloads: Vec<Vec<u8>> = (0..SENDS).map(payload).collect();

    for payload in &payloads[..SENDS / 2] {
        send(&mut world, ORIGIN, &client, namespace, payload);
    }
    let stopped = world
        .primary(ORIGIN, namespace)
        .expect("the partition has a primary");
    world.sim_mut(ORIGIN).replica_crash(stopped);
    for payload in &payloads[SENDS / 2..] {
        send(&mut world, ORIGIN, &client, namespace, payload);
    }
    world.sim_mut(ORIGIN).replica_restart(stopped);
    for _ in 0..REJOIN_STEPS {
        world.step();
    }

    assert_proven_in_bucket(&world, ORIGIN, &payloads);
    assert_read_once_in_order(&mut world, ORIGIN, namespace, &payloads);
    assert_no_foreign_writes(&world);
}

#[test]
fn given_a_bucket_that_fails_and_loses_answers_when_sending_should_keep_every_acknowledged_send() {
    const SENDS: usize = 12;
    let (mut world, client) = geo_world(0x6745_4F33, 1, faulty_bucket());
    let namespace = create_geo_topic(&mut world, &client);
    let payloads: Vec<Vec<u8>> = (0..SENDS).map(payload).collect();

    for payload in &payloads {
        send(&mut world, ORIGIN, &client, namespace, payload);
    }

    assert_proven_in_bucket(&world, ORIGIN, &payloads);
    assert_read_once_in_order(&mut world, ORIGIN, namespace, &payloads);
}

#[test]
fn given_one_seed_when_a_faulty_run_goes_twice_should_replay_the_schedule_and_the_bucket() {
    const SENDS: usize = 6;
    let run = || {
        let (mut world, client) = geo_world(0x6745_4F34, 1, faulty_bucket());
        let namespace = create_geo_topic(&mut world, &client);
        for index in 0..SENDS {
            send(&mut world, ORIGIN, &client, namespace, &payload(index));
        }
        (
            world.sim(ORIGIN).schedule_hash(),
            world.bucket(ORIGIN).keys(),
            world.bucket(ORIGIN).accesses().len(),
        )
    };

    assert_eq!(run(), run());
}

/// Step until the peer's shadow serves every one of `payloads` once, in
/// order, and return the shadow. `None` when it does not within `steps`.
fn await_shadow_serving(
    world: &mut GeoWorld,
    payloads: &[Vec<u8>],
    steps: usize,
) -> Option<IggyNamespace> {
    const CHECK_EVERY: usize = 100;
    for _ in 0..steps / CHECK_EVERY {
        for _ in 0..CHECK_EVERY {
            world.step();
        }
        let Some(shadow) = shadow_of(world) else {
            continue;
        };
        let Some(replica) = world.primary(PEER, shadow) else {
            continue;
        };
        if world
            .poll(PEER, replica, shadow, 0, SEND_STEPS)
            .is_ok_and(|bytes| holds_once_in_order(&bytes, payloads))
        {
            return Some(shadow);
        }
    }
    None
}

#[test]
fn given_two_clusters_when_the_origin_publishes_should_serve_its_log_on_the_peer() {
    const SENDS: usize = 8;
    let (mut world, client) = geo_world(0x6745_4F41, 2, BucketFaults::default());
    let namespace = create_geo_topic(&mut world, &client);
    let payloads: Vec<Vec<u8>> = (0..SENDS).map(payload).collect();
    for payload in &payloads {
        send(&mut world, ORIGIN, &client, namespace, payload);
    }

    let shadow = await_shadow_serving(&mut world, &payloads, FOLLOW_STEPS);

    assert!(
        shadow.is_some(),
        "the peer serves the origin's log in {FOLLOW_STEPS} steps"
    );
    assert_no_foreign_writes(&world);
}

#[test]
fn given_a_shadow_when_a_client_sends_to_it_should_refuse_and_store_nothing() {
    const SENDS: usize = 4;
    let (mut world, client) = geo_world(0x6745_4F42, 2, BucketFaults::default());
    let namespace = create_geo_topic(&mut world, &client);
    let payloads: Vec<Vec<u8>> = (0..SENDS).map(payload).collect();
    for payload in &payloads {
        send(&mut world, ORIGIN, &client, namespace, payload);
    }
    let shadow = await_shadow_serving(&mut world, &payloads, FOLLOW_STEPS)
        .expect("the peer serves the origin's log");
    world.sim(PEER).discover_partition_contexts(&client);
    let foreign = b"geo-write-to-a-shadow".to_vec();

    let target = world
        .primary(PEER, shadow)
        .expect("the shadow has a primary");
    let request = client.send_messages(shadow, &[Bytes::copy_from_slice(&foreign)]);
    let request_id = request.header().request;
    world
        .sim_mut(PEER)
        .submit_request(client.client_id(), target, request.into_generic());
    let status = (0..ANSWER_STEPS).find_map(|_| {
        world.step()[PEER]
            .iter()
            .find(|reply| reply.header().request == request_id)
            .map(|reply| reply.header().status)
    });
    let after = await_shadow_serving(&mut world, &payloads, ANSWER_STEPS);

    assert!(
        status.is_some_and(|status| status != 0),
        "the shadow refuses the send: {status:?}"
    );
    assert_eq!(
        after,
        Some(shadow),
        "the shadow still serves the origin's log"
    );
    let bytes = world
        .poll(PEER, target, shadow, 0, SEND_STEPS)
        .expect("the poll is served");
    assert_eq!(
        positions(&bytes, std::slice::from_ref(&foreign)),
        None,
        "the shadow holds no foreign write"
    );
    assert_proven_in_bucket(&world, ORIGIN, &payloads);
    let peer_bucket = world.bucket(PEER);
    assert!(
        peer_bucket
            .keys()
            .iter()
            .filter_map(|key| peer_bucket.object(key))
            .all(|object| {
                !object
                    .windows(foreign.len())
                    .any(|window| window == foreign.as_slice())
            }),
        "the peer stored nothing of the refused send"
    );
    assert_no_foreign_writes(&world);
}

#[test]
fn given_a_cut_link_to_the_origin_when_it_heals_should_catch_up_and_never_move_back() {
    const SENDS: usize = 12;
    let (mut world, client) = geo_world(0x6745_4F43, 2, BucketFaults::default());
    let namespace = create_geo_topic(&mut world, &client);
    let payloads: Vec<Vec<u8>> = (0..SENDS).map(payload).collect();
    for payload in &payloads[..SENDS / 2] {
        send(&mut world, ORIGIN, &client, namespace, payload);
    }
    let shadow = await_shadow_serving(&mut world, &payloads[..SENDS / 2], FOLLOW_STEPS)
        .expect("the peer serves the first half");
    let mut highest = world.commits(PEER, shadow);

    world
        .bucket(ORIGIN)
        .set_cut(crate::bucket::ClusterId(1), true);
    for payload in &payloads[SENDS / 2..] {
        send(&mut world, ORIGIN, &client, namespace, payload);
    }
    for _ in 0..FOLLOW_STEPS / 4 {
        world.step();
        // G5: a peer's floor never moves back.
        for (replica, commit) in world.commits(PEER, shadow) {
            if let Some((_, seen)) = highest.iter_mut().find(|(seen, _)| *seen == replica) {
                assert!(
                    commit >= *seen,
                    "replica {replica} moved back to {commit} from {seen}"
                );
                *seen = commit;
            }
        }
    }
    let cut_off = world.commits(PEER, shadow);
    world
        .bucket(ORIGIN)
        .set_cut(crate::bucket::ClusterId(1), false);
    let healed = await_shadow_serving(&mut world, &payloads, FOLLOW_STEPS);

    assert!(
        cut_off
            .iter()
            .all(|(_, commit)| *commit < u64::try_from(SENDS).expect("fits") - 1),
        "a cut peer reads nothing new: {cut_off:?}"
    );
    assert_eq!(
        healed,
        Some(shadow),
        "the peer catches up once the link heals"
    );
    assert_no_foreign_writes(&world);
}

#[test]
fn given_the_peer_dc_stops_when_it_starts_again_should_catch_up() {
    const SENDS: usize = 8;
    let (mut world, client) = geo_world(0x6745_4F44, 2, BucketFaults::default());
    let namespace = create_geo_topic(&mut world, &client);
    let payloads: Vec<Vec<u8>> = (0..SENDS).map(payload).collect();
    for payload in &payloads[..SENDS / 2] {
        send(&mut world, ORIGIN, &client, namespace, payload);
    }
    await_shadow_serving(&mut world, &payloads[..SENDS / 2], FOLLOW_STEPS)
        .expect("the peer serves the first half");

    for replica in 0..REPLICAS {
        world.sim_mut(PEER).replica_crash(replica);
    }
    for payload in &payloads[SENDS / 2..] {
        send(&mut world, ORIGIN, &client, namespace, payload);
    }
    for replica in 0..REPLICAS {
        world.sim_mut(PEER).replica_restart(replica);
    }

    assert!(
        await_shadow_serving(&mut world, &payloads, FOLLOW_STEPS).is_some(),
        "the peer catches up once it starts again"
    );
    assert_no_foreign_writes(&world);
}

#[test]
fn given_the_origin_dc_stops_when_it_starts_again_should_keep_serving_and_take_sends() {
    const SENDS: usize = 8;
    let (mut world, client) = geo_world(0x6745_4F45, 2, BucketFaults::default());
    let namespace = create_geo_topic(&mut world, &client);
    let payloads: Vec<Vec<u8>> = (0..SENDS).map(payload).collect();
    for payload in &payloads[..SENDS / 2] {
        send(&mut world, ORIGIN, &client, namespace, payload);
    }
    await_shadow_serving(&mut world, &payloads[..SENDS / 2], FOLLOW_STEPS)
        .expect("the peer serves the first half");

    for replica in 0..REPLICAS {
        world.sim_mut(ORIGIN).replica_crash(replica);
    }
    let while_down = await_shadow_serving(&mut world, &payloads[..SENDS / 2], ANSWER_STEPS);
    for replica in 0..REPLICAS {
        world.sim_mut(ORIGIN).replica_restart(replica);
    }
    for payload in &payloads[SENDS / 2..] {
        send(&mut world, ORIGIN, &client, namespace, payload);
    }

    assert!(
        while_down.is_some(),
        "the peer serves what the origin published while the origin is down"
    );
    assert_proven_in_bucket(&world, ORIGIN, &payloads);
    assert_read_once_in_order(&mut world, ORIGIN, namespace, &payloads);
    assert!(
        await_shadow_serving(&mut world, &payloads, FOLLOW_STEPS).is_some(),
        "the peer follows the origin again"
    );
    assert_no_foreign_writes(&world);
}

#[test]
fn given_a_deleted_geo_topic_when_its_grace_passes_should_retire_the_shadow() {
    const SENDS: usize = 4;
    const RETIRE_STEPS: usize = 30_000;
    let (mut world, client) = geo_world(0x6745_4F46, 2, BucketFaults::default());
    let namespace = create_geo_topic(&mut world, &client);
    let payloads: Vec<Vec<u8>> = (0..SENDS).map(payload).collect();
    for payload in &payloads {
        send(&mut world, ORIGIN, &client, namespace, payload);
    }
    await_shadow_serving(&mut world, &payloads, FOLLOW_STEPS).expect("the peer serves the log");

    world.complete(ORIGIN, &client, &client.delete_topic(STREAM, TOPIC));
    let retired = (0..RETIRE_STEPS).any(|_| {
        world.step();
        shadow_of(&world).is_none()
    });

    assert!(retired, "the peer retires the shadow after the grace");
    assert_no_foreign_writes(&world);
}

#[test]
fn given_clusters_whose_clocks_disagree_when_the_origin_publishes_and_deletes_should_follow_and_retire()
 {
    const SENDS: usize = 4;
    const RETIRE_STEPS: usize = 30_000;
    // The origin's clock runs 45 s behind the shared one and the peer's 45 s
    // ahead of it, a gap of three graces.
    let skews = [-45_000_000, 45_000_000];
    for (seed, skews) in [(0x6745_4F47, skews), (0x6745_4F48, [skews[1], skews[0]])] {
        let (mut world, client) = skewed_geo_world(seed, 2, BucketFaults::default(), skews);
        let namespace = create_geo_topic(&mut world, &client);
        let payloads: Vec<Vec<u8>> = (0..SENDS).map(payload).collect();
        for payload in &payloads {
            send(&mut world, ORIGIN, &client, namespace, payload);
        }

        let served = await_shadow_serving(&mut world, &payloads, FOLLOW_STEPS);
        world.complete(ORIGIN, &client, &client.delete_topic(STREAM, TOPIC));
        let retired = (0..RETIRE_STEPS).any(|_| {
            world.step();
            shadow_of(&world).is_none()
        });

        assert!(served.is_some(), "skews {skews:?}: the peer serves the log");
        assert!(retired, "skews {skews:?}: the peer retires the shadow");
        assert_no_foreign_writes(&world);
    }
}
