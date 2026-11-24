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

//! One cluster with a geo topic. A send goes to the cluster's bucket before
//! the partition commits it, so every acknowledged send must be in the
//! bucket (G8) and must be read back once, in order (G1).

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use consensus::{Consensus, MetadataHandle, PartitionsHandle};
use iggy_binary_protocol::{ReplyHeader, WireIdentifier};
use iggy_common::PollingStrategy;
use metadata::impls::metadata::StreamsFrontend;
use partitions::{PollingArgs, PollingConsumer};
use server_common::Message;
use server_common::sharding::IggyNamespace;

use crate::Simulator;
use crate::bucket::{BucketFaults, ClusterId, SimBucket};
use crate::client::SimClient;
use crate::geo::SimGeo;
use crate::packet::PacketSimulatorOptions;
use crate::tests::complete_metadata_op;

const REPLICAS: u8 = 3;
const CLIENT_ID: u128 = 1;
const ORIGIN: &str = "dc-a";
const CLUSTER: ClusterId = ClusterId(0);
const STREAM: &str = "geo";
const TOPIC: &str = "orders";
/// Steps one send gets: the wait for the topic object, its upload window,
/// the upload and the commit.
const SEND_STEPS: usize = 3_000;
/// Steps one attempt of a send waits for its answer.
const ANSWER_STEPS: usize = 300;
/// Steps the cluster gets to report that every node can tier topics.
const READY_STEPS: usize = 3_000;

/// Three replicas whose geo topics keep their data in one bucket.
fn geo_cluster(seed: u64, faults: BucketFaults) -> (Simulator, SimClient, SimBucket) {
    server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
        enabled: false,
        size: iggy_common::IggyByteSize::from(0u64),
        bucket_capacity: 1,
    });
    // The dispatch shell, because a geo partition takes only the sends that
    // the server's handler hands to the upload path.
    let mut sim = Simulator::with_shards_shell(
        usize::from(REPLICAS),
        1,
        std::iter::once(CLIENT_ID),
        PacketSimulatorOptions {
            node_count: REPLICAS,
            client_count: 1,
            seed,
            ..PacketSimulatorOptions::default()
        },
    );
    let bucket = SimBucket::new(CLUSTER, seed, faults);
    sim.enable_geo(SimGeo::new(ORIGIN, CLUSTER, bucket.clone()));
    let client = SimClient::new(CLIENT_ID);
    sim.shell_login(&client);
    (sim, client, bucket)
}

/// Create the geo topic once the metadata primary admits one, and return its
/// partition.
fn create_geo_topic(sim: &mut Simulator, client: &SimClient) -> IggyNamespace {
    complete_metadata_op(sim, client, client.create_stream(STREAM));
    let ready = (0..READY_STEPS).any(|_| {
        sim.step();
        sim.replicas[0].shards[0]
            .plane
            .metadata()
            .tiered_storage_cluster_ready()
    });
    assert!(ready, "every node reports that it can tier topics");
    complete_metadata_op(sim, client, client.create_geo_topic(STREAM, TOPIC, 1));
    sim.discover_partition_contexts(client);
    sim.replicas[0].shards[0]
        .plane
        .metadata()
        .mux_stm
        .streams()
        .namespace_from_partition(
            &WireIdentifier::named(STREAM).expect("a valid name"),
            &WireIdentifier::named(TOPIC).expect("a valid name"),
            0,
        )
        .expect("the geo topic is committed")
}

/// The live replica that leads `namespace` in normal status, if one does.
fn primary(sim: &Simulator, namespace: IggyNamespace) -> Option<u8> {
    (0..REPLICAS).find(|replica| {
        !sim.is_crashed(*replica)
            && sim.replicas[usize::from(*replica)]
                .partition_shard(namespace)
                .plane
                .partitions()
                .peek_by_ns(&namespace)
                .is_some_and(|partition| {
                    let consensus = partition.consensus();
                    consensus.is_primary() && consensus.is_normal()
                })
    })
}

/// Send `payload` to the primary of `namespace` until the cluster commits it.
/// A primary refuses sends until it read the topic's object, and a send can
/// go unanswered when its primary stops. Each attempt sends the same request,
/// so the cluster commits it once whatever answers were lost. A node that the
/// client never logged in to evicts the request, and the client logs in there
/// and sends a new one.
fn send(sim: &mut Simulator, client: &SimClient, namespace: IggyNamespace, payload: &[u8]) {
    let new_request = || client.send_messages(namespace, &[Bytes::copy_from_slice(payload)]);
    let mut request = new_request();
    let mut steps = 0;
    while steps < SEND_STEPS {
        let Some(target) = primary(sim, namespace) else {
            sim.step();
            steps += 1;
            continue;
        };
        let request_id = request.header().request;
        sim.submit_request(client.client_id(), target, request.clone().into_generic());
        let mut answered: Option<Message<ReplyHeader>> = None;
        for _ in 0..ANSWER_STEPS {
            answered = sim
                .step()
                .into_iter()
                .find(|reply| reply.header().request == request_id);
            steps += 1;
            if answered.is_some() {
                break;
            }
        }
        if answered.is_some_and(|reply| reply.header().status == 0) {
            return;
        }
        let evictions = sim.take_evictions();
        if evictions
            .iter()
            .any(|(evicted, from)| *evicted == client.client_id() && *from == target)
        {
            sim.shell_login_via(client, target);
            request = new_request();
        }
    }
    panic!(
        "a send of {:?} was not committed in {SEND_STEPS} steps",
        String::from_utf8_lossy(payload)
    );
}

/// Every byte that `replica` returns from offset 0 on.
fn polled(sim: &mut Simulator, replica: u8, namespace: IggyNamespace) -> Vec<u8> {
    let args = PollingArgs::new(PollingStrategy::offset(0), 1000, false);
    let poll = sim.poll_messages(
        usize::from(replica),
        namespace,
        PollingConsumer::Consumer(7, 0),
        &args,
    );
    let output = Rc::new(RefCell::new(None));
    let ready = Rc::clone(&output);
    sim.executor.spawn(async move {
        *ready.borrow_mut() = Some(poll.await);
    });
    for _ in 0..SEND_STEPS {
        if output.borrow().is_some() {
            break;
        }
        sim.step();
    }
    output
        .borrow_mut()
        .take()
        .expect("the poll ends")
        .expect("the poll is served")
        .iter()
        .flat_map(|fragment| fragment.as_slice().to_vec())
        .collect()
}

/// Where each of `payloads` starts in `bytes`, or a panic naming the one
/// that is missing or held twice.
fn positions(bytes: &[u8], payloads: &[Vec<u8>]) -> Vec<usize> {
    payloads
        .iter()
        .map(|payload| {
            let at: Vec<usize> = bytes
                .windows(payload.len())
                .enumerate()
                .filter(|(_, window)| window == payload)
                .map(|(at, _)| at)
                .collect();
            assert_eq!(
                at.len(),
                1,
                "{:?} is read once",
                String::from_utf8_lossy(payload)
            );
            at[0]
        })
        .collect()
}

/// G8: every acknowledged payload is in an object of the bucket.
fn assert_proven_in_bucket(bucket: &SimBucket, payloads: &[Vec<u8>]) {
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
fn assert_read_once_in_order(sim: &mut Simulator, namespace: IggyNamespace, payloads: &[Vec<u8>]) {
    let replica = primary(sim, namespace).expect("the partition has a primary");
    let at = positions(&polled(sim, replica, namespace), payloads);
    assert!(
        at.windows(2).all(|pair| pair[0] < pair[1]),
        "the payloads are read in send order: {at:?}"
    );
}

fn payload(index: usize) -> Vec<u8> {
    format!("geo-message-{index:04}-end").into_bytes()
}

#[test]
fn given_a_geo_topic_when_sends_are_acknowledged_should_hold_them_in_the_bucket_and_serve_them_in_order()
 {
    const SENDS: usize = 8;
    let (mut sim, client, bucket) = geo_cluster(0x6745_4F31, BucketFaults::default());
    let namespace = create_geo_topic(&mut sim, &client);

    let payloads: Vec<Vec<u8>> = (0..SENDS).map(payload).collect();
    for payload in &payloads {
        send(&mut sim, &client, namespace, payload);
    }

    assert_proven_in_bucket(&bucket, &payloads);
    assert_read_once_in_order(&mut sim, namespace, &payloads);
    assert_eq!(bucket.foreign_writes(), Vec::new());
}

#[test]
fn given_a_primary_that_stops_when_sends_go_on_should_keep_every_acknowledged_send() {
    const SENDS: usize = 12;
    const SETTLE_STEPS: usize = 1_000;
    let (mut sim, client, bucket) = geo_cluster(0x6745_4F32, BucketFaults::default());
    let namespace = create_geo_topic(&mut sim, &client);
    let payloads: Vec<Vec<u8>> = (0..SENDS).map(payload).collect();

    for payload in &payloads[..SENDS / 2] {
        send(&mut sim, &client, namespace, payload);
    }
    let stopped = primary(&sim, namespace).expect("the partition has a primary");
    sim.replica_crash(stopped);
    for payload in &payloads[SENDS / 2..] {
        send(&mut sim, &client, namespace, payload);
    }
    sim.replica_restart(stopped);
    for _ in 0..SETTLE_STEPS {
        sim.step();
    }

    assert_proven_in_bucket(&bucket, &payloads);
    assert_read_once_in_order(&mut sim, namespace, &payloads);
    assert_eq!(bucket.foreign_writes(), Vec::new());
}

#[test]
fn given_a_bucket_that_fails_and_loses_answers_when_sending_should_keep_every_acknowledged_send() {
    const SENDS: usize = 12;
    let faults = BucketFaults {
        latency_min: Duration::from_millis(1),
        latency_max: Duration::from_millis(40),
        error_probability: 0.1,
        lost_answer_probability: 0.1,
    };
    let (mut sim, client, bucket) = geo_cluster(0x6745_4F33, faults);
    let namespace = create_geo_topic(&mut sim, &client);
    let payloads: Vec<Vec<u8>> = (0..SENDS).map(payload).collect();

    for payload in &payloads {
        send(&mut sim, &client, namespace, payload);
    }

    assert_proven_in_bucket(&bucket, &payloads);
    assert_read_once_in_order(&mut sim, namespace, &payloads);
}

#[test]
fn given_one_seed_when_a_faulty_run_goes_twice_should_replay_the_schedule_and_the_bucket() {
    const SENDS: usize = 6;
    let run = || {
        let faults = BucketFaults {
            latency_min: Duration::from_millis(1),
            latency_max: Duration::from_millis(40),
            error_probability: 0.2,
            lost_answer_probability: 0.2,
        };
        let (mut sim, client, bucket) = geo_cluster(0x6745_4F34, faults);
        let namespace = create_geo_topic(&mut sim, &client);
        for index in 0..SENDS {
            send(&mut sim, &client, namespace, &payload(index));
        }
        (sim.schedule_hash(), bucket.keys(), bucket.accesses().len())
    };

    assert_eq!(run(), run());
}
