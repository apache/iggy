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

//! A replica must never commit a prepare the cluster did not commit.
//!
//! The old primary journals an op, is cut off before any peer sees it, and the
//! other two replicas elect a new view that commits a DIFFERENT prepare at the
//! same op. When the old primary rejoins, the `StartView` it adopts names only
//! the head of the new view's log, so its stale prepare sits below the announced
//! commit point, and nothing checks it against the log the view committed.

use bytes::Bytes;
use consensus::{PartitionsHandle, Status};
use futures::FutureExt;
use iggy_common::PollingStrategy;
use partitions::{PollingArgs, PollingConsumer};
use server_common::send_messages::{BatchIntegrity, decode_batch_slice_with};
use server_common::sharding::IggyNamespace;

use crate::Simulator;
use crate::client::SimClient;
use crate::packet::{PacketSimulatorOptions, ProcessId};

const REPLICAS: u8 = 3;
const CLIENT_ID: u128 = 1;
const OLD_PRIMARY: u8 = 0;
const STEPS_PER_SEND: usize = 12;
/// Well past `NORMAL_HEARTBEAT_TICKS` (500), so the majority elects.
const ELECTION_STEPS: usize = 5_000;
const SETTLE_STEPS: usize = 5_000;

fn cluster(seed: u64) -> (Simulator, SimClient) {
    server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
        enabled: false,
        size: iggy_common::IggyByteSize::from(0u64),
        bucket_capacity: 1,
    });
    let options = PacketSimulatorOptions {
        node_count: REPLICAS,
        client_count: 1,
        seed,
        ..PacketSimulatorOptions::default()
    };
    let sim = Simulator::new(usize::from(REPLICAS), std::iter::once(CLIENT_ID), options);
    (sim, SimClient::new(CLIENT_ID))
}

fn group_state(sim: &Simulator, replica: u8, namespace: IggyNamespace) -> (Status, u32, u64, u64) {
    let partitions = sim.replicas[usize::from(replica)]
        .partition_shard(namespace)
        .plane
        .partitions();
    let partition = partitions
        .get_by_ns(&namespace)
        .expect("replica hosts the group");
    let consensus = partition.consensus();
    (
        consensus.status(),
        consensus.view(),
        consensus.commit_min(),
        consensus.commit_max(),
    )
}

fn send(sim: &mut Simulator, client: &SimClient, namespace: IggyNamespace, to: u8, payload: &str) {
    let request = client.send_messages(namespace, &[Bytes::from(payload.to_owned())]);
    sim.submit_request(client.client_id(), to, request.into_generic());
    for _ in 0..STEPS_PER_SEND {
        sim.step();
    }
}

fn isolate(sim: &mut Simulator, replica: u8, isolated: bool) {
    for peer in (0..REPLICAS).filter(|peer| *peer != replica) {
        sim.network.set_link_filter(
            ProcessId::Replica(replica),
            ProcessId::Replica(peer),
            !isolated,
        );
        sim.network.set_link_filter(
            ProcessId::Replica(peer),
            ProcessId::Replica(replica),
            !isolated,
        );
    }
}

/// `(offset, payload)` for every message a consumer can poll from `replica`,
/// read through the ordinary client poll path.
fn polled(sim: &mut Simulator, replica: u8, namespace: IggyNamespace) -> Vec<(u64, String)> {
    let mut out = Vec::new();
    loop {
        let next = out.last().map_or(0, |(offset, _)| offset + 1);
        let args = PollingArgs::new(PollingStrategy::offset(next), 1_000, false);
        let poll = sim.poll_messages(
            usize::from(replica),
            namespace,
            PollingConsumer::Consumer(1, 0),
            &args,
        );
        let (tx, rx) = shard::channel(1);
        sim.executor.spawn(async move {
            let _ = tx.try_send(poll.await);
        });
        sim.run_pumps();
        let fragments = rx
            .recv()
            .now_or_never()
            .expect("poll completes")
            .expect("reply channel open")
            .expect("poll accepted");
        let before = out.len();
        for fragment in &fragments {
            let mut bytes = fragment.as_slice();
            while let Ok(batch) = decode_batch_slice_with(bytes, BatchIntegrity::LayoutOnly) {
                for message in &batch {
                    out.push((
                        batch.header.base_offset + u64::from(message.header.offset_delta),
                        String::from_utf8_lossy(message.payload).into_owned(),
                    ));
                }
                let Some(rest) = bytes.get(batch.header.total_size()..) else {
                    break;
                };
                bytes = rest;
            }
        }
        if out.len() == before {
            return out;
        }
    }
}

#[test]
fn given_an_old_primary_holding_an_uncommitted_prepare_when_it_rejoins_a_view_that_committed_another_prepare_at_that_op_should_not_commit_its_own()
 {
    let (mut sim, client) = cluster(0x5EED_57A1);
    let namespace = IggyNamespace::new(1, 1, 0);
    sim.init_partition(namespace);
    sim.register_client_with_primary(&client);

    for index in 0..3 {
        send(
            &mut sim,
            &client,
            namespace,
            OLD_PRIMARY,
            &format!("warmup-{index}"),
        );
    }
    let (_, _, warm_commit, _) = group_state(&sim, OLD_PRIMARY, namespace);
    assert_eq!(warm_commit, 3, "warmup did not commit on the old primary");

    // Cut the old primary off, then hand it a request: it journals op 4 and
    // can never replicate it.
    isolate(&mut sim, OLD_PRIMARY, true);
    send(&mut sim, &client, namespace, OLD_PRIMARY, "stale");

    // The majority elects a new view without it.
    let new_view = (0..ELECTION_STEPS)
        .find_map(|_| {
            sim.step();
            let (status, view, _, _) = group_state(&sim, 1, namespace);
            (status == Status::Normal && view > 0).then_some(view)
        })
        .expect("replicas 1 and 2 never elected a new view");
    let new_primary = sim.replicas[1]
        .partition_shard(namespace)
        .plane
        .partitions()
        .get_by_ns(&namespace)
        .expect("replica hosts the group")
        .consensus()
        .primary_index(new_view);
    assert_ne!(new_primary, OLD_PRIMARY);

    // The new view commits different prepares at op 4 and beyond.
    for index in 0..4 {
        send(
            &mut sim,
            &client,
            namespace,
            new_primary,
            &format!("fresh-{index}"),
        );
    }
    let (_, _, majority_commit, _) = group_state(&sim, new_primary, namespace);
    assert!(
        majority_commit >= 5,
        "the new view committed only through {majority_commit}"
    );

    isolate(&mut sim, OLD_PRIMARY, false);
    for _ in 0..SETTLE_STEPS {
        sim.step();
    }
    let (_, _, rejoined_commit, _) = group_state(&sim, OLD_PRIMARY, namespace);
    assert!(
        rejoined_commit >= majority_commit,
        "the old primary never caught up: committed {rejoined_commit} of {majority_commit}"
    );

    let reference = polled(&mut sim, new_primary, namespace);
    let rejoined = polled(&mut sim, OLD_PRIMARY, namespace);
    assert!(
        !reference.iter().any(|(_, payload)| payload == "stale"),
        "the new view committed the isolated primary's request: {reference:?}"
    );
    assert_eq!(
        rejoined, reference,
        "the rejoined replica serves a committed log that differs from the view's"
    );
}
