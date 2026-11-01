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

//! A batch of an idempotent producer lands once. Its primary is cut off
//! before it commits the batch, the next view commits it, and another client
//! sends the batch again to the new primary.

use bytes::Bytes;
use consensus::Status;
use iggy_binary_protocol::batch::BatchProducer;
use iggy_binary_protocol::responses::messages::SendMessagesResponse;
use iggy_binary_protocol::{ReplyHeader, RoutedRequestHeader, WireDecode};
use iggy_common::IggyError;
use server_common::Message;
use server_common::sharding::IggyNamespace;

use crate::Simulator;
use crate::client::SimClient;
use crate::packet::{PacketSimulatorOptions, ProcessId};
use crate::partition_repair_driver_tests::group_state;
use crate::stale_prepare_commit_tests::{isolate, polled, primary_of};

const SEED: u64 = 0x5052_4F44;
const REPLICAS: u8 = 3;
const OLD_PRIMARY: u8 = 0;
const FIRST_CLIENT: u128 = 0x1D3_0001;
const SECOND_CLIENT: u128 = 0x1D3_0002;
const STEPS_PER_SEND: usize = 12;
const REPLY_STEPS: usize = 100;
/// Well past `NORMAL_HEARTBEAT_TICKS` (500), so the majority elects.
const ELECTION_STEPS: usize = 5_000;
const RESEND_ATTEMPTS: usize = 50;

#[test]
fn given_a_producer_batch_cut_off_before_its_commit_when_another_client_resends_it_to_the_next_primary_should_land_once()
 {
    let (mut sim, first, second) = two_client_cluster(SEED);
    let namespace = IggyNamespace::new(1, 1, 0);
    sim.init_partition(namespace);
    sim.register_client_with_primary(&first);
    sim.register_client_with_primary(&second);
    let producer = BatchProducer::new(21_000, 0, 0).expect("a producer within the field limits");

    let warmup = first.send_messages(namespace, &[Bytes::from_static(b"warmup")]);
    let reply = submit_and_wait(&mut sim, &first, OLD_PRIMARY, warmup);
    assert_eq!(reply.header().status, 0, "the warmup send must commit");
    let (_, _, warm_commit, _) = group_state(&sim, OLD_PRIMARY, namespace);

    // The backups journal the batch, but their acks never reach the primary,
    // so it never commits there.
    set_acks(&mut sim, false);
    let batch = first.send_producer_messages(namespace, producer, &[Bytes::from_static(b"once")]);
    sim.submit_request(first.client_id(), OLD_PRIMARY, batch.into_generic());
    for _ in 0..STEPS_PER_SEND {
        sim.step();
    }
    let (_, _, cut_commit, _) = group_state(&sim, OLD_PRIMARY, namespace);
    assert_eq!(
        cut_commit, warm_commit,
        "the old primary must not commit the batch"
    );

    isolate(&mut sim, OLD_PRIMARY, true);
    let new_view = (0..ELECTION_STEPS)
        .find_map(|_| {
            sim.step();
            let (status, view, _, _) = group_state(&sim, 1, namespace);
            (status == Status::Normal && view > 0).then_some(view)
        })
        .expect("replicas 1 and 2 never elected a new view");
    let new_primary = primary_of(&sim, 1, namespace, new_view);
    assert_ne!(new_primary, OLD_PRIMARY);

    let base_offset = resend_until_answered(&mut sim, &second, new_primary, namespace, producer);
    assert_eq!(
        base_offset, 1,
        "the resent batch must get the offset the view committed"
    );
    let payloads: Vec<String> = polled(&mut sim, new_primary, namespace)
        .into_iter()
        .map(|(_, payload)| payload)
        .collect();
    assert_eq!(payloads, ["warmup", "once"], "the batch must land once");
}

#[test]
fn given_a_producer_batch_in_flight_when_another_client_resends_it_to_the_same_primary_should_hold_it_until_the_first_commits()
 {
    let (mut sim, first, second) = two_client_cluster(SEED);
    let namespace = IggyNamespace::new(1, 1, 0);
    sim.init_partition(namespace);
    sim.register_client_with_primary(&first);
    sim.register_client_with_primary(&second);
    let producer = BatchProducer::new(22_000, 0, 0).expect("a producer within the field limits");

    let warmup = first.send_messages(namespace, &[Bytes::from_static(b"warmup")]);
    let reply = submit_and_wait(&mut sim, &first, OLD_PRIMARY, warmup);
    assert_eq!(reply.header().status, 0, "the warmup send must commit");

    set_acks(&mut sim, false);
    let batch = first.send_producer_messages(namespace, producer, &[Bytes::from_static(b"once")]);
    sim.submit_request(first.client_id(), OLD_PRIMARY, batch.into_generic());
    for _ in 0..STEPS_PER_SEND {
        sim.step();
    }
    let resend = second.send_producer_messages(namespace, producer, &[Bytes::from_static(b"once")]);
    let reply = submit_and_wait(&mut sim, &second, OLD_PRIMARY, resend);
    assert_eq!(
        reply.header().status,
        IggyError::TransientNotCommitted.as_code(),
        "a resend of an uncommitted batch must wait for it"
    );

    set_acks(&mut sim, true);
    let base_offset = resend_until_answered(&mut sim, &second, OLD_PRIMARY, namespace, producer);
    assert_eq!(base_offset, 1);
    let payloads: Vec<String> = polled(&mut sim, OLD_PRIMARY, namespace)
        .into_iter()
        .map(|(_, payload)| payload)
        .collect();
    assert_eq!(payloads, ["warmup", "once"], "the batch must land once");
}

/// Cut or restore the links from the backups to the old primary, which carry
/// their acks.
fn set_acks(sim: &mut Simulator, delivered: bool) {
    for backup in 1..REPLICAS {
        sim.network.set_link_filter(
            ProcessId::Replica(backup),
            ProcessId::Replica(OLD_PRIMARY),
            delivered,
        );
    }
}

/// Send the batch again until the new primary answers it. It answers
/// `TransientNotCommitted` while the batch is still in flight there, and
/// `TransientNotAccepted` while it cannot take the request yet.
fn resend_until_answered(
    sim: &mut Simulator,
    client: &SimClient,
    target: u8,
    namespace: IggyNamespace,
    producer: BatchProducer,
) -> u64 {
    for _ in 0..RESEND_ATTEMPTS {
        let resend =
            client.send_producer_messages(namespace, producer, &[Bytes::from_static(b"once")]);
        let reply = submit_and_wait(sim, client, target, resend);
        match reply.header().status {
            0 => {
                let response = SendMessagesResponse::decode_from(reply.body())
                    .expect("a send reply names its offset");
                return response.confirmations[0].base_offset;
            }
            status
                if status == IggyError::TransientNotCommitted.as_code()
                    || status == IggyError::TransientNotAccepted.as_code() =>
            {
                for _ in 0..STEPS_PER_SEND {
                    sim.step();
                }
            }
            status => panic!("the resent batch was refused with {status}"),
        }
    }
    panic!("the new primary never answered the resent batch");
}

fn submit_and_wait(
    sim: &mut Simulator,
    client: &SimClient,
    target: u8,
    request: Message<RoutedRequestHeader>,
) -> Message<ReplyHeader> {
    let request_id = request.header().request;
    sim.submit_request(client.client_id(), target, request.into_generic());
    for _ in 0..REPLY_STEPS {
        if let Some(reply) = sim.step().into_iter().find(|reply| {
            reply.header().client == client.client_id() && reply.header().request == request_id
        }) {
            return reply;
        }
    }
    panic!(
        "request {request_id} of client {} got no reply",
        client.client_id()
    );
}

/// The partition cluster of `partition_repair_driver_tests::cluster`, with a
/// second client: the gateway that takes over a producer.
fn two_client_cluster(seed: u64) -> (Simulator, SimClient, SimClient) {
    server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
        enabled: false,
        size: iggy_common::IggyByteSize::from(0u64),
        bucket_capacity: 1,
    });
    let clients = [FIRST_CLIENT, SECOND_CLIENT];
    let network_options = PacketSimulatorOptions {
        node_count: REPLICAS,
        client_count: u8::try_from(clients.len()).expect("two clients"),
        seed,
        ..PacketSimulatorOptions::default()
    };
    let sim = Simulator::with_shards(
        usize::from(REPLICAS),
        1,
        clients.into_iter(),
        network_options,
    );
    (
        sim,
        SimClient::new(FIRST_CLIENT),
        SimClient::new(SECOND_CLIENT),
    )
}
