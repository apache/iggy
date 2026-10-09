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

//! Queue-history coverage under seeded scheduling and delayed replica replies.
//! The existing log reload seam changes history while real shard queues and
//! replication run.

use crate::Simulator;
use crate::client::SimClient;
use crate::packet::{Packet, PacketSimulatorOptions, ProcessId};
use crate::tests::submit_and_wait_for_reply;
use bytes::Bytes;
use consensus::{PIPELINE_PREPARE_QUEUE_MAX, PartitionsHandle, Sequencer};
use iggy_binary_protocol::{AckLevel, Command, PrepareOkHeader};
use iggy_common::{ConsumerKind, IggyByteSize, IggyError};
use partitions::PollingConsumer;
use server_common::sharding::IggyNamespace;

#[test]
fn given_queued_offset_when_owner_history_is_reloaded_should_reject_across_seeded_schedules() {
    for seed in 0x0FF5_E700..0x0FF5_E708 {
        let first = history_reload_trace(seed);
        let replay = history_reload_trace(seed);
        assert_eq!(
            first, replay,
            "seed {seed:#x} must replay the same schedule"
        );
    }
}

#[allow(clippy::too_many_lines)]
fn history_reload_trace(seed: u64) -> u64 {
    server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
        enabled: false,
        size: IggyByteSize::from(0_u64),
        bucket_capacity: 1,
    });
    let namespace = IggyNamespace::new(1, 1, 0);
    let producers = (3..3 + PIPELINE_PREPARE_QUEUE_MAX as u128)
        .map(SimClient::new)
        .collect::<Vec<_>>();
    let mut simulator = Simulator::with_shards(
        3,
        2,
        [1, 2]
            .into_iter()
            .chain(producers.iter().map(SimClient::client_id)),
        PacketSimulatorOptions {
            node_count: 3,
            client_count: u8::try_from(producers.len() + 2).unwrap(),
            seed,
            ..PacketSimulatorOptions::default()
        },
    );
    simulator.init_partition(namespace);
    let producer = SimClient::new(1);
    let consumer = SimClient::new(2);
    simulator.register_client_with_primary(&producer);
    simulator.register_client_with_primary(&consumer);
    for producer in &producers {
        simulator.register_client_with_primary(producer);
    }
    let reply = submit_and_wait_for_reply(
        &mut simulator,
        1,
        0,
        producer.send_messages(namespace, &[Bytes::from_static(b"old record")]),
    );
    assert_eq!(reply.header().status, 0);
    for _ in 0..20 {
        simulator.step();
    }

    // Keep the primary alive and its prepare pipeline full by withholding only
    // this partition's remote acknowledgments. Other control traffic still runs.
    for backup in 1..3 {
        *simulator
            .network
            .link_drop_packet_fn(ProcessId::Replica(backup), ProcessId::Replica(0)) =
            Some(drop_partition_acknowledgment);
    }
    for producer in &producers {
        simulator.submit_request(
            producer.client_id(),
            0,
            producer
                .send_messages(namespace, &[Bytes::from_static(b"pending record")])
                .into_generic(),
        );
    }
    assert!(
        (0..100).any(|_| {
            simulator.step();
            simulator.replicas[0]
                .partition_shard(namespace)
                .plane
                .partitions()
                .with_partition(&namespace, |partition| {
                    partition.consensus().pipeline_is_full()
                })
                == Some(true)
        }),
        "seed {seed:#x}: the prepare pipeline never filled"
    );
    let request = consumer.store_consumer_offset(
        namespace,
        ConsumerKind::Consumer.as_code(),
        7,
        0,
        AckLevel::Quorum,
    );
    let request_id = request.header().request;
    simulator.submit_request(2, 0, request.into_generic());
    assert!(
        (0..100).any(|_| {
            simulator.step();
            simulator.replicas[0]
                .partition_shard(namespace)
                .plane
                .partitions()
                .with_partition(&namespace, |partition| {
                    partition.consensus().request_queue_len()
                })
                == Some(1)
        }),
        "seed {seed:#x}: the offset store never entered the request queue"
    );
    simulator.run_pumps();

    // Reinstall the retained log only while every pump is quiescent. This
    // production helper invalidates the same history identity as a state
    // transfer install, while retaining bytes avoids real filesystem timing in
    // the seeded executor.
    let owner = simulator.replicas[0].partition_shard(namespace);
    let partition = owner.plane.partitions().get_mut_by_ns(&namespace).unwrap();
    let retained = partition.take_retained_state();
    partition.adopt_retained_log(retained);
    let expected_operation = partition.consensus().sequencer().current_sequence();
    assert_eq!(
        partition.consensus().view(),
        0,
        "the test must not recover through election"
    );

    for backup in 1..3 {
        *simulator
            .network
            .link_drop_packet_fn(ProcessId::Replica(backup), ProcessId::Replica(0)) = None;
    }
    let mut observed_status = None;
    let converged = (0..800).any(|_| {
        for reply in simulator.step() {
            if reply.header().client == 2 && reply.header().request == request_id {
                observed_status = Some(reply.header().status);
            }
        }
        observed_status.is_some()
            && simulator.replicas.iter().all(|replica| {
                replica
                    .partition_shard(namespace)
                    .plane
                    .partitions()
                    .with_partition(&namespace, |partition| {
                        partition.consensus().commit_min() == expected_operation
                    })
                    == Some(true)
            })
    });
    assert_eq!(
        observed_status,
        Some(IggyError::InvalidOffset(0).as_code()),
        "seed {seed:#x}: obsolete progress must return a terminal rejection"
    );
    assert!(
        converged,
        "seed {seed:#x}: unrelated prepared writes must converge"
    );
    for replica in &simulator.replicas {
        let partitions = replica.partition_shard(namespace).plane.partitions();
        assert_eq!(
            partitions
                .consumer_offset_read(&namespace, PollingConsumer::Consumer(7, 0))
                .unwrap()
                .0,
            None
        );
        assert_eq!(
            partitions.with_partition(&namespace, |partition| partition
                .consensus()
                .sequencer()
                .current_sequence()),
            Some(expected_operation),
            "the rejected offset must not receive a replicated operation"
        );
    }
    simulator.executor.schedule_hash()
}

fn drop_partition_acknowledgment(packet: &Packet) -> bool {
    if packet.message.header().command != Command::PrepareOk {
        return false;
    }
    let header: &PrepareOkHeader =
        bytemuck::checked::from_bytes(&packet.message.as_slice()[..size_of::<PrepareOkHeader>()]);
    header.group == IggyNamespace::new(1, 1, 0).inner()
}
