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

//! A grouped fetch (109) evaluates a partition that another shard of the same
//! replica owns against a mirror of its state on the shard of the fetch's
//! connection. The owner pushes each commit to that mirror only while a wait
//! parks on it, so a fetch that another ready partition ends at once must not
//! answer from a mirror that missed newer records. Each round commits one
//! record to each of two partitions that different shards own, then fetches
//! both, and every round must return both records, whichever shard the fetch
//! enters.

use bytes::Bytes;
use consensus::MetadataHandle;
use iggy_binary_protocol::responses::messages::fetch_messages::FetchMessagesResponse;
use iggy_binary_protocol::{Operation, ReplyHeader, RoutedRequestHeader, WirePollingStrategy};
use server_common::Message;
use server_common::sharding::IggyNamespace;
use shard::shards_table::calculate_shard_assignment;

use crate::Simulator;
use crate::client::SimClient;
use crate::packet::PacketSimulatorOptions;

const CLIENT: u128 = 1;
/// Leads view 0.
const PRIMARY: u8 = 0;
const REPLICAS: u8 = 3;
const SEED: u64 = 0x4D49_5252;
const SHARDS: u16 = 2;
const ROUNDS: u64 = 8;
const REPLY_STEPS: usize = 400;
const FETCH_COUNT: u32 = 16;
/// Partition ids to look through for one that the other shard owns.
const PARTITION_CANDIDATES: usize = 16;
/// Far beyond a round, so a fetch that waits instead of answering at once
/// returns no reply within `REPLY_STEPS`.
const WAIT_US: u64 = 60_000_000;
const TIMEOUT_US: u64 = 61_000_000;

#[test]
fn given_records_on_partitions_of_two_shards_when_fetched_each_round_should_return_every_record() {
    let (mut sim, client, partitions) = cluster();
    let ids: Vec<u32> = partitions.into_iter().map(partition_id).collect();
    for round in 0..ROUNDS {
        for namespace in partitions {
            produce(&mut sim, &client, namespace, round);
        }
        let request = client.fetch_messages(
            &partitions,
            &WirePollingStrategy::offset(round),
            (FETCH_COUNT, 1),
            false,
            (WAIT_US, TIMEOUT_US),
        );
        let reply = submit_and_await(&mut sim, &client, request);

        assert_eq!(
            reply.header().status,
            0,
            "round {round}: the fetch is served"
        );
        let response = FetchMessagesResponse::decode(reply.body()).expect("a fetch reply decodes");
        let mut served: Vec<u32> = response
            .sections
            .iter()
            .filter(|section| section.header.messages_count == 1)
            .map(|section| section.header.partition_id)
            .collect();
        served.sort_unstable();
        assert_eq!(
            served, ids,
            "round {round}: the fetch returns the new record of each partition"
        );
    }
}

/// Three replicas with two shards each, and the first two partitions of one topic
/// that different shards own.
fn cluster() -> (Simulator, SimClient, [IggyNamespace; 2]) {
    server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
        enabled: false,
        size: iggy_common::IggyByteSize::from(0u64),
        bucket_capacity: 1,
    });
    let network = PacketSimulatorOptions {
        node_count: REPLICAS,
        client_count: 1,
        seed: SEED,
        ..PacketSimulatorOptions::default()
    };
    let mut sim = Simulator::with_shards_shell(
        usize::from(REPLICAS),
        SHARDS,
        std::iter::once(CLIENT),
        network,
    );
    let first = IggyNamespace::new(0, 0, 0);
    let owner =
        |namespace: &IggyNamespace| calculate_shard_assignment(namespace, u32::from(SHARDS));
    let second = (1..PARTITION_CANDIDATES)
        .map(|partition| IggyNamespace::new(0, 0, partition))
        .find(|namespace| owner(namespace) != owner(&first))
        .expect("some partition hashes to the other shard");
    for namespace in [first, second] {
        sim.init_partition(namespace);
        sim.seed_stream_topic_partition(namespace);
    }
    for replica in &sim.replicas {
        replica.shards[0]
            .plane
            .metadata()
            .seed_applied_frontier_from_consensus();
    }
    let client = SimClient::new(CLIENT);
    sim.shell_login(&client);
    sim.discover_partition_contexts(&client);
    (sim, client, [first, second])
}

fn produce(sim: &mut Simulator, client: &SimClient, namespace: IggyNamespace, round: u64) {
    let payload = Bytes::from(format!("record {round}"));
    let reply = sim.await_setup_reply(
        client,
        PRIMARY,
        &client.send_messages(namespace, &[payload]).into_generic(),
        Operation::SendMessages,
        "produce",
    );
    assert_eq!(reply.header().status, 0, "the produce commits");
}

fn submit_and_await(
    sim: &mut Simulator,
    client: &SimClient,
    request: Message<RoutedRequestHeader>,
) -> Message<ReplyHeader> {
    let request_id = request.header().request;
    sim.submit_request(client.client_id(), PRIMARY, request.into_generic());
    for _ in 0..REPLY_STEPS {
        if let Some(reply) = sim.step().into_iter().find(|reply| {
            reply.header().client == client.client_id() && reply.header().request == request_id
        }) {
            return reply;
        }
    }
    panic!("request {request_id} got no reply within {REPLY_STEPS} steps");
}

fn partition_id(namespace: IggyNamespace) -> u32 {
    u32::try_from(namespace.partition_id()).expect("partition id fits u32")
}
