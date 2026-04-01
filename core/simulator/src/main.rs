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

use iggy_binary_protocol::{Message, ReplyHeader};
use iggy_common::sharding::IggyNamespace;
use iggy_common::{IggyByteSize, MemoryPool, MemoryPoolConfigOther, PollingStrategy};
use partitions::{PollingArgs, PollingConsumer};
use simulator::Simulator;
use simulator::client::SimClient;
use simulator::packet::PacketSimulatorOptions;

/// Step the simulator until at least one client reply is received,
/// or `max_ticks` is reached. Returns all collected replies.
fn step_until_reply(sim: &mut Simulator, max_ticks: u64) -> Vec<Message<ReplyHeader>> {
    let mut all_replies = Vec::new();
    for _ in 0..max_ticks {
        all_replies.extend(sim.step());
        if !all_replies.is_empty() {
            return all_replies;
        }
    }
    all_replies
}

fn main() {
    // PooledBuffer::from (used by poll_messages) panics if the global pool is uninitialized.
    // Disabled pooling just falls through to the system allocator.
    MemoryPool::init_pool(&MemoryPoolConfigOther {
        enabled: false,
        size: IggyByteSize::from(0u64),
        bucket_capacity: 1,
    });

    let client_id: u128 = 1;
    let leader: u8 = 0;

    // Deterministic network: minimum delay, no loss, no partitions.
    let network_opts = PacketSimulatorOptions {
        node_count: 3,
        client_count: 1,
        ..PacketSimulatorOptions::default()
    };

    let mut sim = Simulator::new(3, std::iter::once(client_id), network_opts);
    let client = SimClient::new(client_id);

    // Hardcoded partition for testing: stream_id=1, topic_id=1, partition_id=0
    let test_namespace = IggyNamespace::new(1, 1, 0);

    // Initialize partition on all replicas
    println!("[sim] Initializing test partition: {test_namespace:?}");
    sim.init_partition(test_namespace);

    // 1. Send messages to a partition
    println!("[sim] Sending messages to partition");
    let test_messages = vec![
        b"Hello, partition!".as_slice(),
        b"Message 2".as_slice(),
        b"Message 3".as_slice(),
    ];
    let send_msg = client.send_messages(test_namespace, &test_messages);
    sim.submit_request(client_id, leader, send_msg.into_generic());

    let replies = step_until_reply(&mut sim, 100);
    assert!(!replies.is_empty(), "expected send_messages reply");
    println!("[sim] Got send_messages reply: {:?}", replies[0].header());

    // 2. Metadata operations (create + delete stream)
    let create_msg = client.create_stream("test-stream");
    sim.submit_request(client_id, leader, create_msg.into_generic());

    let replies = step_until_reply(&mut sim, 100);
    assert!(!replies.is_empty(), "expected create_stream reply");
    println!("[sim] Got create_stream reply: {:?}", replies[0].header());

    let delete_msg = client.delete_stream("test-stream");
    sim.submit_request(client_id, leader, delete_msg.into_generic());

    let replies = step_until_reply(&mut sim, 100);
    assert!(!replies.is_empty(), "expected delete_stream reply");
    println!("[sim] Got delete_stream reply: {:?}", replies[0].header());

    // 3. Crash a follower and verify the cluster still commits
    println!("\n[sim] === Crash demo ===");
    println!("[sim] Crashing replica 2 (follower)");
    sim.replica_crash(2);
    assert!(sim.is_crashed(2));

    let send_msg2 = client.send_messages(test_namespace, &[b"After crash".as_slice()]);
    sim.submit_request(client_id, leader, send_msg2.into_generic());

    let replies = step_until_reply(&mut sim, 100);
    assert!(
        !replies.is_empty(),
        "expected reply even with one follower crashed"
    );
    println!(
        "[sim] Got send_messages reply with replica 2 down: {:?}",
        replies[0].header()
    );

    // 4. Poll messages and check offsets on the leader
    let consumer = PollingConsumer::Consumer(1, 0);
    let args = PollingArgs::new(PollingStrategy::first(), 10, false);
    match sim.poll_messages(leader as usize, test_namespace, consumer, args) {
        Ok((fragments, _last_matching_offset)) => {
            println!(
                "[sim] Poll returned {} fragments (expected 4)",
                fragments.len()
            );
        }
        Err(e) => {
            println!("[sim] Poll failed: {e}");
        }
    }

    if let Some(offsets) = sim.offsets(leader as usize, test_namespace) {
        println!(
            "[sim] Partition offsets: commit={}, write={}",
            offsets.commit_offset, offsets.write_offset
        );
    }

    println!("[sim] Simulator finished successfully");
}
