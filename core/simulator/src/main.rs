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
use iggy_common::PollingStrategy;
use iggy_common::sharding::IggyNamespace;
use iggy_common::{IggyByteSize, MemoryPool, MemoryPoolConfigOther};
use message_bus::MessageBus;
use partitions::{PollingArgs, PollingConsumer};
use simulator::{Simulator, client::SimClient};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// Shared response queue for client replies
#[derive(Default)]
pub struct Responses {
    queue: VecDeque<Message<ReplyHeader>>,
}

impl Responses {
    pub fn push(&mut self, msg: Message<ReplyHeader>) {
        self.queue.push_back(msg);
    }

    pub fn pop(&mut self) -> Option<Message<ReplyHeader>> {
        self.queue.pop_front()
    }
}

#[allow(clippy::too_many_lines)]
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
    let mut sim = Simulator::new(3, std::iter::once(client_id));
    let bus = sim.message_bus.clone();

    // Hardcoded partition for testing: stream_id=1, topic_id=1, partition_id=0
    let test_namespace = IggyNamespace::new(1, 1, 0);

    // Initialize partition on all replicas
    println!("[sim] Initializing test partition: {test_namespace:?}");
    sim.init_partition(test_namespace);

    // Responses queue
    let responses = Arc::new(Mutex::new(Responses::default()));
    let responses_clone = responses.clone();

    // TODO: Scuffed client/simulator setup.
    // We need a better interface on simulator
    let client_handle = std::thread::spawn(move || {
        futures::executor::block_on(async {
            let client = SimClient::new(client_id);

            // Send some test messages to the partition
            println!("[client] Sending messages to partition");
            let test_messages = vec![
                b"Hello, partition!".as_slice(),
                b"Message 2".as_slice(),
                b"Message 3".as_slice(),
            ];

            let send_msg = client.send_messages(test_namespace, &test_messages);
            bus.send_to_replica(leader, send_msg.into_generic())
                .await
                .expect("failed to send messages");

            loop {
                let reply = responses_clone.lock().unwrap().pop();
                if let Some(reply) = reply {
                    println!("[client] Got send_messages reply: {:?}", reply.header());
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }

            // Send metadata operations
            let create_msg = client.create_stream("test-stream");
            bus.send_to_replica(leader, create_msg.into_generic())
                .await
                .expect("failed to send create_stream");

            loop {
                let reply = responses_clone.lock().unwrap().pop();
                if let Some(reply) = reply {
                    println!("[client] Got create_stream reply: {:?}", reply.header());
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }

            let delete_msg = client.delete_stream("test-stream");
            bus.send_to_replica(leader, delete_msg.into_generic())
                .await
                .expect("failed to send delete_stream");

            loop {
                let reply = responses_clone.lock().unwrap().pop();
                if let Some(reply) = reply {
                    println!("[client] Got delete_stream reply: {:?}", reply.header());
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        });
    });

    println!("[sim] Starting simulator loop");
    futures::executor::block_on(async {
        loop {
            if let Some(reply) = sim.step().await {
                responses.lock().unwrap().push(reply);
            }

            if client_handle.is_finished() {
                break;
            }
        }

        // Poll messages directly from the leader's partition (bypassing consensus)
        let consumer = PollingConsumer::Consumer(1, 0);
        let args = PollingArgs::new(PollingStrategy::first(), 10, false);
        match sim
            .poll_messages(leader as usize, test_namespace, consumer, args)
            .await
        {
            Ok(batch_set) => {
                println!(
                    "[sim] Poll returned {} messages (expected 3)",
                    batch_set.count()
                );
            }
            Err(e) => {
                println!("[sim] Poll failed: {e}");
            }
        }

        let args_auto = PollingArgs::new(PollingStrategy::first(), 2, true);
        if let Ok(batch) = sim
            .poll_messages(leader as usize, test_namespace, consumer, args_auto)
            .await
        {
            println!("[sim] Auto-commit poll returned {} messages", batch.count());
        }

        // Next poll should start from offset 2 (after auto-commit of 0,1)
        let args_next = PollingArgs::new(PollingStrategy::next(), 10, false);
        if let Ok(batch) = sim
            .poll_messages(leader as usize, test_namespace, consumer, args_next)
            .await
        {
            println!(
                "[sim] Next poll returned {} messages (expected 1)",
                batch.count()
            );
        }

        // Check offsets
        if let Some(offsets) = sim.offsets(leader as usize, test_namespace) {
            println!(
                "[sim] Partition offsets: commit={}, write={}",
                offsets.commit_offset, offsets.write_offset
            );
        }
    });

    client_handle.join().expect("client thread panicked");
    println!("[sim] Simulator loop ended");
}
