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

use iggy_common::header::ReplyHeader;
use iggy_common::message::Message;
use message_bus::MessageBus;
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

fn main() {
    let client_id: u128 = 1;
    let leader: u8 = 0;
    let sim = Simulator::new(3, std::iter::once(client_id));
    let bus = sim.message_bus.clone();

    // Responses queue
    let responses = Arc::new(Mutex::new(Responses::default()));
    let responses_clone = responses.clone();

    // TODO: Scuffed client/simulator setup.
    // We need a better interface on simulator
    let client_handle = std::thread::spawn(move || {
        futures::executor::block_on(async {
            let client = SimClient::new(client_id);

            let create_msg = client.create_stream("test-stream");
            bus.send_to_replica(leader, create_msg.into_generic())
                .await
                .expect("failed to send create_stream");

            loop {
                if let Some(reply) = responses_clone.lock().unwrap().pop() {
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
                if let Some(reply) = responses_clone.lock().unwrap().pop() {
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
    });

    client_handle.join().expect("client thread panicked");
    println!("[sim] Simulator loop ended");
}
