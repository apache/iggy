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

use std::collections::{BinaryHeap, HashMap};

use rand::RngCore;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

use super::types::VsrMessage;

/// Configuration for the packet simulator network.
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// Minimum one-way delay in ticks.
    pub one_way_delay_min: u64,
    /// Mean one-way delay (exponential distribution).
    pub one_way_delay_mean: u64,
    /// Probability of dropping a packet [0.0, 1.0].
    pub packet_loss: f64,
    /// Probability of duplicating a packet [0.0, 1.0].
    pub duplicate_probability: f64,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            one_way_delay_min: 3,
            one_way_delay_mean: 10,
            packet_loss: 0.0,
            duplicate_probability: 0.0,
        }
    }
}

/// A packet in flight with a delivery tick.
#[derive(Debug)]
struct InFlightPacket {
    message: VsrMessage,
    deliver_at: u64,
}

impl PartialEq for InFlightPacket {
    fn eq(&self, other: &Self) -> bool {
        self.deliver_at == other.deliver_at
    }
}

impl Eq for InFlightPacket {}

impl PartialOrd for InFlightPacket {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InFlightPacket {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse ordering for min-heap behavior.
        other.deliver_at.cmp(&self.deliver_at)
    }
}

/// Simulated network with configurable delays, packet loss, and partitions.
pub struct Network {
    config: NetworkConfig,
    rng: Xoshiro256PlusPlus,
    packets: BinaryHeap<InFlightPacket>,
    current_tick: u64,
    /// Partition matrix: partitioned[from][to] = true means link is down.
    partitioned: HashMap<(u8, u8), bool>,
    /// Track which replicas are alive.
    alive: HashMap<u8, bool>,
}

impl Network {
    pub fn new(config: NetworkConfig, seed: u64) -> Self {
        Self {
            config,
            rng: Xoshiro256PlusPlus::seed_from_u64(seed),
            packets: BinaryHeap::new(),
            current_tick: 0,
            partitioned: HashMap::new(),
            alive: HashMap::new(),
        }
    }

    pub fn register_replica(&mut self, id: u8) {
        self.alive.insert(id, true);
    }

    #[allow(dead_code)]
    pub fn current_tick(&self) -> u64 {
        self.current_tick
    }

    /// Submit a message to the network. Returns true if accepted, false if dropped.
    pub fn submit(&mut self, message: VsrMessage) -> bool {
        // Check if sender or receiver is dead.
        if !self.is_alive(message.from) || !self.is_alive(message.to) {
            return false;
        }

        // Check if link is partitioned.
        if self.is_partitioned(message.from, message.to) {
            return false;
        }

        // Random packet loss.
        if self.random_f64() < self.config.packet_loss {
            return false;
        }

        let delay = self.calculate_delay();
        let deliver_at = self.current_tick + delay;

        // Possible duplicate.
        if self.random_f64() < self.config.duplicate_probability {
            let dup_delay = self.calculate_delay();
            self.packets.push(InFlightPacket {
                message: message.clone(),
                deliver_at: self.current_tick + dup_delay,
            });
        }

        self.packets.push(InFlightPacket {
            message,
            deliver_at,
        });
        true
    }

    /// Advance the tick and return all packets ready for delivery.
    pub fn tick(&mut self) -> Vec<VsrMessage> {
        self.current_tick += 1;
        let mut delivered = Vec::new();

        while let Some(pkt) = self.packets.peek() {
            if pkt.deliver_at <= self.current_tick {
                let pkt = self.packets.pop().unwrap();
                // Re-check partition and alive status at delivery time.
                if self.is_alive(pkt.message.from)
                    && self.is_alive(pkt.message.to)
                    && !self.is_partitioned(pkt.message.from, pkt.message.to)
                {
                    delivered.push(pkt.message);
                }
            } else {
                break;
            }
        }

        delivered
    }

    /// Partition the link between two replicas.
    pub fn partition(&mut self, from: u8, to: u8) {
        self.partitioned.insert((from, to), true);
        self.partitioned.insert((to, from), true);
    }

    /// Heal the link between two replicas.
    pub fn heal(&mut self, from: u8, to: u8) {
        self.partitioned.remove(&(from, to));
        self.partitioned.remove(&(to, from));
    }

    /// Kill a replica (drop all messages to/from it).
    pub fn kill_replica(&mut self, id: u8) {
        self.alive.insert(id, false);
    }

    /// Restart a replica.
    pub fn restart_replica(&mut self, id: u8) {
        self.alive.insert(id, true);
    }

    pub fn is_alive(&self, id: u8) -> bool {
        *self.alive.get(&id).unwrap_or(&true)
    }

    pub fn is_partitioned(&self, from: u8, to: u8) -> bool {
        *self.partitioned.get(&(from, to)).unwrap_or(&false)
    }

    #[allow(dead_code)]
    pub fn packets_in_flight(&self) -> usize {
        self.packets.len()
    }

    #[allow(dead_code)]
    pub fn config_mut(&mut self) -> &mut NetworkConfig {
        &mut self.config
    }

    fn calculate_delay(&mut self) -> u64 {
        let min = self.config.one_way_delay_min;
        let mean = self.config.one_way_delay_mean;
        let u = self.random_f64();
        let exp = (-(mean as f64) * u.ln()) as u64;
        min.max(exp)
    }

    fn random_f64(&mut self) -> f64 {
        (self.rng.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}
