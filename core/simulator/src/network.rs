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

//! Network abstraction layer for the cluster simulator.
//!
//! **Note:** Currently a thin passthrough over `PacketSimulator`. Once the
//! Cluster and MessageBus layers are built, this will own
//! process-to-bus routing, and node enable/disable logic.

use crate::packet::{
    ALLOW_ALL, BLOCK_ALL, LinkFilter, Packet, PacketSimulator, PacketSimulatorOptions, ProcessId,
};
use iggy_common::{header::GenericHeader, message::Message};

/// Network layer for the cluster simulation.
///
/// This provides an interface over the `PacketSimulator` for the
/// `Cluster` orchestrator to use. It handles:
/// - Submitting packets into the network
/// - Stepping the network to deliver ready packets
/// - Managing network partitions and link states
#[derive(Debug)]
pub struct Network {
    simulator: PacketSimulator,
}

impl Network {
    /// Create a new network.
    pub fn new(options: PacketSimulatorOptions) -> Self {
        Self {
            simulator: PacketSimulator::new(options),
        }
    }

    /// Submit a message into the network.
    ///
    /// The message will be queued with a simulated delay and may be:
    /// - Delivered normally after the delay
    /// - Dropped (based on packet_loss_probability)
    /// - Replayed/duplicated (based on replay_probability)
    pub fn submit(&mut self, from: ProcessId, to: ProcessId, message: Message<GenericHeader>) {
        self.simulator.submit(from, to, message);
    }

    /// Deliver all ready packets.
    ///
    /// The returned `Vec` is taken from an internal buffer. Pass it back via
    /// [`recycle_buffer`](Self::recycle_buffer) after processing to reuse the
    /// allocation on the next call.
    pub fn step(&mut self) -> Vec<Packet> {
        self.simulator.step()
    }

    /// Return a previously taken buffer for reuse. See [`PacketSimulator::recycle_buffer`].
    pub fn recycle_buffer(&mut self, buf: Vec<Packet>) {
        self.simulator.recycle_buffer(buf);
    }

    /// Advance network time by one tick.
    ///
    /// This should be called once per simulation tick, after all ready
    /// packets have been delivered. Handles automatic partition lifecycle
    /// and random path clogging.
    pub fn tick(&mut self) {
        self.simulator.tick();
    }

    /// Get the current network tick.
    pub fn current_tick(&self) -> u64 {
        self.simulator.current_tick()
    }

    /// Register a client with the network.
    ///
    /// Clients must be registered before they can send or receive packets.
    pub fn register_client(&mut self, client_id: u128) {
        self.simulator.register_client(client_id);
    }

    /// Set the enabled/disabled state of a specific link.
    /// Maps `enabled = true` to [`ALLOW_ALL`] and `enabled = false` to [`BLOCK_ALL`].
    pub fn set_link_filter(&mut self, from: ProcessId, to: ProcessId, enabled: bool) {
        let filter = self.simulator.link_filter(from, to);
        *filter = if enabled { ALLOW_ALL } else { BLOCK_ALL };
    }

    /// Get a mutable reference to a link's command filter.
    /// Allows per-command filtering (e.g., block only Prepare messages).
    pub fn link_filter_mut(&mut self, from: ProcessId, to: ProcessId) -> &mut LinkFilter {
        self.simulator.link_filter(from, to)
    }

    /// Check whether a specific link is enabled (filter is not empty).
    pub fn is_link_enabled(&self, from: ProcessId, to: ProcessId) -> bool {
        self.simulator.is_link_enabled(from, to)
    }

    /// Clear all partitions, restoring full connectivity.
    ///
    /// **Warning:** resets all link filters to [`ALLOW_ALL`], including
    /// manually-set per-command filters.
    pub fn clear_partition(&mut self) {
        self.simulator.clear_partition();
    }

    /// Clear all pending packets on a specific link.
    pub fn link_clear(&mut self, from: ProcessId, to: ProcessId) {
        self.simulator.link_clear(from, to);
    }

    /// Returns a mutable reference to the link's optional drop-packet predicate.
    pub fn link_drop_packet_fn(
        &mut self,
        from: ProcessId,
        to: ProcessId,
    ) -> &mut Option<fn(&Packet) -> bool> {
        self.simulator.link_drop_packet_fn(from, to)
    }

    /// Clog a specific link (bidirectionally).
    ///
    /// Clogged links do not deliver any packets until unclogged.
    pub fn clog(&mut self, from: ProcessId, to: ProcessId) {
        self.simulator.clog(from, to);
    }

    /// Unclog a specific link (bidirectionally).
    pub fn unclog(&mut self, from: ProcessId, to: ProcessId) {
        self.simulator.unclog(from, to);
    }

    /// Get the number of packets currently in flight.
    pub fn packets_in_flight(&self) -> usize {
        self.simulator.packets_in_flight()
    }
}
