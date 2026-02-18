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

//! Packet simulation layer for deterministic network testing.
//!
//! This module provides a packet-level network simulator that models:
//! - Per-path latency with exponential delay distribution
//! - Packet loss and replay (duplication) at delivery time
//! - Automatic network partitioning with configurable lifecycle
//! - Automatic path clogging with exponential duration
//! - Link capacity limits with random eviction
//! - Per-command link filtering via `LinkFilter` (`EnumSet<Command2>`)
//!
//! Partitions are implemented via per-link `LinkFilter`s. When a link's
//! filter is empty, all packets are silently dropped. When specific commands
//! are removed from the filter, only those command types are blocked.
//! External code can manipulate individual link filters via
//! [`PacketSimulator::link_filter`].
//!
//! # Delivery model
//!
//! This simulator uses **pull-based batch delivery**, [`PacketSimulator::step()`] returns
//! a `Vec<Packet>` containing all packets ready in the current tick. The caller
//! processes the batch after `step()` returns. This means packets delivered in
//! the same tick cannot trigger chain reactions within that tick.

use crate::ready_queue::{Ready, ReadyQueue};
use enumset::EnumSet;
use iggy_common::header::{Command2, GenericHeader};
use iggy_common::message::Message;
use rand::RngExt;
use rand_xoshiro::Xoshiro256Plus;
use rand_xoshiro::rand_core::SeedableRng;
use std::collections::HashMap;

/// Per-link command filter. An `EnumSet<Command2>` where:
/// - [`ALLOW_ALL`] = all commands pass (link fully enabled)
/// - [`BLOCK_ALL`] = all commands blocked (link fully disabled/partitioned)
/// - Custom sets = only matching commands pass through
pub type LinkFilter = EnumSet<Command2>;

/// Link filter that allows all commands through (link fully enabled).
pub const ALLOW_ALL: LinkFilter = EnumSet::all();

/// Link filter that blocks all commands (link fully disabled / partitioned).
pub const BLOCK_ALL: LinkFilter = EnumSet::empty();

/// Identifies a process (replica or client) in the simulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProcessId {
    Replica(u8),
    Client(u128),
}

/// A packet in flight through the simulated network.
#[derive(Debug, Clone)]
pub struct Packet {
    pub from: ProcessId,
    pub to: ProcessId,
    pub message: Message<GenericHeader>,
    /// Tick at which this packet becomes deliverable.
    pub ready_at: u64,
}

impl Ready for Packet {
    fn ready_at(&self) -> u64 {
        self.ready_at
    }
}

/// Configuration for the packet simulator.
#[derive(Debug, Clone)]
pub struct PacketSimulatorOptions {
    /// Minimum one-way delay in ticks.
    pub one_way_delay_min: u64,
    /// Mean one-way delay in ticks (exponential distribution).
    pub one_way_delay_mean: u64,
    /// Probability of dropping a packet at delivery time [0.0, 1.0].
    pub packet_loss_probability: f64,
    /// Probability of replaying/duplicating a packet at delivery time [0.0, 1.0].
    pub replay_probability: f64,
    /// Maximum number of packets in a single link's queue.
    pub link_capacity: u8,
    /// Probability per tick that a partition occurs (when not partitioned).
    pub partition_probability: f64,
    /// Probability per tick that a partition resolves (when partitioned).
    pub unpartition_probability: f64,
    /// Minimum ticks a partition lasts.
    pub partition_stability: u32,
    /// Minimum ticks of full connectivity before next partition.
    pub unpartition_stability: u32,
    /// How partitions are generated.
    pub partition_mode: PartitionMode,
    /// Whether partitions are symmetric or asymmetric.
    pub partition_symmetry: PartitionSymmetry,
    /// Probability per tick that any given path gets clogged.
    pub path_clog_probability: f64,
    /// Mean duration (ticks) of a clog (exponential distribution).
    pub path_clog_duration_mean: u64,
    /// Number of replica (node) processes.
    pub node_count: u8,
    /// Maximum number of client processes.
    pub client_count: u8,
    /// PRNG seed for deterministic behavior.
    pub seed: u64,
}

impl Default for PacketSimulatorOptions {
    fn default() -> Self {
        Self {
            one_way_delay_min: 1,
            one_way_delay_mean: 3,
            packet_loss_probability: 0.0,
            replay_probability: 0.0,
            link_capacity: 64,
            partition_probability: 0.0,
            unpartition_probability: 0.0,
            partition_stability: 0,
            unpartition_stability: 0,
            partition_mode: PartitionMode::None,
            partition_symmetry: PartitionSymmetry::Symmetric,
            path_clog_probability: 0.0,
            path_clog_duration_mean: 0,
            node_count: 1,
            client_count: 0,
            seed: 0,
        }
    }
}

/// Per-path link: holds packets in a ReadyQueue sorted by ready_at.
struct Link {
    /// Packets waiting to be delivered, ordered by ready_at (min-heap).
    packets: ReadyQueue<Packet>,
    /// Tick until which this link is clogged. Clogged when clogged_till > current_tick.
    clogged_till: u64,
    /// Per-command filter controlling which commands pass through this link.
    /// [`ALLOW_ALL`] = fully enabled (default), [`BLOCK_ALL`] = fully disabled.
    filter: LinkFilter,
    /// Optional predicate to drop specific packets. Checked after the link filter
    /// but before the random loss check.
    drop_packet_fn: Option<fn(&Packet) -> bool>,
}

impl std::fmt::Debug for Link {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Link")
            .field("packets", &self.packets)
            .field("clogged_till", &self.clogged_till)
            .field("filter", &self.filter)
            .field("drop_packet_fn", &self.drop_packet_fn.map(|_| "<fn>"))
            .finish()
    }
}

impl Link {
    fn new(capacity: u8) -> Self {
        Self {
            packets: ReadyQueue::with_capacity(capacity as usize),
            clogged_till: 0,
            filter: ALLOW_ALL,
            drop_packet_fn: None,
        }
    }
}

/// Determines how automatic partitions are created.
/// Only nodes (replicas) are partitioned. There will always be exactly two partitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PartitionMode {
    /// Disable automatic partitioning.
    #[default]
    None,
    /// Draws the size of the partition uniformly at random from [1, n-1].
    /// Replicas are randomly assigned a partition.
    UniformSize,
    /// Assigns each node to a partition uniformly at random.
    /// Biases towards equal-size partitions.
    UniformPartition,
    /// Isolates exactly one random node.
    IsolateSingle,
}

/// Whether partitions are symmetric or asymmetric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PartitionSymmetry {
    #[default]
    Symmetric,
    Asymmetric,
}

/// The packet simulator manages a matrix of links between processes.
pub struct PacketSimulator {
    options: PacketSimulatorOptions,
    /// Flat array of links. Index = from_idx * max_processes + to_idx.
    links: Vec<Link>,
    /// Maximum number of processes (determines link array size).
    max_processes: usize,
    /// Mapping from ProcessId to flat index.
    process_indices: HashMap<ProcessId, usize>,
    /// Next flat index to assign to a newly registered client.
    /// Initialized to `replica_count` and incremented on each new registration.
    next_index: usize,
    /// Current tick (network global time).
    current_tick: u64,
    /// PRNG for deterministic randomness.
    prng: Xoshiro256Plus,
    /// Whether an automatic partition is currently active.
    auto_partition_active: bool,
    /// Per-node partition assignment (true = partition A, false = partition B).
    auto_partition: Vec<bool>,
    /// Countdown timer for partition/unpartition stability.
    auto_partition_stability: u32,
    /// Scratch buffer for Fisher-Yates shuffle in UniformSize partition mode.
    auto_partition_nodes: Vec<usize>,
    /// Reusable buffer for delivered packets.
    delivered: Vec<Packet>,
}

impl PacketSimulator {
    /// Create a new packet simulator.
    pub fn new(options: PacketSimulatorOptions) -> Self {
        let node_count = options.node_count as usize;
        let client_count = options.client_count as usize;

        assert!(node_count > 0, "node_count must be > 0, got {}", node_count);
        assert!(
            options.one_way_delay_min >= 1,
            "one_way_delay_min must be >= 1 (got {}), zero causes unbounded replay loops",
            options.one_way_delay_min
        );
        assert!(
            options.one_way_delay_mean >= options.one_way_delay_min,
            "one_way_delay_mean ({}) must be >= one_way_delay_min ({})",
            options.one_way_delay_mean,
            options.one_way_delay_min
        );
        assert!(
            options.packet_loss_probability >= 0.0 && options.packet_loss_probability <= 1.0,
            "packet_loss_probability must be in [0.0, 1.0], got {}",
            options.packet_loss_probability
        );
        assert!(
            options.replay_probability >= 0.0 && options.replay_probability <= 1.0,
            "replay_probability must be in [0.0, 1.0], got {}",
            options.replay_probability
        );
        assert!(
            options.partition_probability >= 0.0 && options.partition_probability <= 1.0,
            "partition_probability must be in [0.0, 1.0], got {}",
            options.partition_probability
        );
        assert!(
            options.unpartition_probability >= 0.0 && options.unpartition_probability <= 1.0,
            "unpartition_probability must be in [0.0, 1.0], got {}",
            options.unpartition_probability
        );
        assert!(
            options.path_clog_probability >= 0.0 && options.path_clog_probability <= 1.0,
            "path_clog_probability must be in [0.0, 1.0], got {}",
            options.path_clog_probability
        );

        let max_processes = node_count + client_count;

        // Pre-register all replicas
        let mut process_indices = HashMap::new();
        for i in 0..node_count {
            process_indices.insert(ProcessId::Replica(i as u8), i);
        }

        // Create link matrix (NxN)
        let link_count = max_processes * max_processes;
        let link_capacity = options.link_capacity;
        let links = (0..link_count).map(|_| Link::new(link_capacity)).collect();

        // Start with unpartition_stability grace period
        let initial_stability = options.unpartition_stability;
        let seed = options.seed;

        Self {
            options,
            links,
            max_processes,
            process_indices,
            next_index: node_count,
            current_tick: 0,
            prng: Xoshiro256Plus::seed_from_u64(seed),
            auto_partition_active: false,
            auto_partition: vec![false; node_count],
            auto_partition_stability: initial_stability,
            auto_partition_nodes: (0..node_count).collect(),
            delivered: Vec::new(),
        }
    }

    /// Register a client process. Returns its flat index.
    /// Re-registering a client with the same ID returns the same index.
    pub fn register_client(&mut self, client_id: u128) -> usize {
        if let Some(&idx) = self.process_indices.get(&ProcessId::Client(client_id)) {
            return idx;
        }
        let idx = self.next_index;
        assert!(
            idx < self.max_processes,
            "Too many processes registered (max: {})",
            self.max_processes
        );
        self.process_indices
            .insert(ProcessId::Client(client_id), idx);
        self.next_index += 1;
        idx
    }

    /// Resolve a `ProcessId` to its flat index.
    /// Fast path for replicas (direct arithmetic), HashMap fallback for clients.
    fn process_index(&self, id: ProcessId) -> Option<usize> {
        match id {
            ProcessId::Replica(i) => {
                let idx = i as usize;
                if idx < self.auto_partition.len() {
                    Some(idx)
                } else {
                    None
                }
            }
            ProcessId::Client(_) => self.process_indices.get(&id).copied(),
        }
    }

    /// Get the flat link index for a (from, to) pair.
    /// Panics on unknown processes.
    fn link_index(&self, from: ProcessId, to: ProcessId) -> usize {
        let from_idx = self
            .process_index(from)
            .unwrap_or_else(|| panic!("unknown process: {from:?}"));
        let to_idx = self
            .process_index(to)
            .unwrap_or_else(|| panic!("unknown process: {to:?}"));
        from_idx * self.max_processes + to_idx
    }

    /// Submit a packet into the network.
    ///
    /// Always enqueues the packet. Loss and replay checks happen at delivery
    /// time in `step()`. If the link is at capacity, a random existing packet
    /// is evicted to make room.
    pub fn submit(&mut self, from: ProcessId, to: ProcessId, message: Message<GenericHeader>) {
        let delay = Self::calculate_delay(&mut self.prng, &self.options);
        let ready_at = self.current_tick.saturating_add(delay);

        let packet = Packet {
            from,
            to,
            message,
            ready_at,
        };

        let idx = self.link_index(from, to);

        // If at capacity, evict a random existing packet
        if self.links[idx].packets.len() >= self.options.link_capacity as usize {
            self.links[idx].packets.remove_random(&mut self.prng);
            tracing::trace!(?from, ?to, "evicted random packet (link at capacity)");
        }
        self.links[idx].packets.push(packet);
    }

    /// Calculate a random delay using exponential distribution.
    /// Returns max(min, exponential(mean)).
    fn calculate_delay(prng: &mut Xoshiro256Plus, options: &PacketSimulatorOptions) -> u64 {
        let min = options.one_way_delay_min;
        let mean = options.one_way_delay_mean;
        let exp = Self::random_exponential(prng, mean);
        min.max(exp)
    }

    /// Generate an exponentially distributed random value with the given mean.
    /// Uses inverse CDF: -mean * ln(U) where U ~ Uniform(0,1).
    fn random_exponential(prng: &mut Xoshiro256Plus, mean: u64) -> u64 {
        let u: f64 = prng.random::<f64>();
        if u > 0.0 {
            (-(mean as f64) * u.ln()) as u64
        } else {
            // Fallback for u == 0.0 (ln(0) is -inf).
            mean.saturating_mul(20)
        }
    }

    /// Number of registered processes.
    fn process_count(&self) -> usize {
        self.next_index
    }

    /// Returns a mutable reference to the link's filter.
    /// This is the per-link command filter — `EnumSet<Command2>`.
    /// Set to [`BLOCK_ALL`] to block all packets (partition).
    /// Set to [`ALLOW_ALL`] to allow all packets (default).
    /// Remove specific commands to selectively filter.
    pub fn link_filter(&mut self, from: ProcessId, to: ProcessId) -> &mut LinkFilter {
        let idx = self.link_index(from, to);
        &mut self.links[idx].filter
    }

    /// Check whether a link is currently enabled (filter is not empty).
    pub fn is_link_enabled(&self, from: ProcessId, to: ProcessId) -> bool {
        let idx = self.link_index(from, to);
        !self.links[idx].filter.is_empty()
    }

    /// Clear all pending packets on a specific link.
    pub fn link_clear(&mut self, from: ProcessId, to: ProcessId) {
        let idx = self.link_index(from, to);
        self.links[idx].packets.clear();
    }

    /// Returns a mutable reference to the link's optional drop-packet predicate.
    ///
    /// When set, the predicate is called for each packet after the link filter check
    /// but before the random loss check. If it returns `true`, the packet is dropped.
    pub fn link_drop_packet_fn(
        &mut self,
        from: ProcessId,
        to: ProcessId,
    ) -> &mut Option<fn(&Packet) -> bool> {
        let idx = self.link_index(from, to);
        &mut self.links[idx].drop_packet_fn
    }

    // TODO: implement record/replay_recorded for deterministic replay support.

    /// Deliver all packets that are ready at the current tick.
    /// Returns a `Vec` of packets that should be delivered. Does NOT advance the tick.
    ///
    /// This returns the full batch at once. Packets delivered in the same tick cannot trigger
    /// chain reactions within that tick - the caller processes them after this returns.
    ///
    /// The returned `Vec` is taken from an internal buffer via `std::mem::take`.
    /// After processing, pass it back via [`recycle_buffer`](Self::recycle_buffer).
    ///
    /// At delivery time, the following happens:
    /// 1. If link is clogged -> skip (don't dequeue, clogging delays delivery)
    /// 2. Remove a random ready packet via reservoir sampling
    /// 3. If packet's command is not in the link filter -> drop
    /// 4. If `drop_packet_fn` returns true -> drop
    /// 5. Random packet loss -> drop
    /// 6. Random replay -> clone and re-enqueue with new delay
    /// 7. If survived -> include in delivered vec
    pub fn step(&mut self) -> Vec<Packet> {
        self.delivered.clear();

        let Self {
            links,
            prng,
            options,
            current_tick,
            delivered,
            max_processes,
            next_index,
            ..
        } = self;

        let process_count = *next_index;

        for from in 0..process_count {
            for to in 0..process_count {
                let idx = from * *max_processes + to;
                let link = &mut links[idx];

                // Clogged links don't deliver — packets stay in queue
                if link.clogged_till > *current_tick {
                    continue;
                }

                loop {
                    let Some(packet) = link.packets.remove_ready(prng, *current_tick) else {
                        break;
                    };

                    // Per-command link filter check: drop if command not in filter
                    let command = packet.message.header().command;
                    if !link.filter.contains(command) {
                        tracing::trace!(?command, "packet dropped (command filtered)");
                        continue;
                    }

                    // Custom drop predicate check
                    if let Some(should_drop) = link.drop_packet_fn
                        && should_drop(&packet)
                    {
                        tracing::trace!("packet dropped (drop_packet_fn)");
                        continue;
                    }

                    // Random loss check
                    if prng.random::<f64>() < options.packet_loss_probability {
                        tracing::trace!("packet dropped (loss probability)");
                        continue;
                    }

                    // Random replay check: clone and re-enqueue with eviction at capacity
                    if prng.random::<f64>() < options.replay_probability {
                        let delay = Self::calculate_delay(prng, options);
                        let replay = Packet {
                            ready_at: (*current_tick).saturating_add(delay),
                            ..packet.clone()
                        };
                        if link.packets.len() >= options.link_capacity as usize {
                            link.packets.remove_random(prng);
                            tracing::trace!("evicted random packet for replay (link at capacity)");
                        }
                        link.packets.push(replay);
                        tracing::trace!("packet replayed");
                    }

                    delivered.push(packet);
                }
            }
        }

        std::mem::take(&mut self.delivered)
    }

    /// Return a previously taken buffer for reuse.
    pub fn recycle_buffer(&mut self, mut buf: Vec<Packet>) {
        buf.clear();
        self.delivered = buf;
    }

    /// Advance the network's tick counter.
    /// Handles automatic partition lifecycle and random path clogging.
    pub fn tick(&mut self) {
        self.current_tick += 1;

        // Partition lifecycle
        if self.auto_partition_stability > 0 {
            self.auto_partition_stability -= 1;
        } else if self.auto_partition_active {
            if self.prng.random::<f64>() < self.options.unpartition_probability {
                self.auto_partition_active = false;
                self.auto_partition_stability = self.options.unpartition_stability;
                self.auto_partition.iter_mut().for_each(|p| *p = false);
                // This resets ALL link filters to ALLOW_ALL, cancelling any
                // manually-set per-command filters. Use partition_mode: None if you
                // need custom filters to persist across partition cycles.
                for link in &mut self.links {
                    link.filter = ALLOW_ALL;
                }
                tracing::debug!(partition = ?self.auto_partition, "unpartitioned network");
            }
        } else if self.options.node_count > 1
            && self.prng.random::<f64>() < self.options.partition_probability
        {
            self.auto_partition_network();
            tracing::debug!(partition = ?self.auto_partition, "partitioned network");
        }

        // Random path clogging
        let process_count = self.process_count();
        for from in 0..process_count {
            for to in 0..process_count {
                if self.prng.random::<f64>() < self.options.path_clog_probability {
                    let duration = Self::random_exponential(
                        &mut self.prng,
                        self.options.path_clog_duration_mean,
                    );
                    let idx = from * self.max_processes + to;
                    self.links[idx].clogged_till = self.current_tick.saturating_add(duration);
                    tracing::debug!(from, to, duration, "path clogged");
                }
            }
        }
    }

    /// Partition the network into two groups. Guaranteed to isolate at least one replica.
    fn auto_partition_network(&mut self) {
        let node_count = self.options.node_count as usize;
        assert!(node_count > 1);

        match self.options.partition_mode {
            PartitionMode::None => {
                self.auto_partition.iter_mut().for_each(|p| *p = false);
            }
            PartitionMode::UniformSize => {
                let partition_size = self.prng.random_range(1..node_count);
                // Reset and shuffle pre-allocated node indices using Fisher-Yates
                for (i, node) in self.auto_partition_nodes.iter_mut().enumerate() {
                    *node = i;
                }
                for i in (1..node_count).rev() {
                    let j = self.prng.random_range(0..=i);
                    self.auto_partition_nodes.swap(i, j);
                }
                for (i, &node) in self.auto_partition_nodes.iter().enumerate() {
                    self.auto_partition[node] = i < partition_size;
                }
            }
            PartitionMode::UniformPartition => {
                let mut all_same = true;
                self.auto_partition[0] = self.prng.random::<bool>();
                for i in 1..node_count {
                    self.auto_partition[i] = self.prng.random::<bool>();
                    if self.auto_partition[i] != self.auto_partition[i - 1] {
                        all_same = false;
                    }
                }
                if all_same {
                    // Force at least one node into the opposite partition.
                    let n = self.prng.random_range(0..node_count);
                    self.auto_partition[n] = !self.auto_partition[0];
                }
            }
            PartitionMode::IsolateSingle => {
                self.auto_partition.iter_mut().for_each(|p| *p = false);
                let n = self.prng.random_range(0..node_count);
                self.auto_partition[n] = true;
            }
        }

        self.auto_partition_active = true;
        self.auto_partition_stability = self.options.partition_stability;

        // Apply partition to links
        let asymmetric_side = self.prng.random::<bool>();
        let process_count = self.process_count();

        for from in 0..process_count {
            for to in 0..process_count {
                let from_is_node = from < self.options.node_count as usize;
                let to_is_node = to < self.options.node_count as usize;

                let enabled = !from_is_node
                    || !to_is_node
                    || self.auto_partition[from] == self.auto_partition[to]
                    || (self.options.partition_symmetry == PartitionSymmetry::Asymmetric
                        && self.auto_partition[from] == asymmetric_side);

                let idx = from * self.max_processes + to;
                self.links[idx].filter = if enabled { ALLOW_ALL } else { BLOCK_ALL };
            }
        }
    }

    /// Get the current tick.
    pub fn current_tick(&self) -> u64 {
        self.current_tick
    }

    /// Clear all partitions, restoring full connectivity.
    ///
    /// Resets auto partition state and sets **all** link filters to `ALLOW_ALL`,
    /// including any manually-set per-command filters. If you need custom filters
    /// to survive partition clearing, set `partition_mode: None` and manage
    /// partitions manually.
    pub fn clear_partition(&mut self) {
        self.auto_partition_active = false;
        self.auto_partition_stability = self.options.unpartition_stability;
        self.auto_partition.iter_mut().for_each(|p| *p = false);
        for link in &mut self.links {
            link.filter = ALLOW_ALL;
        }
        tracing::debug!("partitions cleared");
    }

    /// Clog a specific link (both directions) indefinitely until `unclog()` is called.
    pub fn clog(&mut self, from: ProcessId, to: ProcessId) {
        let idx = self.link_index(from, to);
        self.links[idx].clogged_till = u64::MAX;
        let idx = self.link_index(to, from);
        self.links[idx].clogged_till = u64::MAX;
        tracing::debug!(?from, ?to, "link clogged");
    }

    /// Unclog a specific link (both directions).
    pub fn unclog(&mut self, from: ProcessId, to: ProcessId) {
        let idx = self.link_index(from, to);
        self.links[idx].clogged_till = 0;
        let idx = self.link_index(to, from);
        self.links[idx].clogged_till = 0;
        tracing::debug!(?from, ?to, "link unclogged");
    }

    /// Get the number of packets in flight across all links.
    pub fn packets_in_flight(&self) -> usize {
        self.links.iter().map(|l| l.packets.len()).sum()
    }
}

impl std::fmt::Debug for PacketSimulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PacketSimulator")
            .field("current_tick", &self.current_tick)
            .field("max_processes", &self.max_processes)
            .field("processes_registered", &self.process_indices.len())
            .field("packets_in_flight", &self.packets_in_flight())
            .field("auto_partition_active", &self.auto_partition_active)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_message() -> Message<GenericHeader> {
        Message::<GenericHeader>::new(std::mem::size_of::<GenericHeader>())
    }

    fn create_test_message_with_command(command: Command2) -> Message<GenericHeader> {
        let size = std::mem::size_of::<GenericHeader>();
        let mut buf = vec![0u8; size];
        let header: &mut GenericHeader = bytemuck::from_bytes_mut(&mut buf);
        header.command = command;
        Message::<GenericHeader>::from_bytes(bytes::Bytes::from(buf)).unwrap()
    }

    /// Helper: disable all links to/from a given replica (isolate it).
    fn isolate_replica(sim: &mut PacketSimulator, replica: u8, replica_count: u8) {
        for i in 0..replica_count {
            if i != replica {
                *sim.link_filter(ProcessId::Replica(i), ProcessId::Replica(replica)) = BLOCK_ALL;
                *sim.link_filter(ProcessId::Replica(replica), ProcessId::Replica(i)) = BLOCK_ALL;
            }
        }
    }

    #[test]
    fn test_basic_packet_delivery() {
        let options = PacketSimulatorOptions {
            one_way_delay_min: 1,
            one_way_delay_mean: 1,
            packet_loss_probability: 0.0,
            replay_probability: 0.0,
            link_capacity: 64,
            node_count: 3,
            client_count: 1,
            seed: 42,
            ..Default::default()
        };

        let mut sim = PacketSimulator::new(options);

        let msg = create_test_message();
        sim.submit(ProcessId::Replica(0), ProcessId::Replica(1), msg);

        // At tick 0, packet not ready (delay >= 1)
        assert!(sim.step().is_empty());

        // Advance ticks until delivery (exponential distribution, so may need more than 1)
        for _ in 0..20 {
            sim.tick();
        }

        let delivered = sim.step();
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].from, ProcessId::Replica(0));
        assert_eq!(delivered[0].to, ProcessId::Replica(1));
    }

    #[test]
    fn test_partition_drops_not_buffers() {
        // Verify that packets sent during a partition are dropped, not buffered
        let options = PacketSimulatorOptions {
            one_way_delay_min: 1,
            one_way_delay_mean: 1,
            node_count: 3,
            client_count: 0,
            seed: 42,
            ..Default::default()
        };

        let mut sim = PacketSimulator::new(options);
        isolate_replica(&mut sim, 1, 3);

        let msg = create_test_message();

        // Send packet during partition
        sim.submit(ProcessId::Replica(0), ProcessId::Replica(1), msg.clone());
        for _ in 0..20 {
            sim.tick();
            sim.step(); // drain and drop
        }

        // Clear partition
        sim.clear_partition();

        // The old packet should NOT be delivered — it was dropped
        let delivered = sim.step();
        assert!(delivered.is_empty());

        // New packet should work after partition clears
        sim.submit(ProcessId::Replica(0), ProcessId::Replica(1), msg);
        for _ in 0..20 {
            sim.tick();
        }
        let delivered = sim.step();
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].to, ProcessId::Replica(1));
    }

    #[test]
    fn test_clog_unclog() {
        let options = PacketSimulatorOptions {
            one_way_delay_min: 1,
            one_way_delay_mean: 1,
            node_count: 2,
            client_count: 0,
            seed: 42,
            ..Default::default()
        };

        let mut sim = PacketSimulator::new(options);
        let msg = create_test_message();

        // Clog the link
        sim.clog(ProcessId::Replica(0), ProcessId::Replica(1));

        // Submit a packet
        sim.submit(ProcessId::Replica(0), ProcessId::Replica(1), msg.clone());
        for _ in 0..20 {
            sim.tick();
        }

        // Should not be delivered (clogged)
        assert!(sim.step().is_empty());
        // Packet should still be in flight (buffered, not dropped)
        assert_eq!(sim.packets_in_flight(), 1);

        // Unclog
        sim.unclog(ProcessId::Replica(0), ProcessId::Replica(1));

        // Now it should deliver
        let delivered = sim.step();
        assert_eq!(delivered.len(), 1);
    }

    #[test]
    fn test_auto_partition_lifecycle() {
        let options = PacketSimulatorOptions {
            one_way_delay_min: 1,
            one_way_delay_mean: 1,
            partition_probability: 1.0,   // always partition
            unpartition_probability: 1.0, // always unpartition
            partition_stability: 5,       // partition lasts at least 5 ticks
            unpartition_stability: 3,     // unpartition lasts at least 3 ticks
            partition_mode: PartitionMode::IsolateSingle,
            node_count: 3,
            client_count: 0,
            seed: 42,
            ..Default::default()
        };

        let mut sim = PacketSimulator::new(options);

        // Initially no partition
        assert!(!sim.auto_partition_active);

        // Initial grace period: unpartition_stability = 3 ticks of guaranteed connectivity
        for _ in 0..3 {
            sim.tick();
            assert!(
                !sim.auto_partition_active,
                "should not partition during initial grace period"
            );
        }

        // After grace period, should partition (probability = 1.0)
        sim.tick();
        assert!(sim.auto_partition_active);
        let disabled_count = sim.links.iter().filter(|l| l.filter.is_empty()).count();
        assert!(disabled_count > 0, "partition should disable some links");

        // Partition should be stable for 5 ticks
        for _ in 0..5 {
            sim.tick();
            assert!(sim.auto_partition_active, "partition should be stable");
        }

        // After stability period, should unpartition (probability = 1.0)
        sim.tick();
        assert!(!sim.auto_partition_active);
        assert!(sim.links.iter().all(|l| l.filter == ALLOW_ALL));

        // Unpartition should be stable for 3 ticks
        for _ in 0..3 {
            sim.tick();
            assert!(!sim.auto_partition_active, "unpartition should be stable");
        }

        // After unpartition stability, should partition again
        sim.tick();
        assert!(sim.auto_partition_active);
    }

    #[test]
    fn test_loss_at_delivery_time() {
        // With 100% loss, packets should be enqueued but dropped at delivery
        let options = PacketSimulatorOptions {
            one_way_delay_min: 1,
            one_way_delay_mean: 1,
            packet_loss_probability: 1.0,
            node_count: 2,
            client_count: 0,
            seed: 42,
            ..Default::default()
        };

        let mut sim = PacketSimulator::new(options);
        let msg = create_test_message();

        sim.submit(ProcessId::Replica(0), ProcessId::Replica(1), msg);

        // Packet should be enqueued
        assert_eq!(sim.packets_in_flight(), 1);

        for _ in 0..20 {
            sim.tick();
        }

        // Packet should be dequeued and dropped at delivery
        let delivered = sim.step();
        assert!(delivered.is_empty());
        assert_eq!(sim.packets_in_flight(), 0);
    }

    #[test]
    fn test_command_level_filtering() {
        // Test that per-command filtering works: allow Ping but block Prepare
        let options = PacketSimulatorOptions {
            one_way_delay_min: 1,
            one_way_delay_mean: 1,
            packet_loss_probability: 0.0,
            replay_probability: 0.0,
            link_capacity: 64,
            node_count: 2,
            client_count: 0,
            seed: 42,
            ..Default::default()
        };

        let mut sim = PacketSimulator::new(options);

        // Set filter to only allow Ping on link 0->1
        let filter = sim.link_filter(ProcessId::Replica(0), ProcessId::Replica(1));
        *filter = EnumSet::only(Command2::Ping);

        // Submit a Ping message and a Prepare message
        let ping_msg = create_test_message_with_command(Command2::Ping);
        let prepare_msg = create_test_message_with_command(Command2::Prepare);

        sim.submit(ProcessId::Replica(0), ProcessId::Replica(1), ping_msg);
        sim.submit(ProcessId::Replica(0), ProcessId::Replica(1), prepare_msg);

        // Advance time so both are ready
        for _ in 0..20 {
            sim.tick();
        }

        let delivered = sim.step();

        // Only the Ping should be delivered
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].message.header().command, Command2::Ping);

        // Nothing left in flight
        assert_eq!(sim.packets_in_flight(), 0);
    }
}
