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

//! A fuzz run of geo topics in two clusters.
//!
//! Each cluster is the origin of one geo topic and a peer of the other's.
//! The clients of each cluster send to the partitions of its own topic,
//! while faults drawn from the seed crash replicas, stop whole DCs, cut a
//! cluster off from its peer's bucket, stall buckets, take them down, and
//! make their calls slow, fail or lose their answers. Then every fault heals,
//! every send drains, and every shadow catches up.
//!
//! Checked as the run goes: no cluster writes to a bucket that it does not
//! own (G3), and the offset that a replica committed for a shadow never
//! moves back (G5). Every poll of a shadow during the run is kept and checked
//! at the end against its origin's log: a shadow serves a prefix of it (G2).
//! At the end, every acknowledged send is read once and each client's in the
//! order it sent them (G1), each is in its origin's bucket (G8), and each
//! shadow serves its origin's whole log within a bound of steps once the
//! faults healed (G6).

use std::collections::{BTreeMap, BTreeSet};

use iggy_binary_protocol::{Operation, RoutedRequestHeader};
use iggy_common::IggyError;
use rand::RngExt;
use rand_xoshiro::Xoshiro256PlusPlus;
use rand_xoshiro::rand_core::SeedableRng;
use server_common::Message;
use server_common::sharding::IggyNamespace;
use std::time::Duration;

use crate::bucket::{BucketFaults, ClusterId};
use crate::client::SimClient;
use crate::geo_world::{GeoSeeds, GeoWorld, GeoWorldOptions, ORIGINS, POLL_COUNT, PollOutput};
use crate::packet::PacketSimulatorOptions;

/// The stream and the geo topic that each cluster creates.
const STREAM: &str = "geo";
const TOPIC: &str = "orders";
/// The start of every payload. The rest names the origin, the client and
/// the client's sequence number, so a payload is found in any bytes by a
/// scan.
const PAYLOAD_MARK: &[u8] = b"geo-payload-";
const PAYLOAD_END: &[u8] = b"-end";
/// Below [`POLL_COUNT`], so one poll reads a whole log.
const MAX_SENDS_PER_PARTITION: usize = 900;
const _: () = assert!(MAX_SENDS_PER_PARTITION < POLL_COUNT as usize);
/// Steps a send waits for its answer before it goes out again.
const ANSWER_STEPS: u64 = 300;
/// Steps a login waits for its answer before it goes out again.
const LOGIN_STEPS: u64 = 200;
/// How often the run polls every shadow.
const SHADOW_POLL_STEPS: u64 = 400;
/// How often the run checks that no cluster wrote to another's bucket.
const FOREIGN_WRITE_STEPS: u64 = 500;
/// Steps every send gets to end once the faults healed.
const DRAIN_STEPS: u64 = 20_000;
/// Steps every shadow gets to serve its origin's whole log once the sends
/// drained: a rewrite, a manifest, and the peer's listings of both.
const CONVERGE_STEPS: u64 = 30_000;
/// How often the convergence wait polls.
const CONVERGE_POLL_STEPS: u64 = 200;
/// Steps a poll at the end gets.
const FINAL_POLL_STEPS: usize = 3_000;

/// How a run is built.
#[derive(Debug, Clone)]
pub struct GeoFuzzOptions {
    pub seed: u64,
    /// Steps of the run before every fault heals.
    pub ticks: u64,
    pub replicas: u8,
    /// Clients of each cluster.
    pub clients: u8,
    /// Partitions of each cluster's geo topic.
    pub partitions: u32,
    /// The network of each cluster, by cluster index, from the moment the
    /// geo topics exist.
    pub networks: [PacketSimulatorOptions; 2],
    /// The faults of the buckets and the DCs, and the skew of the clocks.
    pub faults: GeoFaults,
    /// The chance that an idle client starts a send on a step.
    pub send_per_tick: f64,
}

/// The faults of a run besides its networks' own. Each chance is per step.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct GeoFaults {
    pub bucket: BucketFaults,
    /// A replica stops, while a majority of its cluster stays up.
    pub crash: f64,
    /// A replica that stopped on its own starts again.
    pub restart: f64,
    /// Every replica of one cluster stops, while no DC is down.
    pub dc_stop: f64,
    /// A stopped DC starts again.
    pub dc_start: f64,
    /// A cluster loses its link to a peer's bucket.
    pub cut: f64,
    /// A lost link comes back.
    pub reconnect: f64,
    /// A bucket stalls every call until it heals.
    pub stall: f64,
    pub stall_end: f64,
    /// A bucket fails every call.
    pub outage: f64,
    pub outage_end: f64,
    /// How far each cluster's wall clock is from the shared one.
    pub clock_skews_micros: [i64; 2],
}

impl GeoFaults {
    /// Every fault parameter drawn from `seed`. Each kind of fault is off for
    /// about half the seeds, so a campaign meets each one alone and with the
    /// others.
    #[must_use]
    pub fn swarm(seed: u64) -> Self {
        let mut prng = Xoshiro256PlusPlus::seed_from_u64(GeoSeeds::derive(seed).swarm);
        let latency_min = Duration::from_millis(u64::from(prng.random_range(0..=5u32)));
        let latency_max = latency_min + Duration::from_millis(prng.random_range(0..=80u64));
        let mut faults = Self {
            bucket: BucketFaults {
                latency_min,
                latency_max,
                error_probability: per_mille(&mut prng, 150),
                lost_answer_probability: per_mille(&mut prng, 100),
            },
            crash: per_mille(&mut prng, 3),
            restart: per_mille(&mut prng, 20).max(0.002),
            dc_stop: per_mille(&mut prng, 1) / 2.0,
            dc_start: per_mille(&mut prng, 5).max(0.001),
            cut: per_mille(&mut prng, 1),
            reconnect: per_mille(&mut prng, 10).max(0.002),
            stall: per_mille(&mut prng, 1) / 2.0,
            stall_end: per_mille(&mut prng, 20).max(0.002),
            outage: per_mille(&mut prng, 1) / 2.0,
            outage_end: per_mille(&mut prng, 20).max(0.002),
            clock_skews_micros: [
                prng.random_range(-120_000_000..=120_000_000),
                prng.random_range(-120_000_000..=120_000_000),
            ],
        };
        // Swarm testing: a fault that is always on hides the paths that only
        // a run without it reaches.
        let mut off = || prng.random_bool(0.5);
        if off() {
            faults.bucket = BucketFaults::default();
        }
        if off() {
            faults.crash = 0.0;
        }
        if off() {
            faults.dc_stop = 0.0;
        }
        if off() {
            faults.cut = 0.0;
        }
        if off() {
            faults.stall = 0.0;
        }
        if off() {
            faults.outage = 0.0;
        }
        if off() {
            faults.clock_skews_micros = [0, 0];
        }
        faults
    }
}

/// A chance of up to `max` in a thousand.
fn per_mille(prng: &mut Xoshiro256PlusPlus, max: u32) -> f64 {
    f64::from(prng.random_range(0..=max)) / 1_000.0
}

/// What a run did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GeoFuzzReport {
    /// Sends started, acknowledged, and left with an unknown outcome, per
    /// cluster.
    pub sends: [u64; 2],
    pub acknowledged: [u64; 2],
    pub unknown: [u64; 2],
    /// Answers that refused a send and did not ask for a retry, by code.
    pub refusals: BTreeMap<u32, u64>,
    pub evictions: u64,
    pub crashes: u64,
    pub dc_stops: u64,
    pub cuts: u64,
    pub stalls: u64,
    pub outages: u64,
    /// Polls of a shadow during the run that held at least one message, all
    /// of them checked against the origin's log.
    pub shadow_reads: u64,
    /// Steps of the drain and of the convergence wait.
    pub drain_steps: u64,
    pub converge_steps: u64,
    pub schedule_hashes: [u64; 2],
}

impl GeoFuzzReport {
    /// Whether the run compared nothing: no send was acknowledged.
    #[must_use]
    pub fn is_vacuous(&self) -> bool {
        self.acknowledged.iter().sum::<u64>() == 0
    }
}

/// Run `options` and check every geo invariant.
///
/// # Panics
/// When an invariant fails, or when the run does not drain or converge once
/// every fault healed.
#[must_use]
pub fn run(options: &GeoFuzzOptions) -> GeoFuzzReport {
    let mut run = Run::new(options);
    for _ in 0..options.ticks {
        run.inject_faults();
        run.tick(true);
    }
    run.world.heal();
    run.drain();
    run.converge();
    run.check_logs();
    run.report.schedule_hashes = [
        run.world.sim(0).schedule_hash(),
        run.world.sim(1).schedule_hash(),
    ];
    run.report
}

/// One send of a client.
struct Send {
    request: Message<RoutedRequestHeader>,
    payload: String,
    partition: usize,
    /// Times the request went out.
    attempts: u32,
    /// The answers that asked for a retry, by code, for a report of a send
    /// that never ends.
    retries: BTreeMap<u32, u32>,
}

enum State {
    Idle,
    /// The send went to `target` at `at`, or goes out on the next step.
    Sending {
        send: Send,
        target: Option<(u8, u64)>,
    },
    /// The client logs in to `target`, which evicted it, and then resends.
    LoggingIn {
        send: Send,
        target: u8,
        at: u64,
    },
}

struct Producer {
    cluster: usize,
    client: SimClient,
    next_sequence: u64,
    state: State,
}

/// What the run knows of the sends to one partition of an origin.
#[derive(Default)]
struct Partition {
    started: usize,
    /// In the order each client got its answers.
    acknowledged: BTreeMap<u128, Vec<String>>,
    unknown: BTreeSet<String>,
}

struct Run {
    world: GeoWorld,
    producers: Vec<Producer>,
    /// By cluster, the partitions of its geo topic.
    namespaces: [Vec<IggyNamespace>; 2],
    /// By cluster and partition.
    partitions: [Vec<Partition>; 2],
    faults: GeoFaults,
    fault_prng: Xoshiro256PlusPlus,
    workload_prng: Xoshiro256PlusPlus,
    send_per_tick: f64,
    step: u64,
    /// Which cluster's DC is down, if one is.
    dc_down: Option<usize>,
    /// The highest offset each replica committed for each shadow, by peer
    /// cluster, shadow and replica.
    shadow_floors: BTreeMap<(usize, IggyNamespace, u8), u64>,
    /// Polls of shadows in flight: the origin, the partition and the poll.
    shadow_polls: Vec<(usize, usize, PollOutput)>,
    /// What each poll of a shadow served, by origin and partition.
    shadow_reads: Vec<(usize, usize, Vec<String>)>,
    report: GeoFuzzReport,
}

impl Run {
    fn new(options: &GeoFuzzOptions) -> Self {
        let seeds = GeoSeeds::derive(options.seed);
        let client_ids: Vec<u128> = (1..=u128::from(options.clients)).collect();
        let mut world_options =
            GeoWorldOptions::new(options.seed, 2, options.replicas, &client_ids);
        world_options.clock_skews_micros = options.faults.clock_skews_micros;
        let mut world = GeoWorld::new(&world_options);
        let mut producers = Vec::new();
        let mut namespaces = [Vec::new(), Vec::new()];
        for (cluster, namespaces) in namespaces.iter_mut().enumerate() {
            let clients: Vec<SimClient> = client_ids.iter().map(|id| SimClient::new(*id)).collect();
            for client in &clients {
                world.sim_mut(cluster).shell_login(client);
            }
            *namespaces =
                world.create_geo_topic(cluster, &clients[0], STREAM, TOPIC, options.partitions);
            for client in clients {
                world.sim(cluster).discover_partition_contexts(&client);
                producers.push(Producer {
                    cluster,
                    client,
                    next_sequence: 0,
                    state: State::Idle,
                });
            }
        }
        // The networks and the buckets fail only once the topics exist: the
        // setup is not what the run checks.
        for cluster in 0..2 {
            world
                .sim_mut(cluster)
                .network
                .set_faults(&options.networks[cluster]);
            world.bucket(cluster).set_faults(options.faults.bucket);
        }
        let partitions = [0, 1].map(|cluster| {
            (0..namespaces[cluster].len())
                .map(|_| Partition::default())
                .collect()
        });
        Self {
            world,
            producers,
            namespaces,
            partitions,
            faults: options.faults,
            fault_prng: Xoshiro256PlusPlus::seed_from_u64(seeds.faults),
            workload_prng: Xoshiro256PlusPlus::seed_from_u64(seeds.workload),
            send_per_tick: options.send_per_tick,
            step: 0,
            dc_down: None,
            shadow_floors: BTreeMap::new(),
            shadow_polls: Vec::new(),
            shadow_reads: Vec::new(),
            report: GeoFuzzReport::default(),
        }
    }

    /// One step of the run: the clients send, the world steps, the answers
    /// land, and the checks that run as it goes run.
    fn tick(&mut self, start_sends: bool) {
        self.drive_producers(start_sends);
        let replies = self.world.step();
        self.step += 1;
        for (cluster, replies) in replies.into_iter().enumerate() {
            for reply in replies {
                self.on_reply(cluster, &reply);
            }
            for (client, from) in self.world.sim_mut(cluster).take_evictions() {
                self.on_eviction(cluster, client, from);
            }
        }
        self.check_shadow_floors();
        self.collect_shadow_polls();
        if self.step.is_multiple_of(SHADOW_POLL_STEPS) {
            self.start_shadow_polls();
        }
        if self.step.is_multiple_of(FOREIGN_WRITE_STEPS) {
            self.check_foreign_writes();
        }
    }

    fn drive_producers(&mut self, start_sends: bool) {
        for index in 0..self.producers.len() {
            let cluster = self.producers[index].cluster;
            if start_sends && matches!(self.producers[index].state, State::Idle) {
                self.maybe_start_send(index);
            }
            let step = self.step;
            let producer = &mut self.producers[index];
            match &mut producer.state {
                State::Idle => {}
                State::Sending { send, target } => {
                    if target.is_some_and(|(_, at)| step < at + ANSWER_STEPS) {
                        continue;
                    }
                    let namespace = self.namespaces[cluster][send.partition];
                    let Some(primary) = self.world.primary(cluster, namespace) else {
                        continue;
                    };
                    *target = Some((primary, step));
                    send.attempts += 1;
                    let message = send.request.clone().into_generic();
                    let client_id = producer.client.client_id();
                    self.world
                        .sim_mut(cluster)
                        .submit_request(client_id, primary, message);
                }
                State::LoggingIn { target, at, .. } => {
                    if step >= *at + LOGIN_STEPS {
                        *at = step;
                        let target = *target;
                        let client = &producer.client;
                        self.world.sim_mut(cluster).submit_handshake(client, target);
                    }
                }
            }
        }
    }

    fn maybe_start_send(&mut self, index: usize) {
        let send_per_tick = self.send_per_tick;
        if !self.workload_prng.random_bool(send_per_tick) {
            return;
        }
        let cluster = self.producers[index].cluster;
        let partition = self
            .workload_prng
            .random_range(0..self.namespaces[cluster].len());
        if self.partitions[cluster][partition].started >= MAX_SENDS_PER_PARTITION {
            return;
        }
        self.partitions[cluster][partition].started += 1;
        self.report.sends[cluster] += 1;
        let send = self.new_send(index, partition);
        self.producers[index].state = State::Sending { send, target: None };
    }

    /// A send of a payload that no other send carries.
    fn new_send(&mut self, index: usize, partition: usize) -> Send {
        let producer = &mut self.producers[index];
        producer.next_sequence += 1;
        let payload = format!(
            "{}{}-c{}-{:06}{}",
            String::from_utf8_lossy(PAYLOAD_MARK),
            ORIGINS[producer.cluster],
            producer.client.client_id(),
            producer.next_sequence,
            String::from_utf8_lossy(PAYLOAD_END),
        );
        let namespace = self.namespaces[producer.cluster][partition];
        let request = producer
            .client
            .send_messages(namespace, &[bytes::Bytes::from(payload.clone())]);
        Send {
            request,
            payload,
            partition,
            attempts: 0,
            retries: BTreeMap::new(),
        }
    }

    fn on_reply(&mut self, cluster: usize, reply: &Message<iggy_binary_protocol::ReplyHeader>) {
        let header = reply.header();
        let Some(index) = self.producers.iter().position(|producer| {
            producer.cluster == cluster && producer.client.client_id() == header.client
        }) else {
            return;
        };
        if header.operation == Operation::Register {
            self.on_login(index, reply);
            return;
        }
        let State::Sending { send, .. } = &self.producers[index].state else {
            return;
        };
        if send.request.header().request != header.request {
            return;
        }
        let status = header.status;
        let retry = [
            IggyError::TransientNotAccepted.as_code(),
            IggyError::TransientNotCommitted.as_code(),
            IggyError::NotPrimary.as_code(),
        ];
        if status == 0 {
            let State::Sending { send, .. } =
                std::mem::replace(&mut self.producers[index].state, State::Idle)
            else {
                unreachable!("the producer is sending");
            };
            let client = self.producers[index].client.client_id();
            self.partitions[cluster][send.partition]
                .acknowledged
                .entry(client)
                .or_default()
                .push(send.payload);
            self.report.acknowledged[cluster] += 1;
        } else if retry.contains(&status) {
            if let State::Sending { send, target } = &mut self.producers[index].state {
                *send.retries.entry(status).or_default() += 1;
                // Again once the answer wait would have run out.
                *target = target.map(|(replica, _)| (replica, self.step));
            }
        } else {
            *self.report.refusals.entry(status).or_default() += 1;
            self.abandon(index);
        }
    }

    /// Give up the send of producer `index`, whose outcome is unknown, and
    /// leave the producer idle.
    fn abandon(&mut self, index: usize) {
        let state = std::mem::replace(&mut self.producers[index].state, State::Idle);
        if let State::Sending { send, .. } | State::LoggingIn { send, .. } = state {
            let cluster = self.producers[index].cluster;
            self.partitions[cluster][send.partition]
                .unknown
                .insert(send.payload);
            self.report.unknown[cluster] += 1;
        }
    }

    fn on_eviction(&mut self, cluster: usize, client: u128, from: u8) {
        self.report.evictions += 1;
        let Some(index) = self.producers.iter().position(|producer| {
            producer.cluster == cluster && producer.client.client_id() == client
        }) else {
            return;
        };
        let state = std::mem::replace(&mut self.producers[index].state, State::Idle);
        self.producers[index].state = match state {
            State::Sending { send, .. } => {
                let producer = &self.producers[index];
                self.world
                    .sim_mut(cluster)
                    .submit_handshake(&producer.client, from);
                State::LoggingIn {
                    send,
                    target: from,
                    at: self.step,
                }
            }
            other => other,
        };
    }

    /// Bind the session that a login answer carries. The same session keeps
    /// the send's request, which the cluster commits at most once. A new one
    /// leaves its outcome unknown, and a new send takes its place.
    fn on_login(&mut self, index: usize, reply: &Message<iggy_binary_protocol::ReplyHeader>) {
        let cluster = self.producers[index].cluster;
        if !matches!(self.producers[index].state, State::LoggingIn { .. }) {
            return;
        }
        let Some(session) = self.world.sim(cluster).handshake_session(reply) else {
            return;
        };
        let producer = &self.producers[index];
        let same = producer.client.session_id() == session;
        producer.client.bind_session(session);
        self.world
            .sim(cluster)
            .discover_partition_contexts(&producer.client);
        let State::LoggingIn { send, .. } =
            std::mem::replace(&mut self.producers[index].state, State::Idle)
        else {
            unreachable!("the producer is logging in");
        };
        if same {
            self.producers[index].state = State::Sending { send, target: None };
            return;
        }
        let partition = send.partition;
        self.producers[index].state = State::Sending { send, target: None };
        self.abandon(index);
        self.partitions[cluster][partition].started += 1;
        self.report.sends[cluster] += 1;
        let send = self.new_send(index, partition);
        self.producers[index].state = State::Sending { send, target: None };
    }

    fn inject_faults(&mut self) {
        let faults = self.faults;
        let replicas = self.world.sim(0).replica_count;
        let tolerated = usize::from(replicas.saturating_sub(1) / 2);
        for cluster in 0..2 {
            if self.dc_down == Some(cluster) {
                continue;
            }
            let crashed: Vec<u8> = (0..replicas)
                .filter(|replica| self.world.sim(cluster).is_crashed(*replica))
                .collect();
            if crashed.len() < tolerated && self.chance(faults.crash) {
                let live: Vec<u8> = (0..replicas)
                    .filter(|replica| !crashed.contains(replica))
                    .collect();
                let replica = live[self.fault_prng.random_range(0..live.len())];
                self.world.sim_mut(cluster).replica_crash(replica);
                self.report.crashes += 1;
            }
            if !crashed.is_empty() && self.chance(faults.restart) {
                let replica = crashed[self.fault_prng.random_range(0..crashed.len())];
                self.world.sim_mut(cluster).replica_restart(replica);
            }
        }
        if let Some(cluster) = self.dc_down {
            if self.chance(faults.dc_start) {
                for replica in 0..replicas {
                    if self.world.sim(cluster).is_crashed(replica) {
                        self.world.sim_mut(cluster).replica_restart(replica);
                    }
                }
                self.dc_down = None;
            }
        } else if self.chance(faults.dc_stop) {
            let cluster = self.fault_prng.random_range(0..2usize);
            for replica in 0..replicas {
                if !self.world.sim(cluster).is_crashed(replica) {
                    self.world.sim_mut(cluster).replica_crash(replica);
                }
            }
            self.dc_down = Some(cluster);
            self.report.dc_stops += 1;
        }
        for owner in 0..2 {
            let peer = ClusterId(u8::try_from(1 - owner).expect("a cluster index fits u8"));
            let bucket = self.world.bucket(owner).clone();
            if self.chance(faults.cut) {
                bucket.set_cut(peer, true);
                self.report.cuts += 1;
            } else if self.chance(faults.reconnect) {
                bucket.set_cut(peer, false);
            }
            if self.chance(faults.stall) {
                bucket.stall();
                self.report.stalls += 1;
            } else if self.chance(faults.stall_end) {
                bucket.heal();
            }
            if self.chance(faults.outage) {
                bucket.set_down(true);
                self.report.outages += 1;
            } else if self.chance(faults.outage_end) {
                bucket.set_down(false);
            }
        }
    }

    /// A draw from the fault stream, which draws nothing for a fault that is
    /// off, so a run without faults replays one with an unused stream.
    fn chance(&mut self, probability: f64) -> bool {
        probability > 0.0 && self.fault_prng.random_bool(probability.min(1.0))
    }

    /// Step with no new sends and no new faults until every send ended.
    fn drain(&mut self) {
        while self
            .producers
            .iter()
            .any(|producer| !matches!(producer.state, State::Idle))
        {
            assert!(
                self.report.drain_steps < DRAIN_STEPS,
                "the sends did not drain in {DRAIN_STEPS} steps once every fault healed: {}",
                self.pending_sends()
            );
            self.tick(false);
            self.report.drain_steps += 1;
        }
    }

    fn pending_sends(&self) -> String {
        self.producers
            .iter()
            .filter_map(|producer| match &producer.state {
                State::Idle => None,
                State::Sending { send, target } => Some(format!(
                    "{} client {} sending {} to {target:?}, {} attempts, retries asked {:?}",
                    ORIGINS[producer.cluster],
                    producer.client.client_id(),
                    send.payload,
                    send.attempts,
                    send.retries
                )),
                State::LoggingIn { send, target, .. } => Some(format!(
                    "{} client {} logging in to {target} for {}",
                    ORIGINS[producer.cluster],
                    producer.client.client_id(),
                    send.payload
                )),
            })
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// G6: step until every shadow serves its origin's whole log.
    fn converge(&mut self) {
        loop {
            let mut behind = Vec::new();
            for origin in 0..2 {
                for partition in 0..self.namespaces[origin].len() {
                    let log = self.origin_log(origin, partition);
                    let shadow = self.shadow_log(origin, partition);
                    if shadow.as_ref() != Some(&log) {
                        behind.push(format!(
                            "{} partition {partition}: origin {} messages, shadow {}",
                            ORIGINS[origin],
                            log.len(),
                            shadow.map_or_else(
                                || "none".to_owned(),
                                |shadow| describe_difference(&log, &shadow)
                            )
                        ));
                    }
                }
            }
            if behind.is_empty() {
                return;
            }
            assert!(
                self.report.converge_steps < CONVERGE_STEPS,
                "G6: the shadows did not catch up in {CONVERGE_STEPS} steps once every fault \
                 healed: {}",
                behind.join("; ")
            );
            for _ in 0..CONVERGE_POLL_STEPS {
                self.tick(false);
                self.report.converge_steps += 1;
            }
        }
    }

    /// The payloads of `partition` of `origin`'s geo topic, in log order, as
    /// its primary serves them.
    fn origin_log(&mut self, origin: usize, partition: usize) -> Vec<String> {
        let namespace = self.namespaces[origin][partition];
        let log = (0..FINAL_POLL_STEPS).find_map(|_| {
            let log = self.read_log(origin, namespace);
            if log.is_none() {
                self.tick(false);
            }
            log
        });
        log.unwrap_or_else(|| {
            panic!(
                "{} partition {partition} serves its log once every fault healed",
                ORIGINS[origin]
            )
        })
    }

    /// What the shadow of `partition` of `origin`'s geo topic serves, in log
    /// order, or `None` while the peer has no shadow that serves it.
    fn shadow_log(&mut self, origin: usize, partition: usize) -> Option<Vec<String>> {
        let peer = 1 - origin;
        let shadow =
            self.world
                .shadow_of(peer, origin, STREAM, TOPIC, u32::try_from(partition).ok()?)?;
        self.read_log(peer, shadow)
    }

    /// Every payload that the primary of `namespace` on `cluster` serves, a
    /// page at a time, or `None` while it has no primary or refuses a poll.
    /// Each send holds one message, so the next page starts at the number of
    /// messages read so far.
    fn read_log(&mut self, cluster: usize, namespace: IggyNamespace) -> Option<Vec<String>> {
        let mut log = Vec::new();
        loop {
            let primary = self.world.primary(cluster, namespace)?;
            let offset = u64::try_from(log.len()).ok()?;
            let bytes = self
                .world
                .poll(cluster, primary, namespace, offset, FINAL_POLL_STEPS)
                .ok()?;
            let page = payloads_in(&bytes);
            if page.is_empty() {
                return Some(log);
            }
            log.extend(page);
        }
    }

    /// G5: no replica moves the offset it committed for a shadow back.
    fn check_shadow_floors(&mut self) {
        for origin in 0..2 {
            let peer = 1 - origin;
            for partition in 0..self.namespaces[origin].len() {
                let Some(shadow) = u32::try_from(partition).ok().and_then(|partition| {
                    self.world.shadow_of(peer, origin, STREAM, TOPIC, partition)
                }) else {
                    continue;
                };
                for (replica, commit) in self.world.commits(peer, shadow) {
                    let floor = self
                        .shadow_floors
                        .entry((peer, shadow, replica))
                        .or_insert(commit);
                    assert!(
                        commit >= *floor,
                        "G5: replica {replica} of {} moved the shadow of {} partition \
                         {partition} back to offset {commit} from {floor} at step {}",
                        ORIGINS[peer],
                        ORIGINS[origin],
                        self.step
                    );
                    *floor = commit;
                }
            }
        }
    }

    fn start_shadow_polls(&mut self) {
        for origin in 0..2 {
            let peer = 1 - origin;
            for partition in 0..self.namespaces[origin].len() {
                let Some(shadow) = u32::try_from(partition).ok().and_then(|partition| {
                    self.world.shadow_of(peer, origin, STREAM, TOPIC, partition)
                }) else {
                    continue;
                };
                let Some(primary) = self.world.primary(peer, shadow) else {
                    continue;
                };
                let poll = self.world.start_poll(peer, primary, shadow, 0);
                self.shadow_polls.push((origin, partition, poll));
            }
        }
    }

    fn collect_shadow_polls(&mut self) {
        let mut pending = Vec::new();
        for (origin, partition, poll) in std::mem::take(&mut self.shadow_polls) {
            let polled = poll.borrow_mut().take();
            match polled {
                None => pending.push((origin, partition, poll)),
                Some(Ok(bytes)) => {
                    let read = payloads_in(&bytes);
                    if !read.is_empty() {
                        self.report.shadow_reads += 1;
                        self.shadow_reads.push((origin, partition, read));
                    }
                }
                // A shadow whose primary moved or stopped refuses the poll.
                Some(Err(_)) => {}
            }
        }
        self.shadow_polls = pending;
    }

    /// G3: no cluster wrote to a bucket that it does not own.
    fn check_foreign_writes(&self) {
        for (cluster, origin) in ORIGINS.iter().enumerate() {
            let foreign = self.world.bucket(cluster).foreign_writes();
            assert!(
                foreign.is_empty(),
                "G3: a cluster wrote to the bucket of {origin}: {foreign:?}"
            );
        }
    }

    /// G1, G2, G3 and G8 against each origin's final log.
    fn check_logs(&mut self) {
        self.check_foreign_writes();
        for origin in 0..2 {
            let in_bucket = self.payloads_in_bucket(origin);
            for partition in 0..self.namespaces[origin].len() {
                let log = self.origin_log(origin, partition);
                let sends = &self.partitions[origin][partition];
                let mut at: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
                for (position, payload) in log.iter().enumerate() {
                    at.entry(payload.as_str()).or_default().push(position);
                }
                let label = format!("{} partition {partition}", ORIGINS[origin]);
                for (client, acknowledged) in &sends.acknowledged {
                    let mut previous = None;
                    for payload in acknowledged {
                        let positions = at.get(payload.as_str()).map_or(&[][..], Vec::as_slice);
                        assert_eq!(
                            positions.len(),
                            1,
                            "G1: {label} holds the acknowledged {payload} {} times",
                            positions.len()
                        );
                        assert!(
                            previous < Some(positions[0]),
                            "G1: {label} holds client {client}'s {payload} before an \
                             earlier send of the client"
                        );
                        previous = Some(positions[0]);
                        assert!(
                            in_bucket.contains(payload),
                            "G8: the bucket of {} holds no {payload}",
                            ORIGINS[origin]
                        );
                    }
                }
                for (payload, positions) in &at {
                    let acknowledged = sends
                        .acknowledged
                        .values()
                        .any(|sent| sent.iter().any(|sent| sent == payload));
                    assert!(
                        acknowledged || sends.unknown.contains(*payload),
                        "G1: {label} holds {payload}, which no client sent there"
                    );
                    assert_eq!(
                        positions.len(),
                        1,
                        "G1: {label} holds {payload} {} times",
                        positions.len()
                    );
                }
                for (_, _, read) in
                    self.shadow_reads
                        .iter()
                        .filter(|(read_origin, read_partition, _)| {
                            *read_origin == origin && *read_partition == partition
                        })
                {
                    assert!(
                        log.starts_with(read),
                        "G2: a shadow of {label} served {} messages that are not a prefix of \
                         the origin's {}: {:?}",
                        read.len(),
                        log.len(),
                        read.iter().zip(&log).find(|(read, logged)| read != logged)
                    );
                }
            }
        }
    }

    /// Every payload that an object of `cluster`'s bucket holds.
    fn payloads_in_bucket(&self, cluster: usize) -> BTreeSet<String> {
        let bucket = self.world.bucket(cluster);
        bucket
            .keys()
            .iter()
            .filter_map(|key| bucket.object(key))
            .flat_map(|object| payloads_in(&object))
            .collect()
    }
}

/// How `shadow` differs from the origin's `log`.
fn describe_difference(log: &[String], shadow: &[String]) -> String {
    let first = log
        .iter()
        .zip(shadow)
        .position(|(logged, served)| logged != served)
        .unwrap_or_else(|| log.len().min(shadow.len()));
    let extra: Vec<&String> = shadow
        .iter()
        .filter(|served| !log.contains(served))
        .collect();
    let missing: Vec<&String> = log
        .iter()
        .filter(|logged| !shadow.contains(logged))
        .collect();
    format!(
        "{} messages, first differing at {first} (origin {:?}, shadow {:?}), not in the \
         origin {extra:?}, missing {missing:?}",
        shadow.len(),
        log.get(first),
        shadow.get(first)
    )
}

/// Every payload in `bytes`, in the order they appear.
fn payloads_in(bytes: &[u8]) -> Vec<String> {
    let mut payloads = Vec::new();
    let mut rest = bytes;
    while let Some(start) = find(rest, PAYLOAD_MARK) {
        let from = &rest[start..];
        let Some(end) = find(from, PAYLOAD_END) else {
            break;
        };
        let payload = &from[..end + PAYLOAD_END.len()];
        payloads.push(String::from_utf8_lossy(payload).into_owned());
        rest = &from[end + PAYLOAD_END.len()..];
    }
    payloads
}

fn find(bytes: &[u8], needle: &[u8]) -> Option<usize> {
    bytes
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::{GeoFaults, GeoFuzzOptions, payloads_in, run};
    use crate::packet::PacketSimulatorOptions;

    fn options(seed: u64, faults: GeoFaults) -> GeoFuzzOptions {
        GeoFuzzOptions {
            seed,
            ticks: 3_000,
            replicas: 3,
            clients: 2,
            partitions: 2,
            networks: [
                PacketSimulatorOptions::default(),
                PacketSimulatorOptions::default(),
            ],
            faults,
            send_per_tick: 0.05,
        }
    }

    #[test]
    fn given_bytes_with_payloads_when_scanning_should_find_each_in_order() {
        let bytes = b"\x00geo-payload-dc-a-c1-000001-end\x07\x07geo-payload-dc-a-c2-000001-end";

        assert_eq!(
            payloads_in(bytes),
            [
                "geo-payload-dc-a-c1-000001-end",
                "geo-payload-dc-a-c2-000001-end"
            ]
        );
    }

    #[test]
    fn given_no_faults_when_running_should_hold_every_invariant() {
        let report = run(&options(1, GeoFaults::default()));

        assert!(!report.is_vacuous(), "{report:?}");
        assert!(report.shadow_reads > 0, "{report:?}");
    }

    #[test]
    fn given_swarm_faults_when_running_the_same_seed_twice_should_replay() {
        let faults = GeoFaults::swarm(2);

        assert_eq!(run(&options(2, faults)), run(&options(2, faults)));
    }
}
