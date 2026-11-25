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

//! Simulated clusters whose geo topics replicate through their buckets.
//!
//! Each cluster is a [`Simulator`] of its own, with its own executor, clock,
//! network and seeds. No packet crosses clusters: they share only the
//! buckets. Each cluster owns one bucket and reads every other one as a
//! peer. One world step steps every cluster once, in an order that the
//! world's PRNG draws, so a run replays from its seed.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use consensus::{Consensus, MetadataHandle, PartitionsHandle};
use iggy_binary_protocol::{Operation, ReplyHeader, RoutedRequestHeader, WireIdentifier};
use iggy_common::{IggyError, PollingStrategy};
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::shadow::shadow_stream_name;
use partitions::{PollingArgs, PollingConsumer};
use rand::RngExt;
use rand_xoshiro::Xoshiro256PlusPlus;
use rand_xoshiro::rand_core::SeedableRng;
use server_common::Message;
use server_common::sharding::IggyNamespace;

use crate::Simulator;
use crate::bucket::{BucketFaults, ClusterId, SimBucket};
use crate::client::SimClient;
use crate::geo::SimGeo;
use crate::packet::PacketSimulatorOptions;

/// The origin names of the clusters, by cluster index.
pub const ORIGINS: [&str; 2] = ["dc-a", "dc-b"];

/// The most messages one poll of [`GeoWorld::start_poll`] returns.
pub const POLL_COUNT: u32 = 1000;

/// Steps one attempt of a metadata operation waits for its answer.
const ANSWER_STEPS: usize = 300;
/// Steps a cluster gets to report that every node can tier topics, or to
/// apply a metadata operation everywhere.
const SETTLE_STEPS: usize = 3_000;

/// How a world is built.
#[derive(Debug, Clone)]
pub struct GeoWorldOptions {
    pub seed: u64,
    /// One or two.
    pub clusters: u8,
    /// Replicas per cluster.
    pub replicas: u8,
    /// The clients of each cluster. The clusters share no network, so they
    /// can use the same ids.
    pub clients: Vec<u128>,
    /// The faults of every bucket.
    pub faults: BucketFaults,
    /// How long a peer keeps a deleted topic's shadow after the origin
    /// tombstoned it.
    pub tombstone_grace: Duration,
    /// How far each cluster's wall clock is from the shared one, in
    /// microseconds, by cluster index. A peer compares the origin's
    /// `deleted_at` with its own clock, so a skew moves when it retires a
    /// shadow.
    pub clock_skews_micros: [i64; 2],
    /// The network of each cluster, by cluster index. The world fills in the
    /// node count, the client count and the seed.
    pub networks: [PacketSimulatorOptions; 2],
}

impl GeoWorldOptions {
    /// `clusters` clusters of `replicas` replicas and `clients` clients each,
    /// on perfect networks, with buckets that never fail and clocks that agree.
    #[must_use]
    pub fn new(seed: u64, clusters: u8, replicas: u8, clients: &[u128]) -> Self {
        Self {
            seed,
            clusters,
            replicas,
            clients: clients.to_vec(),
            faults: BucketFaults::default(),
            tombstone_grace: configs::server::ServerConfig::default()
                .geo
                .tombstone_grace
                .get_duration(),
            clock_skews_micros: [0, 0],
            networks: [
                PacketSimulatorOptions::default(),
                PacketSimulatorOptions::default(),
            ],
        }
    }
}

/// Per-stream seeds of a world, drawn from its seed. Each cluster keeps its
/// own [`crate::seeds::SimSeeds`], derived from its seed here.
#[derive(Debug, Clone, Copy)]
pub struct GeoSeeds {
    /// The order in which the clusters step.
    pub world: u64,
    pub clusters: [u64; 2],
    /// The faults of each cluster's bucket.
    pub stores: [u64; 2],
    /// The draw of a run's fault parameters, apart from the faults it then
    /// injects, as [`crate::seeds::SimSeeds::swarm`] is.
    pub swarm: u64,
    /// The faults a run injects: crashes, DC stops, cut links and stalls.
    pub faults: u64,
    /// The sends of a run.
    pub workload: u64,
}

impl GeoSeeds {
    /// APPEND fields, never insert, as [`crate::seeds::SimSeeds::derive`]
    /// says.
    #[must_use]
    pub fn derive(seed: u64) -> Self {
        let mut parent = Xoshiro256PlusPlus::seed_from_u64(seed);
        Self {
            world: parent.random(),
            clusters: [parent.random(), parent.random()],
            stores: [parent.random(), parent.random()],
            swarm: parent.random(),
            faults: parent.random(),
            workload: parent.random(),
        }
    }
}

/// Where a poll that [`GeoWorld::start_poll`] started puts what it read: every
/// byte of the messages, or the error of the poll.
pub type PollOutput = Rc<RefCell<Option<Result<Vec<u8>, IggyError>>>>;

/// One or two clusters with geo topics.
pub struct GeoWorld {
    clusters: Vec<Simulator>,
    buckets: Vec<SimBucket>,
    prng: Xoshiro256PlusPlus,
}

impl GeoWorld {
    /// # Panics
    /// When `options` asks for no cluster or for more than two.
    #[must_use]
    pub fn new(options: &GeoWorldOptions) -> Self {
        let count = usize::from(options.clusters);
        assert!((1..=ORIGINS.len()).contains(&count), "one or two clusters");
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let seeds = GeoSeeds::derive(options.seed);
        let buckets: Vec<SimBucket> = (0..count)
            .map(|index| SimBucket::new(cluster_id(index), seeds.stores[index], options.faults))
            .collect();
        let clusters = (0..count)
            .map(|index| {
                // The dispatch shell, because a geo partition takes only the
                // sends that the server's handler hands to the upload path.
                let mut sim = Simulator::with_shards_shell(
                    usize::from(options.replicas),
                    1,
                    options.clients.iter().copied(),
                    PacketSimulatorOptions {
                        node_count: options.replicas,
                        client_count: u8::try_from(options.clients.len())
                            .expect("client count fits u8"),
                        seed: seeds.clusters[index],
                        ..options.networks[index].clone()
                    },
                );
                let mut geo =
                    SimGeo::new(ORIGINS[index], cluster_id(index), buckets[index].clone())
                        .with_tombstone_grace(options.tombstone_grace);
                for (peer, bucket) in buckets.iter().enumerate() {
                    if peer != index {
                        geo = geo.with_peer(ORIGINS[peer], bucket.clone());
                    }
                }
                sim.set_clock_skew_micros(options.clock_skews_micros[index]);
                sim.enable_geo(geo);
                sim
            })
            .collect();
        Self {
            clusters,
            buckets,
            prng: Xoshiro256PlusPlus::seed_from_u64(seeds.world),
        }
    }

    #[must_use]
    pub const fn cluster_count(&self) -> usize {
        self.clusters.len()
    }

    #[must_use]
    pub fn sim(&self, cluster: usize) -> &Simulator {
        &self.clusters[cluster]
    }

    pub fn sim_mut(&mut self, cluster: usize) -> &mut Simulator {
        &mut self.clusters[cluster]
    }

    #[must_use]
    pub fn bucket(&self, cluster: usize) -> &SimBucket {
        &self.buckets[cluster]
    }

    /// Step every cluster once, in a drawn order, and return each cluster's
    /// replies. Then each cluster learns which cluster owns each peer bucket,
    /// as an identity check that reads it would.
    pub fn step(&mut self) -> Vec<Vec<Message<ReplyHeader>>> {
        let count = self.clusters.len();
        let first = if count > 1 {
            self.prng.random_range(0..count)
        } else {
            0
        };
        let mut replies = vec![Vec::new(); count];
        for offset in 0..count {
            let index = (first + offset) % count;
            replies[index] = self.clusters[index].step();
        }
        self.verify_peers();
        replies
    }

    /// End every fault: start every replica that is down, heal every
    /// network, and give every bucket back to every cluster, without faults.
    pub fn heal(&mut self) {
        for sim in &mut self.clusters {
            for replica in 0..sim.replica_count {
                if sim.is_crashed(replica) {
                    sim.replica_restart(replica);
                }
            }
            sim.network.heal();
        }
        for bucket in &self.buckets {
            bucket.set_faults(BucketFaults::default());
            bucket.set_down(false);
            bucket.heal();
            for cluster in 0..self.clusters.len() {
                bucket.set_cut(cluster_id(cluster), false);
            }
        }
    }

    /// Log `client` in to replica 0 of `cluster`, as
    /// [`Simulator::shell_login`] does, while every cluster steps.
    ///
    /// # Panics
    /// As [`Self::shell_login_via`].
    pub fn shell_login(&mut self, cluster: usize, client: &SimClient) {
        self.shell_login_via(cluster, client, 0);
    }

    /// Log `client` in to `target` of `cluster`, as
    /// [`Simulator::shell_login_via`] does, but step the world and not the
    /// one cluster. Every cluster's clock then moves with the login, so the
    /// skews between the clocks stay the ones the world set.
    ///
    /// # Panics
    /// When no answer with a session arrives within the setup steps of a
    /// login.
    pub fn shell_login_via(&mut self, cluster: usize, client: &SimClient, target: u8) {
        self.sim(cluster).admit_shell_client(client);
        let replicas = self.sim(cluster).replica_count.max(1);
        let mut target = target;
        for step in 0..crate::SETUP_TOTAL_STEPS {
            if step % crate::SETUP_RETRY_STEPS == 0 {
                self.sim_mut(cluster).submit_handshake(client, target);
                target = (target + 1) % replicas;
            }
            let replies = self.step();
            let sim = self.sim(cluster);
            let session = replies[cluster].iter().find_map(|reply| {
                let header = reply.header();
                (header.client == client.client_id() && header.operation == Operation::Register)
                    .then(|| sim.handshake_session(reply))
                    .flatten()
            });
            if let Some(session) = session {
                client.bind_session(session);
                sim.discover_partition_contexts(client);
                return;
            }
        }
        panic!(
            "client {} logs in to cluster {cluster} within {} steps",
            client.client_id(),
            crate::SETUP_TOTAL_STEPS
        );
    }

    /// The namespace of `partition` of `topic` in `stream`, once `cluster`
    /// committed it.
    #[must_use]
    pub fn namespace_of(
        &self,
        cluster: usize,
        stream: &str,
        topic: &str,
        partition: u32,
    ) -> Option<IggyNamespace> {
        let sim = self.sim(cluster);
        let replica = (0..sim.replica_count).find(|replica| !sim.is_crashed(*replica))?;
        sim.replicas[usize::from(replica)].shards[0]
            .plane
            .metadata()
            .mux_stm
            .streams()
            .namespace_from_partition(
                &WireIdentifier::named(stream).ok()?,
                &WireIdentifier::named(topic).ok()?,
                partition,
            )
    }

    /// The namespace of the shadow on `peer` of `partition` of the geo topic
    /// `topic` in `stream` of `origin`, once `peer` committed it.
    #[must_use]
    pub fn shadow_of(
        &self,
        peer: usize,
        origin: usize,
        stream: &str,
        topic: &str,
        partition: u32,
    ) -> Option<IggyNamespace> {
        let shadow_stream = shadow_stream_name(ORIGINS[origin], stream)?;
        self.namespace_of(peer, &shadow_stream, topic, partition)
    }

    /// The live replica of `cluster` that leads `namespace` in normal status,
    /// if one does.
    #[must_use]
    pub fn primary(&self, cluster: usize, namespace: IggyNamespace) -> Option<u8> {
        let sim = self.sim(cluster);
        (0..sim.replica_count).find(|replica| {
            !sim.is_crashed(*replica)
                && sim.replicas[usize::from(*replica)]
                    .partition_shard(namespace)
                    .plane
                    .partitions()
                    .peek_by_ns(&namespace)
                    .is_some_and(|partition| {
                        let consensus = partition.consensus();
                        consensus.is_primary() && consensus.is_normal()
                    })
        })
    }

    /// The offset that each live replica of `cluster` committed for
    /// `namespace`.
    #[must_use]
    pub fn commits(&self, cluster: usize, namespace: IggyNamespace) -> Vec<(u8, u64)> {
        let sim = self.sim(cluster);
        (0..sim.replica_count)
            .filter(|replica| !sim.is_crashed(*replica))
            .filter_map(|replica| {
                sim.offsets(usize::from(replica), namespace)
                    .map(|offsets| (replica, offsets.commit_offset))
            })
            .collect()
    }

    /// Start a poll of up to [`POLL_COUNT`] messages of `namespace` from
    /// `offset` on `replica` of `cluster`. Its output is there once a later
    /// step ends it.
    pub fn start_poll(
        &mut self,
        cluster: usize,
        replica: u8,
        namespace: IggyNamespace,
        offset: u64,
    ) -> PollOutput {
        let args = PollingArgs::new(PollingStrategy::offset(offset), POLL_COUNT, false);
        let poll = self.sim(cluster).poll_messages(
            usize::from(replica),
            namespace,
            PollingConsumer::Consumer(7, 0),
            &args,
        );
        let output = PollOutput::default();
        let ready = Rc::clone(&output);
        self.sim_mut(cluster).executor.spawn(async move {
            let polled = poll.await.map(|fragments| {
                fragments
                    .iter()
                    .flat_map(|fragment| fragment.as_slice().iter().copied())
                    .collect()
            });
            *ready.borrow_mut() = Some(polled);
        });
        output
    }

    /// [`Self::start_poll`], stepping until it ends.
    ///
    /// # Errors
    /// The error of the poll, such as a refusal of a replica that is not the
    /// partition's primary.
    ///
    /// # Panics
    /// When the poll does not end within `steps` steps.
    pub fn poll(
        &mut self,
        cluster: usize,
        replica: u8,
        namespace: IggyNamespace,
        offset: u64,
        steps: usize,
    ) -> Result<Vec<u8>, IggyError> {
        let output = self.start_poll(cluster, replica, namespace, offset);
        for _ in 0..steps {
            if output.borrow().is_some() {
                break;
            }
            self.step();
        }
        output.borrow_mut().take().expect("the poll ends")
    }

    /// Submit `request` to replica 0 of `cluster` until it is answered with
    /// anything but a refusal to take it now, then step until every live
    /// replica applied it.
    ///
    /// # Panics
    /// When the operation fails or does not apply everywhere in time.
    pub fn complete(
        &mut self,
        cluster: usize,
        client: &SimClient,
        request: &Message<RoutedRequestHeader>,
    ) {
        let request_id = request.header().request;
        let mut answered: Option<Message<ReplyHeader>> = None;
        for _ in 0..SETTLE_STEPS / ANSWER_STEPS {
            self.sim_mut(cluster).submit_request(
                client.client_id(),
                0,
                request.clone().into_generic(),
            );
            answered = None;
            for _ in 0..ANSWER_STEPS {
                answered = self.step()[cluster]
                    .iter()
                    .find(|reply| reply.header().request == request_id)
                    .map(Message::deep_copy);
                if answered.is_some() {
                    break;
                }
            }
            let code = answered
                .as_ref()
                .and_then(|reply| metadata::stm::result::result_code(reply.body()));
            if answered.is_some() && code != Some(IggyError::TransientNotAccepted.as_code()) {
                break;
            }
        }
        let reply = answered.expect("the metadata operation is answered");
        let code = metadata::stm::result::result_code(reply.body());
        // A delete answers before the fences of its partitions install.
        assert!(
            code == Some(0) || code == Some(IggyError::TransientNotCommitted.as_code()),
            "the metadata operation succeeds: {code:?}"
        );
        let op = reply.header().op;
        let applied = (0..SETTLE_STEPS).any(|_| {
            self.step();
            let sim = self.sim(cluster);
            (0..sim.replica_count)
                .filter(|replica| !sim.is_crashed(*replica))
                .all(|replica| {
                    let metadata = sim.replicas[usize::from(replica)].shards[0]
                        .plane
                        .metadata();
                    metadata.applied_frontier().get() >= op
                        && !metadata.mux_stm.streams().lifecycle_pending(op)
                })
        });
        assert!(applied, "metadata op {op} applies on every live replica");
    }

    /// Create `stream` and the geo topic `topic` of `partitions` partitions
    /// in it on `cluster`, once its metadata primary admits a geo topic, and
    /// return the partitions. `client` then knows their contexts.
    ///
    /// # Panics
    /// When the cluster does not get ready or refuses the topic.
    pub fn create_geo_topic(
        &mut self,
        cluster: usize,
        client: &SimClient,
        stream: &str,
        topic: &str,
        partitions: u32,
    ) -> Vec<IggyNamespace> {
        self.complete(cluster, client, &client.create_stream(stream));
        let ready = (0..SETTLE_STEPS).any(|_| {
            self.step();
            self.sim(cluster).replicas[0].shards[0]
                .plane
                .metadata()
                .tiered_storage_cluster_ready()
        });
        assert!(ready, "every node reports that it can tier topics");
        self.complete(
            cluster,
            client,
            &client.create_geo_topic(stream, topic, partitions),
        );
        self.sim(cluster).discover_partition_contexts(client);
        (0..partitions)
            .map(|partition| {
                self.namespace_of(cluster, stream, topic, partition)
                    .expect("the geo topic is committed")
            })
            .collect()
    }

    fn verify_peers(&mut self) {
        let owners: Vec<Option<u128>> = self.clusters.iter().map(Simulator::cluster_uid).collect();
        for cluster in &mut self.clusters {
            cluster.verify_geo_peers(|origin| {
                ORIGINS
                    .iter()
                    .position(|candidate| *candidate == origin)
                    .and_then(|index| owners.get(index).copied().flatten())
            });
        }
    }
}

fn cluster_id(index: usize) -> ClusterId {
    ClusterId(u8::try_from(index).expect("cluster index fits u8"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::GeoSeeds;

    #[test]
    fn given_a_seed_when_deriving_should_give_each_stream_its_own_seed() {
        let seeds = GeoSeeds::derive(0xDEAD_BEEF);
        let all = [
            seeds.world,
            seeds.clusters[0],
            seeds.clusters[1],
            seeds.stores[0],
            seeds.stores[1],
            seeds.swarm,
            seeds.faults,
            seeds.workload,
        ];

        assert_eq!(all.iter().copied().collect::<HashSet<_>>().len(), all.len());
        assert_eq!(GeoSeeds::derive(7).world, GeoSeeds::derive(7).world);
    }
}
