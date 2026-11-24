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

//! The listings of the consumer group positions that the geo peers publish,
//! which carry a group's progress from one datacenter to another.
//!
//! Every `[geo] positions_poll_interval`, shard 0 of a node that leads a
//! partition of a geo or shadow topic lists `meta/positions/` in the store of
//! each peer it reads. Each node of a peer stores one object there for each
//! log in which it leads a partition, see [`PositionsObject`], so a partition
//! may be in one object or in several. Shard 0 reads the objects whose etag
//! changed since its last read, and keeps the last one of each key, so an
//! unchanged object costs no read. It then takes, for each partition and
//! group, the highest position across the peer's nodes, maps each log to the
//! topic of this cluster that holds it, and hands every shard the positions
//! of the partitions it owns, see [`shard::geo_positions::PeerPositions`]. A
//! shard that owns none gets an empty hand-out, which still tells it that the
//! peer was read.
//!
//! The listings and reads go to the peer's own tier service in its
//! housekeeping class, as the listings of its manifests do, with the same
//! back-off for a peer whose listing failed. A peer whose origin retired is
//! listed no more.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use configs::geo::GeoConfig;
use consensus::MetadataHandle;
use iggy_common::IggyTimestamp;
use iggy_object_store::{
    GeoLog, GeoPosition, KeyLayout, MAX_POSITIONS_OBJECT_LEN, ObjectStore, PositionHolder,
    PositionsObject, TierExecutor, TierJobClass, TierJobRequest, TierScope, TierScopeToken,
};
use message_bus::MessageBus;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::StreamsInner;
use server_common::sharding::IggyNamespace;
use shard::geo_positions::{GeoPositionSettings, PeerPositions, local_logs};
use shard::shards_table::PartitionShards;
use tracing::{debug, warn};

use crate::peer_manifests::{NodePlace, backoff, list_all, micros};
use crate::shell::ServerShard;

/// The listings of the geo peers' consumer group positions that shard 0
/// runs.
pub struct PeerPositionLister {
    /// In microseconds. `None` on a node without geo peers.
    interval: Option<u64>,
    origin: String,
    state: Rc<RefCell<State>>,
}

#[derive(Default)]
struct State {
    /// The task in flight. Dropping the scope cancels it.
    running: Option<TierScope>,
    /// When the next pass may start, in microseconds since the Unix epoch.
    next_pass_at: u64,
    /// Per origin, the listings in a row that failed and when the next one
    /// may start.
    backoff: BTreeMap<String, (u32, u64)>,
    /// Per origin, the last object read under each key, with its etag.
    objects: BTreeMap<String, Objects>,
}

type Objects = BTreeMap<String, (String, PositionsObject)>;

/// The positions of one partition of a log, by holder.
type PartitionPositions = BTreeMap<PositionHolder, GeoPosition>;

/// What every shard of a node needs to carry its groups' positions, `None`
/// on a node without an origin.
#[must_use]
pub fn settings(config: &GeoConfig) -> Option<GeoPositionSettings> {
    (!config.origin.is_empty()).then(|| GeoPositionSettings {
        origin: config.origin.clone(),
        peers: config
            .peers
            .iter()
            .map(|peer| peer.origin.clone())
            .collect(),
        publish_interval: config.positions_publish_interval.get_duration(),
        offset_upload_interval: (!config.consumer_offset_upload_interval.is_zero())
            .then(|| config.consumer_offset_upload_interval.get_duration()),
        resume_timeout: config.positions_resume_timeout.get_duration(),
    })
}

impl PeerPositionLister {
    #[must_use]
    pub fn new(config: &GeoConfig) -> Self {
        Self {
            interval: (!config.peers.is_empty() && !config.origin.is_empty())
                .then(|| micros(config.positions_poll_interval.get_duration())),
            origin: config.origin.clone(),
            state: Rc::default(),
        }
    }

    /// Start a pass of listings in a detached task, on shard 0, at most once
    /// an interval. A pass while that task runs starts nothing.
    pub fn pass(&self, shard: &Rc<ServerShard>, now: IggyTimestamp, place: NodePlace) {
        let Some(interval) = self.interval else {
            return;
        };
        if shard.id != 0 {
            return;
        }
        let mut state = self.state.borrow_mut();
        if state.running.is_some() || now.as_micros() < state.next_pass_at {
            return;
        }
        state.next_pass_at = now.as_micros().saturating_add(interval);
        let (leads, retired) = shard.plane.metadata().mux_stm.streams().read(|inner| {
            (
                leads_geo_partition(inner, &self.origin, place),
                inner.retired_origins.clone(),
            )
        });
        if !leads {
            return;
        }
        let peers = shard.peer_stores();
        let scope = TierScope::new();
        let mut reads = Vec::new();
        for peer in peers.iter() {
            let origin = peer.origin().to_owned();
            if retired.contains_key(&origin)
                || state
                    .backoff
                    .get(&origin)
                    .is_some_and(|(_, next_at)| now.as_micros() < *next_at)
            {
                continue;
            }
            let Some(service) = peer.service() else {
                continue;
            };
            let objects = state.objects.remove(&origin).unwrap_or_default();
            reads.push(PeerRead {
                origin,
                layout: peer.layout().clone(),
                service: service.clone(),
                token: scope.token(),
                objects,
            });
        }
        if reads.is_empty() {
            return;
        }
        state.running = Some(scope);
        drop(state);
        let task_shard = Rc::clone(shard);
        let state = Rc::clone(&self.state);
        let origin = self.origin.clone();
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        shard.bus.spawn(async move {
            let shard = task_shard;
            let mut failed = BTreeMap::new();
            let mut read = Vec::new();
            for peer in reads {
                let peer_origin = peer.origin.clone();
                let (objects, listed) = peer.read(&shard).await;
                failed.insert(peer_origin.clone(), !listed);
                read.push((peer_origin, objects, listed));
            }
            let logs = shard
                .plane
                .metadata()
                .mux_stm
                .streams()
                .read(|inner| local_logs(inner, &origin));
            let mut state_ref = state.borrow_mut();
            state_ref.settle(failed, now, interval);
            for (peer_origin, objects, listed) in read {
                if listed {
                    hand_out(
                        &shard,
                        &peer_origin,
                        &objects,
                        &logs,
                        place.partition_shards,
                    );
                }
                state_ref.objects.insert(peer_origin, objects);
            }
        });
    }

    /// Cancel the task in flight.
    pub fn stop(&self) {
        self.state.borrow_mut().running = None;
    }
}

/// One peer of a pass: where to read it, and what the last pass read.
struct PeerRead<E> {
    origin: String,
    layout: KeyLayout,
    service: E,
    token: TierScopeToken,
    objects: Objects,
}

impl<E: TierExecutor + Clone> PeerRead<E> {
    /// List the peer's positions and read the objects that changed. Returns
    /// the objects it holds after the pass, and whether the listing
    /// succeeded. An object whose read failed keeps its last copy.
    #[allow(
        clippy::future_not_send,
        reason = "the task runs on shard 0, which owns the peer's tier service"
    )]
    async fn read(self, shard: &ServerShard) -> (Objects, bool) {
        let Self {
            origin,
            layout,
            service,
            token,
            mut objects,
        } = self;
        let listed = list_all(service.clone(), token.clone(), layout.positions_prefix()).await;
        shard
            .metrics()
            .record_peer_position_request(&origin, "list", listed.is_ok());
        let keys = match listed {
            Ok(keys) => keys,
            Err(error) => {
                debug!(
                    origin,
                    error, "listing the consumer group positions of a geo peer failed"
                );
                return (objects, false);
            }
        };
        let mut kept = Objects::new();
        for (key, etag) in keys {
            let Some(expected) = layout.parse_positions_key(&key) else {
                continue;
            };
            if let Some((known, object)) = objects.remove(&key)
                && known == etag
            {
                kept.insert(key, (known, object));
                continue;
            }
            let request = TierJobRequest::new(TierJobClass::Housekeeping, token.clone());
            let read_key = key.clone();
            let max_len = MAX_POSITIONS_OBJECT_LEN as u64;
            let got = service
                .submit(request, move |store| async move {
                    ObjectStore::get_range(&*store, &read_key, 0..max_len + 1).await
                })
                .await;
            let decoded = match got {
                Ok(Ok(range)) if range.size > max_len => Err(format!(
                    "the object is {} bytes, more than {MAX_POSITIONS_OBJECT_LEN}",
                    range.size
                )),
                Ok(Ok(range)) => PositionsObject::decode_under(&expected, &range.bytes)
                    .map(|object| (range.etag, object))
                    .map_err(|error| error.to_string()),
                Ok(Err(error)) => Err(error.to_string()),
                Err(cancelled) => Err(cancelled.to_string()),
            };
            shard
                .metrics()
                .record_peer_position_request(&origin, "get", decoded.is_ok());
            match decoded {
                Ok((stored_etag, object)) => {
                    let etag = if stored_etag.is_empty() {
                        etag
                    } else {
                        stored_etag
                    };
                    kept.insert(key, (etag, object));
                }
                Err(error) => {
                    debug!(
                        origin,
                        key, error, "reading consumer group positions of a geo peer failed"
                    );
                }
            }
        }
        (kept, true)
    }
}

impl State {
    /// End a pass: start the wait of each peer whose listing failed, and end
    /// it for each one whose listing succeeded.
    fn settle(&mut self, failed: BTreeMap<String, bool>, now: IggyTimestamp, interval: u64) {
        for (origin, failed) in failed {
            if !failed {
                self.backoff.remove(&origin);
                continue;
            }
            let (failures, _) = self.backoff.get(&origin).copied().unwrap_or_default();
            let failures = failures.saturating_add(1);
            if failures == 1 {
                warn!(
                    origin,
                    "listing the consumer group positions of a geo peer failed; groups keep the positions they hold"
                );
            }
            let next_at = now.as_micros().saturating_add(backoff(interval, failures));
            self.backoff.insert(origin, (failures, next_at));
        }
        self.running = None;
    }
}

/// The highest position of each holder across the objects of a peer's
/// nodes, by log and partition id.
fn merged(objects: &Objects) -> HashMap<&GeoLog, BTreeMap<u32, PartitionPositions>> {
    let mut merged: HashMap<&GeoLog, BTreeMap<u32, PartitionPositions>> = HashMap::new();
    for (_, object) in objects.values() {
        let log = merged.entry(&object.key.log).or_default();
        for (partition_id, holders) in &object.partitions {
            let partition = log.entry(*partition_id).or_default();
            for (holder, position) in holders {
                partition
                    .entry(holder.clone())
                    .and_modify(|held| *held = (*held).max(*position))
                    .or_insert(*position);
            }
        }
    }
    merged
}

/// Hand every shard the positions of `origin` for the partitions it owns.
fn hand_out(
    shard: &ServerShard,
    origin: &str,
    objects: &Objects,
    logs: &[shard::geo_positions::LocalLog],
    partition_shards: PartitionShards,
) {
    let merged = merged(objects);
    let mut by_shard: Vec<Vec<(IggyNamespace, u64, PartitionPositions)>> =
        vec![Vec::new(); usize::from(partition_shards.shard_count())];
    for local in logs {
        let Some(partitions) = merged.get(&local.log) else {
            continue;
        };
        for (partition_id, namespace, created_revision) in &local.partitions {
            let Some(positions) = partitions.get(partition_id) else {
                continue;
            };
            let owner = partition_shards.shard_for(namespace);
            if let Some(owned) = by_shard.get_mut(usize::from(owner)) {
                owned.push((*namespace, *created_revision, positions.clone()));
            }
        }
    }
    for (owner, partitions) in by_shard.into_iter().enumerate() {
        let Ok(owner) = u16::try_from(owner) else {
            continue;
        };
        shard.send_peer_positions(
            owner,
            PeerPositions {
                origin: origin.to_owned(),
                partitions,
            },
        );
    }
}

/// Whether this node leads a partition of a geo or shadow topic, as the
/// partition's last reported view names it.
fn leads_geo_partition(inner: &StreamsInner, origin: &str, place: NodePlace) -> bool {
    local_logs(inner, origin).iter().any(|local| {
        let Some(topic) = inner
            .items
            .get(local.stream_id)
            .and_then(|stream| stream.topics.get(local.topic_id))
        else {
            return false;
        };
        topic.partitions.iter().any(|partition| {
            let Some(namespace) =
                IggyNamespace::try_new(local.stream_id, local.topic_id, partition.id)
            else {
                return false;
            };
            let view = inner
                .stats_registry
                .directory()
                .get(namespace, partition.created_revision)
                .map_or(partition.created_view, |status| status.view);
            partition.primary_node(view, place.replica_count) == Some(place.self_replica_id)
        })
    })
}

#[cfg(test)]
mod tests {
    use iggy_object_store::{PositionValue, PositionsKey};

    use super::*;

    fn object(replica: u8, offset: u64, reset: u64) -> PositionsObject {
        led_object(replica, &[(0, offset, reset)])
    }

    /// The object of node `replica`, which leads the partitions of `led`,
    /// each with the group at `(partition id, offset, reset)`.
    fn led_object(replica: u8, led: &[(u32, u64, u64)]) -> PositionsObject {
        PositionsObject {
            key: PositionsKey {
                log: GeoLog {
                    origin: "dc-a".to_owned(),
                    stream_id: 1,
                    topic_id: 2,
                    created_revision: 3,
                },
                replica,
            },
            published_at: 0,
            partitions: led
                .iter()
                .map(|(partition_id, offset, reset)| {
                    (
                        *partition_id,
                        BTreeMap::from([(
                            PositionHolder::Group("billing".to_owned()),
                            GeoPosition {
                                reset: *reset,
                                value: PositionValue::Offset(*offset),
                            },
                        )]),
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn given_objects_of_several_nodes_when_merging_should_take_the_highest_position_per_group() {
        let objects = Objects::from([
            ("a".to_owned(), ("1".to_owned(), object(0, 90, 0))),
            ("b".to_owned(), ("2".to_owned(), object(1, 120, 0))),
            ("c".to_owned(), ("3".to_owned(), object(2, 10, 1))),
        ]);

        let merged = merged(&objects);

        let log = &objects["a"].1.key.log;
        assert_eq!(
            merged[log][&0][&PositionHolder::Group("billing".to_owned())],
            GeoPosition {
                reset: 1,
                value: PositionValue::Offset(10)
            },
            "a seek backwards on one node wins over the higher offsets of the others"
        );
    }

    #[test]
    fn given_objects_that_each_hold_the_partitions_their_node_leads_when_merging_should_take_every_partition()
     {
        let objects = Objects::from([
            (
                "a".to_owned(),
                ("1".to_owned(), led_object(0, &[(0, 40, 0)])),
            ),
            (
                "b".to_owned(),
                ("2".to_owned(), led_object(1, &[(1, 50, 0), (0, 30, 0)])),
            ),
        ]);

        let merged = merged(&objects);

        let log = &objects["a"].1.key.log;
        let offsets: Vec<(u32, PositionValue)> = merged[log]
            .iter()
            .map(|(partition_id, groups)| {
                (
                    *partition_id,
                    groups[&PositionHolder::Group("billing".to_owned())].value,
                )
            })
            .collect();
        assert_eq!(
            offsets,
            [
                (0, PositionValue::Offset(40)),
                (1, PositionValue::Offset(50))
            ],
            "a former leader's older position of partition 0 does not win, and partition 1 comes from its only object"
        );
    }
}
