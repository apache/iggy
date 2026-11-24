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

//! The listings of the manifests that the geo peers publish, which keep the
//! shadow partitions of this node up to date.
//!
//! Every `[geo] manifest_poll_interval`, shard 0 of each node finds the
//! shadow partitions whose primary this node is, as their last reported view
//! names it. For each peer stream that one of them serves, it lists
//! `meta/{stream_id}/` in the peer's store. That prefix holds the manifests
//! of the stream's partitions and nothing else: the fresh objects and topic
//! objects of the peer sit under other words of `meta/`. Shard 0 then hands
//! each shard the listed manifests of the shadow partitions it owns, with
//! their etags. The shard reads and commits the ones that changed, see
//! `shard::ListedPeerManifest`.
//!
//! A peer whose origin retired is listed no more.
//!
//! A listing of a peer that fails is tried again after a wait that doubles,
//! up to [`LIST_BACKOFF_MAX`]. The listings and reads go to the peer's own
//! tier service in its housekeeping class, so neither a slow peer nor a busy
//! one takes a job slot of this cluster's store, or one that a poll of a
//! shadow partition waits for.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::time::Duration;

use configs::geo::GeoConfig;
use consensus::MetadataHandle;
use iggy_common::IggyTimestamp;
use iggy_object_store::{
    KeyLayout, ListRequest, ObjectPrefix, ObjectStore, TierExecutor, TierJobClass, TierJobRequest,
    TierScope, TierScopeToken,
};
use message_bus::MessageBus;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::StreamsInner;
use server_common::sharding::IggyNamespace;
use shard::ListedPeerManifest;
use shard::shards_table::PartitionShards;
use tracing::{debug, warn};

use crate::shell::ServerShard;

/// The longest wait before a listing of a peer that failed is tried again.
pub const LIST_BACKOFF_MAX: Duration = Duration::from_mins(1);

/// The listings of the geo peers' manifests that shard 0 runs.
pub struct PeerManifestLister {
    /// In microseconds. `None` on a node without geo peers.
    interval: Option<u64>,
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
}

/// The shadow partitions of one peer stream whose primary this node is,
/// by the incarnation of the peer's partition.
struct Wanted {
    origin: String,
    peer_stream_id: u64,
    partitions: HashMap<ObjectPrefix, (IggyNamespace, u64)>,
}

/// Where this node is in its cluster.
#[derive(Clone, Copy)]
pub struct NodePlace {
    pub self_replica_id: u8,
    pub replica_count: u8,
    pub partition_shards: PartitionShards,
}

impl PeerManifestLister {
    #[must_use]
    pub fn new(config: &GeoConfig) -> Self {
        Self {
            interval: (!config.peers.is_empty())
                .then(|| micros(config.manifest_poll_interval.get_duration())),
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
        let wanted = shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .read(|inner| wanted(inner, place));
        let due: Vec<Wanted> = wanted
            .into_iter()
            .filter(|wanted| {
                state
                    .backoff
                    .get(&wanted.origin)
                    .is_none_or(|(_, next_at)| now.as_micros() >= *next_at)
            })
            .collect();
        let peers = shard.peer_stores();
        let mut listings = Vec::new();
        let scope = TierScope::new();
        for wanted in due {
            let Some(peer) = peers.get(&wanted.origin) else {
                continue;
            };
            let Some(service) = peer.service() else {
                continue;
            };
            let layout = peer.layout().clone();
            let service = service.clone();
            let token = scope.token();
            let prefix = layout.stream_manifests_prefix(wanted.peer_stream_id);
            let list = move || list_all(service.clone(), token.clone(), prefix.clone());
            listings.push((wanted, layout, list));
        }
        if listings.is_empty() {
            return;
        }
        state.running = Some(scope);
        drop(state);
        let task_shard = Rc::clone(shard);
        let state = Rc::clone(&self.state);
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        shard.bus.spawn(async move {
            let shard = task_shard;
            let mut found: BTreeMap<String, Vec<ListedPeerManifest>> = BTreeMap::new();
            let mut failed: BTreeMap<String, bool> = BTreeMap::new();
            for (wanted, layout, list) in listings {
                match list().await {
                    Ok(keys) => {
                        shard.metrics().record_peer_manifest_list(
                            &wanted.origin,
                            Some(now.as_micros() / 1_000_000),
                        );
                        failed.entry(wanted.origin.clone()).or_insert(false);
                        found
                            .entry(wanted.origin.clone())
                            .or_default()
                            .extend(listed(&layout, &wanted, keys));
                    }
                    Err(error) => {
                        shard
                            .metrics()
                            .record_peer_manifest_list(&wanted.origin, None);
                        debug!(
                            origin = wanted.origin,
                            stream_id = wanted.peer_stream_id,
                            error,
                            "listing the manifests of a geo peer stream failed; the next pass lists them again"
                        );
                        failed.insert(wanted.origin.clone(), true);
                    }
                }
            }
            state.borrow_mut().settle(failed, now, interval);
            for (origin, manifests) in found {
                hand_out(&shard, &origin, now, manifests, place.partition_shards);
            }
        });
    }

    /// Cancel the task in flight.
    pub fn stop(&self) {
        let mut state = self.state.borrow_mut();
        state.running = None;
    }
}

impl State {
    /// End a pass: start the wait of each peer whose listing failed, and end
    /// it for each one whose listings succeeded.
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
                    "listing the manifests of a geo peer failed; its shadow partitions keep what they hold"
                );
            }
            let next_at = now.as_micros().saturating_add(backoff(interval, failures));
            self.backoff.insert(origin, (failures, next_at));
        }
        self.running = None;
    }
}

/// The wait before the next listing of a peer after `failures` in a row
/// that failed, in microseconds.
pub fn backoff(interval: u64, failures: u32) -> u64 {
    let factor = 1_u64 << failures.min(16);
    interval
        .saturating_mul(factor)
        .min(micros(LIST_BACKOFF_MAX))
        .max(interval)
}

/// Every key under `prefix` in the store of `service`, page by page.
pub async fn list_all<E: TierExecutor>(
    service: E,
    token: TierScopeToken,
    prefix: String,
) -> Result<Vec<(String, String)>, String> {
    let mut keys = Vec::new();
    let mut next: Option<String> = None;
    loop {
        let request = TierJobRequest::new(TierJobClass::Housekeeping, token.clone());
        let page_prefix = prefix.clone();
        let continuation = next.clone();
        let page = service
            .submit(request, move |store| async move {
                let first = ListRequest::new(&page_prefix);
                let request = continuation
                    .as_deref()
                    .map_or(first, |after| first.after(after));
                ObjectStore::list(&*store, &request).await
            })
            .await
            .map_err(|cancelled| cancelled.to_string())?
            .map_err(|error| error.to_string())?;
        keys.extend(
            page.objects
                .into_iter()
                .map(|object| (object.key, object.etag)),
        );
        match page.next {
            Some(after) => next = Some(after),
            None => return Ok(keys),
        }
    }
}

/// The listed manifests that name a wanted shadow partition.
fn listed(
    layout: &KeyLayout,
    wanted: &Wanted,
    keys: Vec<(String, String)>,
) -> Vec<ListedPeerManifest> {
    keys.into_iter()
        .filter_map(|(key, etag)| {
            let prefix = layout.parse_manifest_key(&key)?;
            let (namespace, created_revision) = *wanted.partitions.get(&prefix)?;
            Some(ListedPeerManifest {
                namespace,
                created_revision,
                key,
                etag,
            })
        })
        .collect()
}

/// Hand every shard the manifests of `origin` of the shadow partitions it
/// owns. A shard that owns none gets an empty list, which ends what it
/// followed of partitions that are not wanted here any more.
fn hand_out(
    shard: &ServerShard,
    origin: &str,
    now: IggyTimestamp,
    manifests: Vec<ListedPeerManifest>,
    partition_shards: PartitionShards,
) {
    let mut by_shard: Vec<Vec<ListedPeerManifest>> =
        vec![Vec::new(); usize::from(partition_shards.shard_count())];
    for manifest in manifests {
        let owner = partition_shards.shard_for(&manifest.namespace);
        if let Some(owned) = by_shard.get_mut(usize::from(owner)) {
            owned.push(manifest);
        }
    }
    for (owner, manifests) in by_shard.into_iter().enumerate() {
        let Ok(owner) = u16::try_from(owner) else {
            continue;
        };
        shard.send_peer_manifests_listed(owner, origin.to_owned(), now, manifests);
    }
}

/// The shadow partitions whose primary this node is, as their last reported
/// view names it, grouped by peer stream. A detached shadow topic takes no
/// new data, and nothing of a retired origin is followed, so both are left
/// out.
fn wanted(inner: &StreamsInner, place: NodePlace) -> Vec<Wanted> {
    let mut wanted = Vec::new();
    for (stream_id, stream) in &inner.items {
        let Some(source) = stream
            .shadow
            .as_ref()
            .filter(|source| !inner.retired_origins.contains_key(&source.origin))
        else {
            continue;
        };
        let mut partitions = HashMap::new();
        for (topic_id, topic) in &stream.topics {
            let Some(shadow) = topic.shadow.filter(|shadow| !shadow.is_detached()) else {
                continue;
            };
            for partition in &topic.partitions {
                let namespace = IggyNamespace::new(stream_id, topic_id, partition.id);
                let view = inner
                    .stats_registry
                    .directory()
                    .get(namespace, partition.created_revision)
                    .map_or(partition.created_view, |status| status.view);
                if partition.primary_node(view, place.replica_count) != Some(place.self_replica_id)
                {
                    continue;
                }
                partitions.insert(
                    ObjectPrefix {
                        stream_id: source.stream_id,
                        topic_id: topic_id as u64,
                        partition_id: partition.id as u64,
                        created_revision: shadow.created_revision,
                    },
                    (namespace, partition.created_revision),
                );
            }
        }
        if !partitions.is_empty() {
            wanted.push(Wanted {
                origin: source.origin.clone(),
                peer_stream_id: source.stream_id,
                partitions,
            });
        }
    }
    wanted
}

pub fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_failures_in_a_row_when_waiting_should_double_up_to_a_minute() {
        let interval = micros(Duration::from_secs(5));
        let waits: Vec<u64> = (1..=6)
            .map(|failures| backoff(interval, failures) / 1_000_000)
            .collect();

        assert_eq!(waits, [10, 20, 40, 60, 60, 60]);
    }

    #[test]
    fn given_listed_keys_when_matching_should_hand_out_only_wanted_manifests() {
        let layout = KeyLayout::new("dc-a").unwrap();
        let wanted_prefix = ObjectPrefix {
            stream_id: 7,
            topic_id: 3,
            partition_id: 1,
            created_revision: 41,
        };
        let namespace = IggyNamespace::new(2, 3, 1);
        let wanted = Wanted {
            origin: "dc-a".to_owned(),
            peer_stream_id: 7,
            partitions: HashMap::from([(wanted_prefix, (namespace, 9))]),
        };
        let other_incarnation = ObjectPrefix {
            created_revision: 40,
            ..wanted_prefix
        };
        let keys = vec![
            (layout.manifest_key(&wanted_prefix), "etag-1".to_owned()),
            (layout.manifest_key(&other_incarnation), "etag-2".to_owned()),
            (
                "dc-a/7/3/1/41/00000000000000000000.log".to_owned(),
                "x".to_owned(),
            ),
        ];

        assert_eq!(
            listed(&layout, &wanted, keys),
            [ListedPeerManifest {
                namespace,
                created_revision: 9,
                key: layout.manifest_key(&wanted_prefix),
                etag: "etag-1".to_owned(),
            }]
        );
    }
}
