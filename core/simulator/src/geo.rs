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

//! Geo topics in one simulated cluster.
//!
//! Each shard reaches the cluster's [`SimBucket`] through the tier service
//! that boot gives a server shard, see [`server::serve_tiered_storage`]. The
//! simulator runs no store identity check: a node counts as verified once its
//! metadata names the cluster, and then its shards hold their writers. Each
//! step runs the shards' [`GeoPasses`] on the virtual clock, as the partition
//! reconciler runs them after its passes.

use clock::Clock;
use configs::geo::GeoConfig;
use configs::tiered_storage::TieredStorageConfig;
use consensus::MetadataHandle;
use iggy_object_store::KeyLayout;
use server::{GeoPasses, NodePlace, TierHandlers};

use crate::Simulator;
use crate::bucket::{ClusterId, SimBucket};
use crate::deps::SimClock;

/// The geo setup of a simulated cluster.
pub struct SimGeo {
    bucket: SimBucket,
    cluster: ClusterId,
    geo: GeoConfig,
    tiered: TieredStorageConfig,
    layout: KeyLayout,
    /// Per replica, the state of each of its shards. Empty for a replica
    /// that was never wired.
    replicas: Vec<Vec<GeoShard>>,
}

/// What one shard keeps between steps.
struct GeoShard {
    handlers: TierHandlers,
    passes: GeoPasses,
}

impl SimGeo {
    /// The geo setup of `cluster`, whose geo topics take `origin` as their
    /// origin and keep their data in `bucket`.
    ///
    /// # Panics
    /// When the default prefix of tiered storage is not a valid key prefix.
    #[must_use]
    pub fn new(origin: &str, cluster: ClusterId, bucket: SimBucket) -> Self {
        let defaults = configs::server::ServerConfig::default();
        let geo = GeoConfig {
            origin: origin.to_owned(),
            ..defaults.geo
        };
        let tiered = defaults.tiered_storage;
        let layout = KeyLayout::new(&tiered.prefix).expect("the default prefix is valid");
        Self {
            bucket,
            cluster,
            geo,
            tiered,
            layout,
            replicas: Vec::new(),
        }
    }

    #[must_use]
    pub const fn bucket(&self) -> &SimBucket {
        &self.bucket
    }

    #[must_use]
    pub const fn layout(&self) -> &KeyLayout {
        &self.layout
    }
}

impl Simulator {
    /// Give every replica of this cluster geo topics with `geo`'s origin and
    /// bucket. A replica that restarts gets them again.
    pub fn enable_geo(&mut self, geo: SimGeo) {
        self.geo = Some(geo);
        for replica in 0..self.replica_count {
            self.wire_geo(replica);
        }
    }

    #[must_use]
    pub const fn geo(&self) -> Option<&SimGeo> {
        self.geo.as_ref()
    }

    /// Wire each shard of `replica` to the bucket, as boot does with the
    /// configured store.
    pub(crate) fn wire_geo(&mut self, replica: u8) {
        let Some(geo) = self.geo.as_mut() else {
            return;
        };
        let index = usize::from(replica);
        let shards = &self.replicas[index].shards;
        let wired = shards
            .iter()
            .map(|shard| {
                let store = geo.bucket.handle(geo.cluster, self.executor.timer());
                let handlers = server::serve_tiered_storage(&geo.tiered, store, &geo.layout, shard);
                shard.set_long_term_rewrite_max_interval(
                    geo.geo.long_term_rewrite_max_interval.get_duration(),
                );
                GeoShard {
                    handlers,
                    passes: GeoPasses::new(&geo.geo),
                }
            })
            .collect();
        if let Some(shard) = shards.first() {
            shard
                .plane
                .metadata()
                .applied_frontier()
                .set_store_identity_verified(true);
        }
        if geo.replicas.len() <= index {
            geo.replicas.resize_with(index + 1, Vec::new);
        }
        geo.replicas[index] = wired;
    }

    /// One step of the geo passes of every live replica: each shard takes
    /// its writers once its metadata names the cluster, then runs the passes
    /// that are due.
    pub(crate) fn drive_geo(&mut self) {
        let Some(geo) = self.geo.as_mut() else {
            return;
        };
        let now = SimClock::new(self.executor.timer()).realtime();
        for (index, replica) in self.replicas.iter().enumerate() {
            let id = u8::try_from(index).expect("replica id fits u8");
            if self.crashed.contains(&id) {
                continue;
            }
            let Some(wired) = geo.replicas.get_mut(index) else {
                continue;
            };
            let Some(first) = replica.shards.first() else {
                continue;
            };
            let place = NodePlace {
                self_replica_id: id,
                replica_count: self.replica_count,
                partition_shards: first.partition_shards(),
            };
            for (shard, state) in replica.shards.iter().zip(wired.iter_mut()) {
                state
                    .handlers
                    .sync(shard, server::own_cluster_uid(shard.as_ref()));
                state.passes.pass(shard, now, place);
            }
        }
    }
}
