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

//! The geo passes of a shard: the objects of this cluster's geo topics, the
//! shadow streams of the peers' topics, the listings of the peers' manifests
//! and positions, and the reads of topic objects that a new partition
//! primary waits for.
//!
//! The partition reconciler runs them after each of its passes. The caller
//! passes the time, so the simulator runs them on its virtual clock.

use std::cell::Cell;
use std::rc::Rc;

use configs::geo::GeoConfig;
use iggy_binary_protocol::PrepareHeader;
use iggy_common::IggyTimestamp;
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use partitions::TOPIC_OBJECT_RETRY;
use server_common::Message;

use crate::geo_topics::GeoTopics;
use crate::peer_manifests::{NodePlace, PeerManifestLister};
use crate::peer_positions::PeerPositionLister;
use crate::shadow_topics::ShadowTopics;
use crate::shell::{ShellBus, ShellShard};

/// The geo passes of one shard.
pub struct GeoPasses {
    /// The objects of this cluster's geo topics in its store, which the
    /// metadata primary keeps in step with committed metadata.
    pub(crate) geo_topics: GeoTopics,
    /// The shadow streams of the geo peers' topics, which the metadata
    /// primary keeps in step with the peers' stores.
    shadow_topics: ShadowTopics,
    /// The listings of the geo peers' manifests, which keep the shadow
    /// partitions whose primary this node is up to date. Shard 0 only.
    peer_manifests: PeerManifestLister,
    /// The listings of the geo peers' consumer group positions, which the
    /// primaries here adopt when they win. Shard 0 only.
    peer_positions: PeerPositionLister,
    /// When the next pass over the partitions of geo topics may be staged,
    /// in microseconds since the Unix epoch.
    next_topic_object_check_at: Cell<u64>,
}

impl GeoPasses {
    #[must_use]
    pub fn new(config: &GeoConfig) -> Self {
        Self {
            geo_topics: GeoTopics::new(config),
            shadow_topics: ShadowTopics::new(config),
            peer_manifests: PeerManifestLister::new(config),
            peer_positions: PeerPositionLister::new(config),
            next_topic_object_check_at: Cell::new(0),
        }
    }

    /// Run each pass that is due at `now`. A pass starts its work in a
    /// detached task on the shard's bus.
    pub fn pass<B, MJ, S, SB>(
        &self,
        shard: &Rc<ShellShard<B, MJ, S, SB>>,
        now: IggyTimestamp,
        place: NodePlace,
    ) where
        B: ShellBus + Unpin,
        MJ: JournalHandle + 'static,
        MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        S: 'static,
        SB: SuperblockStore + 'static,
    {
        self.geo_topics.pass(shard, now);
        self.shadow_topics.pass(shard, now);
        self.peer_manifests.pass(shard, now, place);
        self.peer_positions.pass(shard, now, place);
        self.stage_topic_object_checks(shard, now);
    }

    /// Cancel the tasks in flight and forget what the passes read.
    pub fn stop<B, MJ, S, SB>(&self, shard: &ShellShard<B, MJ, S, SB>)
    where
        B: ShellBus + Unpin,
        MJ: JournalHandle + 'static,
        MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        S: 'static,
        SB: SuperblockStore + 'static,
    {
        self.geo_topics.stop();
        self.shadow_topics.stop(shard);
        self.peer_manifests.stop();
        self.peer_positions.stop();
        shard.stop_geo_positions();
    }

    /// Stage a pass over the partitions of geo topics on this shard's pump,
    /// at most once per [`TOPIC_OBJECT_RETRY`]. The pump starts the read of
    /// the topic's object for each partition whose primary does not take
    /// messages yet, so a new primary reads without waiting for a produce.
    fn stage_topic_object_checks<B, MJ, S, SB>(
        &self,
        shard: &ShellShard<B, MJ, S, SB>,
        now: IggyTimestamp,
    ) where
        B: ShellBus,
        SB: SuperblockStore,
    {
        if now.as_micros() < self.next_topic_object_check_at.get() {
            return;
        }
        let wait = u64::try_from(TOPIC_OBJECT_RETRY.as_micros()).unwrap_or(u64::MAX);
        self.next_topic_object_check_at
            .set(now.as_micros().saturating_add(wait));
        shard.request_check_topic_objects(now);
    }
}
