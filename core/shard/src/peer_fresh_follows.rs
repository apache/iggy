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

//! The fresh tails of the shadow partitions of a shard.
//!
//! Shard 0 finds the new fresh objects of each geo peer it follows and
//! hands every shard what each object holds of the shadow partitions it
//! owns, see [`LifecycleFrame::PeerFreshFound`]. The shard offers those
//! rows to the partitions, see [`partitions::PeerFreshTail`]. A proof that
//! names an object whose rows the partition does not hold yet makes the
//! shard read that object's index from the peer's store, in a detached
//! task that hands the rows back the same way.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use consensus::PartitionsHandle;
use iggy_object_store::{FreshObjectId, KeyLayout};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use partitions::{FreshIndexRead, PeerFreshLag, PeerFreshRows, PeerLog, ProofTrust, RemoteObjects};
use server_common::sharding::IggyNamespace;

use crate::shards_table::ShardsTable;
use crate::{IggyShard, LifecycleFrame, ShardFrame};

/// What the reads of a geo peer's fresh objects found for the shadow
/// partitions of one shard.
#[derive(Debug, Clone, Default)]
pub struct PeerFreshFound {
    pub origin: String,
    /// What one object holds of a shadow partition, with the partition's
    /// namespace and incarnation in this cluster.
    pub rows: Vec<(IggyNamespace, u64, PeerFreshRows)>,
    /// Objects the peer's store no longer holds, which proofs of these
    /// partitions name.
    pub gone: Vec<(IggyNamespace, u64, FreshObjectId)>,
}

/// Where the fresh objects of one peer are read: its origin, its tier
/// service and the layout of its root.
pub type PeerFreshObjects = (String, Rc<dyn RemoteObjects>, KeyLayout);

/// What a shard knows of the fresh objects its shadow partitions wait for.
#[derive(Default)]
pub struct PeerFreshFollows {
    /// Where the fresh objects of each peer are read, by origin, with the
    /// layout of its root. See [`crate::PeerStore::follow_objects`].
    objects: RefCell<Vec<PeerFreshObjects>>,
    /// Reads in flight, so a proof that names an object twice reads it once.
    reading: Rc<RefCell<BTreeSet<(IggyNamespace, FreshObjectId)>>>,
    /// Results whose frame the inbox refused, which the next frame takes.
    undelivered: Rc<RefCell<Vec<PeerFreshFound>>>,
    /// How far each shadow partition here is behind, by origin.
    lags: RefCell<BTreeMap<String, BTreeMap<IggyNamespace, PeerFreshLag>>>,
}

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// Give this shard where it reads the fresh objects of each peer, by
    /// origin.
    pub(crate) fn set_peer_fresh_objects(&self, objects: Vec<PeerFreshObjects>) {
        *self.peer_fresh_follows.objects.borrow_mut() = objects;
    }

    /// Hand shard `shard` what reads of the geo peer's fresh objects found
    /// for its shadow partitions. Returns `found` when the inbox refused it,
    /// so the caller sends it again.
    ///
    /// # Errors
    /// The refused rows.
    pub fn send_peer_fresh_found(
        &self,
        shard: u16,
        found: PeerFreshFound,
    ) -> Result<(), PeerFreshFound> {
        let Some(sender) = self.senders.get(usize::from(shard)) else {
            return Ok(());
        };
        match sender.try_send(ShardFrame::lifecycle(LifecycleFrame::PeerFreshFound(
            Box::new(found),
        ))) {
            Ok(()) => Ok(()),
            Err(
                crossfire::TrySendError::Full(frame) | crossfire::TrySendError::Disconnected(frame),
            ) => match frame {
                ShardFrame::Lifecycle(LifecycleFrame::PeerFreshFound(found)) => Err(*found),
                _ => Ok(()),
            },
        }
    }

    /// Offer the rows to the shadow partitions they describe, and start
    /// the reads of the objects whose rows their proofs wait for.
    pub(crate) fn on_peer_fresh_found(&self, found: PeerFreshFound) {
        let undelivered = std::mem::take(&mut *self.peer_fresh_follows.undelivered.borrow_mut());
        for earlier in undelivered {
            self.offer_peer_fresh(earlier);
        }
        self.offer_peer_fresh(found);
    }

    fn offer_peer_fresh(&self, found: PeerFreshFound) {
        let PeerFreshFound { origin, rows, gone } = found;
        let partitions = self.plane.partitions();
        let mut touched = BTreeSet::new();
        for (namespace, created_revision, object) in gone {
            if let Some(partition) = partitions
                .get_mut_by_ns(&namespace)
                .filter(|partition| partition.created_revision() == created_revision)
            {
                partition.mark_peer_fresh_gone(object);
                touched.insert((namespace, created_revision));
            }
        }
        for (namespace, created_revision, rows) in rows {
            let Some(partition) = partitions
                .get_mut_by_ns(&namespace)
                .filter(|partition| partition.created_revision() == created_revision)
                .filter(|partition| partition.peer_log().is_some())
            else {
                continue;
            };
            let change = partition.offer_peer_fresh(&rows);
            if let Some(offset) = change.conflict_at {
                self.metrics.record_shadow_fresh_conflict(&origin);
                tracing::error!(
                    shard = self.id,
                    namespace_raw = namespace.inner(),
                    origin,
                    offset,
                    "two proof rows of the geo peer bind one offset to different batches, so this \
                     shadow partition serves no fresh batch until the node restarts and keeps \
                     serving the published ones"
                );
            }
            touched.insert((namespace, created_revision));
        }
        for (namespace, created_revision) in touched {
            let Some(partition) = partitions
                .get_by_ns(&namespace)
                .filter(|partition| partition.created_revision() == created_revision)
            else {
                continue;
            };
            if let Some(lag) = partition.peer_fresh_lag() {
                self.peer_fresh_follows
                    .lags
                    .borrow_mut()
                    .entry(origin.clone())
                    .or_default()
                    .insert(namespace, lag);
            }
            let Some(origin_namespace) = partition.peer_log().map(PeerLog::origin_namespace) else {
                continue;
            };
            for object in partition.peer_fresh_missing_sources() {
                self.spawn_peer_fresh_source_read(
                    &origin,
                    (namespace, created_revision),
                    origin_namespace,
                    object,
                );
            }
        }
        self.publish_peer_fresh_lag(&origin);
    }

    /// Set the lag gauges of `origin` to the partition here that is the
    /// furthest behind, forgetting the partitions that left.
    pub(crate) fn publish_peer_fresh_lag(&self, origin: &str) {
        let partitions = self.plane.partitions();
        let mut lags = self.peer_fresh_follows.lags.borrow_mut();
        let Some(of_origin) = lags.get_mut(origin) else {
            return;
        };
        of_origin.retain(|namespace, lag| {
            let now = partitions
                .get_by_ns(namespace)
                .and_then(partitions::IggyPartition::peer_fresh_lag);
            if let Some(now) = now {
                *lag = now;
            }
            now.is_some()
        });
        let worst = of_origin
            .values()
            .fold(PeerFreshLag::default(), |worst, lag| PeerFreshLag {
                messages: worst.messages.max(lag.messages),
                micros: worst.micros.max(lag.micros),
            });
        self.metrics
            .set_shadow_fresh_lag(origin, worst.messages, worst.micros);
    }

    /// Read the index of `object` from the store of `origin` in a detached
    /// task, which hands what it holds of the origin's partition
    /// `origin_namespace` back to this shard for the shadow partition
    /// `namespace`. A read that failed is tried again when the next rows of
    /// the partition come.
    fn spawn_peer_fresh_source_read(
        &self,
        origin: &str,
        (namespace, created_revision): (IggyNamespace, u64),
        origin_namespace: u64,
        object: FreshObjectId,
    ) {
        let reading = Rc::clone(&self.peer_fresh_follows.reading);
        if !reading.borrow_mut().insert((namespace, object)) {
            return;
        }
        let peer = self
            .peer_fresh_follows
            .objects
            .borrow()
            .iter()
            .find(|(peer, ..)| peer == origin)
            .map(|(_, objects, layout)| (Rc::clone(objects), layout.clone()));
        let (Some((objects, layout)), Some(sender)) =
            (peer, self.senders.get(self.id as usize).cloned())
        else {
            reading.borrow_mut().remove(&(namespace, object));
            return;
        };
        let origin = origin.to_owned();
        let undelivered = Rc::clone(&self.peer_fresh_follows.undelivered);
        let metrics = self.metrics.clone();
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        self.bus.spawn(async move {
            let read = partitions::read_fresh_index(&*objects, &layout, object, 0).await;
            reading.borrow_mut().remove(&(namespace, object));
            metrics.record_peer_fresh_request(&origin, "index", read.is_ok());
            let mut found = PeerFreshFound {
                origin,
                ..PeerFreshFound::default()
            };
            match read {
                Ok(FreshIndexRead::Found(index)) => {
                    if let Some(row) = index.partition(origin_namespace) {
                        found.rows.push((
                            namespace,
                            created_revision,
                            PeerFreshRows::of(&index, row, ProofTrust::None),
                        ));
                    }
                }
                Ok(FreshIndexRead::Missing | FreshIndexRead::Unreadable(_)) => {
                    found.gone.push((namespace, created_revision, object));
                }
                Err(_) => return,
            }
            let frame = ShardFrame::lifecycle(LifecycleFrame::PeerFreshFound(Box::new(found)));
            if let Err(
                crossfire::TrySendError::Full(frame) | crossfire::TrySendError::Disconnected(frame),
            ) = sender.try_send(frame)
                && let ShardFrame::Lifecycle(LifecycleFrame::PeerFreshFound(found)) = frame
            {
                undelivered.borrow_mut().push(*found);
            }
        });
    }
}
