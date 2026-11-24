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

//! The primaries of shadow partitions, which commit the peer manifests that
//! shard 0 found changed.
//!
//! Shard 0 lists the manifests of each peer stream that a shadow stream
//! serves, see [`LifecycleFrame::PeerManifestsListed`], and hands each
//! shard the manifests of the shadow partitions it owns, with their etags.
//! For each one whose primary this replica is and whose etag the partition
//! has not committed, the shard reads the manifest from the peer's store in
//! a detached task. The pump checks the manifest against the peer log and
//! commits it through the partition-submit path, as one or more commands of
//! `partitions::peer_manifests`.
//!
//! What a shard knows of etags lives in memory and only on the primary. A
//! new primary reads every manifest once, and one that the partition already
//! committed costs no command.
//!
//! Once the origin of a shadow partition retired, no listing comes again.
//! The segment cleaner then stages [`LifecycleFrame::RetainRetiredPeer`],
//! and the primary commits the committed manifest without the segments that
//! the topic's retention removes, through the same commands.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use consensus::PartitionsHandle;
use iggy_binary_protocol::PrepareHeader;
use iggy_common::{IggyExpiry, IggyTimestamp};
use iggy_object_store::PartitionManifest;
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use partitions::{DiskIndexFiles, IndexFiles, RemoteObjects};
use server_common::Message;
use server_common::sharding::IggyNamespace;

use crate::shards_table::ShardsTable;
use crate::{IggyShard, LifecycleFrame, RestorableMetadataStm, ShardFrame, channel};

/// One manifest a listing of a peer stream found, for the shadow partition
/// it describes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedPeerManifest {
    /// The shadow partition in this cluster.
    pub namespace: IggyNamespace,
    /// Its incarnation in this cluster.
    pub created_revision: u64,
    /// Where the peer's store holds the manifest.
    pub key: String,
    pub etag: String,
}

/// What the task reading a peer manifest hands back to the pump.
pub struct FinishedPeerManifestRead {
    pub namespace: IggyNamespace,
    pub created_revision: u64,
    pub origin: String,
    pub etag: String,
    /// The manifest, or why it was not read.
    pub outcome: Result<PartitionManifest, String>,
}

/// What this shard knows of the manifests of the shadow partitions whose
/// primary it is.
#[derive(Default)]
pub struct PeerManifestFollows {
    /// Where the manifests of each peer are read, by origin. See
    /// [`crate::PeerStore::manifest_objects`].
    objects: RefCell<Vec<(String, Rc<dyn RemoteObjects>)>>,
    follows: RefCell<HashMap<IggyNamespace, Follow>>,
    /// Results of reads whose frame the inbox refused. The partition counts
    /// such a read as in flight until it sees the result, so the next listing
    /// takes them from here.
    undelivered: Rc<RefCell<Vec<FinishedPeerManifestRead>>>,
    /// This shard's numbers for the commands it proposes.
    requests: Cell<u64>,
}

#[derive(Default)]
struct Follow {
    origin: String,
    created_revision: u64,
    /// The etag of the manifest that the partition committed.
    committed: Option<String>,
    /// A manifest whose commands this shard proposed, by its etag.
    proposed: Option<(String, PartitionManifest)>,
    /// The etag of a manifest that the peer log refused. The shard reads it
    /// again only once the etag changes.
    refused: Option<String>,
    /// When a listing first found an etag the partition has not committed,
    /// in microseconds since the Unix epoch.
    behind_since: Option<u64>,
    reading: bool,
}

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// Give this shard where it reads the manifests of each peer, by origin.
    pub(crate) fn set_peer_manifest_objects(&self, objects: Vec<(String, Rc<dyn RemoteObjects>)>) {
        *self.peer_manifest_follows.objects.borrow_mut() = objects;
    }

    /// Hand shard `shard` the manifests that a listing of the peer `origin`
    /// found for the shadow partitions it owns. A refused frame is picked up
    /// by the next listing.
    pub fn send_peer_manifests_listed(
        &self,
        shard: u16,
        origin: String,
        now: IggyTimestamp,
        manifests: Vec<ListedPeerManifest>,
    ) {
        let Some(sender) = self.senders.get(usize::from(shard)) else {
            return;
        };
        let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::PeerManifestsListed {
            origin,
            now,
            manifests,
        }));
    }

    /// Start a read of each listed manifest that the partition here has not
    /// committed, on the partitions whose primary this replica is.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_peer_manifests_listed(
        &self,
        origin: &str,
        now: IggyTimestamp,
        manifests: Vec<ListedPeerManifest>,
    ) where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let undelivered = std::mem::take(&mut *self.peer_manifest_follows.undelivered.borrow_mut());
        for finished in undelivered {
            self.on_peer_manifest_read(finished).await;
        }
        if self.senders.get(self.id as usize).is_none() {
            return;
        }
        let Some(objects) = self
            .peer_manifest_follows
            .objects
            .borrow()
            .iter()
            .find(|(peer, _)| peer == origin)
            .map(|(_, objects)| Rc::clone(objects))
        else {
            return;
        };
        let listed: HashMap<IggyNamespace, ListedPeerManifest> = manifests
            .into_iter()
            .map(|manifest| (manifest.namespace, manifest))
            .collect();
        let partitions = self.plane.partitions();
        let mut follows = self.peer_manifest_follows.follows.borrow_mut();
        // A partition that left the listing, or whose primary moved away,
        // starts afresh when it comes back.
        follows.retain(|namespace, follow| {
            follow.origin != origin
                || listed
                    .get(namespace)
                    .is_some_and(|manifest| manifest.created_revision == follow.created_revision)
        });
        for manifest in listed.into_values() {
            let Some(partition) = partitions
                .get_mut_by_ns(&manifest.namespace)
                .filter(|partition| partition.created_revision() == manifest.created_revision)
            else {
                follows.remove(&manifest.namespace);
                continue;
            };
            let retired = partition.take_retired_peer_indexes();
            if !retired.is_empty() {
                let files = self.peer_stores().get(origin).map_or_else(
                    || Rc::new(DiskIndexFiles) as Rc<dyn IndexFiles>,
                    crate::peers::PeerStore::index_files,
                );
                self.bus.spawn(async move {
                    for path in retired {
                        let _ = files.remove(&path).await;
                    }
                });
            }
            if partition.peer_log().is_none() || !partition.state_machines_current() {
                follows.remove(&manifest.namespace);
                continue;
            }
            let follow = follows.entry(manifest.namespace).or_default();
            origin.clone_into(&mut follow.origin);
            follow.created_revision = manifest.created_revision;
            if let Some((etag, target)) = &follow.proposed
                && partition
                    .committed_peer_manifest()
                    .is_some_and(|committed| {
                        committed.segments == target.segments
                            && committed.floor == target.floor
                            && committed.trim_point == target.trim_point
                    })
            {
                follow.committed = Some(etag.clone());
                follow.proposed = None;
            }
            if follow.committed.as_ref() == Some(&manifest.etag) {
                follow.behind_since = None;
                continue;
            }
            follow.behind_since.get_or_insert(now.as_micros());
            if follow.reading || follow.refused.as_ref() == Some(&manifest.etag) {
                continue;
            }
            follow.reading = true;
            self.metrics.record_peer_manifest_read(origin);
            self.spawn_peer_manifest_read(Rc::clone(&objects), origin.to_owned(), manifest);
        }
        let (behind, lag) = follows
            .values()
            .filter(|follow| follow.origin == origin)
            .filter_map(|follow| follow.behind_since)
            .fold((0_usize, 0_u64), |(count, lag), since| {
                (count + 1, lag.max(now.as_micros().saturating_sub(since)))
            });
        self.metrics.set_shadow_manifest_lag(origin, behind, lag);
    }

    /// Read `manifest` from the peer's store in a detached task, which sends
    /// the result back to this shard's pump.
    fn spawn_peer_manifest_read(
        &self,
        objects: Rc<dyn RemoteObjects>,
        origin: String,
        manifest: ListedPeerManifest,
    ) {
        let Some(sender) = self.senders.get(self.id as usize).cloned() else {
            return;
        };
        let undelivered = Rc::clone(&self.peer_manifest_follows.undelivered);
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        self.bus.spawn(async move {
            let outcome = match objects.get_range(&manifest.key, 0..u64::MAX).await {
                Ok(object) => {
                    PartitionManifest::decode(&object.bytes).map_err(|error| error.to_string())
                }
                Err(error) => Err(error.to_string()),
            };
            let finished = FinishedPeerManifestRead {
                namespace: manifest.namespace,
                created_revision: manifest.created_revision,
                origin,
                etag: manifest.etag,
                outcome,
            };
            let frame = ShardFrame::lifecycle(LifecycleFrame::PeerManifestRead(Box::new(finished)));
            // The partition starts no other read until it sees this
            // result, so a refused frame must not lose it.
            if let Err(
                crossfire::TrySendError::Full(frame) | crossfire::TrySendError::Disconnected(frame),
            ) = sender.try_send(frame)
                && let ShardFrame::Lifecycle(LifecycleFrame::PeerManifestRead(finished)) = frame
            {
                undelivered.borrow_mut().push(*finished);
            }
        });
    }

    /// Commit a manifest that a read brought back, when the partition is
    /// still the one that started the read and this replica its primary.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_peer_manifest_read(&self, finished: FinishedPeerManifestRead)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let requests = {
            let mut follows = self.peer_manifest_follows.follows.borrow_mut();
            let Some(follow) = follows
                .get_mut(&finished.namespace)
                .filter(|follow| follow.created_revision == finished.created_revision)
            else {
                return;
            };
            follow.reading = false;
            let manifest = match finished.outcome {
                Ok(manifest) => manifest,
                Err(error) => {
                    self.metrics
                        .record_peer_manifest_read_failure(&finished.origin);
                    tracing::debug!(
                        shard = self.id,
                        namespace_raw = finished.namespace.inner(),
                        origin = finished.origin,
                        error,
                        "reading a peer manifest failed; the next listing reads it again"
                    );
                    return;
                }
            };
            let Some(partition) = self
                .plane
                .partitions()
                .get_by_ns(&finished.namespace)
                .filter(|partition| partition.created_revision() == finished.created_revision)
                .filter(|partition| partition.state_machines_current())
            else {
                return;
            };
            let first = self.peer_manifest_follows.requests.get().wrapping_add(1);
            match partition.peer_manifest_requests(&manifest, first) {
                Ok(requests) if requests.is_empty() => {
                    follow.committed = Some(finished.etag);
                    follow.behind_since = None;
                    return;
                }
                Ok(requests) => {
                    self.peer_manifest_follows
                        .requests
                        .set(first.wrapping_add(requests.len() as u64));
                    follow.proposed = Some((finished.etag, manifest));
                    requests
                }
                Err(error) => {
                    self.metrics.record_peer_manifest_refused(&finished.origin);
                    tracing::warn!(
                        shard = self.id,
                        namespace_raw = finished.namespace.inner(),
                        origin = finished.origin,
                        etag = finished.etag,
                        %error,
                        "refused a peer manifest; the shadow partition keeps the log it has"
                    );
                    follow.refused = Some(finished.etag);
                    return;
                }
            }
        };
        self.metrics
            .record_peer_manifest_commands(&finished.origin, requests.len());
        for request in requests {
            // Nothing waits for the reply: a command that is refused or lost
            // leaves the committed manifest behind the etag, and the next
            // listing reads the manifest again.
            let (reply, _unread) = channel(1);
            self.dispatch_partition_submit(request, reply, None, None)
                .await;
        }
    }

    /// Stage the retention of `namespace`, a shadow partition whose origin
    /// retired, on this shard's pump. A refused frame is picked up by the
    /// cleaner's next tick.
    pub fn request_retain_retired_peer(
        &self,
        namespace: IggyNamespace,
        created_revision: u64,
        now: IggyTimestamp,
        message_expiry: IggyExpiry,
        max_bytes: Option<u64>,
    ) {
        let Some(sender) = self.senders.get(self.id as usize) else {
            return;
        };
        let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::RetainRetiredPeer {
            namespace,
            created_revision,
            now,
            message_expiry,
            max_bytes,
        }));
    }

    /// Forget what this replica knew of the manifests of `namespace`, so its
    /// lag no longer counts, and on its primary commit what the retention
    /// removes from the committed manifest.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_retain_retired_peer(
        &self,
        namespace: IggyNamespace,
        created_revision: u64,
        now: IggyTimestamp,
        message_expiry: IggyExpiry,
        max_bytes: Option<u64>,
    ) where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let requests = {
            let partitions = self.plane.partitions();
            let Some(partition) = partitions
                .get_mut_by_ns(&namespace)
                .filter(|partition| partition.created_revision() == created_revision)
            else {
                return;
            };
            if let Some(follow) = self
                .peer_manifest_follows
                .follows
                .borrow_mut()
                .remove(&namespace)
            {
                self.metrics.set_shadow_manifest_lag(&follow.origin, 0, 0);
            }
            let Some(trimmed) = partition.retired_peer_retention(now, message_expiry, max_bytes)
            else {
                return;
            };
            let first = self.peer_manifest_follows.requests.get().wrapping_add(1);
            match partition.peer_manifest_requests(&trimmed, first) {
                Ok(requests) => {
                    self.peer_manifest_follows
                        .requests
                        .set(first.wrapping_add(requests.len() as u64));
                    requests
                }
                Err(error) => {
                    tracing::warn!(
                        shard = self.id,
                        namespace_raw = namespace.inner(),
                        %error,
                        "the retention of a shadow partition of a retired origin cannot be committed"
                    );
                    return;
                }
            }
        };
        for request in requests {
            // The next tick of the cleaner proposes again what did not
            // commit.
            let (reply, _unread) = channel(1);
            self.dispatch_partition_submit(request, reply, None, None)
                .await;
        }
    }
}
