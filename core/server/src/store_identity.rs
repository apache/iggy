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

//! The store identity check of a node, see [`StoreIdentity`].
//!
//! Replica 0 claims the root of the object store for this cluster, and every
//! node reads the claim back through its own store. A node passes the check
//! once it reads back its own `cluster_uid`, and fails it again when a check
//! finds the object missing, foreign or refused. Each shard holds its
//! uploader and its object deleter only while its node passes, and a node
//! can tier topics only while it passes, see
//! [`AppliedFrontier::tiered_storage_ready`].
//!
//! `cluster_uid` is the id of the metadata change log: the checksum of the
//! first committed operation, which every replica applies or restores with
//! the same value. That operation creates the root user with a password hash
//! under a fresh salt, so two clusters with the same name get two ids.
//!
//! Replica 0 claims only while it is the caught-up metadata primary and no
//! topic keeps data in the store, so while another replica leads metadata,
//! nothing claims the root. A tiered topic keeps data there, and so does a
//! topic that places its batches. Until a topic does, replica 0 claims again
//! after any check that did not pass, so a replica 0 whose `fs` mount drops
//! claims the empty mount point. The check also cannot tell two stores apart
//! when replica 0 reaches an empty store and the other nodes reach one that
//! holds an earlier claim of this cluster.
//!
//! The first check of a process that reads back this cluster's id also puts
//! the same bytes once more as a conditional create, which the store must
//! refuse. A store that takes it ignores the condition that every object two
//! writers can race on is created with, so the node fails the check, see
//! [`StoreIdentity::check_conditional_create`].
//!
//! A store that stops answering after the check passed is an outage, and the
//! node still passes: uploads retry. `object_store_unreachable` is 1 while
//! the store does not answer the check.
//!
//! A root that another cluster claimed is refused, unless the operator named
//! that cluster in `[geo] recover_cluster_uid`: replica 0, as the caught-up
//! metadata primary, then recovers the geo topics of that cluster and claims
//! the root, see [`crate::origin_recovery`]. Until then replica 0, and every
//! node whose metadata holds the topics it restored, checks again every few
//! seconds.

use std::pin::pin;
use std::rc::Rc;
use std::time::{Duration, Instant};

use consensus::MetadataHandle;
use futures::future::{Either, select};
use iggy_common::IggyTimestamp;
use iggy_object_store::{
    ErrorClass, IdentityRead, KeyLayout, ObjectStore, ObjectStoreError, StoreIdentity,
};
use journal::superblock::SuperblockStore;
use metadata::AppliedFrontier;
use metadata::change_log::ChangeLog;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::{Streams, StreamsInner};
use partitions::{
    FreshObjectSweeper, FreshObjectUploader, LongTermMerger, LongTermWriter, ManifestPublisher,
    ObjectDeleter, SegmentUploader, TopicObjects,
};
use shard::Receiver;
use shard::metrics::ShardMetrics;
use tracing::{debug, error, info, warn};

use crate::origin_recovery::{OriginRecovery, may_recover};
use crate::shell::{ServerShard, ShellBus, ShellShard};

/// How long a node waits before it checks again, once its check passed or
/// found something that only an operator can change.
const RECHECK_INTERVAL: Duration = Duration::from_secs(60);
/// How long a node waits before it checks again, while no cluster claimed
/// the root or the store does not answer.
const RETRY_INTERVAL: Duration = Duration::from_secs(5);
/// How long a node waits for `cluster_uid`, which the first committed
/// metadata operation sets.
const CLUSTER_UID_WAIT: Duration = Duration::from_secs(1);
/// How often a shard without the check compares its uploader and deleter
/// with the check of its node.
const WATCH_INTERVAL: Duration = Duration::from_secs(1);

/// The identity check of a node. Shard 0 runs it, with one bounded attempt
/// on a store outside its tier service, so that no queued job and no retry
/// delays it.
pub struct IdentityChecker<S> {
    store: S,
    layout: KeyLayout,
    /// Whether this node is replica 0, the only node that claims the root.
    /// One fixed claimer is what tells per-node stores apart: every other
    /// node finds no object in a store of its own.
    claimer: bool,
    /// The longest one check waits for the store.
    timeout: Duration,
    /// What the last check found, so that a log line marks each change.
    last: Option<Verdict>,
    /// The recovery the operator asked for, `None` when it named no cluster.
    recovery: Option<OriginRecovery>,
    /// Whether a check of this process saw the store refuse a conditional
    /// create over the identity object, see
    /// [`StoreIdentity::check_conditional_create`]. A check sends one until
    /// it does.
    conditional_create_checked: bool,
    /// The cluster the last check found in the store while metadata held no
    /// topic restored from it. See [`Self::wait`].
    unrestored_claim: Option<u128>,
}

/// What one check of the store identity found.
#[derive(Debug)]
enum IdentityCheck {
    /// The object names this cluster. `claimed` when this check wrote it.
    Verified { claimed: bool },
    /// No cluster claimed the root yet.
    Missing,
    /// The object names another cluster, or, as `None`, holds bytes that no
    /// cluster wrote.
    Foreign(Option<u128>),
    /// The store did not answer, or failed in a way that a retry can get
    /// past.
    Unreachable(ObjectStoreError),
    /// The call failed in a way that only an operator can fix, such as
    /// rejected credentials or an `fs` file without an object header.
    Refused(ObjectStoreError),
    /// The object names this cluster, but the store took a conditional
    /// create over it, so it ignores the condition that every object two
    /// writers can race on is created with.
    NoConditionalCreate(ObjectStoreError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Verified,
    Missing,
    Foreign,
    Unreachable,
    Refused,
}

/// What a shard writes to the object store with.
///
/// They are its segment uploader, its object deleter, its manifest
/// publisher, the uploader of its fresh objects, the writer and the merger
/// of its long-term segments, the sweeper of fresh objects and the jobs on
/// geo topic objects. The shard holds them only while its node passes the
/// store identity check.
pub struct TierHandlers {
    uploader: Rc<dyn SegmentUploader>,
    deleter: Rc<dyn ObjectDeleter>,
    publisher: Rc<dyn ManifestPublisher>,
    fresh_uploader: Rc<dyn FreshObjectUploader>,
    long_term_writer: Rc<dyn LongTermWriter>,
    long_term_merger: Rc<dyn LongTermMerger>,
    fresh_sweeper: Rc<dyn FreshObjectSweeper>,
    topic_objects: Rc<dyn TopicObjects>,
    held: bool,
}

/// Run the identity check when `checker` is there, and keep the uploader
/// and deleter of `shard` in step with the check of its node, until `stop`
/// fires.
pub async fn run<S: ObjectStore>(
    shard: Rc<ServerShard>,
    mut handlers: TierHandlers,
    mut checker: Option<IdentityChecker<S>>,
    stop: Receiver<()>,
) {
    loop {
        let delay = match checker.as_mut() {
            Some(checker) => match select(pin!(checker.step(&shard)), pin!(stop.recv())).await {
                Either::Left((delay, _)) => delay,
                Either::Right(_) => return,
            },
            None => WATCH_INTERVAL,
        };
        let verified = shard
            .plane
            .metadata()
            .applied_frontier()
            .store_identity_verified();
        handlers.sync(&shard, verified.then(|| own_cluster_uid(&shard)).flatten());
        let wait = async {
            match checker.as_ref() {
                Some(checker) => {
                    checker
                        .wait(shard.plane.metadata().mux_stm.streams(), delay)
                        .await;
                }
                None => compio::time::sleep(delay).await,
            }
        };
        // `Right`: the stop fired, or its sender is gone.
        if let Either::Right(_) = select(pin!(wait), pin!(stop.recv())).await {
            return;
        }
    }
}

/// This cluster's `cluster_uid`: the id of its metadata change log.
pub fn own_cluster_uid<B, MJ, S, SB>(shard: &ShellShard<B, MJ, S, SB>) -> Option<u128>
where
    B: ShellBus,
    SB: SuperblockStore,
{
    shard
        .plane
        .metadata()
        .change_log()
        .read(ChangeLog::log_id)
        .flatten()
}

impl<S: ObjectStore> IdentityChecker<S> {
    pub const fn new(
        store: S,
        layout: KeyLayout,
        claimer: bool,
        timeout: Duration,
        recovery: Option<OriginRecovery>,
    ) -> Self {
        Self {
            store,
            layout,
            claimer,
            timeout,
            last: None,
            recovery,
            conditional_create_checked: false,
            unrestored_claim: None,
        }
    }

    /// Check once with what `shard` knows, record the result, and return how
    /// long to wait before the next check.
    async fn step(&mut self, shard: &ServerShard) -> Duration {
        let metadata = shard.plane.metadata();
        let Some(cluster_uid) = metadata.change_log().read(ChangeLog::log_id).flatten() else {
            return CLUSTER_UID_WAIT;
        };
        let store_data = metadata.mux_stm.streams().has_topic_with_store_data();
        let claim = may_claim(
            self.claimer,
            self.last,
            metadata.is_caught_up_primary(),
            store_data,
        );
        let mut check = self.check(cluster_uid, claim).await;
        let retired = self
            .recovery
            .as_ref()
            .map(OriginRecovery::retired_cluster_uid);
        let found = match check {
            IdentityCheck::Foreign(found) => found,
            _ => None,
        };
        if may_recover(
            self.claimer,
            retired,
            found,
            metadata.is_caught_up_primary(),
        ) && self.recover(shard, cluster_uid).await
        {
            check = self.check(cluster_uid, false).await;
        }
        // A node told to recover, or one whose metadata holds topics restored
        // from the cluster its store names, checks again soon: replica 0
        // claims the store once it restored them.
        let awaits_recovery = match check {
            IdentityCheck::Foreign(Some(found)) => {
                retired == Some(found)
                    || metadata
                        .mux_stm
                        .streams()
                        .read(|inner| restores_from(inner, found))
            }
            _ => false,
        };
        self.unrestored_claim = match check {
            IdentityCheck::Foreign(Some(found)) if !awaits_recovery => Some(found),
            _ => None,
        };
        let delay = self.record(
            check,
            cluster_uid,
            store_data,
            metadata.applied_frontier(),
            shard.metrics(),
        );
        if awaits_recovery {
            delay.min(RETRY_INTERVAL)
        } else {
            delay
        }
    }

    /// Wait `delay` before the next check. Ends early once `streams` holds
    /// topics restored from the cluster the last check found, since replica
    /// 0 claims the store right after the restore.
    async fn wait(&self, streams: &Streams, delay: Duration) {
        let Some(found) = self.unrestored_claim else {
            compio::time::sleep(delay).await;
            return;
        };
        let deadline = Instant::now() + delay;
        while !streams.read(|inner| restores_from(inner, found)) {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return;
            }
            compio::time::sleep(left.min(WATCH_INTERVAL)).await;
        }
    }

    /// Run the recovery the operator asked for. Whether it finished: one
    /// that failed runs again at the next check.
    async fn recover(&self, shard: &ServerShard, cluster_uid: u128) -> bool {
        let Some(recovery) = &self.recovery else {
            return false;
        };
        let retired_cluster_uid = format!("{:032x}", recovery.retired_cluster_uid());
        info!(
            root = self.layout.root(),
            retired_cluster_uid,
            "the store names the cluster that recover_cluster_uid retires; recovering its geo              topics into this cluster"
        );
        match recovery
            .run(
                &self.store,
                &self.layout,
                shard,
                cluster_uid,
                IggyTimestamp::now(),
            )
            .await
        {
            Ok(_) => true,
            Err(error) => {
                error!(
                    root = self.layout.root(),
                    retired_cluster_uid,
                    %error,
                    "the recovery of the retired cluster stopped; it runs again at the next check"
                );
                false
            }
        }
    }

    /// One attempt of [`IdentityCheck::run`] that waits at most the timeout.
    async fn check(&self, cluster_uid: u128, claim: bool) -> IdentityCheck {
        let run = IdentityCheck::run(
            &self.store,
            &self.layout,
            cluster_uid,
            claim,
            !self.conditional_create_checked,
        );
        compio::time::timeout(self.timeout, run)
            .await
            .unwrap_or_else(|_| {
                IdentityCheck::Unreachable(ObjectStoreError::Transport(format!(
                    "the store identity check got no answer within {:?}",
                    self.timeout
                )))
            })
    }

    /// Record what `check` found for `cluster_uid` while a topic kept
    /// `store_data` or not, and return how long to wait before the next
    /// check.
    fn record(
        &mut self,
        check: IdentityCheck,
        cluster_uid: u128,
        store_data: bool,
        frontier: &AppliedFrontier,
        metrics: &ShardMetrics,
    ) -> Duration {
        let (verdict, verified, delay) = match &check {
            IdentityCheck::Verified { .. } => (Verdict::Verified, Some(true), RECHECK_INTERVAL),
            IdentityCheck::Missing => (Verdict::Missing, Some(false), RETRY_INTERVAL),
            IdentityCheck::Foreign(_) => (Verdict::Foreign, Some(false), RECHECK_INTERVAL),
            // An outage leaves the check as it was.
            IdentityCheck::Unreachable(_) => (Verdict::Unreachable, None, RETRY_INTERVAL),
            IdentityCheck::Refused(_) | IdentityCheck::NoConditionalCreate(_) => {
                (Verdict::Refused, Some(false), RECHECK_INTERVAL)
            }
        };
        if verdict == Verdict::Verified {
            self.conditional_create_checked = true;
        }
        let was = self.last.replace(verdict);
        let was_verified = frontier.store_identity_verified();
        if let Some(verified) = verified {
            frontier.set_store_identity_verified(verified);
            frontier.set_store_claimed_by_another(verdict == Verdict::Foreign);
        }
        metrics.set_object_store_identity_verified(frontier.store_identity_verified());
        metrics.set_object_store_unreachable(verdict == Verdict::Unreachable);
        let root = self.layout.root();
        if was == Some(verdict) {
            debug!(root, ?check, "store identity check");
            return delay;
        }
        let cluster_uid = format!("{cluster_uid:032x}");
        match check {
            IdentityCheck::Verified { claimed } => info!(
                root,
                cluster_uid,
                claimed,
                "the object store root belongs to this cluster, so this node runs its uploads and \
                 object deletes"
            ),
            IdentityCheck::Missing if store_data => error!(
                root,
                cluster_uid,
                "the store identity object is missing, and no node claims the root again once a \
                 topic keeps data in the store. This node runs no uploads or object deletes until an operator \
                 restores the object with this cluster_uid, as the server README describes"
            ),
            IdentityCheck::Missing if was_verified => error!(
                root,
                cluster_uid,
                "the store identity object is gone, so this node stops its uploads and object \
                 deletes until replica 0 claims the root again"
            ),
            IdentityCheck::Missing => warn!(
                root,
                cluster_uid,
                "no cluster claimed the object store root yet. Replica 0 claims it while it is \
                 the caught-up metadata primary and no topic keeps data in the store, so while \
                 another replica leads metadata, restart that replica until replica 0 leads. If \
                 replica 0 claimed it already, this node does not reach the store that replica \
                 0 reaches"
            ),
            IdentityCheck::Foreign(found) => error!(
                root,
                cluster_uid,
                found = found.map(|uid| format!("{uid:032x}")),
                "the store identity object names another cluster, an earlier lifetime of this \
                 one, or no cluster at all, so this node uploads, deletes and reads nothing \
                 under the root. The server README says what to do"
            ),
            IdentityCheck::Unreachable(error) => warn!(
                root,
                %error,
                "the object store does not answer the identity check, so \
                 object_store_unreachable is raised until it does"
            ),
            IdentityCheck::Refused(error) => error!(
                root,
                %error,
                "the identity check failed in a way that only an operator can fix, so this node \
                 runs no uploads or object deletes"
            ),
            IdentityCheck::NoConditionalCreate(error) => error!(
                root,
                %error,
                "the object store ignores conditional creates, which fresh objects, segment \
                 uploads, the store identity and recovery records rely on, so this node runs no \
                 uploads or object deletes. Point tiered_storage.backend and \
                 tiered_storage.endpoint at a store that refuses a create over an existing \
                 object: If-None-Match on s3, ifGenerationMatch=0 on gcs"
            ),
        }
        delay
    }
}

impl IdentityCheck {
    /// Claim the root for `cluster_uid` when `claim` allows it, then read the
    /// identity back. When it names this cluster and `check_create` asks for
    /// it, also check that the store refuses a conditional create over it.
    async fn run<S: ObjectStore>(
        store: &S,
        layout: &KeyLayout,
        cluster_uid: u128,
        claim: bool,
        check_create: bool,
    ) -> Self {
        let identity = StoreIdentity { cluster_uid };
        let claimed = if claim {
            match identity.claim(store, layout).await {
                Ok(claimed) => claimed,
                Err(error) => return Self::failed(error),
            }
        } else {
            false
        };
        match StoreIdentity::read(store, layout).await {
            Ok(IdentityRead::Found(found)) if found == identity && check_create => {
                match identity.check_conditional_create(store, layout).await {
                    Ok(()) => Self::Verified { claimed },
                    Err(error @ ObjectStoreError::Unsupported(_)) => {
                        Self::NoConditionalCreate(error)
                    }
                    Err(error) => Self::failed(error),
                }
            }
            Ok(IdentityRead::Found(found)) if found == identity => Self::Verified { claimed },
            Ok(IdentityRead::Found(found)) => Self::Foreign(Some(found.cluster_uid)),
            Ok(IdentityRead::Invalid) => Self::Foreign(None),
            Ok(IdentityRead::Missing) => Self::Missing,
            Err(error) => Self::failed(error),
        }
    }

    const fn failed(error: ObjectStoreError) -> Self {
        match error.class() {
            ErrorClass::Transient => Self::Unreachable(error),
            ErrorClass::NotFound | ErrorClass::PreconditionFailed | ErrorClass::Permanent => {
                Self::Refused(error)
            }
        }
    }
}

/// Whether committed metadata holds a geo topic restored from the store of
/// `cluster_uid`.
fn restores_from(inner: &StreamsInner, cluster_uid: u128) -> bool {
    inner.items.iter().any(|(_, stream)| {
        stream
            .topics
            .iter()
            .any(|(_, topic)| topic.recovered_from == Some(cluster_uid))
    })
}

/// Whether a check may claim the root. Only a caught-up primary knows every
/// topic that keeps data in the store: a replica 0 that is behind, or rebuilt
/// empty, could otherwise claim a new empty store that the other nodes never
/// see. The check after a pass only reads, and the check after any other
/// result claims again, unless `store_data` says that a topic keeps data in
/// the store: a root without the claim is then a store that lost it, or
/// another store, and never a new one.
const fn may_claim(
    claimer: bool,
    last: Option<Verdict>,
    caught_up_primary: bool,
    store_data: bool,
) -> bool {
    claimer && !matches!(last, Some(Verdict::Verified)) && caught_up_primary && !store_data
}

impl TierHandlers {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        uploader: Rc<dyn SegmentUploader>,
        deleter: Rc<dyn ObjectDeleter>,
        publisher: Rc<dyn ManifestPublisher>,
        fresh_uploader: Rc<dyn FreshObjectUploader>,
        long_term_writer: Rc<dyn LongTermWriter>,
        long_term_merger: Rc<dyn LongTermMerger>,
        fresh_sweeper: Rc<dyn FreshObjectSweeper>,
        topic_objects: Rc<dyn TopicObjects>,
    ) -> Self {
        Self {
            uploader,
            deleter,
            publisher,
            fresh_uploader,
            long_term_writer,
            long_term_merger,
            fresh_sweeper,
            topic_objects,
            held: false,
        }
    }

    /// Hand them to `shard` when its node is verified as the owner of the
    /// store, whose `cluster_uid` is `verified`, and take them back when
    /// not. A job in flight ends on the handler that started it.
    pub fn sync<B, MJ, S, SB>(&mut self, shard: &ShellShard<B, MJ, S, SB>, verified: Option<u128>)
    where
        B: ShellBus,
        SB: SuperblockStore,
    {
        // Every fresh object names the cluster that wrote it, which is the
        // one the check found in the store.
        if let Some(cluster_uid) = verified {
            shard.set_fresh_cluster_uid(cluster_uid);
        }
        let verified = verified.is_some();
        if verified == self.held {
            return;
        }
        shard.set_segment_uploader(verified.then(|| Rc::clone(&self.uploader)));
        shard.set_object_deleter(verified.then(|| Rc::clone(&self.deleter)));
        shard.set_manifest_publisher(verified.then(|| Rc::clone(&self.publisher)));
        shard.set_fresh_uploader(verified.then(|| Rc::clone(&self.fresh_uploader)));
        shard.set_long_term_writer(verified.then(|| Rc::clone(&self.long_term_writer)));
        shard.set_long_term_merger(verified.then(|| Rc::clone(&self.long_term_merger)));
        shard.set_fresh_sweeper(verified.then(|| Rc::clone(&self.fresh_sweeper)));
        shard.set_topic_objects(verified.then(|| Rc::clone(&self.topic_objects)));
        self.held = verified;
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, pending, ready};
    use std::ops::Range;

    use bytes::Bytes;
    use futures::executor::block_on;
    use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
    use iggy_binary_protocol::requests::topics::{
        CreateTopicRequest, CreateTopicWithAssignmentsRequest,
    };
    use iggy_binary_protocol::{Operation, WireEncode, WireIdentifier, WireName, WireOptions};
    use iggy_object_store::{
        DeleteOutcome, FsStore, InlineTierExecutor, ListPage, ListRequest, MemoryStore, ObjectMeta,
        ObjectRange, PutBody, PutMode, PutOptions, RecordingStore, StoreCall,
    };
    use metadata::stm::shadow::{ShadowStream, ShadowTopicRequest, ShadowTopicTarget};
    use metadata::stm::{StateHandler, StateMachine as _};
    use partitions::{
        ExecutorDeleter, ExecutorFreshSweeper, ExecutorFreshUploader, ExecutorLongTermMerger,
        ExecutorLongTermWriter, ExecutorManifestPublisher, ExecutorTopicObjects, ExecutorUploader,
    };

    use super::test_stores::IgnoresCondition;
    use super::*;
    use crate::dispatch::test_support::{SpyBus, prepare_message, test_shard};

    const OURS: u128 = 0x0123_4567_89ab_cdef_0011_2233_4455_6677;
    const THEIRS: u128 = 7;

    fn layout() -> KeyLayout {
        KeyLayout::new("iggy").unwrap()
    }

    /// The stream and the one-partition topic that a recovery of
    /// `cluster_uid` restores.
    fn restore_requests(cluster_uid: u128) -> [ShadowTopicRequest; 2] {
        let source = ShadowStream {
            origin: "dc-a".to_owned(),
            cluster_uid,
            stream_id: 0,
        };
        [
            ShadowTopicRequest::RestoreStream {
                source: source.clone(),
                name: WireName::new("orders").unwrap(),
            },
            ShadowTopicRequest::RestoreTopic {
                target: ShadowTopicTarget {
                    stream_id: 0,
                    source,
                    topic_id: 0,
                    created_revision: 5,
                },
                create: Box::new(CreateTopicWithAssignmentsRequest {
                    request: CreateTopicRequest {
                        stream_id: WireIdentifier::numeric(0),
                        partitions_count: 1,
                        name: WireName::new("payments").unwrap(),
                        options: WireOptions::empty(),
                    },
                    derived_options: WireOptions::empty(),
                    partitions: vec![CreatedPartitionAssignment {
                        partition_id: 0,
                        consensus_group_id: 1,
                        initial_view: 0,
                        members: Vec::new(),
                    }],
                }),
            },
        ]
    }

    fn check(store: &impl ObjectStore, cluster_uid: u128, claim: bool) -> IdentityCheck {
        block_on(IdentityCheck::run(
            store,
            &layout(),
            cluster_uid,
            claim,
            true,
        ))
    }

    fn found(store: &impl ObjectStore) -> IdentityRead {
        block_on(StoreIdentity::read(store, &layout())).unwrap()
    }

    /// A store whose every call fails with the error that its function makes.
    struct FailingStore(fn() -> ObjectStoreError);

    impl ObjectStore for FailingStore {
        fn put(
            &self,
            _: &str,
            _: PutBody,
            _: &PutOptions,
        ) -> impl Future<Output = Result<(), ObjectStoreError>> {
            ready(Err((self.0)()))
        }

        fn get_range(
            &self,
            _: &str,
            _: Range<u64>,
        ) -> impl Future<Output = Result<ObjectRange, ObjectStoreError>> {
            ready(Err((self.0)()))
        }

        fn head(&self, _: &str) -> impl Future<Output = Result<ObjectMeta, ObjectStoreError>> {
            ready(Err((self.0)()))
        }

        fn delete(
            &self,
            _: &[String],
        ) -> impl Future<Output = Result<DeleteOutcome, ObjectStoreError>> {
            ready(Err((self.0)()))
        }

        fn list(
            &self,
            _: &ListRequest<'_>,
        ) -> impl Future<Output = Result<ListPage, ObjectStoreError>> {
            ready(Err((self.0)()))
        }
    }

    /// A store that never answers.
    struct SilentStore;

    impl ObjectStore for SilentStore {
        fn put(
            &self,
            _: &str,
            _: PutBody,
            _: &PutOptions,
        ) -> impl Future<Output = Result<(), ObjectStoreError>> {
            pending()
        }

        fn get_range(
            &self,
            _: &str,
            _: Range<u64>,
        ) -> impl Future<Output = Result<ObjectRange, ObjectStoreError>> {
            pending()
        }

        fn head(&self, _: &str) -> impl Future<Output = Result<ObjectMeta, ObjectStoreError>> {
            pending()
        }

        fn delete(
            &self,
            _: &[String],
        ) -> impl Future<Output = Result<DeleteOutcome, ObjectStoreError>> {
            pending()
        }

        fn list(
            &self,
            _: &ListRequest<'_>,
        ) -> impl Future<Output = Result<ListPage, ObjectStoreError>> {
            pending()
        }
    }

    #[test]
    fn given_each_node_state_when_deciding_to_claim_should_claim_only_as_replica_0_primary() {
        // (row, claimer, last check, caught-up primary, data in the store, claims)
        for (row, claimer, last, primary, store_data, claims) in [
            ("replica 1", false, None, true, false, false),
            ("replica 0 as backup", true, None, false, false, false),
            (
                "replica 0 primary with a topic that keeps data in the store",
                true,
                None,
                true,
                true,
                false,
            ),
            (
                "replica 0 primary after a missing claim, with data in the store",
                true,
                Some(Verdict::Missing),
                true,
                true,
                false,
            ),
            (
                "replica 0 primary after a pass",
                true,
                Some(Verdict::Verified),
                true,
                false,
                false,
            ),
            (
                "replica 0 primary at its first check",
                true,
                None,
                true,
                false,
                true,
            ),
            (
                "replica 0 primary after a missing claim",
                true,
                Some(Verdict::Missing),
                true,
                false,
                true,
            ),
            (
                "replica 0 primary after an outage",
                true,
                Some(Verdict::Unreachable),
                true,
                false,
                true,
            ),
        ] {
            assert_eq!(
                may_claim(claimer, last, primary, store_data),
                claims,
                "{row}"
            );
        }
    }

    #[compio::test]
    async fn given_store_that_never_answers_when_checking_should_give_up_at_the_timeout() {
        let checker =
            IdentityChecker::new(SilentStore, layout(), true, Duration::from_millis(10), None);

        for claim in [false, true] {
            assert!(matches!(
                checker.check(OURS, claim).await,
                IdentityCheck::Unreachable(ObjectStoreError::Transport(_))
            ));
        }
    }

    #[compio::test]
    async fn given_check_changes_when_syncing_should_give_and_take_the_handlers() {
        let bus = SpyBus::default();
        let shard = test_shard(&bus, 0, 1, 1);
        let mut handlers = TierHandlers::new(
            Rc::new(ExecutorUploader::new(
                InlineTierExecutor::new(MemoryStore::new()),
                layout(),
            )),
            Rc::new(ExecutorDeleter::new(
                InlineTierExecutor::new(MemoryStore::new()),
                layout(),
            )),
            Rc::new(ExecutorManifestPublisher::new(
                InlineTierExecutor::new(MemoryStore::new()),
                layout(),
            )),
            Rc::new(ExecutorFreshUploader::new(
                InlineTierExecutor::new(MemoryStore::new()),
                layout(),
            )),
            Rc::new(ExecutorLongTermWriter::new(
                InlineTierExecutor::new(MemoryStore::new()),
                layout(),
            )),
            Rc::new(ExecutorLongTermMerger::new(
                InlineTierExecutor::new(MemoryStore::new()),
                layout(),
            )),
            Rc::new(ExecutorFreshSweeper::new(
                InlineTierExecutor::new(MemoryStore::new()),
                layout(),
            )),
            Rc::new(ExecutorTopicObjects::new(
                InlineTierExecutor::new(MemoryStore::new()),
                layout(),
            )),
        );
        let held = |handlers: &mut TierHandlers, verified: bool| {
            handlers.sync(&shard, verified.then_some(OURS));
            [
                shard.segment_uploader().is_some(),
                shard.object_deleter().is_some(),
                shard.manifest_publisher().is_some(),
                shard.fresh_uploader().is_some(),
                shard.long_term_writer().is_some(),
                shard.long_term_merger().is_some(),
                shard.fresh_sweeper().is_some(),
                shard.topic_objects().is_some(),
            ]
        };

        assert_eq!(held(&mut handlers, false), [false; 8]);
        assert_eq!(shard.fresh_cluster_uid(), 0);
        assert_eq!(held(&mut handlers, true), [true; 8]);
        assert_eq!(
            shard.fresh_cluster_uid(),
            OURS,
            "fresh objects name the cluster the check verified"
        );
        assert_eq!(held(&mut handlers, true), [true; 8]);
        assert_eq!(held(&mut handlers, false), [false; 8]);
        assert_eq!(held(&mut handlers, true), [true; 8]);
    }

    #[test]
    fn given_free_root_when_replica_0_checks_should_claim_it_and_verify() {
        let store = MemoryStore::new();

        assert!(matches!(
            check(&store, OURS, true),
            IdentityCheck::Verified { claimed: true }
        ));
        assert_eq!(store.keys(), vec![layout().identity_key()]);
        assert!(matches!(
            check(&store, OURS, true),
            IdentityCheck::Verified { claimed: false }
        ));
    }

    #[test]
    fn given_free_root_when_another_node_checks_should_find_no_claim_and_write_nothing() {
        let store = MemoryStore::new();

        assert!(matches!(check(&store, OURS, false), IdentityCheck::Missing));
        assert!(store.is_empty());
    }

    #[test]
    fn given_root_of_another_cluster_when_replica_0_checks_should_refuse_and_write_nothing() {
        let store = MemoryStore::new();
        let theirs = StoreIdentity {
            cluster_uid: THEIRS,
        };
        assert!(block_on(theirs.claim(&store, &layout())).unwrap());

        assert!(matches!(
            check(&store, OURS, true),
            IdentityCheck::Foreign(Some(THEIRS))
        ));
        assert_eq!(store.keys(), vec![layout().identity_key()]);
        assert_eq!(found(&store), IdentityRead::Found(theirs));
    }

    #[test]
    fn given_object_that_no_cluster_wrote_when_checking_should_treat_root_as_foreign() {
        let store = MemoryStore::new();
        block_on(store.put(
            &layout().identity_key(),
            PutBody::from_bytes(Bytes::from_static(b"not an identity")),
            &PutOptions::default(),
        ))
        .unwrap();

        assert!(matches!(
            check(&store, OURS, true),
            IdentityCheck::Foreign(None)
        ));
    }

    #[test]
    fn given_store_errors_when_checking_should_tell_an_outage_from_a_refusal() {
        let unreachable = FailingStore(|| ObjectStoreError::Transport("refused".to_owned()));
        let no_bucket = FailingStore(|| ObjectStoreError::BucketNotFound("segments".to_owned()));
        let no_access = FailingStore(|| ObjectStoreError::Auth("denied".to_owned()));

        for claim in [false, true] {
            assert!(matches!(
                check(&unreachable, OURS, claim),
                IdentityCheck::Unreachable(_)
            ));
            assert!(matches!(
                check(&no_bucket, OURS, claim),
                IdentityCheck::Refused(_)
            ));
            assert!(matches!(
                check(&no_access, OURS, claim),
                IdentityCheck::Refused(_)
            ));
        }
    }

    #[test]
    fn given_store_that_ignores_conditional_creates_when_checking_should_refuse_it() {
        let store = IgnoresCondition(MemoryStore::new());

        let claimed = check(&store, OURS, true);
        let found = check(&store, OURS, false);

        assert!(
            matches!(
                &claimed,
                IdentityCheck::NoConditionalCreate(ObjectStoreError::Unsupported(_))
            ),
            "{claimed:?}"
        );
        assert!(
            matches!(&found, IdentityCheck::NoConditionalCreate(_)),
            "{found:?}"
        );
        assert_eq!(
            block_on(StoreIdentity::read(&store, &layout())).unwrap(),
            IdentityRead::Found(StoreIdentity { cluster_uid: OURS })
        );
    }

    #[test]
    fn given_no_conditional_create_when_recording_should_refuse_uploads_and_check_it_again() {
        let mut checker = IdentityChecker::new(
            MemoryStore::new(),
            layout(),
            true,
            Duration::from_secs(5),
            None,
        );
        let frontier = AppliedFrontier::new(AppliedFrontier::DEFAULT_READ_BUDGET);
        let metrics = ShardMetrics::for_shard();
        checker.record(
            IdentityCheck::Verified { claimed: false },
            OURS,
            false,
            &frontier,
            &metrics,
        );
        let ignored = || {
            IdentityCheck::NoConditionalCreate(ObjectStoreError::Unsupported(
                "conditional creates".to_owned(),
            ))
        };
        let mut fresh = IdentityChecker::new(
            MemoryStore::new(),
            layout(),
            true,
            Duration::from_secs(5),
            None,
        );

        let delay = fresh.record(ignored(), OURS, false, &frontier, &metrics);

        assert_eq!(delay, RECHECK_INTERVAL);
        assert!(!frontier.store_identity_verified());
        assert!(!frontier.store_claimed_by_another());
        assert!(!fresh.conditional_create_checked);
        assert!(checker.conditional_create_checked);
    }

    /// The check sends its conditional create until the store refuses one,
    /// and never again after that.
    #[compio::test]
    async fn given_a_passed_check_when_checking_again_should_send_no_more_conditional_creates() {
        let store = RecordingStore::new(MemoryStore::new());
        let calls = store.calls();
        let mut checker = IdentityChecker::new(store, layout(), true, Duration::from_secs(5), None);
        let frontier = AppliedFrontier::new(AppliedFrontier::DEFAULT_READ_BUDGET);
        let metrics = ShardMetrics::for_shard();
        let puts = || {
            calls
                .mutations()
                .into_iter()
                .filter(|call| {
                    matches!(
                        call,
                        StoreCall::Put {
                            mode: PutMode::IfAbsent,
                            ..
                        }
                    )
                })
                .count()
        };

        let first = checker.check(OURS, true).await;
        assert!(
            matches!(first, IdentityCheck::Verified { claimed: true }),
            "{first:?}"
        );
        assert_eq!(puts(), 2, "the claim and the check");
        checker.record(first, OURS, false, &frontier, &metrics);
        let second = checker.check(OURS, false).await;

        assert!(
            matches!(second, IdentityCheck::Verified { claimed: false }),
            "{second:?}"
        );
        assert_eq!(puts(), 2);
    }

    /// Two nodes that each keep an `fs` directory at the same path look alike
    /// in their configuration. Only replica 0 claims, so the other node finds
    /// no claim in its own directory.
    #[compio::test]
    async fn given_per_node_fs_directories_when_checking_should_tell_the_stores_apart() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let replica_0 = FsStore::new(first.path());
        let replica_1 = FsStore::new(second.path());
        let shared = FsStore::new(first.path());

        assert!(matches!(
            IdentityCheck::run(&replica_0, &layout(), OURS, true, true).await,
            IdentityCheck::Verified { claimed: true }
        ));
        assert!(matches!(
            IdentityCheck::run(&replica_1, &layout(), OURS, false, true).await,
            IdentityCheck::Missing
        ));
        assert!(matches!(
            IdentityCheck::run(&shared, &layout(), OURS, false, true).await,
            IdentityCheck::Verified { claimed: false }
        ));
    }

    #[test]
    fn given_topics_restored_from_a_cluster_when_its_root_is_checked_should_know_it_is_recovered() {
        let mut inner = StreamsInner::new();
        assert!(!restores_from(&inner, THEIRS));
        let now = IggyTimestamp::now();
        for request in restore_requests(THEIRS) {
            assert_eq!(request.apply(&mut inner, now).code, 0);
        }

        assert!(restores_from(&inner, THEIRS));
        assert!(!restores_from(&inner, OURS));
    }

    /// A node that found the claim before replica 0 restored the topics
    /// checks again once the restore applies, not at the recheck interval.
    #[compio::test]
    async fn given_a_foreign_claim_when_its_restore_applies_during_the_wait_should_end_the_wait() {
        let bus = SpyBus::default();
        let shard = test_shard(&bus, 1, 3, 1);
        let mux_stm = &shard.plane.metadata().mux_stm;
        let mut checker = IdentityChecker::new(
            MemoryStore::new(),
            layout(),
            false,
            Duration::from_secs(5),
            None,
        );
        checker.unrestored_claim = Some(THEIRS);
        let short = Duration::from_millis(10);

        let started = Instant::now();
        checker.wait(mux_stm.streams(), short).await;
        assert!(
            started.elapsed() >= short,
            "no restore, so the wait runs out"
        );

        let restore = async {
            compio::time::sleep(short).await;
            for (request, id) in restore_requests(THEIRS).iter().zip(1..) {
                mux_stm
                    .update(prepare_message(
                        Operation::ShadowTopic,
                        1,
                        id,
                        &request.to_bytes(),
                    ))
                    .unwrap();
            }
        };
        let wait = compio::time::timeout(
            RETRY_INTERVAL,
            checker.wait(mux_stm.streams(), RECHECK_INTERVAL),
        );
        let (waited, ()) = futures::join!(wait, restore);

        assert!(mux_stm.streams().read(|inner| restores_from(inner, THEIRS)));
        assert!(waited.is_ok(), "the restore ends the wait");
    }

    #[test]
    fn given_a_root_of_another_cluster_when_recording_should_mark_it_until_a_check_finds_ours() {
        let mut checker = IdentityChecker::new(
            MemoryStore::new(),
            layout(),
            false,
            Duration::from_secs(5),
            None,
        );
        let frontier = AppliedFrontier::new(AppliedFrontier::DEFAULT_READ_BUDGET);
        let metrics = ShardMetrics::for_shard();
        let mut claimed = |check| {
            checker.record(check, OURS, false, &frontier, &metrics);
            frontier.store_claimed_by_another()
        };

        assert!(!claimed(IdentityCheck::Missing));
        assert!(claimed(IdentityCheck::Foreign(Some(THEIRS))));
        assert!(
            claimed(IdentityCheck::Unreachable(ObjectStoreError::Transport(
                "reset".to_owned()
            ))),
            "an outage leaves it as it was"
        );
        assert!(!claimed(IdentityCheck::Verified { claimed: false }));
    }

    /// Each row gives what the frontier records, `object_store_unreachable`,
    /// and the wait before the next check.
    #[test]
    fn given_checks_when_recording_should_track_the_check_and_both_gauges() {
        let mut checker = IdentityChecker::new(
            MemoryStore::new(),
            layout(),
            true,
            Duration::from_secs(5),
            None,
        );
        let frontier = AppliedFrontier::new(AppliedFrontier::DEFAULT_READ_BUDGET);
        let metrics = ShardMetrics::for_shard();
        let mut record = |check| {
            let delay = checker.record(check, OURS, false, &frontier, &metrics);
            assert!(
                !frontier.tiered_storage_ready(),
                "passing the check admits no tiered topic"
            );
            assert_eq!(
                metrics.object_store_identity_verified_value(),
                frontier.store_identity_verified()
            );
            (
                frontier.store_identity_verified(),
                metrics.object_store_unreachable_value(),
                delay,
            )
        };
        let outage = || IdentityCheck::Unreachable(ObjectStoreError::Transport("reset".to_owned()));

        assert_eq!(record(outage()), (false, true, RETRY_INTERVAL));
        assert_eq!(
            record(IdentityCheck::Verified { claimed: true }),
            (true, false, RECHECK_INTERVAL)
        );
        assert_eq!(
            record(outage()),
            (true, true, RETRY_INTERVAL),
            "an outage leaves the check as it was"
        );
        assert_eq!(
            record(IdentityCheck::Verified { claimed: false }),
            (true, false, RECHECK_INTERVAL)
        );
        assert_eq!(
            record(IdentityCheck::Missing),
            (false, false, RETRY_INTERVAL)
        );
        assert_eq!(
            record(IdentityCheck::Verified { claimed: false }),
            (true, false, RECHECK_INTERVAL)
        );
        assert_eq!(
            record(IdentityCheck::Foreign(Some(THEIRS))),
            (false, false, RECHECK_INTERVAL)
        );
        assert_eq!(
            record(IdentityCheck::Refused(ObjectStoreError::Auth(
                "denied".to_owned()
            ))),
            (false, false, RECHECK_INTERVAL)
        );
    }
}

/// Stores that break a rule of the contract, for the tests of the checks.
#[cfg(test)]
pub mod test_stores {
    use std::ops::Range;

    use iggy_object_store::{
        DeleteOutcome, ListPage, ListRequest, MemoryStore, ObjectMeta, ObjectRange, ObjectStore,
        ObjectStoreError, PutBody, PutMode, PutOptions,
    };

    /// A store that takes every put as an overwrite, as a store does that
    /// ignores `If-None-Match`.
    pub struct IgnoresCondition(pub MemoryStore);

    impl ObjectStore for IgnoresCondition {
        async fn put(
            &self,
            key: &str,
            body: PutBody,
            options: &PutOptions,
        ) -> Result<(), ObjectStoreError> {
            let overwrite = PutOptions {
                mode: PutMode::Overwrite,
                ..options.clone()
            };
            self.0.put(key, body, &overwrite).await
        }

        async fn get_range(
            &self,
            key: &str,
            range: Range<u64>,
        ) -> Result<ObjectRange, ObjectStoreError> {
            self.0.get_range(key, range).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
            self.0.head(key).await
        }

        async fn delete(&self, keys: &[String]) -> Result<DeleteOutcome, ObjectStoreError> {
            self.0.delete(keys).await
        }

        async fn list(&self, request: &ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
            self.0.list(request).await
        }
    }
}
