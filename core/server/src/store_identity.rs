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
//! topic is tiered, so while another replica leads metadata, nothing claims
//! the root. Until a topic is tiered, it claims again after any check that
//! did not pass, so a replica 0 whose `fs` mount drops claims the empty
//! mount point. The check also cannot tell two stores apart when replica 0
//! reaches an empty store and the other nodes reach one that holds an
//! earlier claim of this cluster.
//!
//! A store that stops answering after the check passed is an outage, and the
//! node still passes: uploads retry. `object_store_unreachable` is 1 while
//! the store does not answer the check.

use std::pin::pin;
use std::rc::Rc;
use std::time::Duration;

use consensus::MetadataHandle;
use futures::future::{Either, select};
use iggy_object_store::{
    ErrorClass, IdentityRead, KeyLayout, ObjectStore, ObjectStoreError, StoreIdentity,
};
use journal::superblock::SuperblockStore;
use metadata::AppliedFrontier;
use metadata::change_log::ChangeLog;
use metadata::impls::metadata::StreamsFrontend;
use partitions::{ObjectDeleter, SegmentUploader};
use shard::Receiver;
use shard::metrics::ShardMetrics;
use tracing::{debug, error, info, warn};

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Verified,
    Missing,
    Foreign,
    Unreachable,
    Refused,
}

/// The uploader and the object deleter of a shard. The shard holds them only
/// while its node passes the store identity check.
pub struct TierHandlers {
    uploader: Rc<dyn SegmentUploader>,
    deleter: Rc<dyn ObjectDeleter>,
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
        handlers.sync(&shard, verified);
        // `Ok`: the stop fired, or its sender is gone. `Err`: the wait ran
        // out.
        if compio::time::timeout(delay, stop.recv()).await.is_ok() {
            return;
        }
    }
}

impl<S: ObjectStore> IdentityChecker<S> {
    pub const fn new(store: S, layout: KeyLayout, claimer: bool, timeout: Duration) -> Self {
        Self {
            store,
            layout,
            claimer,
            timeout,
            last: None,
        }
    }

    /// Check once with what `shard` knows, record the result, and return how
    /// long to wait before the next check.
    async fn step(&mut self, shard: &ServerShard) -> Duration {
        let metadata = shard.plane.metadata();
        let Some(cluster_uid) = metadata.change_log().read(ChangeLog::log_id).flatten() else {
            return CLUSTER_UID_WAIT;
        };
        let tiered = metadata.mux_stm.streams().has_tiered_topic();
        let claim = may_claim(
            self.claimer,
            self.last,
            metadata.is_caught_up_primary(),
            tiered,
        );
        let check = self.check(cluster_uid, claim).await;
        self.record(
            check,
            cluster_uid,
            tiered,
            metadata.applied_frontier(),
            shard.metrics(),
        )
    }

    /// One attempt of [`IdentityCheck::run`] that waits at most the timeout.
    async fn check(&self, cluster_uid: u128, claim: bool) -> IdentityCheck {
        let run = IdentityCheck::run(&self.store, &self.layout, cluster_uid, claim);
        compio::time::timeout(self.timeout, run)
            .await
            .unwrap_or_else(|_| {
                IdentityCheck::Unreachable(ObjectStoreError::Transport(format!(
                    "the store identity check got no answer within {:?}",
                    self.timeout
                )))
            })
    }

    /// Record what `check` found for `cluster_uid` while a topic was
    /// `tiered` or not, and return how long to wait before the next check.
    fn record(
        &mut self,
        check: IdentityCheck,
        cluster_uid: u128,
        tiered: bool,
        frontier: &AppliedFrontier,
        metrics: &ShardMetrics,
    ) -> Duration {
        let (verdict, verified, delay) = match &check {
            IdentityCheck::Verified { .. } => (Verdict::Verified, Some(true), RECHECK_INTERVAL),
            IdentityCheck::Missing => (Verdict::Missing, Some(false), RETRY_INTERVAL),
            IdentityCheck::Foreign(_) => (Verdict::Foreign, Some(false), RECHECK_INTERVAL),
            // An outage leaves the check as it was.
            IdentityCheck::Unreachable(_) => (Verdict::Unreachable, None, RETRY_INTERVAL),
            IdentityCheck::Refused(_) => (Verdict::Refused, Some(false), RECHECK_INTERVAL),
        };
        let was = self.last.replace(verdict);
        let was_verified = frontier.store_identity_verified();
        if let Some(verified) = verified {
            frontier.set_store_identity_verified(verified);
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
            IdentityCheck::Missing if tiered => error!(
                root,
                cluster_uid,
                "the store identity object is missing, and no node claims the root again once a \
                 topic is tiered. This node runs no uploads or object deletes until an operator \
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
                 the caught-up metadata primary and no topic is tiered, so while another \
                 replica leads metadata, restart that replica until replica 0 leads. If replica \
                 0 claimed it already, this node does not reach the store that replica 0 \
                 reaches"
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
        }
        delay
    }
}

impl IdentityCheck {
    /// Claim the root for `cluster_uid` when `claim` allows it, then read the
    /// identity back.
    async fn run<S: ObjectStore>(
        store: &S,
        layout: &KeyLayout,
        cluster_uid: u128,
        claim: bool,
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

/// Whether a check may claim the root. Only a caught-up primary knows every
/// tiered topic: a replica 0 that is behind, or rebuilt empty, could
/// otherwise claim a new empty store that the other nodes never see. The
/// check after a pass only reads, and the check after any other result
/// claims again.
const fn may_claim(
    claimer: bool,
    last: Option<Verdict>,
    caught_up_primary: bool,
    tiered: bool,
) -> bool {
    claimer && !matches!(last, Some(Verdict::Verified)) && caught_up_primary && !tiered
}

impl TierHandlers {
    pub fn new(uploader: Rc<dyn SegmentUploader>, deleter: Rc<dyn ObjectDeleter>) -> Self {
        Self {
            uploader,
            deleter,
            held: false,
        }
    }

    /// Hand both to `shard` when its node is `verified`, and take both back
    /// when not. A job in flight ends on the handler that started it.
    fn sync<B, MJ, S, SB>(&mut self, shard: &ShellShard<B, MJ, S, SB>, verified: bool)
    where
        B: ShellBus,
        SB: SuperblockStore,
    {
        if verified == self.held {
            return;
        }
        shard.set_segment_uploader(verified.then(|| Rc::clone(&self.uploader)));
        shard.set_object_deleter(verified.then(|| Rc::clone(&self.deleter)));
        self.held = verified;
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, pending, ready};
    use std::ops::Range;

    use bytes::Bytes;
    use futures::executor::block_on;
    use iggy_object_store::{
        DeleteOutcome, FsStore, InlineTierExecutor, ListPage, ListRequest, MemoryStore, ObjectMeta,
        ObjectRange, PutBody, PutOptions,
    };
    use partitions::{ExecutorDeleter, ExecutorUploader};

    use super::*;
    use crate::dispatch::test_support::{SpyBus, test_shard};

    const OURS: u128 = 0x0123_4567_89ab_cdef_0011_2233_4455_6677;
    const THEIRS: u128 = 7;

    fn layout() -> KeyLayout {
        KeyLayout::new("iggy").unwrap()
    }

    fn check(store: &impl ObjectStore, cluster_uid: u128, claim: bool) -> IdentityCheck {
        block_on(IdentityCheck::run(store, &layout(), cluster_uid, claim))
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
        // (row, claimer, last check, caught-up primary, tiered topic, claims)
        for (row, claimer, last, primary, tiered, claims) in [
            ("replica 1", false, None, true, false, false),
            ("replica 0 as backup", true, None, false, false, false),
            (
                "replica 0 primary with a tiered topic",
                true,
                None,
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
            assert_eq!(may_claim(claimer, last, primary, tiered), claims, "{row}");
        }
    }

    #[compio::test]
    async fn given_store_that_never_answers_when_checking_should_give_up_at_the_timeout() {
        let checker = IdentityChecker::new(SilentStore, layout(), true, Duration::from_millis(10));

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
        );
        let held = |handlers: &mut TierHandlers, verified| {
            handlers.sync(&shard, verified);
            (
                shard.segment_uploader().is_some(),
                shard.object_deleter().is_some(),
            )
        };

        assert_eq!(held(&mut handlers, false), (false, false));
        assert_eq!(held(&mut handlers, true), (true, true));
        assert_eq!(held(&mut handlers, true), (true, true));
        assert_eq!(held(&mut handlers, false), (false, false));
        assert_eq!(held(&mut handlers, true), (true, true));
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
            IdentityCheck::run(&replica_0, &layout(), OURS, true).await,
            IdentityCheck::Verified { claimed: true }
        ));
        assert!(matches!(
            IdentityCheck::run(&replica_1, &layout(), OURS, false).await,
            IdentityCheck::Missing
        ));
        assert!(matches!(
            IdentityCheck::run(&shared, &layout(), OURS, false).await,
            IdentityCheck::Verified { claimed: false }
        ));
    }

    /// Each row gives what the frontier records, `object_store_unreachable`,
    /// and the wait before the next check.
    #[test]
    fn given_checks_when_recording_should_track_the_check_and_both_gauges() {
        let mut checker =
            IdentityChecker::new(MemoryStore::new(), layout(), true, Duration::from_secs(5));
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
