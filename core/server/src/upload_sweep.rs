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

//! The sweep of the uploads in parts that a crash or a cancelled put left in
//! the store: the parts of an S3 multipart upload, or the temporary objects
//! of a GCS upload. Shard 0 of the caught-up metadata primary runs it, while
//! its node passes the store identity check, and only once the store
//! identity object it reads names this cluster.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use configs::tiered_storage::{TieredStorageBackend, TieredStorageConfig};
use consensus::MetadataHandle;
use iggy_common::IggyTimestamp;
use iggy_object_store::{TierScope, TierScopeToken};
use message_bus::MessageBus;
use partitions::{ObjectDeleter, sweep_interval};
use tracing::{debug, info, warn};

use crate::shell::ServerShard;
use crate::store_identity::own_cluster_uid;

/// The most pages one sweep removes. The next sweep goes on with the rest.
const PAGES_PER_SWEEP: usize = 64;

pub struct UploadSweep {
    /// `None` when the sweep is off: no backend, or
    /// `abandoned_upload_grace` at zero.
    grace: Option<Duration>,
    state: Rc<RefCell<State>>,
}

#[derive(Default)]
struct State {
    /// The sweep in flight. Dropping the scope cancels it.
    running: Option<TierScope>,
    /// When the next sweep may start, in microseconds since the Unix epoch.
    next_sweep_at: u64,
}

/// What one sweep removed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Removed {
    pub uploads: u64,
    pub kept: u64,
    pub failed: u64,
}

/// Why a sweep removed nothing more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stopped {
    /// Its scope was cancelled.
    Cancelled,
    /// The store identity object names another cluster, or could not be
    /// read.
    NotOurs,
    /// A page failed.
    Failed,
}

impl UploadSweep {
    #[must_use]
    pub fn new(config: &TieredStorageConfig) -> Self {
        let grace = config.abandoned_upload_grace.get_duration();
        Self {
            grace: (config.backend != TieredStorageBackend::None && !grace.is_zero())
                .then_some(grace),
            state: Rc::default(),
        }
    }

    /// Start a sweep in a detached task when one is due at `now`. A shard
    /// that may not sweep stops the sweep in flight.
    pub fn pass(&self, shard: &Rc<ServerShard>, now: IggyTimestamp) {
        let Some(grace) = self.grace else {
            return;
        };
        let (Some(deleter), Some(cluster_uid)) = (sweeping_deleter(shard), own_cluster_uid(shard))
        else {
            self.stop();
            return;
        };
        let mut state = self.state.borrow_mut();
        if state.running.is_some() || now.as_micros() < state.next_sweep_at {
            return;
        }
        let interval = u64::try_from(sweep_interval(grace).as_micros()).unwrap_or(u64::MAX);
        state.next_sweep_at = now.as_micros().saturating_add(interval);
        let scope = TierScope::new();
        let token = scope.token();
        state.running = Some(scope);
        drop(state);
        let state = Rc::clone(&self.state);
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        shard.bus.spawn(async move {
            let swept = sweep_abandoned(&*deleter, cluster_uid, grace, token).await;
            state.borrow_mut().running = None;
            match swept {
                Ok(removed) if removed.uploads > 0 || removed.failed > 0 => info!(
                    removed = removed.uploads,
                    kept = removed.kept,
                    failed = removed.failed,
                    "swept the uploads in parts that never completed"
                ),
                Ok(removed) => debug!(kept = removed.kept, "no abandoned upload in parts"),
                Err(stopped) => debug!(?stopped, "the sweep of abandoned uploads stopped"),
            }
        });
    }

    /// Cancel the sweep in flight.
    pub fn stop(&self) {
        self.state.borrow_mut().running = None;
    }
}

/// Removes the uploads in parts that began more than `grace` ago, page by
/// page, once the store identity object names `cluster_uid`. Another
/// cluster's root under the same prefix is never swept.
///
/// # Errors
/// [`Stopped`] when the scope was cancelled, the identity object is not this
/// cluster's, or a page failed.
pub async fn sweep_abandoned(
    deleter: &dyn ObjectDeleter,
    cluster_uid: u128,
    grace: Duration,
    token: TierScopeToken,
) -> Result<Removed, Stopped> {
    match deleter.read_claim(token.clone()).await {
        Err(_) => return Err(Stopped::Cancelled),
        Ok(Ok(Some(claim))) if claim.cluster_uid == cluster_uid => {}
        Ok(Ok(found)) => {
            warn!(
                found = found.map(|claim| format!("{:032x}", claim.cluster_uid)),
                "the store identity object does not name this cluster, so the sweep of \
                 abandoned uploads removes nothing"
            );
            return Err(Stopped::NotOurs);
        }
        Ok(Err(error)) => {
            warn!(
                %error,
                "reading the store identity object for the sweep of abandoned uploads failed"
            );
            return Err(Stopped::NotOurs);
        }
    }
    let mut removed = Removed::default();
    let mut after = None;
    for _ in 0..PAGES_PER_SWEEP {
        let page = match deleter.sweep_uploads(grace, after, token.clone()).await {
            Err(_) => return Err(Stopped::Cancelled),
            Ok(Err(error)) => {
                warn!(%error, "a page of the sweep of abandoned uploads failed");
                return Err(Stopped::Failed);
            }
            Ok(Ok(page)) => page,
        };
        removed.uploads += page.removed;
        removed.kept += page.kept;
        removed.failed += page.failed;
        after = page.next;
        if after.is_none() {
            break;
        }
    }
    Ok(removed)
}

/// The deleter of `shard` while it may sweep: shard 0 of the caught-up
/// metadata primary, while its node passes the store identity check.
fn sweeping_deleter(shard: &ServerShard) -> Option<Rc<dyn ObjectDeleter>> {
    let metadata = shard.plane.metadata();
    if shard.id != 0 || !metadata.is_caught_up_primary() {
        return None;
    }
    shard.object_deleter()
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::time::SystemTime;

    use futures::FutureExt;
    use futures::future::LocalBoxFuture;
    use iggy_object_store::{ObjectStoreError, SweptUploads, TierScope};
    use partitions::{
        ClaimReadOutcome, ClaimStamp, ObjectDeleteJob, ObjectDeleteOutcome, RootListOutcome,
        UploadSweepOutcome,
    };

    use super::*;

    const OURS: u128 = 0xa;
    const THEIRS: u128 = 0xb;

    /// A deleter whose claim names `claimed`, and that answers each page of
    /// the sweep with the next of `pages`.
    struct Sweeps {
        claimed: Option<u128>,
        pages: RefCell<Vec<SweptUploads>>,
        asked: Cell<usize>,
        grace: Cell<Option<Duration>>,
    }

    impl Sweeps {
        fn new(claimed: Option<u128>, pages: Vec<SweptUploads>) -> Self {
            Self {
                claimed,
                pages: RefCell::new(pages),
                asked: Cell::new(0),
                grace: Cell::new(None),
            }
        }
    }

    impl ObjectDeleter for Sweeps {
        fn run(
            &self,
            _job: ObjectDeleteJob,
            _scope: TierScopeToken,
        ) -> LocalBoxFuture<'static, ObjectDeleteOutcome> {
            unreachable!("the sweep of uploads deletes no incarnation")
        }

        fn read_claim(&self, _scope: TierScopeToken) -> LocalBoxFuture<'static, ClaimReadOutcome> {
            let claim = self.claimed.map(|cluster_uid| ClaimStamp {
                cluster_uid,
                etag: "1".to_owned(),
                claimed_at: SystemTime::UNIX_EPOCH,
            });
            std::future::ready(Ok(Ok(claim))).boxed_local()
        }

        fn list_root(
            &self,
            _after: Option<String>,
            _scope: TierScopeToken,
        ) -> LocalBoxFuture<'static, RootListOutcome> {
            unreachable!("the sweep of uploads lists no root")
        }

        fn sweep_uploads(
            &self,
            older_than: Duration,
            _after: Option<String>,
            _scope: TierScopeToken,
        ) -> LocalBoxFuture<'static, UploadSweepOutcome> {
            self.asked.set(self.asked.get() + 1);
            self.grace.set(Some(older_than));
            let page = self.pages.borrow_mut().remove(0);
            std::future::ready(Ok(Ok(page))).boxed_local()
        }
    }

    fn page(removed: u64, kept: u64, next: Option<&str>) -> SweptUploads {
        SweptUploads {
            removed,
            kept,
            failed: 0,
            next: next.map(str::to_owned),
        }
    }

    #[compio::test]
    async fn given_our_claim_when_sweeping_should_remove_every_page_with_the_grace() {
        let deleter = Sweeps::new(Some(OURS), vec![page(2, 1, Some("next")), page(1, 0, None)]);
        let grace = Duration::from_secs(3600);

        let removed = sweep_abandoned(&deleter, OURS, grace, TierScope::new().token()).await;

        assert_eq!(
            removed,
            Ok(Removed {
                uploads: 3,
                kept: 1,
                failed: 0
            })
        );
        assert_eq!(deleter.asked.get(), 2);
        assert_eq!(deleter.grace.get(), Some(grace));
    }

    #[compio::test]
    async fn given_another_clusters_claim_or_none_when_sweeping_should_remove_nothing() {
        for claimed in [Some(THEIRS), None] {
            let deleter = Sweeps::new(claimed, vec![page(5, 0, None)]);

            let removed = sweep_abandoned(
                &deleter,
                OURS,
                Duration::from_secs(3600),
                TierScope::new().token(),
            )
            .await;

            assert_eq!(removed, Err(Stopped::NotOurs), "{claimed:?}");
            assert_eq!(deleter.asked.get(), 0, "{claimed:?}");
        }
    }

    #[compio::test]
    async fn given_an_unreadable_claim_when_sweeping_should_remove_nothing() {
        struct Unreadable(Sweeps);

        impl ObjectDeleter for Unreadable {
            fn run(
                &self,
                job: ObjectDeleteJob,
                scope: TierScopeToken,
            ) -> LocalBoxFuture<'static, ObjectDeleteOutcome> {
                self.0.run(job, scope)
            }

            fn read_claim(
                &self,
                _scope: TierScopeToken,
            ) -> LocalBoxFuture<'static, ClaimReadOutcome> {
                std::future::ready(Ok(Err(ObjectStoreError::Unavailable("503".to_owned()))))
                    .boxed_local()
            }

            fn list_root(
                &self,
                after: Option<String>,
                scope: TierScopeToken,
            ) -> LocalBoxFuture<'static, RootListOutcome> {
                self.0.list_root(after, scope)
            }

            fn sweep_uploads(
                &self,
                older_than: Duration,
                after: Option<String>,
                scope: TierScopeToken,
            ) -> LocalBoxFuture<'static, UploadSweepOutcome> {
                self.0.sweep_uploads(older_than, after, scope)
            }
        }
        let deleter = Unreadable(Sweeps::new(Some(OURS), vec![page(5, 0, None)]));

        let removed = sweep_abandoned(
            &deleter,
            OURS,
            Duration::from_secs(3600),
            TierScope::new().token(),
        )
        .await;

        assert_eq!(removed, Err(Stopped::NotOurs));
        assert_eq!(deleter.0.asked.get(), 0);
    }
}
