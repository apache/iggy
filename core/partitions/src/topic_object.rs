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

//! The objects of geo topics in this cluster's store, and the check that the
//! primary of a geo topic's partition runs before it takes a produce.
//!
//! Other clusters adopt a geo topic from its object, see
//! [`iggy_object_store::TopicObject`]. The metadata primary stores the
//! object after the topic commits, so a produce can reach a partition first.
//! Data that the partition took then could reach a reader that cannot
//! describe its topic. So the partition refuses messages with a transient
//! error, which the SDKs retry, until its primary reads the object of a live
//! topic back from the store.
//!
//! What the primary found lasts while it stays the primary of the view it
//! read in. A replica that becomes the primary of a later view reads again.

use std::time::Duration;

use consensus::Consensus;
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use iggy_common::IggyTimestamp;
use iggy_object_store::{
    KeyLayout, ListRequest, MAX_TOPIC_OBJECT_LEN, ObjectPrefix, ObjectStore, ObjectStoreError,
    PositionsKey, PositionsObject, TierExecutor, TierJobCancelled, TierJobClass, TierJobRequest,
    TierScope, TierScopeToken, TopicKey, TopicObject, TopicRead,
};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use tracing::{debug, warn};

use crate::IggyPartition;
use crate::object_delete::{delete_incarnation, delete_objects};
use crate::origin_recovery::RecoveryPlan;

/// How long the primary of a geo topic's partition waits after a read that
/// did not find the object of a live topic before it reads again.
pub const TOPIC_OBJECT_RETRY: Duration = Duration::from_secs(1);

/// What a job on topic objects ended as: the store's answer, or that the
/// job's scope was cancelled first.
pub type TopicObjectOutcome<T> = Result<Result<T, ObjectStoreError>, TierJobCancelled>;

/// The jobs on the geo topic objects of this cluster's store, behind a
/// pointer, so the shard does not carry the executor's type.
pub trait TopicObjects {
    /// Read the object of the topic incarnation `key`.
    fn read(
        &self,
        key: TopicKey,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, TopicObjectOutcome<TopicRead>>;

    /// Store `object` under its key, over any object there.
    fn put(
        &self,
        object: TopicObject,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, TopicObjectOutcome<()>>;

    /// The keys of every topic object under the root, in key order.
    fn list(
        &self,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, TopicObjectOutcome<Vec<TopicKey>>>;

    /// Delete every segment object and manifest of the first
    /// `partition_count` partitions of the topic incarnation `key`, and then
    /// the topic's object. Returns how many objects are still to delete. The
    /// topic's object stays while any other object does.
    fn delete_topic(
        &self,
        key: TopicKey,
        partition_count: u32,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, TopicObjectOutcome<usize>>;

    /// Read the plan that the recovery of `retired_cluster_uid` stored for
    /// the partition incarnation `prefix`, `None` when it stored none.
    fn read_recovery_plan(
        &self,
        retired_cluster_uid: u128,
        prefix: ObjectPrefix,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, TopicObjectOutcome<Option<RecoveryPlan>>>;

    /// Store `body`, an encoded [`iggy_object_store::PositionsObject`], under
    /// `key`, over any object there.
    fn put_positions(
        &self,
        key: PositionsKey,
        body: Vec<u8>,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, TopicObjectOutcome<()>>;

    /// Delete the positions object under `key`.
    fn delete_positions(
        &self,
        key: PositionsKey,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, TopicObjectOutcome<()>>;
}

/// The jobs on topic objects, through a tier executor.
///
/// A read goes in the demand fetch class, because a produce waits for it. A
/// put goes in the upload class, as a manifest does, because the partitions
/// of a new topic wait for it. A listing goes in the housekeeping class, and
/// the delete of a topic, its listings included, is one job of the delete
/// class.
pub struct ExecutorTopicObjects<E> {
    executor: E,
    layout: KeyLayout,
}

impl<E> ExecutorTopicObjects<E> {
    #[must_use]
    pub const fn new(executor: E, layout: KeyLayout) -> Self {
        Self { executor, layout }
    }

    /// The executor the jobs go through.
    #[must_use]
    pub const fn executor(&self) -> &E {
        &self.executor
    }
}

impl<E: TierExecutor> TopicObjects for ExecutorTopicObjects<E> {
    fn read(
        &self,
        key: TopicKey,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, TopicObjectOutcome<TopicRead>> {
        let layout = self.layout.clone();
        let request = TierJobRequest::new(TierJobClass::DemandFetch, scope)
            .with_bytes(MAX_TOPIC_OBJECT_LEN as u64);
        self.executor
            .submit(request, move |store| async move {
                TopicObject::read(&*store, &layout, &key).await
            })
            .boxed_local()
    }

    fn put(
        &self,
        object: TopicObject,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, TopicObjectOutcome<()>> {
        let layout = self.layout.clone();
        let request = TierJobRequest::new(TierJobClass::Upload, scope)
            .with_bytes(MAX_TOPIC_OBJECT_LEN as u64);
        self.executor
            .submit(request, move |store| async move {
                object.put(&*store, &layout).await
            })
            .boxed_local()
    }

    fn list(
        &self,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, TopicObjectOutcome<Vec<TopicKey>>> {
        let layout = self.layout.clone();
        let request = TierJobRequest::new(TierJobClass::Housekeeping, scope);
        self.executor
            .submit(request, move |store| async move {
                list_topic_keys(&*store, &layout).await
            })
            .boxed_local()
    }

    fn delete_topic(
        &self,
        key: TopicKey,
        partition_count: u32,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, TopicObjectOutcome<usize>> {
        let layout = self.layout.clone();
        let request = TierJobRequest::new(TierJobClass::Delete, scope);
        self.executor
            .submit(request, move |store| async move {
                delete_topic(&*store, &layout, &key, partition_count).await
            })
            .boxed_local()
    }

    fn read_recovery_plan(
        &self,
        retired_cluster_uid: u128,
        prefix: ObjectPrefix,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, TopicObjectOutcome<Option<RecoveryPlan>>> {
        let layout = self.layout.clone();
        let request = TierJobRequest::new(TierJobClass::DemandFetch, scope);
        self.executor
            .submit(request, move |store| async move {
                RecoveryPlan::read(&*store, &layout, retired_cluster_uid, &prefix).await
            })
            .boxed_local()
    }

    /// In the housekeeping class: positions follow commits by a second, and
    /// nothing a client waits for queues behind them.
    fn put_positions(
        &self,
        key: PositionsKey,
        body: Vec<u8>,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, TopicObjectOutcome<()>> {
        let layout = self.layout.clone();
        let request =
            TierJobRequest::new(TierJobClass::Housekeeping, scope).with_bytes(body.len() as u64);
        self.executor
            .submit(request, move |store| async move {
                PositionsObject::put_encoded(&*store, &layout, &key, body).await
            })
            .boxed_local()
    }

    fn delete_positions(
        &self,
        key: PositionsKey,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, TopicObjectOutcome<()>> {
        let layout = self.layout.clone();
        let request = TierJobRequest::new(TierJobClass::Delete, scope);
        self.executor
            .submit(request, move |store| async move {
                let key = layout.positions_key(&key);
                let outcome = store.delete(std::slice::from_ref(&key)).await?;
                match outcome.failed.into_iter().next() {
                    None => Ok(()),
                    Some(failure) => Err(failure.error),
                }
            })
            .boxed_local()
    }
}

/// The keys of every topic object under the root of `layout`, in key order.
///
/// # Errors
/// The store's error for the page that failed.
pub async fn list_topic_keys<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
) -> Result<Vec<TopicKey>, ObjectStoreError> {
    let listing_prefix = layout.topics_prefix();
    let mut keys = Vec::new();
    let mut continuation: Option<String> = None;
    loop {
        let request = ListRequest::new(&listing_prefix);
        let request = continuation
            .as_deref()
            .map_or(request, |continuation| request.after(continuation));
        let page = store.list(&request).await?;
        // A longer root shares the listing prefix, and its keys do not parse.
        keys.extend(
            page.objects
                .iter()
                .filter_map(|object| layout.parse_topic_key(&object.key)),
        );
        continuation = page.next;
        if continuation.is_none() {
            return Ok(keys);
        }
    }
}

/// Delete the objects of each partition of the topic incarnation `key`, and
/// then the topic's object. Returns how many objects are still to delete.
///
/// # Errors
/// The store's error for a listing that failed.
pub async fn delete_topic<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    key: &TopicKey,
    partition_count: u32,
) -> Result<usize, ObjectStoreError> {
    for partition_id in 0..u64::from(partition_count) {
        let left = delete_incarnation(store, layout, &key.partition_prefix(partition_id)).await?;
        if left > 0 {
            // The topic's object is the one object left to count besides them.
            return Ok(left + 1);
        }
    }
    Ok(delete_objects(store, &[layout.topic_key(key)]).await.len())
}

/// What the primary of a geo topic's partition found of the topic's object.
/// Nothing here is persisted.
#[derive(Default)]
pub struct TopicObjectCheck {
    /// The view this state belongs to, `None` until the replica acts as the
    /// primary.
    view: Option<u32>,
    /// Whether this view read the object of a live topic.
    found: bool,
    /// The read in flight, and the scope whose end stops it.
    running: Option<(u64, TierScope)>,
    /// Number of the last read this partition started. It survives a change
    /// of view, so a late result of an earlier view never passes for the
    /// result of a read of this one.
    jobs: u64,
    /// When the next read may start, in microseconds since the Unix epoch.
    retry_at: u64,
    /// Reads of this view that did not find the object of a live topic. Only
    /// the first one warns, so an outage of the store does not log once a
    /// second for each partition.
    misses: u32,
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// Whether the partition takes messages: a partition of a normal topic
    /// always does, and a geo topic's once this view read the topic's
    /// object.
    #[must_use]
    pub fn takes_messages_of_its_topic(&self) -> bool {
        !self.runtime_options.geo_topic
            || (self.topic_object_check.found
                && self.topic_object_check.view == Some(self.consensus().view()))
    }

    /// The incarnation of this partition's topic, whose object the primary
    /// reads.
    #[must_use]
    pub fn topic_key(&self) -> TopicKey {
        let namespace = self.namespace();
        TopicKey {
            stream_id: namespace.stream_id() as u64,
            topic_id: namespace.topic_id() as u64,
            created_revision: self.created_revision(),
        }
    }

    /// Start the read of the topic's object, when the partition is a geo
    /// topic's, this replica is its primary in normal status, this view has
    /// not read the object of a live topic, no read is in flight, and the
    /// last read that failed is [`TOPIC_OBJECT_RETRY`] old by `now`. The
    /// caller runs it under the returned scope and hands the result to
    /// [`Self::finish_topic_object_check`] with the returned number.
    ///
    /// A replica that is not the primary cancels the read it started, so a
    /// pass over a demoted replica is what stops its read.
    pub fn begin_topic_object_check(
        &mut self,
        now: IggyTimestamp,
    ) -> Option<(u64, TopicKey, TierScopeToken)> {
        if !self.runtime_options.geo_topic {
            return None;
        }
        if !self.checks_topic_object() {
            self.reset_topic_object_check(None);
            return None;
        }
        let key = self.topic_key();
        let check = self.topic_object_check_of_this_view();
        if check.found || check.running.is_some() || now.as_micros() < check.retry_at {
            return None;
        }
        let scope = TierScope::new();
        let token = scope.token();
        check.jobs += 1;
        let id = check.jobs;
        check.running = Some((id, scope));
        Some((id, key, token))
    }

    /// Take the result of the read numbered `id`. A result of a read this
    /// partition does not wait for, or one that arrives after primacy moved,
    /// changes nothing. A read that did not find the object of a live topic
    /// lets the next one start [`TOPIC_OBJECT_RETRY`] after `now`.
    pub fn finish_topic_object_check(
        &mut self,
        id: u64,
        outcome: &TopicObjectOutcome<TopicRead>,
        now: IggyTimestamp,
    ) {
        let awaited = self
            .topic_object_check
            .running
            .as_ref()
            .is_some_and(|(running, _)| *running == id);
        if !awaited {
            return;
        }
        if !self.checks_topic_object()
            || self.topic_object_check.view != Some(self.consensus().view())
        {
            self.reset_topic_object_check(None);
            return;
        }
        let namespace_raw = self.namespace().inner();
        let key = self.topic_key();
        let check = &mut self.topic_object_check;
        check.running = None;
        let miss = match outcome {
            Ok(Ok(TopicRead::Found(object))) if object.is_live() => {
                check.found = true;
                debug!(
                    namespace_raw,
                    ?key,
                    "found the object of the partition's geo topic; the partition takes messages"
                );
                return;
            }
            Ok(Ok(TopicRead::Found(_))) => "the store holds the tombstone of the topic".to_owned(),
            Ok(Ok(TopicRead::Missing)) => "the object is not in the store yet".to_owned(),
            Ok(Ok(TopicRead::Invalid(error))) => format!("the object does not decode: {error}"),
            Ok(Err(error)) => format!("the read failed: {error}"),
            // A read that the scope stopped proves nothing, so the next pass
            // reads at once.
            Err(TierJobCancelled) => return,
        };
        check.misses = check.misses.saturating_add(1);
        let wait = u64::try_from(TOPIC_OBJECT_RETRY.as_micros()).unwrap_or(u64::MAX);
        check.retry_at = now.as_micros().saturating_add(wait);
        if check.misses == 1 {
            warn!(
                namespace_raw,
                ?key,
                miss,
                "the primary of a geo topic's partition did not find the topic's object; the \
                 partition takes no messages until it does, and reads it again every second"
            );
        } else {
            debug!(
                namespace_raw,
                ?key,
                miss,
                misses = check.misses,
                "the object of the partition's geo topic is still not found"
            );
        }
    }

    /// Whether this replica is the one that reads the topic's object: the
    /// primary in normal status, outside a primary transfer.
    fn checks_topic_object(&self) -> bool {
        let consensus = self.consensus();
        consensus.is_primary() && consensus.is_normal() && !consensus.is_transferring()
    }

    /// The check state of the current view. What an earlier view left is
    /// dropped, and its read cancelled.
    fn topic_object_check_of_this_view(&mut self) -> &mut TopicObjectCheck {
        let view = self.consensus().view();
        if self.topic_object_check.view != Some(view) {
            self.reset_topic_object_check(Some(view));
        }
        &mut self.topic_object_check
    }

    /// Drop the check state for one of `view`, keeping the read numbers.
    fn reset_topic_object_check(&mut self, view: Option<u32>) {
        *self.topic_object_check = TopicObjectCheck {
            view,
            jobs: self.topic_object_check.jobs,
            ..TopicObjectCheck::default()
        };
    }
}
