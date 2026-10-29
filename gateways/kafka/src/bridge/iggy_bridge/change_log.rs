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

//! Follows Iggy's log of committed metadata changes. The gateway learns from it which Kafka groups
//! hold offsets, when a topic changes and what its users can do, without listing any of them.
//! `docs/OFFSET_STORAGE.md` has the reasoning.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::mem;
use std::ops::BitOrAssign;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use iggy::prelude::{
    IggyClient, IggyError, MAX_METADATA_CHANGES_LIMIT, MetadataChange, MetadataChangeCursor,
    MetadataChangeGroup, MetadataChangeStart, MetadataChangeStatus, MetadataChanges,
    MetadataChangesQuery, MetadataCurrentItem, MetadataUserChange, Permissions, SystemClient,
    UserStatus,
};
use tokio::sync::{broadcast, watch};
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::group_index::GroupFeed;
use super::group_offsets::kafka_group_of;
use super::{LazyClient, REQUEST_TIMEOUT};
use crate::auth::{UserKey, principal_permissions};
use crate::bridge::config::IggyBridgeConfig;
use crate::bridge::error::BridgeError;
use crate::bridge::topic_map::TopicMapping;
use crate::protocol::acl::PrincipalPermissions;

/// The record groups the follower reads. Members and sessions change with every consumer, and
/// nothing here needs them.
const FOLLOWED: [MetadataChangeGroup; 5] = [
    MetadataChangeGroup::Streams,
    MetadataChangeGroup::Topics,
    MetadataChangeGroup::Partitions,
    MetadataChangeGroup::ConsumerGroups,
    MetadataChangeGroup::Users,
];

/// The pause between two reads once the follower has read every record. Another gateway's
/// change shows here within about this long, plus the replication lag of the node it reads.
const FOLLOW_INTERVAL: Duration = Duration::from_millis(250);

/// The first pause, and the longest, after a read that failed.
const RETRY_PAUSE_FIRST: Duration = Duration::from_millis(250);
const RETRY_PAUSE_MAX: Duration = Duration::from_secs(10);

/// The deleted topics a receiver can fall behind by before it lags.
pub(super) const TOPIC_DELETIONS_CAPACITY: usize = 1024;

/// What the follower keeps up to date from the change log.
pub(super) struct Follower {
    pub(super) mapping: TopicMapping,
    pub(super) groups: GroupFeed,
    /// Moves after each read that showed a stream, topic or partition change.
    pub(super) topics: Arc<watch::Sender<u64>>,
    pub(super) permissions: Arc<PermissionBoard>,
    /// Gets the Kafka name of each topic that the change log shows deleted.
    pub(super) deletions: broadcast::Sender<String>,
}

/// What the change log last said of each Iggy user, for the connections that authenticated as
/// one.
#[derive(Debug, Default)]
pub struct PermissionBoard {
    /// Moves with each change, so a connection looks a user up only after one.
    revision: AtomicU64,
    users: Mutex<Users>,
}

/// What an Iggy user can do, as the change log last said.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserAccess {
    /// An active user, with its permissions.
    Active(PrincipalPermissions),
    /// A user that was deleted or is no longer active.
    Revoked,
}

/// The board's users, by key: Iggy gives the name of a deleted user to new users.
#[derive(Debug, Default)]
struct Users {
    by_key: HashMap<UserKey, UserEntry>,
    /// Per username, the revision of the last revoke of a user of that name. A rename counts,
    /// since a connection that knows only the old name loses its user there.
    revoked_names: HashMap<String, u64>,
    /// The revision of the last rebuild that dropped revokes. A connection that looked before it
    /// cannot tell whether one was of its user.
    dropped: u64,
}

#[derive(Debug)]
struct UserEntry {
    username: String,
    /// The revision of its last change.
    changed: u64,
    /// The revision of its last revoke, 0 if none.
    revoked: u64,
    access: UserAccess,
}

impl PermissionBoard {
    #[must_use]
    pub fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }

    /// What changed for Iggy user `user`, named `username`, after revision `seen`, if anything
    /// did. Moves `seen` on.
    ///
    /// A revoke after `seen` shows even when a later change undid it, so the connection closes.
    /// Without `user`, only a revoke of any user named `username` shows: which user of that name
    /// it is was never read, so no permission change can apply.
    pub fn changed_since(
        &self,
        user: Option<UserKey>,
        username: &str,
        seen: &mut u64,
    ) -> Option<UserAccess> {
        let revision = self.revision();
        if revision == *seen {
            return None;
        }
        let changed = lock(&self.users).changed_since(user, username, *seen);
        *seen = revision;
        changed
    }

    /// What the change log last said of the newest user named `username`. `None` before it said
    /// anything.
    #[must_use]
    pub fn access(&self, username: &str) -> Option<UserAccess> {
        lock(&self.users)
            .by_key
            .iter()
            .filter(|(_, entry)| entry.username == username)
            .max_by_key(|(user, _)| user.created_at)
            .map(|(_, entry)| entry.access)
    }

    fn set(&self, user: UserKey, username: &str, access: UserAccess) {
        let mut users = lock(&self.users);
        let revision = self.next_revision();
        users.set(user, username, access, revision);
    }

    /// Sets the permissions of `user` while it is active. The change has no status, so it neither
    /// re-activates a revoked user nor adds one the board does not hold.
    fn set_permissions(&self, user: UserKey, username: &str, permissions: PrincipalPermissions) {
        let mut users = lock(&self.users);
        let active = users
            .by_key
            .get(&user)
            .is_some_and(|entry| entry.access != UserAccess::Revoked);
        if active {
            let revision = self.next_revision();
            users.set(user, username, UserAccess::Active(permissions), revision);
        }
    }

    /// The users of a current state read. A user that it left out is revoked. Revoked users that
    /// it left out are dropped, so users that never come back do not pile up.
    fn replace(&self, current: HashMap<UserKey, (String, UserAccess)>) {
        let mut users = lock(&self.users);
        let held = users.by_key.len();
        users.by_key.retain(|user, entry| {
            current.contains_key(user) || entry.access != UserAccess::Revoked
        });
        if users.by_key.len() < held || !users.revoked_names.is_empty() {
            users.revoked_names.clear();
            users.dropped = self.next_revision();
        }
        let mut changes: Vec<(UserKey, String, UserAccess)> = users
            .by_key
            .iter()
            .filter(|(user, _)| !current.contains_key(*user))
            .map(|(user, entry)| (*user, entry.username.clone(), UserAccess::Revoked))
            .collect();
        changes.extend(
            current
                .into_iter()
                .filter(|(user, (username, access))| {
                    users
                        .by_key
                        .get(user)
                        .is_none_or(|known| known.username != *username || known.access != *access)
                })
                .map(|(user, (username, access))| (user, username, access)),
        );
        for (user, username, access) in changes {
            let revision = self.next_revision();
            users.set(user, &username, access, revision);
        }
    }

    /// Under the users lock only, so a reader that sees the revision also sees the entry.
    fn next_revision(&self) -> u64 {
        self.revision.fetch_add(1, Ordering::AcqRel) + 1
    }
}

impl Users {
    fn changed_since(
        &self,
        user: Option<UserKey>,
        username: &str,
        seen: u64,
    ) -> Option<UserAccess> {
        let revoked = |at: u64| (at > seen).then_some(UserAccess::Revoked);
        let Some(user) = user else {
            let named = self
                .revoked_names
                .get(username)
                .copied()
                .unwrap_or_default();
            return revoked(named.max(self.dropped));
        };
        self.by_key.get(&user).map_or_else(
            || revoked(self.dropped),
            |entry| {
                revoked(entry.revoked).or_else(|| (entry.changed > seen).then_some(entry.access))
            },
        )
    }

    fn set(&mut self, user: UserKey, username: &str, access: UserAccess, revision: u64) {
        let entry = self.by_key.entry(user).or_insert_with(|| UserEntry {
            username: username.to_owned(),
            changed: revision,
            revoked: 0,
            access,
        });
        if entry.username != username {
            let renamed = mem::replace(&mut entry.username, username.to_owned());
            self.revoked_names.insert(renamed, revision);
        }
        entry.changed = revision;
        entry.access = access;
        if access == UserAccess::Revoked {
            entry.revoked = revision;
            self.revoked_names.insert(username.to_owned(), revision);
        }
    }
}

impl UserAccess {
    fn of(user: &MetadataUserChange) -> Self {
        if user.status == UserStatus::Active {
            Self::Active(granted(user.permissions.as_ref()))
        } else {
            Self::Revoked
        }
    }
}

fn granted(permissions: Option<&Permissions>) -> PrincipalPermissions {
    permissions.map(principal_permissions).unwrap_or_default()
}

/// What one read leaves the follower to do next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Next {
    /// It read the current state, anchored at this cursor. The changes after it come at once.
    Rebuilt(MetadataChangeCursor),
    /// Read the changes after this cursor after a pause.
    At(MetadataChangeCursor),
    /// Read the current state after a pause: the log no longer holds the changes after the
    /// position, or the node answered behind during a current state read.
    Rebuild,
}

impl Follower {
    /// Follows the change log on `client` until `cancel`, or until the gateway turns out not to be
    /// able to read it. See [`Self::follow`].
    pub(super) async fn run(
        self,
        config: IggyBridgeConfig,
        client: LazyClient,
        cancel: CancellationToken,
    ) {
        let config = &config;
        let read = |query: MetadataChangesQuery| {
            let client = client.clone();
            async move {
                let deadline = Instant::now() + REQUEST_TIMEOUT;
                let call = |connected: Arc<IggyClient>| async move {
                    connected
                        .get_metadata_changes(&query)
                        .await
                        .map_err(BridgeError::Iggy)
                };
                client.until(config, deadline, call).await
            }
        };
        tokio::select! {
            () = cancel.cancelled() => {}
            () = self.follow(read) => {}
        }
    }

    /// Reads the current state, then each change after it, with `read`. A position that the log
    /// no longer holds reads the current state again. A read that fails is retried after a pause
    /// that doubles, or at once when a caller waits for the first view of the groups.
    ///
    /// Returns when the bridge user cannot read the log, or Iggy does not serve it. The groups
    /// then come from scans.
    async fn follow<F, Fut>(&self, read: F)
    where
        F: Fn(MetadataChangesQuery) -> Fut + Send + Sync,
        Fut: Future<Output = Result<MetadataChanges, BridgeError>> + Send,
    {
        let mut mirror = Mirror::default();
        let mut position = None;
        let mut pause = RETRY_PAUSE_FIRST;
        let mut failing = false;
        loop {
            let next = match position {
                None => self.rebuild(&read, &mut mirror).await,
                Some(cursor) => self.catch_up(&read, &mut mirror, cursor).await,
            };
            let wait = match next {
                Ok(next) => {
                    if failing {
                        info!("the gateway reads Iggy's metadata change log again");
                        failing = false;
                    }
                    pause = RETRY_PAUSE_FIRST;
                    match next {
                        Next::Rebuilt(anchor) => {
                            position = Some(anchor);
                            continue;
                        }
                        Next::At(cursor) => position = Some(cursor),
                        Next::Rebuild => position = None,
                    }
                    FOLLOW_INTERVAL
                }
                Err(BridgeError::Iggy(IggyError::Unauthorized)) => {
                    warn!(
                        "the bridge's Iggy user lacks the manage_servers permission, so the \
                         gateway cannot read Iggy's metadata change log. It lists the Kafka \
                         groups that hold offsets with one Iggy call per topic instead, and an \
                         open connection keeps its permissions until it closes"
                    );
                    self.groups.fall_back();
                    return;
                }
                // An Iggy server older than the change log does not know the call.
                Err(BridgeError::Iggy(
                    error @ (IggyError::InvalidCommand | IggyError::FeatureUnavailable),
                )) => {
                    warn!(
                        %error,
                        "Iggy does not serve its metadata change log to the gateway. It lists \
                         the Kafka groups that hold offsets with one Iggy call per topic instead, \
                         and an open connection keeps its permissions until it closes"
                    );
                    self.groups.fall_back();
                    return;
                }
                Err(error) => {
                    if failing {
                        debug!(%error, "could not read Iggy's metadata change log");
                    } else {
                        warn!(%error, "could not read Iggy's metadata change log; retrying");
                        failing = true;
                    }
                    self.groups.failed();
                    let wait = pause;
                    pause = pause.saturating_mul(2).min(RETRY_PAUSE_MAX);
                    wait
                }
            };
            tokio::select! {
                () = sleep(wait) => {}
                () = self.groups.woken() => {}
            }
        }
    }

    /// Reads the current state into a new mirror. The mirror replaces `mirror` once every page
    /// is read, so the groups do not shrink while a read runs.
    async fn rebuild<F, Fut>(&self, read: &F, mirror: &mut Mirror) -> Result<Next, BridgeError>
    where
        F: Fn(MetadataChangesQuery) -> Fut + Send + Sync,
        Fut: Future<Output = Result<MetadataChanges, BridgeError>> + Send,
    {
        let mut current = Mirror::default();
        let mut users = HashMap::new();
        let mut token = None;
        loop {
            let page = read(query(MetadataChangeStart::Current(token))).await?;
            if page.status != MetadataChangeStatus::Ok {
                debug!(status = %page.status, "Iggy did not page its current metadata; reading it again");
                return Ok(Next::Rebuild);
            }
            for item in &page.items {
                current.insert(item, &mut users);
            }
            let Some(anchor) = page.next else {
                return Ok(Next::Rebuild);
            };
            token = page.current_page_token;
            if token.is_none() {
                // A gap in the log can hide deletes.
                let deleted = mirror.kafka_topics_left_out(&current, &self.mapping);
                *mirror = current;
                self.permissions.replace(users);
                self.publish(mirror, Changed::ALL);
                self.report(deleted);
                return Ok(Next::Rebuilt(anchor));
            }
        }
    }

    /// Applies each change after `from`, page by page, until none is left.
    async fn catch_up<F, Fut>(
        &self,
        read: &F,
        mirror: &mut Mirror,
        from: MetadataChangeCursor,
    ) -> Result<Next, BridgeError>
    where
        F: Fn(MetadataChangesQuery) -> Fut + Send + Sync,
        Fut: Future<Output = Result<MetadataChanges, BridgeError>> + Send,
    {
        let mut cursor = from;
        loop {
            let page = read(query(MetadataChangeStart::After(cursor))).await?;
            match page.status {
                MetadataChangeStatus::Ok => {}
                // A node that has not applied the position yet. The same read works later.
                MetadataChangeStatus::Behind => return Ok(Next::At(cursor)),
                MetadataChangeStatus::Expired | MetadataChangeStatus::Reset => {
                    info!(
                        status = %page.status,
                        "Iggy no longer holds the metadata changes after the gateway's position; \
                         reading the current state again"
                    );
                    return Ok(Next::Rebuild);
                }
            }
            let mut changed = Changed::default();
            let mut deleted = Vec::new();
            for record in &page.records {
                changed |= mirror.apply(
                    &record.change,
                    &self.permissions,
                    &self.mapping,
                    &mut deleted,
                );
            }
            self.publish(mirror, changed);
            self.report(deleted);
            // Without a cursor, the next read repeats these records, and applying them again
            // changes nothing.
            let Some(next) = page.next else {
                return Ok(Next::At(cursor));
            };
            cursor = next;
            if !page.has_more {
                return Ok(Next::At(cursor));
            }
        }
    }

    fn publish(&self, mirror: &Mirror, changed: Changed) {
        if changed.groups {
            self.groups.publish(mirror.kafka_groups(&self.mapping));
        }
        if changed.topics {
            self.topics.send_modify(|version| *version += 1);
        }
    }

    /// Sends the Kafka name of each deleted topic.
    fn report(&self, deleted: Vec<String>) {
        for kafka_topic in deleted {
            // Fails only without a receiver, and then no one keeps offsets to drop.
            let _ = self.deletions.send(kafka_topic);
        }
    }
}

fn query(start: MetadataChangeStart) -> MetadataChangesQuery {
    MetadataChangesQuery {
        start,
        groups: FOLLOWED.to_vec(),
        limit: MAX_METADATA_CHANGES_LIMIT,
        ..MetadataChangesQuery::default()
    }
}

/// What applying records changed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Changed {
    /// The Kafka groups that hold offsets can differ.
    groups: bool,
    /// A stream, topic or partition changed.
    topics: bool,
}

impl Changed {
    const ALL: Self = Self {
        groups: true,
        topics: true,
    };
    const TOPICS: Self = Self {
        groups: false,
        topics: true,
    };
    const fn groups(groups: bool) -> Self {
        Self {
            groups,
            topics: false,
        }
    }
}

impl BitOrAssign for Changed {
    fn bitor_assign(&mut self, other: Self) {
        self.groups |= other.groups;
        self.topics |= other.topics;
    }
}

/// The streams, topics and Kafka offset groups of Iggy, as the change log tells them.
///
/// Iggy reuses the numeric id of a deleted stream or topic, so every key also holds the op that
/// created it. A current state read pages state that can be newer than its anchor, and the
/// changes after the anchor replay over it. A delete there then names an older incarnation, and
/// leaves a newer one of the same id alone.
#[derive(Debug, Default)]
struct Mirror {
    streams: HashMap<u32, StreamEntry>,
    topics: HashMap<(u32, u32), TopicEntry>,
    /// Each Iggy group named `kafka.cg.<group>`, with its Kafka group id.
    groups: HashMap<GroupKey, String>,
}

#[derive(Debug)]
struct StreamEntry {
    created_op: u64,
    name: String,
}

#[derive(Debug)]
struct TopicEntry {
    key: TopicKey,
    name: String,
}

/// One incarnation of a topic, in one incarnation of its stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TopicKey {
    stream_id: u32,
    stream_created_op: u64,
    topic_id: u32,
    topic_created_op: u64,
}

impl TopicKey {
    /// Whether the topic is in that incarnation of stream `stream_id`.
    const fn in_stream(&self, stream_id: u32, created_op: u64) -> bool {
        self.stream_id == stream_id && self.stream_created_op == created_op
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct GroupKey {
    topic: TopicKey,
    group_id: u32,
}

/// [`UserKey`] from the fields every user record carries.
macro_rules! user_key {
    ($record:expr) => {
        UserKey {
            user_id: $record.user_id,
            created_at: $record.user_created_at,
        }
    };
}

/// [`TopicKey`] from the fields every topic and group record carries.
macro_rules! topic_key {
    ($record:expr) => {
        TopicKey {
            stream_id: $record.stream_id,
            stream_created_op: $record.stream_created_op,
            topic_id: $record.topic_id,
            topic_created_op: $record.topic_created_op,
        }
    };
}

/// [`GroupKey`] from a consumer group record.
macro_rules! group_key {
    ($record:expr) => {
        GroupKey {
            topic: topic_key!($record),
            group_id: $record.group_id,
        }
    };
}

impl Mirror {
    /// Applies one change. User changes go to `permissions`, and the Kafka name by `mapping` of
    /// each topic it deletes to `deleted`.
    fn apply(
        &mut self,
        change: &MetadataChange,
        permissions: &PermissionBoard,
        mapping: &TopicMapping,
        deleted: &mut Vec<String>,
    ) -> Changed {
        match change {
            MetadataChange::StreamCreated(stream) | MetadataChange::StreamUpdated(stream) => {
                self.put_stream(stream.stream_id, stream.stream_created_op, &stream.name);
                Changed::ALL
            }
            MetadataChange::StreamDeleted(stream) => {
                let (stream_id, created_op) = (stream.stream_id, stream.stream_created_op);
                deleted.extend(
                    self.kafka_topics(mapping)
                        .filter(|(key, _)| key.in_stream(stream_id, created_op))
                        .map(|(_, kafka_topic)| kafka_topic.to_owned()),
                );
                self.drop_stream(stream_id, created_op);
                Changed::ALL
            }
            MetadataChange::TopicCreated(topic) | MetadataChange::TopicUpdated(topic) => {
                self.put_topic(topic_key!(topic), &topic.name);
                Changed::ALL
            }
            MetadataChange::TopicDeleted(topic) => {
                let key = topic_key!(topic);
                deleted.extend(self.kafka_topic_of(key, mapping).map(str::to_owned));
                self.drop_topic(key);
                Changed::ALL
            }
            MetadataChange::PartitionsChanged(_) => Changed::TOPICS,
            MetadataChange::ConsumerGroupCreated(group) => {
                Changed::groups(self.put_group(group_key!(group), &group.name))
            }
            MetadataChange::ConsumerGroupDeleted(group) => {
                Changed::groups(self.groups.remove(&group_key!(group)).is_some())
            }
            MetadataChange::UserCreated(user) | MetadataChange::UserUpdated(user) => {
                permissions.set(user_key!(user), &user.username, UserAccess::of(user));
                Changed::default()
            }
            MetadataChange::UserPermissionsUpdated(user) => {
                let granted = granted(user.permissions.as_ref());
                permissions.set_permissions(user_key!(user), &user.username, granted);
                Changed::default()
            }
            MetadataChange::UserDeleted(user) => {
                permissions.set(user_key!(user), &user.username, UserAccess::Revoked);
                Changed::default()
            }
            _ => Changed::default(),
        }
    }

    /// Adds one item of a current state read. Users go to `users`.
    fn insert(
        &mut self,
        item: &MetadataCurrentItem,
        users: &mut HashMap<UserKey, (String, UserAccess)>,
    ) {
        match item {
            MetadataCurrentItem::Stream(stream) => {
                self.put_stream(stream.stream_id, stream.stream_created_op, &stream.name);
            }
            MetadataCurrentItem::Topic(topic) => self.put_topic(topic_key!(topic), &topic.name),
            MetadataCurrentItem::ConsumerGroup(group) => {
                self.put_group(group_key!(group), &group.name);
            }
            MetadataCurrentItem::User(user) => {
                users.insert(
                    user_key!(user),
                    (user.username.clone(), UserAccess::of(user)),
                );
            }
            _ => {}
        }
    }

    /// The Kafka groups with offsets on a topic that a Kafka name lists under `mapping`.
    fn kafka_groups(&self, mapping: &TopicMapping) -> BTreeSet<String> {
        self.groups
            .iter()
            .filter(|(key, _)| self.kafka_topic_of(key.topic, mapping).is_some())
            .map(|(_, group)| group.clone())
            .collect()
    }

    /// Each topic held that `mapping` lists, with its Kafka name.
    fn kafka_topics<'a>(
        &'a self,
        mapping: &'a TopicMapping,
    ) -> impl Iterator<Item = (TopicKey, &'a str)> {
        self.topics
            .values()
            .filter_map(move |topic| Some((topic.key, self.kafka_topic_of(topic.key, mapping)?)))
    }

    /// The Kafka names that this mirror lists under `mapping` and `newer` does not.
    fn kafka_topics_left_out(&self, newer: &Self, mapping: &TopicMapping) -> Vec<String> {
        let kept: HashSet<&str> = newer
            .kafka_topics(mapping)
            .map(|(_, kafka_topic)| kafka_topic)
            .collect();
        self.kafka_topics(mapping)
            .map(|(_, kafka_topic)| kafka_topic)
            .filter(|kafka_topic| !kept.contains(kafka_topic))
            .map(str::to_owned)
            .collect()
    }

    /// The Kafka name of topic `key`, when the mirror holds that incarnation and `mapping` lists
    /// it.
    fn kafka_topic_of<'a>(&'a self, key: TopicKey, mapping: &'a TopicMapping) -> Option<&'a str> {
        let stream = self
            .streams
            .get(&key.stream_id)
            .filter(|stream| stream.created_op == key.stream_created_op)?;
        let topic = self
            .topics
            .get(&(key.stream_id, key.topic_id))
            .filter(|topic| topic.key == key)?;
        mapping.kafka_topic_of(&stream.name, &topic.name)
    }

    /// Ops only grow, so a stream of the same id created by a later op is newer, and the record of
    /// an older one is left out.
    fn put_stream(&mut self, stream_id: u32, created_op: u64, name: &str) {
        match self.streams.get(&stream_id).map(|stream| stream.created_op) {
            Some(known) if known > created_op => return,
            Some(older) if older < created_op => self.drop_stream(stream_id, older),
            _ => {}
        }
        self.streams.insert(
            stream_id,
            StreamEntry {
                created_op,
                name: name.to_owned(),
            },
        );
    }

    /// Drops that incarnation of the stream, with its topics and groups. Iggy deletes them with it
    /// and records no change of its own for them.
    fn drop_stream(&mut self, stream_id: u32, created_op: u64) {
        if self
            .streams
            .get(&stream_id)
            .is_some_and(|stream| stream.created_op == created_op)
        {
            self.streams.remove(&stream_id);
        }
        self.topics
            .retain(|_, topic| !topic.key.in_stream(stream_id, created_op));
        self.groups
            .retain(|key, _| !key.topic.in_stream(stream_id, created_op));
    }

    /// As [`Self::put_stream`], by the op that created the topic.
    fn put_topic(&mut self, key: TopicKey, name: &str) {
        let slot = (key.stream_id, key.topic_id);
        match self.topics.get(&slot).map(|topic| topic.key) {
            Some(known) if known.topic_created_op > key.topic_created_op => return,
            Some(older) if older != key => self.drop_topic(older),
            _ => {}
        }
        self.topics.insert(
            slot,
            TopicEntry {
                key,
                name: name.to_owned(),
            },
        );
    }

    /// Drops that incarnation of the topic, with its groups.
    fn drop_topic(&mut self, key: TopicKey) {
        let slot = (key.stream_id, key.topic_id);
        if self.topics.get(&slot).is_some_and(|topic| topic.key == key) {
            self.topics.remove(&slot);
        }
        self.groups.retain(|group, _| group.topic != key);
    }

    /// Keeps the group when it holds Kafka offsets. Whether it did.
    fn put_group(&mut self, key: GroupKey, name: &str) -> bool {
        let Some(group) = kafka_group_of(name) else {
            return false;
        };
        self.groups.insert(key, group);
        true
    }
}

/// Each update is whole, so the state stays sound after a panic elsewhere.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use iggy::prelude::{
        GlobalPermissions, MetadataChangePhase, MetadataChangeRecord, MetadataConsumerGroupCreated,
        MetadataConsumerGroupDeleted, MetadataConsumerGroupItem, MetadataOptions,
        MetadataPartitionsChanged, MetadataPermissionsUpdatedReason, MetadataStreamChange,
        MetadataStreamDeleted, MetadataTopicChange, MetadataTopicDeleted, MetadataTopicItem,
        MetadataUserIdentity, MetadataUserPermissionsUpdated,
    };

    use super::super::ScannedGroups;
    use super::super::group_index::{GroupIndex, OffsetGroupSource};
    use super::*;
    use crate::bridge::TopicOverride;

    const KAFKA: u32 = 1;
    const OTHER: u32 = 2;
    const NONE: [&str; 0] = [];

    fn mapping() -> TopicMapping {
        let overrides = HashMap::from([(
            "audit".to_owned(),
            TopicOverride {
                stream: "logs".to_owned(),
                topic: "audit_log".to_owned(),
            },
        )]);
        TopicMapping::new("kafka".to_owned(), overrides).expect("a valid mapping")
    }

    fn stream(stream_id: u32, created_op: u64, name: &str) -> MetadataStreamChange {
        MetadataStreamChange {
            stream_id,
            stream_created_op: created_op,
            created_at: 0,
            name: name.to_owned(),
            options: MetadataOptions::Unreadable,
        }
    }

    fn stream_deleted(stream_id: u32, created_op: u64) -> MetadataChange {
        MetadataChange::StreamDeleted(MetadataStreamDeleted {
            stream_id,
            stream_created_op: created_op,
            created_at: 0,
            name: String::new(),
            requested_op: None,
        })
    }

    /// Topic `topic_id`, created by op `created_op`, in stream `stream_id` created by op
    /// `stream_id`, so tests number each stream's incarnation by its id.
    fn topic(stream_id: u32, topic_id: u32, created_op: u64, name: &str) -> MetadataTopicChange {
        MetadataTopicChange {
            stream_id,
            stream_created_op: u64::from(stream_id),
            topic_id,
            topic_created_op: created_op,
            created_at: 0,
            name: name.to_owned(),
            partitions_count: 1,
            options: MetadataOptions::Unreadable,
        }
    }

    fn topic_deleted(stream_id: u32, topic_id: u32, created_op: u64) -> MetadataChange {
        MetadataChange::TopicDeleted(MetadataTopicDeleted {
            stream_id,
            stream_created_op: u64::from(stream_id),
            topic_id,
            topic_created_op: created_op,
            created_at: 0,
            name: String::new(),
            requested_op: None,
        })
    }

    /// Group `group_id` named `name` on the topic that `topic` would create.
    fn group(on: &MetadataTopicChange, group_id: u32, name: &str) -> MetadataChange {
        MetadataChange::ConsumerGroupCreated(MetadataConsumerGroupCreated {
            stream_id: on.stream_id,
            stream_created_op: on.stream_created_op,
            topic_id: on.topic_id,
            topic_created_op: on.topic_created_op,
            group_id,
            name: name.to_owned(),
            consumer_groups_count: 1,
        })
    }

    fn group_deleted(on: &MetadataTopicChange, group_id: u32) -> MetadataChange {
        MetadataChange::ConsumerGroupDeleted(MetadataConsumerGroupDeleted {
            stream_id: on.stream_id,
            stream_created_op: on.stream_created_op,
            topic_id: on.topic_id,
            topic_created_op: on.topic_created_op,
            group_id,
            name: String::new(),
            consumer_groups_count: 0,
            requested_op: None,
        })
    }

    const ALICE: UserKey = UserKey {
        user_id: 1,
        created_at: 100,
    };
    /// A new user named alice, after the first was deleted. Iggy gives it the same id.
    const ALICE_AGAIN: UserKey = UserKey {
        user_id: 1,
        created_at: 200,
    };
    const BOB: UserKey = UserKey {
        user_id: 2,
        created_at: 100,
    };
    const CAROL: UserKey = UserKey {
        user_id: 3,
        created_at: 100,
    };

    fn user(
        key: UserKey,
        name: &str,
        status: UserStatus,
        manage_topics: bool,
    ) -> MetadataUserChange {
        MetadataUserChange {
            user_id: key.user_id,
            user_created_at: key.created_at,
            username: name.to_owned(),
            status,
            permissions: Some(permissions(manage_topics)),
            options: MetadataOptions::Unreadable,
        }
    }

    fn user_deleted(key: UserKey, name: &str) -> MetadataChange {
        MetadataChange::UserDeleted(MetadataUserIdentity {
            user_id: key.user_id,
            user_created_at: key.created_at,
            username: name.to_owned(),
        })
    }

    fn permissions_updated(
        key: UserKey,
        name: &str,
        manage_topics: bool,
        reason: MetadataPermissionsUpdatedReason,
    ) -> MetadataChange {
        MetadataChange::UserPermissionsUpdated(MetadataUserPermissionsUpdated {
            user_id: key.user_id,
            user_created_at: key.created_at,
            username: name.to_owned(),
            permissions: Some(permissions(manage_topics)),
            reason,
        })
    }

    fn permissions(manage_topics: bool) -> Permissions {
        Permissions {
            global: GlobalPermissions {
                manage_topics,
                ..GlobalPermissions::default()
            },
            streams: None,
        }
    }

    fn manages_topics() -> UserAccess {
        UserAccess::Active(principal_permissions(&permissions(true)))
    }

    fn reads_nothing() -> UserAccess {
        UserAccess::Active(principal_permissions(&permissions(false)))
    }

    /// A mirror with stream `kafka`, the default stream, and its topic `orders`.
    fn seeded() -> (Mirror, MetadataTopicChange) {
        let mut mirror = Mirror::default();
        let orders = topic(KAFKA, 1, 10, "orders");
        apply_all(
            &mut mirror,
            &[
                MetadataChange::StreamCreated(stream(KAFKA, u64::from(KAFKA), "kafka")),
                MetadataChange::TopicCreated(orders.clone()),
            ],
        );
        (mirror, orders)
    }

    fn apply_all(mirror: &mut Mirror, records: &[MetadataChange]) -> Changed {
        apply_to(
            mirror,
            records,
            &PermissionBoard::default(),
            &mut Vec::new(),
        )
    }

    /// Applies user `records` to `board`.
    fn apply_users(board: &PermissionBoard, records: &[MetadataChange]) {
        apply_to(&mut Mirror::default(), records, board, &mut Vec::new());
    }

    /// The Kafka names of the topics that applying `records` reports deleted, sorted.
    fn deleted_by(mirror: &mut Mirror, records: &[MetadataChange]) -> Vec<String> {
        let mut deleted = Vec::new();
        apply_to(mirror, records, &PermissionBoard::default(), &mut deleted);
        deleted.sort();
        deleted
    }

    fn apply_to(
        mirror: &mut Mirror,
        records: &[MetadataChange],
        board: &PermissionBoard,
        deleted: &mut Vec<String>,
    ) -> Changed {
        let mapping = mapping();
        let mut changed = Changed::default();
        for change in records {
            changed |= mirror.apply(change, board, &mapping, deleted);
        }
        changed
    }

    fn groups_of(mirror: &Mirror) -> Vec<String> {
        mirror.kafka_groups(&mapping()).into_iter().collect()
    }

    #[test]
    fn given_kafka_groups_and_a_native_one_should_count_only_the_kafka_groups() {
        let (mut mirror, orders) = seeded();

        let changed = apply_all(
            &mut mirror,
            &[
                group(&orders, 1, "kafka.cg.orders-app"),
                group(&orders, 2, "native-app"),
                group(&orders, 3, "kafka.cg.audit-app"),
            ],
        );

        assert_eq!(groups_of(&mirror), ["audit-app", "orders-app"]);
        assert_eq!(changed, Changed::groups(true));
    }

    #[test]
    fn given_a_deleted_group_should_leave_it_out() {
        let (mut mirror, orders) = seeded();
        apply_all(
            &mut mirror,
            &[
                group(&orders, 1, "kafka.cg.orders-app"),
                group(&orders, 2, "kafka.cg.audit-app"),
            ],
        );

        apply_all(&mut mirror, &[group_deleted(&orders, 1)]);

        assert_eq!(groups_of(&mirror), ["audit-app"]);
    }

    /// Iggy deletes the groups of a topic with it and records no group change for them.
    #[test]
    fn given_a_deleted_topic_should_drop_its_groups() {
        let (mut mirror, orders) = seeded();
        let payments = topic(KAFKA, 2, 11, "payments");
        apply_all(
            &mut mirror,
            &[
                MetadataChange::TopicCreated(payments.clone()),
                group(&orders, 1, "kafka.cg.orders-app"),
                group(&payments, 1, "kafka.cg.payments-app"),
            ],
        );

        apply_all(&mut mirror, &[topic_deleted(KAFKA, 1, 10)]);
        let recreated = topic(KAFKA, 1, 20, "orders");
        apply_all(&mut mirror, &[MetadataChange::TopicCreated(recreated)]);

        assert_eq!(groups_of(&mirror), ["payments-app"]);
    }

    #[test]
    fn given_a_deleted_stream_should_drop_its_topics_and_groups() {
        let (mut mirror, orders) = seeded();
        apply_all(&mut mirror, &[group(&orders, 1, "kafka.cg.orders-app")]);

        apply_all(&mut mirror, &[stream_deleted(KAFKA, u64::from(KAFKA))]);

        assert_eq!(groups_of(&mirror), NONE);
        assert!(mirror.topics.is_empty());
        assert!(mirror.groups.is_empty());
    }

    /// A current state read can page a topic newer than its anchor. The changes after the anchor
    /// then replay the older topic of the same id, and its delete.
    #[test]
    fn given_records_of_an_older_topic_of_the_same_id_should_keep_the_newer_one() {
        let (mut mirror, _) = seeded();
        let newer = topic(KAFKA, 1, 30, "orders");
        apply_all(
            &mut mirror,
            &[
                MetadataChange::TopicCreated(newer.clone()),
                group(&newer, 1, "kafka.cg.new-app"),
            ],
        );
        let older = topic(KAFKA, 1, 5, "orders");

        apply_all(
            &mut mirror,
            &[
                MetadataChange::TopicUpdated(older.clone()),
                group(&older, 1, "kafka.cg.old-app"),
                topic_deleted(KAFKA, 1, 5),
            ],
        );

        assert_eq!(groups_of(&mirror), ["new-app"]);
        assert!(
            mirror.groups.len() == 1,
            "the older topic's group went with it"
        );
    }

    #[test]
    fn given_a_topic_outside_every_kafka_name_should_not_count_its_groups() {
        let (mut mirror, _) = seeded();
        let other = topic(OTHER, 1, 12, "orders");
        let audit = topic(OTHER, 2, 13, "audit_log");
        apply_all(
            &mut mirror,
            &[
                MetadataChange::StreamCreated(stream(OTHER, u64::from(OTHER), "logs")),
                MetadataChange::TopicCreated(other.clone()),
                MetadataChange::TopicCreated(audit.clone()),
                group(&other, 1, "kafka.cg.raw-app"),
                group(&audit, 1, "kafka.cg.audit-app"),
            ],
        );

        assert_eq!(
            groups_of(&mirror),
            ["audit-app"],
            "only the override names a topic of stream `logs`"
        );
    }

    #[test]
    fn given_a_renamed_stream_should_list_its_groups_by_the_new_name() {
        let (mut mirror, orders) = seeded();
        apply_all(&mut mirror, &[group(&orders, 1, "kafka.cg.orders-app")]);

        apply_all(
            &mut mirror,
            &[MetadataChange::StreamUpdated(stream(
                KAFKA,
                u64::from(KAFKA),
                "archived",
            ))],
        );
        let renamed_away = groups_of(&mirror);
        apply_all(
            &mut mirror,
            &[MetadataChange::StreamUpdated(stream(
                KAFKA,
                u64::from(KAFKA),
                "kafka",
            ))],
        );

        assert_eq!(renamed_away, NONE);
        assert_eq!(groups_of(&mirror), ["orders-app"]);
    }

    #[test]
    fn given_a_partition_change_should_change_topics_only() {
        let (mut mirror, _) = seeded();

        let changed = apply_all(
            &mut mirror,
            &[MetadataChange::PartitionsChanged(
                MetadataPartitionsChanged {
                    stream_id: KAFKA,
                    stream_created_op: u64::from(KAFKA),
                    topic_id: 1,
                    topic_created_op: 10,
                    partitions_count: 2,
                    removed_partition_ids: vec![],
                    requested_op: None,
                },
            )],
        );

        assert_eq!(changed, Changed::TOPICS);
    }

    #[test]
    fn given_a_deleted_topic_when_applied_should_report_its_kafka_name() {
        let (mut mirror, _) = seeded();

        let deleted = deleted_by(&mut mirror, &[topic_deleted(KAFKA, 1, 10)]);

        assert_eq!(deleted, ["orders"]);
    }

    /// Iggy records no delete of their own for the topics of a deleted stream.
    #[test]
    fn given_a_deleted_stream_when_applied_should_report_each_of_its_topics() {
        let (mut mirror, _) = seeded();
        apply_all(
            &mut mirror,
            &[MetadataChange::TopicCreated(topic(
                KAFKA, 2, 11, "payments",
            ))],
        );

        let deleted = deleted_by(&mut mirror, &[stream_deleted(KAFKA, u64::from(KAFKA))]);

        assert_eq!(deleted, ["orders", "payments"]);
    }

    #[test]
    fn given_a_deleted_topic_outside_every_kafka_name_when_applied_should_report_nothing() {
        let (mut mirror, _) = seeded();
        apply_all(
            &mut mirror,
            &[
                MetadataChange::StreamCreated(stream(OTHER, u64::from(OTHER), "logs")),
                MetadataChange::TopicCreated(topic(OTHER, 1, 12, "orders")),
                MetadataChange::TopicCreated(topic(OTHER, 2, 13, "audit_log")),
            ],
        );

        let unmapped = deleted_by(&mut mirror, &[topic_deleted(OTHER, 1, 12)]);
        let stream = deleted_by(&mut mirror, &[stream_deleted(OTHER, u64::from(OTHER))]);

        assert_eq!(unmapped, NONE);
        assert_eq!(
            stream,
            ["audit"],
            "only the override names a topic of stream `logs`"
        );
    }

    /// A current state read can page a topic newer than its anchor. The changes after the anchor
    /// then replay the delete of the older topic of the same id.
    #[test]
    fn given_a_delete_of_an_older_topic_when_applied_should_report_nothing() {
        let mut mirror = Mirror::default();
        apply_all(
            &mut mirror,
            &[
                MetadataChange::StreamCreated(stream(KAFKA, u64::from(KAFKA), "kafka")),
                MetadataChange::TopicCreated(topic(KAFKA, 1, 30, "orders")),
            ],
        );

        let deleted = deleted_by(&mut mirror, &[topic_deleted(KAFKA, 1, 10)]);

        assert_eq!(deleted, NONE);
        assert_eq!(mirror.topics.len(), 1, "the newer topic stays");
    }

    #[test]
    fn given_user_records_should_set_what_each_user_may_do() {
        let board = PermissionBoard::default();

        apply_users(
            &board,
            &[
                MetadataChange::UserCreated(user(ALICE, "alice", UserStatus::Active, false)),
                MetadataChange::UserCreated(user(BOB, "bob", UserStatus::Active, false)),
                permissions_updated(
                    ALICE,
                    "alice",
                    true,
                    MetadataPermissionsUpdatedReason::Updated,
                ),
                MetadataChange::UserUpdated(user(BOB, "bob", UserStatus::Inactive, false)),
                MetadataChange::UserCreated(user(CAROL, "carol", UserStatus::Active, false)),
                user_deleted(CAROL, "carol"),
            ],
        );

        assert_eq!(board.access("alice"), Some(manages_topics()));
        assert_eq!(board.access("bob"), Some(UserAccess::Revoked));
        assert_eq!(board.access("carol"), Some(UserAccess::Revoked));
        assert_eq!(board.access("dave"), None);
    }

    #[test]
    fn given_a_change_after_the_seen_revision_should_report_it_once() {
        let board = PermissionBoard::default();
        board.set(ALICE, "alice", reads_nothing());
        let mut seen = board.revision();

        assert_eq!(board.changed_since(Some(ALICE), "alice", &mut seen), None);
        board.set(BOB, "bob", manages_topics());
        assert_eq!(
            board.changed_since(Some(ALICE), "alice", &mut seen),
            None,
            "another user"
        );
        board.set(ALICE, "alice", manages_topics());
        assert_eq!(
            board.changed_since(Some(ALICE), "alice", &mut seen),
            Some(manages_topics())
        );
        assert_eq!(
            board.changed_since(Some(ALICE), "alice", &mut seen),
            None,
            "only once"
        );
    }

    /// A stream delete prunes the grants of every user on it, active or not.
    #[test]
    fn given_a_deactivated_user_when_its_permissions_change_should_stay_revoked() {
        let board = PermissionBoard::default();
        apply_users(
            &board,
            &[MetadataChange::UserCreated(user(
                ALICE,
                "alice",
                UserStatus::Active,
                true,
            ))],
        );
        let mut seen = board.revision();

        apply_users(
            &board,
            &[
                MetadataChange::UserUpdated(user(ALICE, "alice", UserStatus::Inactive, true)),
                permissions_updated(
                    ALICE,
                    "alice",
                    false,
                    MetadataPermissionsUpdatedReason::ResourceDeleted,
                ),
            ],
        );

        assert_eq!(board.access("alice"), Some(UserAccess::Revoked));
        assert_eq!(
            board.changed_since(Some(ALICE), "alice", &mut seen),
            Some(UserAccess::Revoked)
        );
    }

    #[test]
    fn given_a_revoke_undone_in_one_read_when_a_connection_saw_neither_should_close_it() {
        let board = PermissionBoard::default();
        apply_users(
            &board,
            &[MetadataChange::UserCreated(user(
                ALICE,
                "alice",
                UserStatus::Active,
                false,
            ))],
        );
        let mut seen = board.revision();

        apply_users(
            &board,
            &[
                MetadataChange::UserUpdated(user(ALICE, "alice", UserStatus::Inactive, false)),
                MetadataChange::UserUpdated(user(ALICE, "alice", UserStatus::Active, true)),
            ],
        );

        assert_eq!(board.access("alice"), Some(manages_topics()));
        assert_eq!(
            board.changed_since(Some(ALICE), "alice", &mut seen),
            Some(UserAccess::Revoked)
        );
    }

    #[test]
    fn given_a_username_deleted_and_created_again_when_following_should_close_only_the_old_user() {
        let board = PermissionBoard::default();
        apply_users(
            &board,
            &[MetadataChange::UserCreated(user(
                ALICE,
                "alice",
                UserStatus::Active,
                false,
            ))],
        );
        // The new user can sign in before the board shows it.
        let mut old_seen = board.revision();
        let mut new_seen = old_seen;

        apply_users(
            &board,
            &[
                user_deleted(ALICE, "alice"),
                MetadataChange::UserCreated(user(ALICE_AGAIN, "alice", UserStatus::Active, true)),
            ],
        );

        assert_eq!(
            board.changed_since(Some(ALICE), "alice", &mut old_seen),
            Some(UserAccess::Revoked)
        );
        assert_eq!(
            board.changed_since(Some(ALICE_AGAIN), "alice", &mut new_seen),
            Some(manages_topics())
        );
        assert_eq!(board.access("alice"), Some(manages_topics()), "the newest");
    }

    /// A connection whose permission read failed knows only its username.
    #[test]
    fn given_a_connection_without_its_user_when_any_user_of_its_name_is_revoked_should_close_it() {
        let board = PermissionBoard::default();
        apply_users(
            &board,
            &[
                MetadataChange::UserCreated(user(ALICE_AGAIN, "alice", UserStatus::Active, false)),
                MetadataChange::UserCreated(user(BOB, "bob", UserStatus::Active, false)),
            ],
        );
        let mut by_name = board.revision();
        let mut by_user = by_name;

        apply_users(
            &board,
            &[
                permissions_updated(
                    ALICE_AGAIN,
                    "alice",
                    true,
                    MetadataPermissionsUpdatedReason::Updated,
                ),
                user_deleted(BOB, "bob"),
            ],
        );
        let unrelated = board.changed_since(None, "alice", &mut by_name);
        // A replayed delete of the older alice.
        apply_users(&board, &[user_deleted(ALICE, "alice")]);

        assert_eq!(
            unrelated, None,
            "neither a permission change nor a revoke of another name"
        );
        assert_eq!(
            board.changed_since(None, "alice", &mut by_name),
            Some(UserAccess::Revoked)
        );
        assert_eq!(
            board.changed_since(Some(ALICE_AGAIN), "alice", &mut by_user),
            Some(manages_topics()),
            "a connection that knows its user"
        );
    }

    #[test]
    fn given_a_renamed_user_when_following_should_keep_its_key_and_close_its_old_name() {
        let board = PermissionBoard::default();
        board.set(ALICE, "alice", reads_nothing());
        let mut by_user = board.revision();
        let mut by_name = by_user;

        apply_users(
            &board,
            &[MetadataChange::UserUpdated(user(
                ALICE,
                "alicia",
                UserStatus::Active,
                true,
            ))],
        );

        assert_eq!(
            board.changed_since(Some(ALICE), "alice", &mut by_user),
            Some(manages_topics())
        );
        assert_eq!(
            board.changed_since(None, "alice", &mut by_name),
            Some(UserAccess::Revoked)
        );
    }

    #[test]
    fn given_a_current_state_without_a_known_user_when_rebuilt_should_revoke_it() {
        let board = PermissionBoard::default();
        board.set(ALICE, "alice", reads_nothing());
        board.set(BOB, "bob", reads_nothing());
        board.set(CAROL, "carol", UserAccess::Revoked);
        let mut alice_seen = board.revision();
        let mut bob_seen = alice_seen;

        board.replace(HashMap::from([(BOB, ("bob".to_owned(), reads_nothing()))]));

        assert_eq!(
            board.changed_since(Some(ALICE), "alice", &mut alice_seen),
            Some(UserAccess::Revoked)
        );
        assert_eq!(
            board.changed_since(Some(BOB), "bob", &mut bob_seen),
            None,
            "an unchanged user keeps its revision"
        );
        assert_eq!(
            board.access("carol"),
            None,
            "a revoked user the state left out is dropped"
        );
    }

    #[test]
    fn given_a_revoke_that_a_rebuild_dropped_when_a_connection_looks_should_close_it() {
        let board = PermissionBoard::default();
        board.set(ALICE, "alice", reads_nothing());
        board.set(BOB, "bob", reads_nothing());
        let mut alice_seen = board.revision();
        let mut bob_seen = alice_seen;
        board.set(ALICE, "alice", UserAccess::Revoked);

        board.replace(HashMap::from([(BOB, ("bob".to_owned(), reads_nothing()))]));

        assert_eq!(board.access("alice"), None, "dropped");
        assert_eq!(
            board.changed_since(Some(ALICE), "alice", &mut alice_seen),
            Some(UserAccess::Revoked)
        );
        assert_eq!(
            board.changed_since(Some(BOB), "bob", &mut bob_seen),
            None,
            "a user the board still holds"
        );
    }

    fn cursor(op: u64) -> MetadataChangeCursor {
        MetadataChangeCursor {
            log_id: 1,
            op,
            checksum: u128::from(op),
            ordinal: 0,
        }
    }

    fn page(status: MetadataChangeStatus, next: Option<MetadataChangeCursor>) -> MetadataChanges {
        MetadataChanges {
            status,
            phase: MetadataChangePhase::Changes,
            log_id: Some(1),
            oldest_op: 0,
            retained_since: 0,
            head_op: 0,
            next,
            has_more: false,
            records: vec![],
            items: vec![],
            current_page_token: None,
        }
    }

    /// The whole current state, in one page anchored at `anchor`: stream `kafka`, its topic
    /// `orders`, and an Iggy group named `name` there.
    fn current(anchor: u64, name: &str) -> MetadataChanges {
        let orders = topic(KAFKA, 1, 10, "orders");
        MetadataChanges {
            phase: MetadataChangePhase::Current,
            items: vec![
                MetadataCurrentItem::Stream(stream(KAFKA, u64::from(KAFKA), "kafka")),
                MetadataCurrentItem::Topic(MetadataTopicItem {
                    stream_id: orders.stream_id,
                    stream_created_op: orders.stream_created_op,
                    topic_id: orders.topic_id,
                    topic_created_op: orders.topic_created_op,
                    created_at: 0,
                    name: orders.name.clone(),
                    options: MetadataOptions::Unreadable,
                }),
                MetadataCurrentItem::ConsumerGroup(MetadataConsumerGroupItem {
                    stream_id: orders.stream_id,
                    stream_created_op: orders.stream_created_op,
                    topic_id: orders.topic_id,
                    topic_created_op: orders.topic_created_op,
                    group_id: 1,
                    name: name.to_owned(),
                }),
                MetadataCurrentItem::User(user(ALICE, "alice", UserStatus::Active, false)),
            ],
            ..page(MetadataChangeStatus::Ok, Some(cursor(anchor)))
        }
    }

    /// Records after a cursor, ending at `next`.
    fn changes(next: u64, records: Vec<MetadataChange>) -> MetadataChanges {
        MetadataChanges {
            records: records
                .into_iter()
                .map(|change| MetadataChangeRecord {
                    op: next,
                    ordinal: 0,
                    checksum: 0,
                    timestamp: 0,
                    actor_user_id: None,
                    change,
                })
                .collect(),
            ..page(MetadataChangeStatus::Ok, Some(cursor(next)))
        }
    }

    type Answer = Result<MetadataChanges, BridgeError>;

    /// A change log that gives `answers` in order, then no new record, and keeps each query.
    #[derive(Clone)]
    struct FakeLog {
        answers: Arc<Mutex<VecDeque<Answer>>>,
        queries: Arc<Mutex<Vec<MetadataChangeStart>>>,
    }

    impl FakeLog {
        fn new(answers: Vec<Answer>) -> Self {
            Self {
                answers: Arc::new(Mutex::new(answers.into())),
                queries: Arc::default(),
            }
        }

        fn read(&self) -> impl Fn(MetadataChangesQuery) -> std::future::Ready<Answer> + use<> {
            let log = self.clone();
            move |query| {
                assert_eq!(query.groups, FOLLOWED);
                let answer = lock(&log.answers).pop_front().unwrap_or_else(|| {
                    let at = match &query.start {
                        MetadataChangeStart::After(at) => Some(*at),
                        _ => None,
                    };
                    Ok(page(MetadataChangeStatus::Ok, at))
                });
                lock(&log.queries).push(query.start);
                std::future::ready(answer)
            }
        }

        fn starts(&self) -> Vec<&'static str> {
            lock(&self.queries)
                .iter()
                .map(|start| match start {
                    MetadataChangeStart::Current(_) => "current",
                    MetadataChangeStart::After(_) => "after",
                    MetadataChangeStart::Earliest | MetadataChangeStart::Latest => "other",
                })
                .collect()
        }
    }

    struct Followed {
        index: GroupIndex,
        topics: watch::Receiver<u64>,
        permissions: Arc<PermissionBoard>,
        deleted: broadcast::Receiver<String>,
        task: tokio::task::JoinHandle<()>,
    }

    fn follow(log: &FakeLog) -> Followed {
        let index = GroupIndex::following();
        let topics = Arc::new(watch::Sender::new(0));
        let permissions = Arc::new(PermissionBoard::default());
        let deletions = broadcast::Sender::new(TOPIC_DELETIONS_CAPACITY);
        let deleted = deletions.subscribe();
        let follower = Follower {
            mapping: mapping(),
            groups: index.feed(),
            topics: Arc::clone(&topics),
            permissions: Arc::clone(&permissions),
            deletions,
        };
        let read = log.read();
        let task = tokio::spawn(async move { follower.follow(read).await });
        Followed {
            index,
            topics: topics.subscribe(),
            permissions,
            deleted,
            task,
        }
    }

    fn later() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    async fn groups_now(index: &GroupIndex) -> ScannedGroups {
        index
            .groups(later(), || {
                std::future::ready(Err::<super::super::group_index::Scanned, _>(
                    BridgeError::Timeout,
                ))
            })
            .await
    }

    fn names(groups: &ScannedGroups) -> Vec<String> {
        let ScannedGroups::Found(groups) = groups else {
            panic!("the change log showed groups: {groups:?}");
        };
        groups.iter().map(str::to_owned).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_current_state_then_changes_should_publish_the_kafka_groups_of_both() {
        let orders = topic(KAFKA, 1, 10, "orders");
        let log = FakeLog::new(vec![
            Ok(current(5, "kafka.cg.first-app")),
            Ok(changes(6, vec![group(&orders, 2, "kafka.cg.second-app")])),
        ]);
        let followed = follow(&log);

        let first = groups_now(&followed.index).await;
        sleep(FOLLOW_INTERVAL * 4).await;
        let then = groups_now(&followed.index).await;

        assert_ne!(names(&first), NONE);
        assert_eq!(names(&then), ["first-app", "second-app"]);
        assert_eq!(&log.starts()[..3], ["current", "after", "after"]);
        assert_eq!(
            followed.permissions.access("alice"),
            Some(reads_nothing()),
            "the users of the current state"
        );
        assert!(*followed.topics.borrow() > 0);
        followed.task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_expired_position_should_read_the_current_state_again() {
        let log = FakeLog::new(vec![
            Ok(current(5, "kafka.cg.first-app")),
            Ok(page(MetadataChangeStatus::Expired, None)),
            Ok(current(50, "kafka.cg.second-app")),
        ]);
        let followed = follow(&log);

        sleep(FOLLOW_INTERVAL * 4).await;
        let groups = groups_now(&followed.index).await;

        assert_eq!(names(&groups), ["second-app"]);
        assert_eq!(&log.starts()[..4], ["current", "after", "current", "after"]);
        followed.task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_node_behind_the_position_should_read_from_the_same_position_again() {
        let orders = topic(KAFKA, 1, 10, "orders");
        let log = FakeLog::new(vec![
            Ok(current(5, "kafka.cg.first-app")),
            Ok(page(MetadataChangeStatus::Behind, Some(cursor(5)))),
            Ok(changes(6, vec![group_deleted(&orders, 1)])),
        ]);
        let followed = follow(&log);

        sleep(FOLLOW_INTERVAL * 4).await;
        let groups = groups_now(&followed.index).await;

        assert_eq!(names(&groups), NONE);
        let queries = lock(&log.queries).clone();
        assert_eq!(queries[1], MetadataChangeStart::After(cursor(5)));
        assert_eq!(queries[2], MetadataChangeStart::After(cursor(5)));
        followed.task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_unauthorized_bridge_user_should_warn_and_fall_back_to_scans() {
        let log = FakeLog::new(vec![Err(BridgeError::Iggy(IggyError::Unauthorized))]);
        let followed = follow(&log);

        followed.task.await.expect("the follower ends");

        assert_eq!(followed.index.source(), OffsetGroupSource::Scans);
        assert_eq!(log.starts(), ["current"], "it reads the log no more");
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_iggy_server_without_the_change_log_should_fall_back_to_scans() {
        let log = FakeLog::new(vec![Err(BridgeError::Iggy(IggyError::InvalidCommand))]);
        let followed = follow(&log);

        followed.task.await.expect("the follower ends");

        assert_eq!(followed.index.source(), OffsetGroupSource::Scans);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_failed_read_should_retry_after_a_pause() {
        let log = FakeLog::new(vec![
            Err(BridgeError::Timeout),
            Err(BridgeError::Timeout),
            Ok(current(5, "kafka.cg.first-app")),
        ]);
        let followed = follow(&log);

        sleep(RETRY_PAUSE_FIRST * 4).await;
        let groups = groups_now(&followed.index).await;

        assert_eq!(names(&groups), ["first-app"]);
        assert_eq!(&log.starts()[..3], ["current", "current", "current"]);
        followed.task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_topic_change_should_move_the_topic_version() {
        let log = FakeLog::new(vec![
            Ok(current(5, "kafka.cg.first-app")),
            Ok(changes(5, vec![])),
            Ok(changes(6, vec![topic_deleted(KAFKA, 1, 10)])),
        ]);
        let mut followed = follow(&log);
        sleep(FOLLOW_INTERVAL / 2).await;
        let rebuilt = *followed.topics.borrow_and_update();

        sleep(FOLLOW_INTERVAL * 4).await;

        assert!(*followed.topics.borrow() > rebuilt);
        assert_eq!(names(&groups_now(&followed.index).await), NONE);
        followed.task.abort();
    }

    /// A gap in the log can hide a delete.
    #[tokio::test(start_paused = true)]
    async fn given_a_rebuild_without_a_known_topic_when_following_should_report_it_deleted() {
        let payments = topic(KAFKA, 2, 11, "payments");
        let log = FakeLog::new(vec![
            Ok(current(5, "kafka.cg.first-app")),
            Ok(changes(6, vec![MetadataChange::TopicCreated(payments)])),
            Ok(page(MetadataChangeStatus::Expired, None)),
            Ok(current(50, "kafka.cg.first-app")),
        ]);
        let mut followed = follow(&log);

        sleep(FOLLOW_INTERVAL * 4).await;

        assert_eq!(&log.starts()[..4], ["current", "after", "after", "current"]);
        assert_eq!(
            followed.deleted.try_recv().ok().as_deref(),
            Some("payments")
        );
        assert!(
            followed.deleted.try_recv().is_err(),
            "the first read of the current state reports nothing"
        );
        followed.task.abort();
    }
}
