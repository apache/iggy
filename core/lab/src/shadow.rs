/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::scenarios::{LaneNamespace, Namespace, NamespacePrefixes, NamespaceScope};
use rand::{Rng, RngExt};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceOrigin {
    Setup,
    Dynamic,
}

const TOMBSTONE_RETENTION: Duration = Duration::from_secs(10);

pub struct ShadowState {
    pub streams: BTreeMap<String, ShadowStream>,
    prefixes: NamespacePrefixes,
    /// Recently deleted resource names with deletion timestamp, for error classification.
    recently_deleted: VecDeque<(String, Instant)>,
    /// Recently purged (stream, topic) pairs — offsets reset after purge.
    recently_purged: VecDeque<(String, Option<String>, Instant)>,
}

pub struct ShadowStream {
    pub topics: BTreeMap<String, ShadowTopic>,
    pub origin: ResourceOrigin,
    pub namespace: Namespace,
}

pub struct ShadowTopic {
    pub partitions: u32,
    pub consumer_groups: BTreeSet<String>,
    pub origin: ResourceOrigin,
}

impl ShadowState {
    pub fn new(prefixes: NamespacePrefixes) -> Self {
        Self {
            streams: BTreeMap::new(),
            prefixes,
            recently_deleted: VecDeque::new(),
            recently_purged: VecDeque::new(),
        }
    }

    pub fn stream_exists(&self, name: &str) -> bool {
        self.streams.contains_key(name)
    }

    pub fn topic_exists(&self, stream: &str, topic: &str) -> bool {
        self.streams
            .get(stream)
            .is_some_and(|s| s.topics.contains_key(topic))
    }

    pub fn get_topic(&self, stream: &str, topic: &str) -> Option<&ShadowTopic> {
        self.streams.get(stream).and_then(|s| s.topics.get(topic))
    }

    pub fn random_stream(
        &self,
        rng: &mut impl Rng,
        scope: NamespaceScope,
        namespace: LaneNamespace,
    ) -> Option<&str> {
        let candidates: Vec<_> = self
            .streams
            .iter()
            .filter(|(_, s)| {
                matches_scope(s.origin, scope) && matches_namespace(s.namespace, namespace)
            })
            .collect();
        if candidates.is_empty() {
            return None;
        }
        let idx = rng.random_range(0..candidates.len());
        Some(candidates[idx].0.as_str())
    }

    /// Returns (stream_name, topic_name) for a random topic that exists.
    pub fn random_topic(
        &self,
        rng: &mut impl Rng,
        scope: NamespaceScope,
        namespace: LaneNamespace,
    ) -> Option<(&str, &str)> {
        let streams_with_topics: Vec<_> = self
            .streams
            .iter()
            .filter(|(_, s)| {
                matches_scope(s.origin, scope) && matches_namespace(s.namespace, namespace)
            })
            .flat_map(|(sname, s)| {
                s.topics
                    .iter()
                    .filter(move |(_, t)| matches_scope(t.origin, scope))
                    .map(move |(tname, _)| (sname.as_str(), tname.as_str()))
            })
            .collect();
        if streams_with_topics.is_empty() {
            return None;
        }
        Some(streams_with_topics[rng.random_range(0..streams_with_topics.len())])
    }

    /// Returns (stream_name, topic_name, partition_count) for a random topic.
    pub fn random_topic_with_partitions(
        &self,
        rng: &mut impl Rng,
        scope: NamespaceScope,
        namespace: LaneNamespace,
    ) -> Option<(&str, &str, u32)> {
        let (s, t) = self.random_topic(rng, scope, namespace)?;
        let partitions = self.streams[s].topics[t].partitions;
        Some((s, t, partitions))
    }

    pub fn apply_create_stream(&mut self, name: String, origin: ResourceOrigin) {
        let namespace = self
            .prefixes
            .namespace_of(&name)
            .unwrap_or(Namespace::Churn);
        self.streams.insert(
            name,
            ShadowStream {
                topics: BTreeMap::new(),
                origin,
                namespace,
            },
        );
    }

    pub fn apply_delete_stream(&mut self, name: &str) {
        if self.streams.remove(name).is_some() {
            self.recently_deleted
                .push_back((name.to_owned(), Instant::now()));
        }
    }

    pub fn apply_create_topic(
        &mut self,
        stream: &str,
        name: String,
        partitions: u32,
        origin: ResourceOrigin,
    ) {
        if let Some(s) = self.streams.get_mut(stream) {
            s.topics.insert(
                name,
                ShadowTopic {
                    partitions,
                    consumer_groups: BTreeSet::new(),
                    origin,
                },
            );
        }
    }

    pub fn apply_delete_topic(&mut self, stream: &str, topic: &str) {
        if let Some(s) = self.streams.get_mut(stream)
            && s.topics.remove(topic).is_some()
        {
            let key = format!("{stream}/{topic}");
            self.recently_deleted.push_back((key, Instant::now()));
        }
    }

    pub fn apply_create_partitions(&mut self, stream: &str, topic: &str, count: u32) {
        if let Some(t) = self
            .streams
            .get_mut(stream)
            .and_then(|s| s.topics.get_mut(topic))
        {
            t.partitions += count;
        }
    }

    pub fn apply_delete_partitions(&mut self, stream: &str, topic: &str, count: u32) {
        if let Some(t) = self
            .streams
            .get_mut(stream)
            .and_then(|s| s.topics.get_mut(topic))
        {
            t.partitions = t.partitions.saturating_sub(count);
        }
    }

    pub fn apply_create_consumer_group(&mut self, stream: &str, topic: &str, name: String) {
        if let Some(t) = self
            .streams
            .get_mut(stream)
            .and_then(|s| s.topics.get_mut(topic))
        {
            t.consumer_groups.insert(name);
        }
    }

    pub fn apply_delete_consumer_group(&mut self, stream: &str, topic: &str, name: &str) {
        if let Some(t) = self
            .streams
            .get_mut(stream)
            .and_then(|s| s.topics.get_mut(topic))
        {
            t.consumer_groups.remove(name);
        }
    }

    pub fn was_recently_deleted(&self, name: &str) -> bool {
        self.recently_deleted.iter().any(|(n, _)| n == name)
    }

    /// Record that a stream or topic was purged (offsets reset).
    /// `topic` is `None` for stream-level purge (all topics affected).
    pub fn record_purge(&mut self, stream: String, topic: Option<String>) {
        self.recently_purged
            .push_back((stream, topic, Instant::now()));
    }

    /// Check whether offsets for this partition may have been reset by a recent purge.
    pub fn was_recently_purged(&self, stream: &str, topic: &str) -> bool {
        self.recently_purged
            .iter()
            .any(|(s, t, _)| s == stream && (t.is_none() || t.as_deref() == Some(topic)))
    }

    pub fn prune_tombstones(&mut self) {
        let cutoff = Instant::now() - TOMBSTONE_RETENTION;
        while self
            .recently_deleted
            .front()
            .is_some_and(|(_, ts)| *ts < cutoff)
        {
            self.recently_deleted.pop_front();
        }
        while self
            .recently_purged
            .front()
            .is_some_and(|(_, _, ts)| *ts < cutoff)
        {
            self.recently_purged.pop_front();
        }
    }
}

fn matches_scope(origin: ResourceOrigin, scope: NamespaceScope) -> bool {
    match scope {
        NamespaceScope::Any => true,
        NamespaceScope::SetupOnly => origin == ResourceOrigin::Setup,
        NamespaceScope::DynamicOnly => origin == ResourceOrigin::Dynamic,
    }
}

fn matches_namespace(ns: Namespace, lane_ns: LaneNamespace) -> bool {
    match lane_ns {
        LaneNamespace::Stable => ns == Namespace::Stable,
        LaneNamespace::Churn => ns == Namespace::Churn,
        LaneNamespace::Both => true,
    }
}
