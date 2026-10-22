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

//! The routes of one client. A route has no age limit. It goes stale after
//! a refusal or a failure, and a stale route keeps its view as the lowest
//! view that its next lookup accepts. So a lookup never goes back to an
//! older primary of the same partition history.

use iggy_binary_protocol::requests::messages::GetPartitionRouteRequest;
use iggy_binary_protocol::responses::messages::PartitionRouteResponse;
use iggy_common::wire_conversions::identifier_to_wire;
use iggy_common::{Identifier, IggyError};
use std::collections::{BTreeMap, HashMap};

use super::PartitionTarget;

/// The most routes that one client keeps. A new route beyond it evicts the
/// oldest lookup.
pub(crate) const MAX_CACHED_ROUTES: usize = 4096;

/// A partition as its caller spelled it. A stream or topic named by number
/// and the same one named by name are two keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartitionKey {
    pub(crate) stream_id: Identifier,
    pub(crate) topic_id: Identifier,
    pub(crate) partition_id: u32,
}

impl From<&PartitionTarget<'_>> for PartitionKey {
    fn from(target: &PartitionTarget<'_>) -> Self {
        Self {
            stream_id: target.stream_id.clone(),
            topic_id: target.topic_id.clone(),
            partition_id: target.partition_id,
        }
    }
}

impl TryFrom<&PartitionKey> for GetPartitionRouteRequest {
    type Error = IggyError;

    fn try_from(key: &PartitionKey) -> Result<Self, Self::Error> {
        Ok(Self {
            stream_id: identifier_to_wire(&key.stream_id)?,
            topic_id: identifier_to_wire(&key.topic_id)?,
            partition_id: key.partition_id,
        })
    }
}

#[derive(Debug)]
struct Entry {
    route: PartitionRouteResponse,
    /// The session fence at the lookup. A route serves only that session.
    fence: u64,
    stale: bool,
    /// The lowest view that the next lookup accepts.
    floor: u32,
    /// The replica that the next lookup asks first. The ids after it follow.
    ask_first: Option<u8>,
    /// Orders the entries for eviction.
    lookup: u64,
}

impl Entry {
    /// Whether the route serves the session of `fence` and is not stale or
    /// older than the metadata `watermark` that the session saw.
    fn is_usable(&self, fence: u64, watermark: u64) -> bool {
        !self.stale && self.fence == fence && self.route.metadata_op >= watermark
    }
}

/// The routes of one topic as its caller spelled it, by partition.
type TopicRoutes = HashMap<u32, Entry>;

#[derive(Debug, Default)]
pub(crate) struct RouteCache {
    /// The routes by stream, then by topic, so the partition count of a
    /// topic reads only the routes of that topic.
    streams: HashMap<Identifier, HashMap<Identifier, TopicRoutes>>,
    /// The key of every entry by its lookup, so an eviction at capacity
    /// finds the oldest entry without a scan.
    by_lookup: BTreeMap<u64, PartitionKey>,
    lookups: u64,
}

impl RouteCache {
    pub(crate) fn partition_count(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        fence: u64,
        watermark: u64,
    ) -> Option<u32> {
        self.topic(stream_id, topic_id)?
            .values()
            .filter(|entry| entry.is_usable(fence, watermark))
            .max_by_key(|entry| (entry.route.metadata_op, entry.lookup))
            .map(|entry| entry.route.partitions_count)
    }

    /// The route of `key`, unless it is stale, from another session, or
    /// older than the metadata `watermark` that the session saw.
    ///
    /// The watermark is global: one metadata write of the session costs every
    /// cached route one more lookup at its next use, even for topics that the
    /// write did not touch. A fence per topic needs trusted provenance of the
    /// topics that each metadata commit touched, and the client has none. A
    /// reply names only its commit. A request names its stream and topic as
    /// the caller spelled them, by number or by name, or names only the stream
    /// for a change to all of its topics. A fence keyed by those spellings
    /// would miss a route cached under another spelling of the same topic, so
    /// a route from before the write could stay usable.
    pub(crate) fn usable(
        &self,
        key: &PartitionKey,
        fence: u64,
        watermark: u64,
    ) -> Option<PartitionRouteResponse> {
        self.entry(key)
            .filter(|entry| entry.is_usable(fence, watermark))
            .map(|entry| entry.route)
    }

    /// The replica that the next lookup of `key` asks first, if a refusal
    /// or a failure named one.
    pub(crate) fn ask_first(&self, key: &PartitionKey) -> Option<u8> {
        self.entry(key).and_then(|entry| entry.ask_first)
    }

    /// Takes a lookup answer for `key`, unless it is older than the cached
    /// route. Another history wins only with metadata at least as new.
    /// Within one history, a view below the floor loses, and so does the
    /// same view with another primary. The metadata facts of the answer
    /// never replace newer ones.
    pub(crate) fn merge(
        &mut self,
        key: &PartitionKey,
        answer: PartitionRouteResponse,
        fence: u64,
    ) -> Option<PartitionRouteResponse> {
        let mut route = answer;
        if let Some(cached) = self.entry(key) {
            let cached_route = cached.route;
            if route.incarnation == cached_route.incarnation {
                let newer = route.view >= cached.floor
                    && (route.view > cached_route.view
                        || route.primary_replica == cached_route.primary_replica);
                if !newer {
                    return None;
                }
                if route.metadata_op < cached_route.metadata_op {
                    route.metadata_op = cached_route.metadata_op;
                    route.partitions_count = cached_route.partitions_count;
                }
            } else if route.metadata_op < cached_route.metadata_op {
                return None;
            }
        } else if self.by_lookup.len() >= MAX_CACHED_ROUTES {
            self.evict_oldest();
        }
        self.lookups += 1;
        let replaced = self
            .streams
            .entry(key.stream_id.clone())
            .or_default()
            .entry(key.topic_id.clone())
            .or_default()
            .insert(
                key.partition_id,
                Entry {
                    route,
                    fence,
                    stale: false,
                    floor: route.view,
                    ask_first: None,
                    lookup: self.lookups,
                },
            );
        if let Some(replaced) = replaced {
            self.by_lookup.remove(&replaced.lookup);
        }
        self.by_lookup.insert(self.lookups, key.clone());
        Some(route)
    }

    /// Marks the history of `route` stale after the history changed or the
    /// owner moved. The next lookup starts at the metadata primary.
    pub(crate) fn invalidate(&mut self, route: &PartitionRouteResponse, partition_id: u32) {
        self.mark_stale(route, partition_id, |entry| entry.ask_first = None);
    }

    /// Marks the history of `route` stale after `replica` refused or failed
    /// a request on it. The next lookup starts after that replica.
    pub(crate) fn failed(
        &mut self,
        route: &PartitionRouteResponse,
        partition_id: u32,
        replica: u8,
    ) {
        self.mark_stale(route, partition_id, |entry| {
            entry.ask_first = Some(replica.wrapping_add(1));
        });
    }

    /// Marks the history of `route` stale after `replica`, its primary,
    /// answered that it is not the primary. That replica knows a later view,
    /// so the next lookup asks it first and accepts no view up to this one.
    pub(crate) fn not_primary(
        &mut self,
        route: &PartitionRouteResponse,
        partition_id: u32,
        replica: u8,
    ) {
        let floor = route.view.saturating_add(1);
        self.mark_stale(route, partition_id, |entry| {
            entry.floor = entry.floor.max(floor);
            entry.ask_first = Some(replica);
        });
    }

    /// Every spelling of one partition history shares its staleness.
    fn mark_stale(
        &mut self,
        route: &PartitionRouteResponse,
        partition_id: u32,
        mut update: impl FnMut(&mut Entry),
    ) {
        let partitions = self
            .streams
            .values_mut()
            .flat_map(HashMap::values_mut)
            .filter_map(|routes| routes.get_mut(&partition_id));
        for entry in partitions {
            if entry.route.incarnation == route.incarnation {
                entry.stale = true;
                update(entry);
            }
        }
    }

    /// Removes the oldest entry, and the maps of its topic and stream once
    /// they hold no route.
    fn evict_oldest(&mut self) {
        let Some((_, oldest)) = self.by_lookup.pop_first() else {
            return;
        };
        let Some(topics) = self.streams.get_mut(&oldest.stream_id) else {
            return;
        };
        if let Some(routes) = topics.get_mut(&oldest.topic_id) {
            routes.remove(&oldest.partition_id);
            if routes.is_empty() {
                topics.remove(&oldest.topic_id);
            }
        }
        if topics.is_empty() {
            self.streams.remove(&oldest.stream_id);
        }
    }

    fn topic(&self, stream_id: &Identifier, topic_id: &Identifier) -> Option<&TopicRoutes> {
        self.streams.get(stream_id)?.get(topic_id)
    }

    fn entry(&self, key: &PartitionKey) -> Option<&Entry> {
        self.topic(&key.stream_id, &key.topic_id)?
            .get(&key.partition_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FENCE: u64 = 1;
    const METADATA_OP: u64 = 10;
    const INCARNATION: u64 = 7;
    const PARTITION_ID: u32 = 3;
    const PARTITIONS_COUNT: u32 = 4;

    fn key(partition_id: u32) -> PartitionKey {
        PartitionKey {
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(2).unwrap(),
            partition_id,
        }
    }

    fn answer(
        incarnation: u64,
        metadata_op: u64,
        view: u32,
        primary: u8,
    ) -> PartitionRouteResponse {
        PartitionRouteResponse {
            incarnation,
            metadata_op,
            view,
            partitions_count: PARTITIONS_COUNT,
            primary_replica: primary,
        }
    }

    fn cached(view: u32, primary: u8) -> (RouteCache, PartitionRouteResponse) {
        let mut cache = RouteCache::default();
        let route = cache
            .merge(
                &key(PARTITION_ID),
                answer(INCARNATION, METADATA_OP, view, primary),
                FENCE,
            )
            .unwrap();
        (cache, route)
    }

    /// Every route in the maps by stream and topic.
    fn cached_routes(cache: &RouteCache) -> usize {
        cache
            .streams
            .values()
            .flat_map(HashMap::values)
            .map(HashMap::len)
            .sum()
    }

    #[test]
    fn given_lower_or_conflicting_hints_when_merging_should_keep_the_highest_view_and_newest_metadata()
     {
        let (mut cache, route) = cached(5, 2);
        let partition = key(PARTITION_ID);

        let lower_view = answer(INCARNATION, METADATA_OP, 4, 1);
        let conflict = answer(INCARNATION, METADATA_OP, 5, 1);
        let older_history = answer(INCARNATION - 1, METADATA_OP - 1, 9, 0);
        for refused in [lower_view, conflict, older_history] {
            assert_eq!(cache.merge(&partition, refused, FENCE), None, "{refused:?}");
        }
        assert_eq!(cache.usable(&partition, FENCE, METADATA_OP), Some(route));

        let lagging = PartitionRouteResponse {
            partitions_count: PARTITIONS_COUNT - 1,
            ..answer(INCARNATION, METADATA_OP - 1, 5, 2)
        };
        let kept = cache.merge(&partition, lagging, FENCE);
        assert_eq!(
            kept,
            Some(route),
            "a lagging replica keeps the newer metadata"
        );
        let later_view = answer(INCARNATION, METADATA_OP, 6, 0);
        let moved = cache.merge(&partition, later_view, FENCE);
        assert_eq!(moved, Some(later_view));
        let new_history = answer(INCARNATION + 1, METADATA_OP + 1, 1, 1);
        let reset = cache.merge(&partition, new_history, FENCE);
        assert_eq!(
            reset,
            Some(new_history),
            "a new history starts its own views"
        );
    }

    #[test]
    fn given_a_not_primary_refusal_when_looking_up_again_should_ask_that_replica_first_above_the_view()
     {
        let (mut cache, route) = cached(5, 2);
        let partition = key(PARTITION_ID);

        cache.not_primary(&route, PARTITION_ID, 2);

        assert_eq!(cache.ask_first(&partition), Some(2));
        let same_view = answer(INCARNATION, METADATA_OP, 5, 2);
        assert_eq!(cache.merge(&partition, same_view, FENCE), None);
        let next_view = answer(INCARNATION, METADATA_OP, 6, 0);
        assert!(cache.merge(&partition, next_view, FENCE).is_some());
        assert_eq!(cache.ask_first(&partition), None);
    }

    #[test]
    fn given_two_spellings_of_one_history_when_one_fails_should_mark_both_stale() {
        let (mut cache, route) = cached(5, 2);
        let named = PartitionKey {
            stream_id: Identifier::named("stream").unwrap(),
            ..key(PARTITION_ID)
        };
        let neighbor = key(PARTITION_ID + 1);
        for other in [&named, &neighbor] {
            cache
                .merge(other, answer(INCARNATION, METADATA_OP, 5, 2), FENCE)
                .unwrap();
        }

        cache.failed(&route, PARTITION_ID, 2);

        assert_eq!(cache.usable(&key(PARTITION_ID), FENCE, METADATA_OP), None);
        assert_eq!(cache.usable(&named, FENCE, METADATA_OP), None);
        assert_eq!(cache.ask_first(&named), Some(3));
        assert!(cache.usable(&neighbor, FENCE, METADATA_OP).is_some());

        cache.invalidate(&route, PARTITION_ID);
        assert_eq!(cache.ask_first(&named), None);
    }

    #[test]
    fn given_a_new_fence_or_a_higher_watermark_when_reading_a_route_should_refuse_it() {
        let (cache, route) = cached(5, 2);
        let partition = key(PARTITION_ID);

        assert_eq!(cache.usable(&partition, FENCE, METADATA_OP), Some(route));
        assert_eq!(cache.usable(&partition, FENCE + 1, METADATA_OP), None);
        assert_eq!(cache.usable(&partition, FENCE, METADATA_OP + 1), None);
    }

    #[test]
    fn given_routes_for_one_topic_when_selecting_should_use_the_newest_usable_partition_count() {
        let (mut cache, old) = cached(1, 0);
        let newest = PartitionRouteResponse {
            partitions_count: PARTITIONS_COUNT + 2,
            metadata_op: METADATA_OP + 1,
            ..old
        };
        let other = key(PARTITION_ID + 1);
        cache.merge(&other, newest, FENCE).unwrap();
        let topic = key(PARTITION_ID);
        assert_eq!(
            cache.partition_count(&topic.stream_id, &topic.topic_id, FENCE, METADATA_OP),
            Some(newest.partitions_count)
        );
        cache.failed(&newest, other.partition_id, 0);
        assert_eq!(
            cache.partition_count(&topic.stream_id, &topic.topic_id, FENCE, METADATA_OP),
            Some(PARTITIONS_COUNT)
        );
        assert_eq!(
            cache.partition_count(&topic.stream_id, &topic.topic_id, FENCE, METADATA_OP + 1),
            None
        );
        assert_eq!(
            cache.partition_count(&topic.stream_id, &topic.topic_id, FENCE + 1, METADATA_OP),
            None
        );
    }

    #[test]
    fn given_newer_routes_of_other_topics_when_selecting_should_count_only_the_asked_topic() {
        let (mut cache, route) = cached(1, 0);
        let newer = PartitionRouteResponse {
            incarnation: INCARNATION + 1,
            partitions_count: PARTITIONS_COUNT + 5,
            metadata_op: METADATA_OP + 1,
            ..route
        };
        let other_topic = PartitionKey {
            topic_id: Identifier::numeric(3).unwrap(),
            ..key(PARTITION_ID)
        };
        let other_stream = PartitionKey {
            stream_id: Identifier::numeric(9).unwrap(),
            ..key(PARTITION_ID)
        };
        let other_spelling = PartitionKey {
            topic_id: Identifier::named("topic").unwrap(),
            ..key(PARTITION_ID)
        };
        for other in [&other_topic, &other_stream, &other_spelling] {
            cache.merge(other, newer, FENCE).unwrap();
        }

        let topic = key(PARTITION_ID);
        assert_eq!(
            cache.partition_count(&topic.stream_id, &topic.topic_id, FENCE, METADATA_OP),
            Some(PARTITIONS_COUNT)
        );
        assert_eq!(
            cache.partition_count(
                &other_topic.stream_id,
                &other_topic.topic_id,
                FENCE,
                METADATA_OP
            ),
            Some(newer.partitions_count)
        );
    }

    #[test]
    fn given_an_identifier_read_back_by_serde_when_counting_partitions_should_find_no_route() {
        let (cache, _) = cached(1, 0);
        let topic = key(PARTITION_ID);
        let read_back = |id: &Identifier| -> Identifier {
            serde_json::from_str(&serde_json::to_string(id).unwrap()).unwrap()
        };

        assert_eq!(
            cache.partition_count(
                &read_back(&topic.stream_id),
                &topic.topic_id,
                FENCE,
                METADATA_OP
            ),
            None
        );
        assert_eq!(
            cache.partition_count(
                &topic.stream_id,
                &read_back(&topic.topic_id),
                FENCE,
                METADATA_OP
            ),
            None
        );
    }

    #[test]
    fn given_a_full_route_cache_when_a_new_lookup_arrives_should_evict_only_the_oldest() {
        let mut cache = RouteCache::default();
        let capacity = u32::try_from(MAX_CACHED_ROUTES).unwrap();
        for partition_id in 0..capacity {
            cache
                .merge(
                    &key(partition_id),
                    answer(INCARNATION, METADATA_OP, 1, 0),
                    FENCE,
                )
                .unwrap();
        }
        let first = key(0);
        cache
            .merge(&first, answer(INCARNATION, METADATA_OP, 2, 1), FENCE)
            .unwrap();

        let newcomers = [key(capacity), key(capacity + 1)];
        for newcomer in &newcomers {
            cache
                .merge(newcomer, answer(INCARNATION, METADATA_OP, 1, 0), FENCE)
                .unwrap();
        }

        assert_eq!(cached_routes(&cache), MAX_CACHED_ROUTES);
        assert_eq!(cache.by_lookup.len(), MAX_CACHED_ROUTES);
        for evicted in [key(1), key(2)] {
            assert_eq!(
                cache.usable(&evicted, FENCE, METADATA_OP),
                None,
                "the oldest lookups go"
            );
        }
        for kept in [&first, &key(3)].into_iter().chain(&newcomers) {
            assert!(cache.usable(kept, FENCE, METADATA_OP).is_some(), "{kept:?}");
        }
    }

    #[test]
    fn given_the_last_route_of_a_stream_when_evicted_should_drop_the_maps_of_its_stream() {
        let mut cache = RouteCache::default();
        let lone = PartitionKey {
            stream_id: Identifier::numeric(9).unwrap(),
            ..key(PARTITION_ID)
        };
        cache
            .merge(&lone, answer(INCARNATION, METADATA_OP, 1, 0), FENCE)
            .unwrap();
        let capacity = u32::try_from(MAX_CACHED_ROUTES).unwrap();
        for partition_id in 0..capacity {
            cache
                .merge(
                    &key(partition_id),
                    answer(INCARNATION, METADATA_OP, 1, 0),
                    FENCE,
                )
                .unwrap();
        }

        assert_eq!(cache.usable(&lone, FENCE, METADATA_OP), None);
        assert!(!cache.streams.contains_key(&lone.stream_id));
        assert_eq!(cache.streams.len(), 1);
        assert_eq!(cached_routes(&cache), MAX_CACHED_ROUTES);
    }

    #[test]
    fn given_the_last_route_of_a_topic_when_evicted_should_keep_the_other_topics_of_its_stream() {
        let mut cache = RouteCache::default();
        let lone = PartitionKey {
            topic_id: Identifier::numeric(3).unwrap(),
            ..key(PARTITION_ID)
        };
        cache
            .merge(&lone, answer(INCARNATION, METADATA_OP, 1, 0), FENCE)
            .unwrap();
        let capacity = u32::try_from(MAX_CACHED_ROUTES).unwrap();
        for partition_id in 0..capacity {
            cache
                .merge(
                    &key(partition_id),
                    answer(INCARNATION, METADATA_OP, 1, 0),
                    FENCE,
                )
                .unwrap();
        }

        assert_eq!(cache.usable(&lone, FENCE, METADATA_OP), None);
        let topics = cache
            .streams
            .get(&lone.stream_id)
            .expect("the stream keeps its other topic");
        assert!(!topics.contains_key(&lone.topic_id));
        assert_eq!(topics.len(), 1);
        assert!(cache.usable(&key(0), FENCE, METADATA_OP).is_some());
        assert_eq!(cached_routes(&cache), MAX_CACHED_ROUTES);
    }
}
