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
use std::collections::HashMap;

use super::PartitionTarget;

/// The most routes that one client keeps. A new route beyond it evicts the
/// oldest lookup.
pub(crate) const MAX_CACHED_ROUTES: usize = 4096;

/// A partition as its caller spelled it. A stream or topic named by number
/// and the same one named by name are two keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

#[derive(Debug, Default)]
pub(crate) struct RouteCache {
    entries: HashMap<PartitionKey, Entry>,
    lookups: u64,
}

impl RouteCache {
    /// The route of `key`, unless it is stale, from another session, or
    /// older than the metadata `watermark` that the session saw.
    pub(crate) fn usable(
        &self,
        key: &PartitionKey,
        fence: u64,
        watermark: u64,
    ) -> Option<PartitionRouteResponse> {
        self.entries
            .get(key)
            .filter(|entry| {
                !entry.stale && entry.fence == fence && entry.route.metadata_op >= watermark
            })
            .map(|entry| entry.route)
    }

    /// The replica that the next lookup of `key` asks first, if a refusal
    /// or a failure named one.
    pub(crate) fn ask_first(&self, key: &PartitionKey) -> Option<u8> {
        self.entries.get(key).and_then(|entry| entry.ask_first)
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
        if let Some(cached) = self.entries.get(key) {
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
        } else if self.entries.len() >= MAX_CACHED_ROUTES {
            self.evict_oldest();
        }
        self.lookups += 1;
        self.entries.insert(
            key.clone(),
            Entry {
                route,
                fence,
                stale: false,
                floor: route.view,
                ask_first: None,
                lookup: self.lookups,
            },
        );
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
        for (key, entry) in &mut self.entries {
            if key.partition_id == partition_id && entry.route.incarnation == route.incarnation {
                entry.stale = true;
                update(entry);
            }
        }
    }

    fn evict_oldest(&mut self) {
        let oldest = self
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.lookup)
            .map(|(key, _)| key.clone());
        if let Some(oldest) = oldest {
            self.entries.remove(&oldest);
        }
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

        let newcomer = key(capacity);
        cache
            .merge(&newcomer, answer(INCARNATION, METADATA_OP, 1, 0), FENCE)
            .unwrap();

        assert_eq!(cache.entries.len(), MAX_CACHED_ROUTES);
        assert_eq!(
            cache.usable(&key(1), FENCE, METADATA_OP),
            None,
            "the oldest lookup goes"
        );
        assert!(cache.usable(&first, FENCE, METADATA_OP).is_some());
        assert!(cache.usable(&newcomer, FENCE, METADATA_OP).is_some());
    }
}
