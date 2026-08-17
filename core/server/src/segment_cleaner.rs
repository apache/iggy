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

//! Per-shard periodic segment cleaner.
//!
//! Local and unreplicated: every replica (primary and backup) trims its own
//! expired or over-budget sealed segments. Divergence in physical log start is
//! invisible to clients because reads are served by the partition primary. The
//! timer resolves each owned partition's retention policy from metadata and
//! stamps `now`, then hands a `CleanPartition` request to the shard pump, the
//! single writer of partition state, which performs the deletion serialized
//! with reads. This mirrors the legacy server's `MessagesCleaner` ->
//! message-pump `CleanTopicMessages` path.

use crate::bootstrap::ServerShard;
use consensus::{MetadataHandle, PartitionsHandle};
use iggy_common::{IggyExpiry, IggyTimestamp, MaxTopicSize};
use metadata::impls::metadata::StreamsFrontend;
use shard::Receiver;
use std::collections::HashSet;
use std::rc::Rc;
use std::time::Duration;
use tracing::{trace, warn};

/// Disclosure for a `max_topic_size` the server cannot enforce as written,
/// shared by admission (which says it once, when the cap is set) and the
/// cleaner (which says it once per topic, when it floors the budget).
pub const UNENFORCEABLE_TOPIC_SIZE_WARN: &str = concat!(
    "max_topic_size is below what retention can enforce; the segment cleaner keeps at ",
    "least one sealed segment per partition instead, and enforces nothing while disabled"
);

/// Run the cleaner until `stop` fires. Wakes every `interval`; expiry and size
/// are evaluated against wall-clock and resident bytes, so no metadata-commit
/// wake is needed.
pub async fn run_segment_cleaner(shard: Rc<ServerShard>, stop: Receiver<()>, interval: Duration) {
    trace!(
        shard = shard.id,
        interval_ms = interval.as_millis(),
        "segment cleaner started"
    );
    // Topic incarnations whose floored budget has already been reported. Only
    // grows, and only for topics whose configured cap is below what can be
    // enforced, so it stays far smaller than the topic table.
    let mut reported_floors = HashSet::new();
    loop {
        // `Ok(_)`: stop signalled -> exit. `Err(_)`: interval elapsed -> pass.
        match compio::time::timeout(interval, stop.recv()).await {
            Ok(_) => break,
            Err(_) => stage_owned_partitions(&shard, &mut reported_floors),
        }
    }
    trace!(shard = shard.id, "segment cleaner exited");
}

/// Stage a cleaner pass for every partition this shard owns whose topic has a
/// retention policy. Reads config off-pump and hands the resolved decision to
/// the pump; partitions with no policy are skipped without a frame.
///
/// `reported_floors` carries the topics already named in a floor-raise log
/// across passes, so the disclosure lands once per topic instead of once per
/// interval tick. Slab ids are reused after a delete, so the creation revision
/// is part of the key: a topic recreated into the same slots discloses again.
fn stage_owned_partitions(
    shard: &Rc<ServerShard>,
    reported_floors: &mut HashSet<(usize, usize, u64)>,
) {
    let now = IggyTimestamp::now();
    let namespaces: Vec<_> = shard.plane.partitions().namespaces().copied().collect();
    let streams = shard.plane.metadata().mux_stm.streams();
    let max_message_size = u64::try_from(shard.bus_max_message_size()).unwrap_or(u64::MAX);
    for namespace in namespaces {
        let Some((message_expiry, max_topic_size, partition_count, segment_size)) =
            streams.topic_retention_config(namespace.stream_id(), namespace.topic_id())
        else {
            continue;
        };

        // `ServerDefault` resolves to "never expire" here, matching the legacy
        // cleaner: a topic created with the server default never expires
        // segments unless an explicit duration was stored.
        let has_expiry = !matches!(
            message_expiry,
            IggyExpiry::NeverExpire | IggyExpiry::ServerDefault
        );
        // A topic that left `segment_size` unset rotates at the node default,
        // which is the same value admission floors its cap against.
        let segment_size = segment_size.map_or(iggy_common::DEFAULT_SEGMENT_SIZE, |segment_size| {
            segment_size.as_bytes_u64()
        });
        let budget = per_partition_size_budget(
            max_topic_size,
            iggy_common::DEFAULT_MAX_TOPIC_SIZE,
            partition_count,
            segment_size.saturating_add(max_message_size),
        );

        if !has_expiry && budget.is_none() {
            continue;
        }
        if let Some(budget) = budget
            && budget.max_bytes > budget.configured_share
            && reported_floors.insert((
                namespace.stream_id(),
                namespace.topic_id(),
                streams
                    .created_revision_for_namespace(namespace)
                    .unwrap_or_default(),
            ))
        {
            warn!(
                shard = shard.id,
                stream_id = namespace.stream_id(),
                topic_id = namespace.topic_id(),
                max_topic_size = budget.resolved_cap,
                partition_count,
                configured_share = budget.configured_share,
                enforced_per_partition = budget.max_bytes,
                "{UNENFORCEABLE_TOPIC_SIZE_WARN}"
            );
        }
        shard.request_clean_partition(
            namespace,
            now,
            message_expiry,
            budget.map(|budget| budget.max_bytes),
        );
    }
}

/// A topic's enforced per-partition budget, next to the share it was derived
/// from. The two differ exactly when the configured cap is too small to be
/// enforceable, which is what the cleaner discloses once per topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartitionSizeBudget {
    /// Cap on this partition's SEALED bytes.
    max_bytes: u64,
    /// `max_topic_size / partition_count`, before the floor.
    configured_share: u64,
    /// The topic-wide cap the share came from, with `ServerDefault` already
    /// resolved to the node default. The sentinel's own `as_bytes_u64()` is 0,
    /// so this is what a caller has to report.
    resolved_cap: u64,
}

/// Per-partition byte budget for a topic, or `None` for "no cap".
///
/// The cluster has no single owner of a topic-wide total, so each partition
/// keeps an equal share.
///
/// That share is raised to `sealed_segment_ceiling`, the largest a single
/// SEALED segment can be. Admission rejects a cap below one segment TOPIC-wide,
/// so an accepted cap can still divide into a share smaller than one segment,
/// and a share below what one sealed segment reaches retains no history at all:
/// the oldest sealed segment is over budget the moment it seals, every pass,
/// forever. The floor makes the cap mean "keep at least the newest sealed
/// segment", which is the least an enforceable retention policy can say. The
/// raise is disclosed by the caller, since the enforced value then diverges
/// from the one `GetTopic` echoes.
///
/// The ceiling is `segment_size` plus the CONFIGURED bus frame cap, not a
/// compile-time constant: rotation fires after the append that crosses the
/// target, so the crossing batch lands whole and a sealed segment runs up to
/// one maximum bus frame past `segment_size`. An operator who raises that knob
/// grows real sealed segments with it.
///
/// `ServerDefault` is resolved against the node default HERE, at enforcement
/// time. Create admission rewrites the sentinel before replication, but an
/// UPDATE to `ServerDefault` leaves it in committed state, and reading that as
/// "no cap" made an updated topic behave differently from an identically
/// configured created one. A node default of unlimited (the shipped config)
/// still yields `None`.
///
/// `ServerDefault` must never reach a sized branch: its `as_bytes_u64()` is 0,
/// which would trim every sealed segment.
fn per_partition_size_budget(
    max_topic_size: MaxTopicSize,
    default_max_topic_size: u64,
    partition_count: usize,
    sealed_segment_ceiling: u64,
) -> Option<PartitionSizeBudget> {
    let resolved = match max_topic_size {
        MaxTopicSize::ServerDefault => MaxTopicSize::from(default_max_topic_size),
        sized => sized,
    };
    match resolved {
        MaxTopicSize::Custom(size) => {
            let divisor = u64::try_from(partition_count).unwrap_or(1).max(1);
            let configured_share = size.as_bytes_u64() / divisor;
            Some(PartitionSizeBudget {
                max_bytes: configured_share.max(sealed_segment_ceiling),
                configured_share,
                resolved_cap: size.as_bytes_u64(),
            })
        }
        // `From<u64>` maps 0 back to `ServerDefault`, so a node default of 0
        // lands here as "no cap" rather than as a trim-everything budget.
        MaxTopicSize::Unlimited | MaxTopicSize::ServerDefault => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{PartitionSizeBudget, per_partition_size_budget};
    use iggy_common::MaxTopicSize;

    /// A ceiling low enough that the share is what the assertion is about.
    const NO_FLOOR: u64 = 1;

    fn budget(max_bytes: u64, configured_share: u64, resolved_cap: u64) -> PartitionSizeBudget {
        PartitionSizeBudget {
            max_bytes,
            configured_share,
            resolved_cap,
        }
    }

    #[test]
    fn server_default_resolves_to_the_node_default_at_enforcement_time() {
        // A topic UPDATED back to `ServerDefault` keeps the sentinel in
        // committed state; the cleaner must enforce the node default anyway, or
        // it diverges from a topic CREATED with the same setting (whose
        // sentinel admission already rewrote).
        assert_eq!(
            per_partition_size_budget(MaxTopicSize::ServerDefault, 4096, 4, NO_FLOOR),
            Some(budget(1024, 1024, 4096)),
            "the node default is resolved and split across partitions"
        );
        // Shipped config: server default is unlimited -> still no cap.
        assert_eq!(
            per_partition_size_budget(MaxTopicSize::ServerDefault, u64::MAX, 4, NO_FLOOR),
            None
        );
        // A zero node default must read as "no cap", never as a zero budget
        // that trims every sealed segment.
        assert_eq!(
            per_partition_size_budget(MaxTopicSize::ServerDefault, 0, 4, NO_FLOOR),
            None
        );
    }

    #[test]
    fn explicit_sizes_ignore_the_node_default() {
        assert_eq!(
            per_partition_size_budget(MaxTopicSize::Custom(4096u64.into()), 64, 2, NO_FLOOR),
            Some(budget(2048, 2048, 4096))
        );
        assert_eq!(
            per_partition_size_budget(MaxTopicSize::Unlimited, 64, 2, NO_FLOOR),
            None
        );
        // Zero partitions must not divide by zero.
        assert_eq!(
            per_partition_size_budget(MaxTopicSize::Custom(4096u64.into()), 64, 0, NO_FLOOR),
            Some(budget(4096, 4096, 4096))
        );
    }

    #[test]
    fn a_share_below_one_sealed_segment_is_raised_to_it() {
        // 8 MiB segments, 1 MiB bus cap: a sealed segment reaches 9 MiB.
        let segment_size = 8 * 1024 * 1024;
        let ceiling = segment_size + 1024 * 1024;

        // The shape admission accepts at the floor: cap == one segment, one
        // partition. The bare share (8 MiB) is under what a sealed segment
        // reaches, so it would delete each segment as it seals.
        assert_eq!(
            per_partition_size_budget(MaxTopicSize::Custom(segment_size.into()), 0, 1, ceiling),
            Some(budget(ceiling, segment_size, segment_size)),
            "an at-floor cap enforces one sealed segment, not zero"
        );

        // The divisor reintroduces the same shape at any accepted cap: 16 x 8
        // MiB passes admission, then splits into 8 MiB per partition.
        assert_eq!(
            per_partition_size_budget(
                MaxTopicSize::Custom((segment_size * 16).into()),
                0,
                16,
                ceiling
            ),
            Some(budget(ceiling, segment_size, segment_size * 16))
        );

        // A cap with room to spare keeps its share untouched, so the floor
        // never loosens a policy that was already enforceable.
        let roomy = ceiling * 10;
        assert_eq!(
            per_partition_size_budget(MaxTopicSize::Custom(roomy.into()), 0, 1, ceiling),
            Some(budget(roomy, roomy, roomy))
        );
    }
}
