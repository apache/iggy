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
use std::rc::Rc;
use std::time::Duration;
use tracing::trace;

/// Run the cleaner until `stop` fires. Wakes every `interval`; expiry and size
/// are evaluated against wall-clock and resident bytes, so no metadata-commit
/// wake is needed.
pub async fn run_segment_cleaner(shard: Rc<ServerShard>, stop: Receiver<()>, interval: Duration) {
    trace!(
        shard = shard.id,
        interval_ms = interval.as_millis(),
        "segment cleaner started"
    );
    loop {
        // `Ok(_)`: stop signalled -> exit. `Err(_)`: interval elapsed -> pass.
        match compio::time::timeout(interval, stop.recv()).await {
            Ok(_) => break,
            Err(_) => stage_owned_partitions(&shard),
        }
    }
    trace!(shard = shard.id, "segment cleaner exited");
}

/// Stage a cleaner pass for every partition this shard owns whose topic has a
/// retention policy. Reads config off-pump and hands the resolved decision to
/// the pump; partitions with no policy are skipped without a frame.
fn stage_owned_partitions(shard: &Rc<ServerShard>) {
    let now = IggyTimestamp::now();
    let namespaces: Vec<_> = shard.plane.partitions().namespaces().copied().collect();
    let streams = shard.plane.metadata().mux_stm.streams();
    // Resolved once per pass, not per partition: the node default is a `Cell`
    // written at bootstrap and never after.
    for namespace in namespaces {
        let Some((message_expiry, max_topic_size, partition_count)) =
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
        let max_bytes = per_partition_size_budget(
            max_topic_size,
            iggy_common::DEFAULT_MAX_TOPIC_SIZE,
            partition_count,
        );

        if !has_expiry && max_bytes.is_none() {
            continue;
        }
        shard.request_clean_partition(namespace, now, message_expiry, max_bytes);
    }
}

/// Per-partition byte budget for a topic, or `None` for "no cap".
///
/// The cluster has no single owner of a topic-wide total, so each partition
/// keeps an equal share.
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
) -> Option<u64> {
    let resolved = match max_topic_size {
        MaxTopicSize::ServerDefault => MaxTopicSize::from(default_max_topic_size),
        sized => sized,
    };
    match resolved {
        MaxTopicSize::Custom(size) => {
            let divisor = u64::try_from(partition_count).unwrap_or(1).max(1);
            Some(size.as_bytes_u64() / divisor)
        }
        // `From<u64>` maps 0 back to `ServerDefault`, so a node default of 0
        // lands here as "no cap" rather than as a trim-everything budget.
        MaxTopicSize::Unlimited | MaxTopicSize::ServerDefault => None,
    }
}

#[cfg(test)]
mod tests {
    use super::per_partition_size_budget;
    use iggy_common::MaxTopicSize;

    #[test]
    fn server_default_resolves_to_the_node_default_at_enforcement_time() {
        // A topic UPDATED back to `ServerDefault` keeps the sentinel in
        // committed state; the cleaner must enforce the node default anyway, or
        // it diverges from a topic CREATED with the same setting (whose
        // sentinel admission already rewrote).
        assert_eq!(
            per_partition_size_budget(MaxTopicSize::ServerDefault, 4096, 4),
            Some(1024),
            "the node default is resolved and split across partitions"
        );
        // Shipped config: server default is unlimited -> still no cap.
        assert_eq!(
            per_partition_size_budget(MaxTopicSize::ServerDefault, u64::MAX, 4),
            None
        );
        // A zero node default must read as "no cap", never as a zero budget
        // that trims every sealed segment.
        assert_eq!(
            per_partition_size_budget(MaxTopicSize::ServerDefault, 0, 4),
            None
        );
    }

    #[test]
    fn explicit_sizes_ignore_the_node_default() {
        assert_eq!(
            per_partition_size_budget(MaxTopicSize::Custom(4096u64.into()), 64, 2),
            Some(2048)
        );
        assert_eq!(
            per_partition_size_budget(MaxTopicSize::Unlimited, 64, 2),
            None
        );
        // Zero partitions must not divide by zero.
        assert_eq!(
            per_partition_size_budget(MaxTopicSize::Custom(4096u64.into()), 64, 0),
            Some(4096)
        );
    }
}
