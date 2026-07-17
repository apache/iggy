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

use crate::bootstrap::ServerNgShard;
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
pub async fn run_segment_cleaner(shard: Rc<ServerNgShard>, stop: Receiver<()>, interval: Duration) {
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
fn stage_owned_partitions(shard: &Rc<ServerNgShard>) {
    let now = IggyTimestamp::now();
    let namespaces: Vec<_> = shard.plane.partitions().namespaces().copied().collect();
    let streams = shard.plane.metadata().mux_stm.streams();
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
        let max_bytes = match max_topic_size {
            // Per-partition budget: the cluster has no single owner of a
            // topic-wide total, so each partition keeps an equal share.
            MaxTopicSize::Custom(size) => {
                let divisor = u64::try_from(partition_count).unwrap_or(1).max(1);
                Some(size.as_bytes_u64() / divisor)
            }
            // No per-partition cap. `ServerDefault` must NOT fall through to a
            // sized branch: its `as_bytes_u64()` is 0, which would trim every
            // sealed segment. The server-ng default topic size is unlimited.
            MaxTopicSize::Unlimited | MaxTopicSize::ServerDefault => None,
        };

        if !has_expiry && max_bytes.is_none() {
            continue;
        }
        shard.request_clean_partition(namespace, now, message_expiry, max_bytes);
    }
}
