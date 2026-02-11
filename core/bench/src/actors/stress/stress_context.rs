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

use iggy::prelude::Identifier;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use tokio::sync::Mutex;

/// Shared state across all stress test actors.
pub struct StressContext {
    pub cancelled: Arc<AtomicBool>,
    pub stats: Arc<StressStats>,
    /// Tracks ephemeral topics created by churners for cleanup: `(stream_id, topic_name)`
    pub ephemeral_topics: Arc<Mutex<Vec<(Identifier, String)>>>,
}

impl StressContext {
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(StressStats::default()),
            ephemeral_topics: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// Per-API atomic counters for stress test telemetry.
#[derive(Default)]
pub struct StressStats {
    pub send_messages_ok: AtomicU64,
    pub send_messages_err: AtomicU64,
    pub poll_messages_ok: AtomicU64,
    pub poll_messages_err: AtomicU64,
    pub create_topic_ok: AtomicU64,
    pub create_topic_err: AtomicU64,
    pub delete_topic_ok: AtomicU64,
    pub delete_topic_err: AtomicU64,
    pub create_partitions_ok: AtomicU64,
    pub create_partitions_err: AtomicU64,
    pub delete_partitions_ok: AtomicU64,
    pub delete_partitions_err: AtomicU64,
    pub create_consumer_group_ok: AtomicU64,
    pub create_consumer_group_err: AtomicU64,
    pub delete_consumer_group_ok: AtomicU64,
    pub delete_consumer_group_err: AtomicU64,
    pub join_consumer_group_ok: AtomicU64,
    pub join_consumer_group_err: AtomicU64,
    pub leave_consumer_group_ok: AtomicU64,
    pub leave_consumer_group_err: AtomicU64,
    pub purge_topic_ok: AtomicU64,
    pub purge_topic_err: AtomicU64,
    pub delete_segments_ok: AtomicU64,
    pub delete_segments_err: AtomicU64,
    pub update_topic_ok: AtomicU64,
    pub update_topic_err: AtomicU64,
    pub purge_stream_ok: AtomicU64,
    pub purge_stream_err: AtomicU64,
    pub stress_poll_ok: AtomicU64,
    pub stress_poll_err: AtomicU64,
    pub create_user_ok: AtomicU64,
    pub create_user_err: AtomicU64,
    pub delete_user_ok: AtomicU64,
    pub delete_user_err: AtomicU64,
    pub create_pat_ok: AtomicU64,
    pub create_pat_err: AtomicU64,
    pub delete_pat_ok: AtomicU64,
    pub delete_pat_err: AtomicU64,
    pub store_offset_ok: AtomicU64,
    pub store_offset_err: AtomicU64,
    pub get_offset_ok: AtomicU64,
    pub get_offset_err: AtomicU64,
    pub ping_ok: AtomicU64,
    pub ping_err: AtomicU64,
    pub get_stats_ok: AtomicU64,
    pub get_stats_err: AtomicU64,
    pub get_me_ok: AtomicU64,
    pub get_me_err: AtomicU64,
    pub get_clients_ok: AtomicU64,
    pub get_clients_err: AtomicU64,
    pub flush_ok: AtomicU64,
    pub flush_err: AtomicU64,
    pub expected_errors: AtomicU64,
    pub unexpected_errors: AtomicU64,
}

impl StressStats {
    pub fn total_ok(&self) -> u64 {
        self.send_messages_ok.load(Ordering::Relaxed)
            + self.poll_messages_ok.load(Ordering::Relaxed)
            + self.create_topic_ok.load(Ordering::Relaxed)
            + self.delete_topic_ok.load(Ordering::Relaxed)
            + self.create_partitions_ok.load(Ordering::Relaxed)
            + self.delete_partitions_ok.load(Ordering::Relaxed)
            + self.create_consumer_group_ok.load(Ordering::Relaxed)
            + self.delete_consumer_group_ok.load(Ordering::Relaxed)
            + self.join_consumer_group_ok.load(Ordering::Relaxed)
            + self.leave_consumer_group_ok.load(Ordering::Relaxed)
            + self.purge_topic_ok.load(Ordering::Relaxed)
            + self.delete_segments_ok.load(Ordering::Relaxed)
            + self.update_topic_ok.load(Ordering::Relaxed)
            + self.purge_stream_ok.load(Ordering::Relaxed)
            + self.stress_poll_ok.load(Ordering::Relaxed)
            + self.create_user_ok.load(Ordering::Relaxed)
            + self.delete_user_ok.load(Ordering::Relaxed)
            + self.create_pat_ok.load(Ordering::Relaxed)
            + self.delete_pat_ok.load(Ordering::Relaxed)
            + self.store_offset_ok.load(Ordering::Relaxed)
            + self.get_offset_ok.load(Ordering::Relaxed)
            + self.ping_ok.load(Ordering::Relaxed)
            + self.get_stats_ok.load(Ordering::Relaxed)
            + self.get_me_ok.load(Ordering::Relaxed)
            + self.get_clients_ok.load(Ordering::Relaxed)
            + self.flush_ok.load(Ordering::Relaxed)
    }

    pub fn total_err(&self) -> u64 {
        self.send_messages_err.load(Ordering::Relaxed)
            + self.poll_messages_err.load(Ordering::Relaxed)
            + self.create_topic_err.load(Ordering::Relaxed)
            + self.delete_topic_err.load(Ordering::Relaxed)
            + self.create_partitions_err.load(Ordering::Relaxed)
            + self.delete_partitions_err.load(Ordering::Relaxed)
            + self.create_consumer_group_err.load(Ordering::Relaxed)
            + self.delete_consumer_group_err.load(Ordering::Relaxed)
            + self.join_consumer_group_err.load(Ordering::Relaxed)
            + self.leave_consumer_group_err.load(Ordering::Relaxed)
            + self.purge_topic_err.load(Ordering::Relaxed)
            + self.delete_segments_err.load(Ordering::Relaxed)
            + self.update_topic_err.load(Ordering::Relaxed)
            + self.purge_stream_err.load(Ordering::Relaxed)
            + self.stress_poll_err.load(Ordering::Relaxed)
            + self.create_user_err.load(Ordering::Relaxed)
            + self.delete_user_err.load(Ordering::Relaxed)
            + self.create_pat_err.load(Ordering::Relaxed)
            + self.delete_pat_err.load(Ordering::Relaxed)
            + self.store_offset_err.load(Ordering::Relaxed)
            + self.get_offset_err.load(Ordering::Relaxed)
            + self.ping_err.load(Ordering::Relaxed)
            + self.get_stats_err.load(Ordering::Relaxed)
            + self.get_me_err.load(Ordering::Relaxed)
            + self.get_clients_err.load(Ordering::Relaxed)
            + self.flush_err.load(Ordering::Relaxed)
    }
}
