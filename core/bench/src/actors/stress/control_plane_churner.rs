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

use super::error_classifier;
use super::stress_context::StressContext;
use crate::args::kinds::stress::args::ApiMix;
use crate::benchmarks::{
    BENCH_STREAM_PREFIX, BENCH_TOPIC_NAME, CONSUMER_GROUP_BASE_ID, CONSUMER_GROUP_NAME_PREFIX,
};
use crate::utils::{ClientFactory, login_root};
use iggy::clients::client::IggyClient;
use iggy::prelude::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{debug, warn};

/// CRUD lifecycle operations exercised during the chaos phase.
#[derive(Debug, Clone, Copy)]
enum ChurnOp {
    CreateDeleteTopic,
    AddRemovePartitions,
    ConsumerGroupJoinLeave,
    PurgeTopic,
    DeleteSegments,
    UpdateTopic,
    StressPoll,
    // Destructive ops — only enabled with ApiMix::All
    DeleteAndRecreateTopic,
    PurgeStream,
}

const STANDARD_OPS: [ChurnOp; 7] = [
    ChurnOp::CreateDeleteTopic,
    ChurnOp::AddRemovePartitions,
    ChurnOp::ConsumerGroupJoinLeave,
    ChurnOp::PurgeTopic,
    ChurnOp::DeleteSegments,
    ChurnOp::UpdateTopic,
    ChurnOp::StressPoll,
];

const ALL_OPS: [ChurnOp; 9] = [
    ChurnOp::CreateDeleteTopic,
    ChurnOp::AddRemovePartitions,
    ChurnOp::ConsumerGroupJoinLeave,
    ChurnOp::PurgeTopic,
    ChurnOp::DeleteSegments,
    ChurnOp::UpdateTopic,
    ChurnOp::StressPoll,
    ChurnOp::DeleteAndRecreateTopic,
    ChurnOp::PurgeStream,
];

/// Topic configuration shared between the benchmark setup and the churner,
/// so that `DeleteAndRecreateTopic` can recreate with the same parameters.
pub struct ChurnerConfig {
    pub api_mix: ApiMix,
    pub streams: u32,
    pub partitions: u32,
    pub consumer_groups: u32,
    pub message_expiry: IggyExpiry,
    pub max_topic_size: MaxTopicSize,
    /// Deadline after which data-destructive ops (`PurgeTopic`, `PurgeStream`,
    /// `DeleteAndRecreateTopic`) are suppressed so messages survive for verification.
    pub purge_cutoff: std::time::Instant,
}

/// Periodically executes CRUD lifecycle operations against the server.
///
/// Targets the TOCTOU race in consumer group rebalance and exercises
/// create/delete paths under concurrent data-plane load. In `ApiMix::All`
/// mode, also exercises destructive ops (topic deletion, stream purge)
/// that race against active data-plane actors.
pub struct ControlPlaneChurner {
    churner_id: u32,
    client_factory: Arc<dyn ClientFactory>,
    ctx: Arc<StressContext>,
    churn_interval: std::time::Duration,
    rng: StdRng,
    api_mix: ApiMix,
    streams: u32,
    partitions: u32,
    consumer_groups: u32,
    message_expiry: IggyExpiry,
    max_topic_size: MaxTopicSize,
    purge_cutoff: std::time::Instant,
}

impl ControlPlaneChurner {
    pub fn new(
        churner_id: u32,
        client_factory: Arc<dyn ClientFactory>,
        ctx: Arc<StressContext>,
        churn_interval: IggyDuration,
        chaos_seed: u64,
        config: &ChurnerConfig,
    ) -> Self {
        let rng = StdRng::seed_from_u64(chaos_seed.wrapping_add(u64::from(churner_id)));
        Self {
            churner_id,
            client_factory,
            ctx,
            churn_interval: churn_interval.get_duration(),
            rng,
            api_mix: config.api_mix,
            streams: config.streams,
            partitions: config.partitions,
            consumer_groups: config.consumer_groups,
            message_expiry: config.message_expiry,
            max_topic_size: config.max_topic_size,
            purge_cutoff: config.purge_cutoff,
        }
    }

    fn available_ops(&self) -> &'static [ChurnOp] {
        match self.api_mix {
            ApiMix::Mixed | ApiMix::ControlPlaneHeavy => &STANDARD_OPS,
            ApiMix::All => &ALL_OPS,
            ApiMix::DataPlaneOnly => unreachable!("churner not spawned for DataPlaneOnly"),
        }
    }

    pub async fn run(mut self) {
        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;

        let mut cycle = 0u64;
        while !self.ctx.is_cancelled() {
            let ops = self.available_ops();
            let op = ops[self.rng.random_range(0..ops.len())];
            debug!(
                "Churner #{} cycle {cycle}: executing {:?}",
                self.churner_id, op
            );

            let past_purge_cutoff = std::time::Instant::now() > self.purge_cutoff;

            match op {
                ChurnOp::CreateDeleteTopic => {
                    self.create_delete_topic(&client, cycle).await;
                }
                ChurnOp::AddRemovePartitions => {
                    self.add_remove_partitions(&client).await;
                }
                ChurnOp::ConsumerGroupJoinLeave => {
                    self.consumer_group_join_leave(&client).await;
                }
                ChurnOp::PurgeTopic if past_purge_cutoff => {
                    debug!(
                        "Churner #{}: skipping PurgeTopic (past purge cutoff)",
                        self.churner_id
                    );
                }
                ChurnOp::PurgeTopic => {
                    self.purge_random_topic(&client).await;
                }
                ChurnOp::DeleteSegments => {
                    self.delete_segments(&client).await;
                }
                ChurnOp::UpdateTopic => {
                    self.update_topic(&client).await;
                }
                ChurnOp::StressPoll => {
                    self.stress_poll(&client).await;
                }
                ChurnOp::DeleteAndRecreateTopic if past_purge_cutoff => {
                    debug!(
                        "Churner #{}: skipping DeleteAndRecreateTopic (past purge cutoff)",
                        self.churner_id
                    );
                }
                ChurnOp::DeleteAndRecreateTopic => {
                    self.delete_and_recreate_topic(&client).await;
                }
                ChurnOp::PurgeStream if past_purge_cutoff => {
                    debug!(
                        "Churner #{}: skipping PurgeStream (past purge cutoff)",
                        self.churner_id
                    );
                }
                ChurnOp::PurgeStream => {
                    self.purge_stream(&client).await;
                }
            }

            cycle += 1;
            tokio::time::sleep(self.churn_interval).await;
        }
    }

    fn random_stream_id(&mut self) -> Identifier {
        let stream_idx = self.rng.random_range(1..=self.streams);
        format!("{BENCH_STREAM_PREFIX}-{stream_idx}")
            .as_str()
            .try_into()
            .expect("valid identifier")
    }

    fn topic_id() -> Identifier {
        BENCH_TOPIC_NAME.try_into().expect("valid identifier")
    }

    // --- Original ops ---

    async fn create_delete_topic(&self, client: &IggyClient, cycle: u64) {
        let stream_id: Identifier = format!("{BENCH_STREAM_PREFIX}-1")
            .as_str()
            .try_into()
            .expect("valid identifier");
        let topic_name = format!("churn-{}-{cycle}", self.churner_id);

        match client
            .create_topic(
                &stream_id,
                &topic_name,
                1,
                CompressionAlgorithm::default(),
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await
        {
            Ok(_) => {
                self.ctx
                    .stats
                    .create_topic_ok
                    .fetch_add(1, Ordering::Relaxed);

                self.ctx
                    .ephemeral_topics
                    .lock()
                    .await
                    .push((stream_id.clone(), topic_name.clone()));

                tokio::time::sleep(std::time::Duration::from_millis(500)).await;

                let topic_id: Identifier =
                    topic_name.as_str().try_into().expect("valid identifier");
                match client.delete_topic(&stream_id, &topic_id).await {
                    Ok(()) => {
                        self.ctx
                            .stats
                            .delete_topic_ok
                            .fetch_add(1, Ordering::Relaxed);
                        let mut topics = self.ctx.ephemeral_topics.lock().await;
                        topics.retain(|(_, name)| name != &topic_name);
                    }
                    Err(e) => {
                        self.ctx
                            .stats
                            .delete_topic_err
                            .fetch_add(1, Ordering::Relaxed);
                        error_classifier::record_error(&self.ctx.stats, &e);
                        warn!("Churner #{}: delete topic failed: {e}", self.churner_id);
                    }
                }
            }
            Err(e) => {
                self.ctx
                    .stats
                    .create_topic_err
                    .fetch_add(1, Ordering::Relaxed);
                error_classifier::record_error(&self.ctx.stats, &e);
                warn!("Churner #{}: create topic failed: {e}", self.churner_id);
            }
        }
    }

    async fn add_remove_partitions(&self, client: &IggyClient) {
        let stream_id: Identifier = format!("{BENCH_STREAM_PREFIX}-1")
            .as_str()
            .try_into()
            .expect("valid identifier");
        let topic_id = Self::topic_id();

        match client.create_partitions(&stream_id, &topic_id, 1).await {
            Ok(()) => {
                self.ctx
                    .stats
                    .create_partitions_ok
                    .fetch_add(1, Ordering::Relaxed);

                tokio::time::sleep(std::time::Duration::from_millis(200)).await;

                match client.delete_partitions(&stream_id, &topic_id, 1).await {
                    Ok(()) => {
                        self.ctx
                            .stats
                            .delete_partitions_ok
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        self.ctx
                            .stats
                            .delete_partitions_err
                            .fetch_add(1, Ordering::Relaxed);
                        error_classifier::record_error(&self.ctx.stats, &e);
                        debug!(
                            "Churner #{}: delete partitions failed: {e}",
                            self.churner_id
                        );
                    }
                }
            }
            Err(e) => {
                self.ctx
                    .stats
                    .create_partitions_err
                    .fetch_add(1, Ordering::Relaxed);
                error_classifier::record_error(&self.ctx.stats, &e);
                debug!(
                    "Churner #{}: create partitions failed: {e}",
                    self.churner_id
                );
            }
        }
    }

    /// Joins and leaves a bench consumer group, forcing a rebalance
    /// mid-poll that races against active data-plane consumers.
    async fn consumer_group_join_leave(&mut self, client: &IggyClient) {
        let stream_idx = self.rng.random_range(1..=self.consumer_groups);
        let stream_id: Identifier = format!("{BENCH_STREAM_PREFIX}-{stream_idx}")
            .as_str()
            .try_into()
            .expect("valid identifier");
        let topic_id = Self::topic_id();
        let cg_id_num = CONSUMER_GROUP_BASE_ID + stream_idx - 1;
        let cg_name = format!("{CONSUMER_GROUP_NAME_PREFIX}-{cg_id_num}");
        let cg_id: Identifier = cg_name.as_str().try_into().expect("valid identifier");

        match client
            .join_consumer_group(&stream_id, &topic_id, &cg_id)
            .await
        {
            Ok(()) => {
                self.ctx
                    .stats
                    .join_consumer_group_ok
                    .fetch_add(1, Ordering::Relaxed);

                tokio::time::sleep(std::time::Duration::from_millis(50)).await;

                match client
                    .leave_consumer_group(&stream_id, &topic_id, &cg_id)
                    .await
                {
                    Ok(()) => {
                        self.ctx
                            .stats
                            .leave_consumer_group_ok
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        self.ctx
                            .stats
                            .leave_consumer_group_err
                            .fetch_add(1, Ordering::Relaxed);
                        error_classifier::record_error(&self.ctx.stats, &e);
                    }
                }
            }
            Err(e) => {
                self.ctx
                    .stats
                    .join_consumer_group_err
                    .fetch_add(1, Ordering::Relaxed);
                error_classifier::record_error(&self.ctx.stats, &e);
                debug!("Churner #{}: CG join/leave failed: {e}", self.churner_id);
            }
        }
    }

    async fn purge_random_topic(&mut self, client: &IggyClient) {
        let stream_id = self.random_stream_id();
        let topic_id = Self::topic_id();

        match client.purge_topic(&stream_id, &topic_id).await {
            Ok(()) => {
                self.ctx
                    .stats
                    .purge_topic_ok
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                self.ctx
                    .stats
                    .purge_topic_err
                    .fetch_add(1, Ordering::Relaxed);
                error_classifier::record_error(&self.ctx.stats, &e);
                debug!("Churner #{}: purge topic failed: {e}", self.churner_id);
            }
        }
    }

    // --- New safe ops ---

    async fn delete_segments(&mut self, client: &IggyClient) {
        let stream_id = self.random_stream_id();
        let topic_id = Self::topic_id();
        let partition_id = self.rng.random_range(0..self.partitions);

        match client
            .delete_segments(&stream_id, &topic_id, partition_id, 1)
            .await
        {
            Ok(()) => {
                self.ctx
                    .stats
                    .delete_segments_ok
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                self.ctx
                    .stats
                    .delete_segments_err
                    .fetch_add(1, Ordering::Relaxed);
                error_classifier::record_error(&self.ctx.stats, &e);
                debug!("Churner #{}: delete segments failed: {e}", self.churner_id);
            }
        }
    }

    async fn update_topic(&mut self, client: &IggyClient) {
        let stream_id = self.random_stream_id();
        let topic_id = Self::topic_id();

        let compression = if self.rng.random_bool(0.5) {
            CompressionAlgorithm::None
        } else {
            CompressionAlgorithm::Gzip
        };

        match client
            .update_topic(
                &stream_id,
                &topic_id,
                BENCH_TOPIC_NAME,
                compression,
                None,
                self.message_expiry,
                self.max_topic_size,
            )
            .await
        {
            Ok(()) => {
                self.ctx
                    .stats
                    .update_topic_ok
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                self.ctx
                    .stats
                    .update_topic_err
                    .fetch_add(1, Ordering::Relaxed);
                error_classifier::record_error(&self.ctx.stats, &e);
                debug!("Churner #{}: update topic failed: {e}", self.churner_id);
            }
        }
    }

    /// One-off polls with First/Last/Timestamp strategies — these are "victim"
    /// operations that race against concurrent segment mutations.
    async fn stress_poll(&mut self, client: &IggyClient) {
        let stream_id = self.random_stream_id();
        let topic_id = Self::topic_id();
        let partition_id = self.rng.random_range(0..self.partitions);
        let consumer = Consumer::new(Identifier::numeric(8888).expect("valid consumer id"));

        let strategy = match self.rng.random_range(0..3u32) {
            0 => PollingStrategy::first(),
            1 => PollingStrategy::last(),
            _ => PollingStrategy::timestamp(IggyTimestamp::now()),
        };

        match client
            .poll_messages(
                &stream_id,
                &topic_id,
                Some(partition_id),
                &consumer,
                &strategy,
                10,
                false,
            )
            .await
        {
            Ok(_) => {
                self.ctx
                    .stats
                    .stress_poll_ok
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                self.ctx
                    .stats
                    .stress_poll_err
                    .fetch_add(1, Ordering::Relaxed);
                error_classifier::record_error(&self.ctx.stats, &e);
                debug!("Churner #{}: stress poll failed: {e}", self.churner_id);
            }
        }
    }

    // --- Destructive ops (ApiMix::All only) ---

    /// Deletes and immediately recreates topic-1 on a random stream.
    /// Exercises 23+ `expect()` panic sites reachable when consumers
    /// hold stale topic references during deletion.
    async fn delete_and_recreate_topic(&mut self, client: &IggyClient) {
        let stream_id = self.random_stream_id();
        let topic_id = Self::topic_id();

        match client.delete_topic(&stream_id, &topic_id).await {
            Ok(()) => {
                self.ctx
                    .stats
                    .delete_topic_ok
                    .fetch_add(1, Ordering::Relaxed);

                tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                match client
                    .create_topic(
                        &stream_id,
                        BENCH_TOPIC_NAME,
                        self.partitions,
                        CompressionAlgorithm::default(),
                        None,
                        self.message_expiry,
                        self.max_topic_size,
                    )
                    .await
                {
                    Ok(_) => {
                        self.ctx
                            .stats
                            .create_topic_ok
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        self.ctx
                            .stats
                            .create_topic_err
                            .fetch_add(1, Ordering::Relaxed);
                        error_classifier::record_error(&self.ctx.stats, &e);
                        warn!("Churner #{}: recreate topic failed: {e}", self.churner_id);
                    }
                }
            }
            Err(e) => {
                self.ctx
                    .stats
                    .delete_topic_err
                    .fetch_add(1, Ordering::Relaxed);
                error_classifier::record_error(&self.ctx.stats, &e);
                debug!(
                    "Churner #{}: delete topic (for recreate) failed: {e}",
                    self.churner_id
                );
            }
        }
    }

    async fn purge_stream(&mut self, client: &IggyClient) {
        let stream_id = self.random_stream_id();

        match client.purge_stream(&stream_id).await {
            Ok(()) => {
                self.ctx
                    .stats
                    .purge_stream_ok
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                self.ctx
                    .stats
                    .purge_stream_err
                    .fetch_add(1, Ordering::Relaxed);
                error_classifier::record_error(&self.ctx.stats, &e);
                debug!("Churner #{}: purge stream failed: {e}", self.churner_id);
            }
        }
    }
}
