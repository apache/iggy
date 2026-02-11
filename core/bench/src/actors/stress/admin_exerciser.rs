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
use crate::benchmarks::{BENCH_STREAM_PREFIX, BENCH_TOPIC_NAME};
use crate::utils::{ClientFactory, login_root};
use iggy::clients::client::IggyClient;
use iggy::prelude::*;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{debug, warn};

const ADMIN_CYCLE_INTERVAL_SECS: u64 = 15;

/// Exercises user management and PAT lifecycle APIs at lower frequency.
///
/// Each cycle: create user -> create PAT -> delete PAT -> delete user.
/// Also exercises consumer offset store/get and `flush_unsaved_buffer`.
pub struct AdminExerciser {
    client_factory: Arc<dyn ClientFactory>,
    ctx: Arc<StressContext>,
}

impl AdminExerciser {
    pub fn new(client_factory: Arc<dyn ClientFactory>, ctx: Arc<StressContext>) -> Self {
        Self {
            client_factory,
            ctx,
        }
    }

    pub async fn run(self) {
        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;

        let mut cycle = 0u64;
        while !self.ctx.is_cancelled() {
            self.user_pat_lifecycle(&client, cycle).await;
            self.offset_lifecycle(&client, cycle).await;
            self.flush_buffers(&client).await;

            cycle += 1;
            tokio::time::sleep(std::time::Duration::from_secs(ADMIN_CYCLE_INTERVAL_SECS)).await;
        }
    }

    async fn user_pat_lifecycle(&self, client: &IggyClient, cycle: u64) {
        let username = format!("stress-user-{cycle}");
        let password = "StressP@ss123!";

        // Create user
        match client
            .create_user(&username, password, UserStatus::Active, None)
            .await
        {
            Ok(_) => {
                self.ctx
                    .stats
                    .create_user_ok
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                self.ctx
                    .stats
                    .create_user_err
                    .fetch_add(1, Ordering::Relaxed);
                error_classifier::record_error(&self.ctx.stats, &e);
                warn!("Admin: create user failed: {e}");
                return;
            }
        }

        // Create PAT for current session (root user)
        let pat_name = format!("stress-pat-{cycle}");
        match client
            .create_personal_access_token(&pat_name, IggyExpiry::NeverExpire)
            .await
        {
            Ok(_) => {
                self.ctx.stats.create_pat_ok.fetch_add(1, Ordering::Relaxed);

                // Delete PAT
                match client.delete_personal_access_token(&pat_name).await {
                    Ok(()) => {
                        self.ctx.stats.delete_pat_ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        self.ctx
                            .stats
                            .delete_pat_err
                            .fetch_add(1, Ordering::Relaxed);
                        error_classifier::record_error(&self.ctx.stats, &e);
                        debug!("Admin: delete PAT failed: {e}");
                    }
                }
            }
            Err(e) => {
                self.ctx
                    .stats
                    .create_pat_err
                    .fetch_add(1, Ordering::Relaxed);
                error_classifier::record_error(&self.ctx.stats, &e);
                debug!("Admin: create PAT failed: {e}");
            }
        }

        // Delete user
        let user_id: Identifier = username.as_str().try_into().expect("valid identifier");
        match client.delete_user(&user_id).await {
            Ok(()) => {
                self.ctx
                    .stats
                    .delete_user_ok
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                self.ctx
                    .stats
                    .delete_user_err
                    .fetch_add(1, Ordering::Relaxed);
                error_classifier::record_error(&self.ctx.stats, &e);
                debug!("Admin: delete user failed: {e}");
            }
        }
    }

    async fn offset_lifecycle(&self, client: &IggyClient, cycle: u64) {
        let stream_id: Identifier = format!("{BENCH_STREAM_PREFIX}-1")
            .as_str()
            .try_into()
            .expect("valid identifier");
        let topic_id: Identifier = BENCH_TOPIC_NAME.try_into().expect("valid identifier");
        let consumer_id = u32::try_from(cycle % 1_000_000).unwrap_or(0) + 1000;
        let consumer = Consumer::new(Identifier::numeric(consumer_id).expect("valid id"));

        // Store offset
        match client
            .store_consumer_offset(&consumer, &stream_id, &topic_id, Some(1), cycle)
            .await
        {
            Ok(()) => {
                self.ctx
                    .stats
                    .store_offset_ok
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                self.ctx
                    .stats
                    .store_offset_err
                    .fetch_add(1, Ordering::Relaxed);
                error_classifier::record_error(&self.ctx.stats, &e);
                return;
            }
        }

        // Get offset
        match client
            .get_consumer_offset(&consumer, &stream_id, &topic_id, Some(1))
            .await
        {
            Ok(_) => {
                self.ctx.stats.get_offset_ok.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                self.ctx
                    .stats
                    .get_offset_err
                    .fetch_add(1, Ordering::Relaxed);
                error_classifier::record_error(&self.ctx.stats, &e);
            }
        }
    }

    async fn flush_buffers(&self, client: &IggyClient) {
        let stream_id: Identifier = format!("{BENCH_STREAM_PREFIX}-1")
            .as_str()
            .try_into()
            .expect("valid identifier");
        let topic_id: Identifier = BENCH_TOPIC_NAME.try_into().expect("valid identifier");

        match client
            .flush_unsaved_buffer(&stream_id, &topic_id, 1, false)
            .await
        {
            Ok(()) => {
                self.ctx.stats.flush_ok.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                self.ctx.stats.flush_err.fetch_add(1, Ordering::Relaxed);
                error_classifier::record_error(&self.ctx.stats, &e);
                debug!("Admin: flush_unsaved_buffer failed: {e}");
            }
        }
    }
}
