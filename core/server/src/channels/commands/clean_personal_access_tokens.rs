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

use std::rc::Rc;

use crate::shard::IggyShard;
use iggy_common::{IggyError, IggyTimestamp};
use tracing::{debug, info, trace};

pub async fn clear_personal_access_tokens(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    let config = &shard.config.personal_access_token.cleaner;
    if !config.enabled {
        info!("Personal access token cleaner is disabled.");
        return Ok(());
    }

    info!(
        "Personal access token cleaner is enabled, expired tokens will be deleted every: {}.",
        config.interval
    );

    let interval = config.interval.get_duration();
    let mut interval_timer = compio::time::interval(interval);

    loop {
        interval_timer.tick().await;
        trace!("Cleaning expired personal access tokens...");

        let users = shard.users.borrow();
        let now = IggyTimestamp::now();
        let mut deleted_tokens_count = 0;

        for (_, user) in users.iter() {
            let expired_tokens = user
                .personal_access_tokens
                .iter()
                .filter(|token| token.is_expired(now))
                .map(|token| token.token.clone())
                .collect::<Vec<_>>();

            for token in expired_tokens {
                debug!(
                    "Personal access token: {} for user with ID: {} is expired.",
                    token, user.id
                );
                deleted_tokens_count += 1;
                user.personal_access_tokens.remove(&token);
                debug!(
                    "Deleted personal access token: {} for user with ID: {}.",
                    token, user.id
                );
            }
        }

        info!(
            "Deleted {} expired personal access tokens.",
            deleted_tokens_count
        );
    }
}
