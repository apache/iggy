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

use crate::shard::IggyShard;
use iggy_common::{IggyError, IggyTimestamp};
use std::rc::Rc;
use std::sync::Arc;
use tracing::{info, trace};

pub fn spawn_personal_access_token_cleaner(shard: Rc<IggyShard>) {
    let period = shard
        .config
        .personal_access_token
        .cleaner
        .interval
        .get_duration();
    info!(
        "Personal access token cleaner is enabled, expired tokens will be deleted every: {:?}.",
        period
    );
    let shard_clone = shard.clone();
    shard
        .task_registry
        .periodic("clear_personal_access_tokens")
        .every(period)
        .tick(move |_shutdown| clear_personal_access_tokens(shard_clone.clone()))
        .spawn();
}

async fn clear_personal_access_tokens(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    trace!("Checking for expired personal access tokens...");

    let now = IggyTimestamp::now();
    let mut total_removed = 0;

    let users = shard.users.values();
    for user in &users {
        let expired_tokens: Vec<Arc<String>> = user
            .personal_access_tokens
            .iter()
            .filter(|entry| entry.value().is_expired(now))
            .map(|entry| entry.key().clone())
            .collect();

        for token_hash in expired_tokens {
            if let Some((_, pat)) = user.personal_access_tokens.remove(&token_hash) {
                info!(
                    "Removed expired personal access token '{}' for user ID {}",
                    pat.name, user.id
                );
                total_removed += 1;
            }
        }
    }

    if total_removed > 0 {
        info!("Removed {total_removed} expired personal access tokens");
    }

    Ok(())
}
