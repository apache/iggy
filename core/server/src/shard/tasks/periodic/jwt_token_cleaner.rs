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

use crate::http::shared::AppState;
use crate::shard::IggyShard;
use iggy_common::{IggyError, IggyTimestamp};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, trace};

const JWT_TOKENS_CLEANER_PERIOD: Duration = Duration::from_secs(300);

pub fn spawn_jwt_token_cleaner(shard: Rc<IggyShard>, app_state: Arc<AppState>) {
    info!(
        "JWT token cleaner is enabled, expired revoked tokens will be deleted every: {} seconds.",
        JWT_TOKENS_CLEANER_PERIOD.as_secs()
    );
    shard
        .task_registry
        .periodic("clear_jwt_tokens")
        .every(JWT_TOKENS_CLEANER_PERIOD)
        .tick(move |_shutdown| clear_jwt_tokens(app_state.clone()))
        .spawn();
}

async fn clear_jwt_tokens(app_state: Arc<AppState>) -> Result<(), IggyError> {
    trace!("Checking for expired revoked JWT tokens...");

    let now = IggyTimestamp::now().to_secs();

    match app_state
        .jwt_manager
        .delete_expired_revoked_tokens(now)
        .await
    {
        Ok(()) => {
            trace!("Successfully cleaned up expired revoked JWT tokens");
        }
        Err(err) => {
            error!("Failed to delete expired revoked JWT tokens: {}", err);
        }
    }

    Ok(())
}
