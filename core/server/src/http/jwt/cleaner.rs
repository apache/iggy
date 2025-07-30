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
use iggy_common::IggyTimestamp;
use std::{sync::Arc, time::Duration};
use tracing::{error, trace};

pub fn start_expired_tokens_cleaner(app_state: Arc<AppState>) {
    compio::runtime::spawn(async move {
        let mut interval_timer = compio::time::interval(Duration::from_secs(300));
        loop {
            interval_timer.tick().await;
            trace!("Deleting expired tokens...");
            let now = IggyTimestamp::now().to_secs();
            app_state
                .jwt_manager
                .delete_expired_revoked_tokens(now)
                .await
                .unwrap_or_else(|err| {
                    error!("Failed to delete expired revoked access tokens. Error: {err}");
                });
        }
    })
    .detach();
}
