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
use iggy_common::{IggyDuration, IggyError, IggyTimestamp};
use std::rc::Rc;
use tracing::{debug, info, trace, warn};

const MAX_THRESHOLD: f64 = 1.2;

pub async fn verify_heartbeats(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    let config = &shard.config.heartbeat;
    if !config.enabled {
        info!("Heartbeats verification is disabled.");
        return Ok(());
    }

    let interval = config.interval;
    let max_interval = IggyDuration::from((MAX_THRESHOLD * interval.as_micros() as f64) as u64);
    info!("Heartbeats will be verified every: {interval}. Max allowed interval: {max_interval}.");

    let mut interval_timer = compio::time::interval(interval.get_duration());

    loop {
        interval_timer.tick().await;
        trace!("Verifying heartbeats...");

        let clients = {
            let client_manager = shard.client_manager.borrow();
            client_manager.get_clients()
        };

        let now = IggyTimestamp::now();
        let heartbeat_to = IggyTimestamp::from(now.as_micros() - max_interval.as_micros());
        debug!("Verifying heartbeats at: {now}, max allowed timestamp: {heartbeat_to}");

        let mut stale_clients = Vec::new();
        for client in clients {
            if client.last_heartbeat.as_micros() < heartbeat_to.as_micros() {
                warn!(
                    "Stale client session: {}, last heartbeat at: {}, max allowed timestamp: {heartbeat_to}",
                    client.session, client.last_heartbeat,
                );
                client.session.set_stale();
                stale_clients.push(client.session.client_id);
            } else {
                debug!(
                    "Valid heartbeat at: {} for client session: {}, max allowed timestamp: {heartbeat_to}",
                    client.last_heartbeat, client.session,
                );
            }
        }

        if stale_clients.is_empty() {
            continue;
        }

        let count = stale_clients.len();
        info!("Removing {count} stale clients...");
        for client_id in stale_clients {
            shard.delete_client(client_id);
        }
        info!("Removed {count} stale clients.");
    }
}
