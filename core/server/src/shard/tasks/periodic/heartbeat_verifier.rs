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

pub fn spawn_heartbeat_verifier(shard: Rc<IggyShard>) {
    let period = shard.config.heartbeat.interval.get_duration();
    let interval = iggy_common::IggyDuration::from(period);
    let max_interval =
        iggy_common::IggyDuration::from((MAX_THRESHOLD * interval.as_micros() as f64) as u64);
    info!(
        "Heartbeats will be verified every: {}. Max allowed interval: {}.",
        interval, max_interval
    );
    let shard_clone = shard.clone();
    shard
        .task_registry
        .periodic("verify_heartbeats")
        .every(period)
        .tick(move |_shutdown| verify_heartbeats(shard_clone.clone()))
        .spawn();
}

async fn verify_heartbeats(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    trace!("Verifying heartbeats...");

    // Get the period from config to compute max_interval
    let period = shard.config.heartbeat.interval.get_duration();
    let interval = IggyDuration::from(period);
    let max_interval = IggyDuration::from((MAX_THRESHOLD * interval.as_micros() as f64) as u64);

    let clients = shard.client_manager.get_clients();

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
        return Ok(());
    }

    let count = stale_clients.len();

    for client_id in stale_clients {
        shard.delete_client(client_id);
    }
    info!("Removed {count} stale clients.");

    Ok(())
}
