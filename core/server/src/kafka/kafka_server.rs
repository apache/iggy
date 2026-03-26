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

use crate::kafka::kafka_listener;
use crate::shard::IggyShard;
use crate::shard::task_registry::ShutdownToken;
use iggy_common::IggyError;
use std::rc::Rc;
use tracing::{error, info};

pub async fn start(shard: Rc<IggyShard>, shutdown: ShutdownToken) -> Result<(), IggyError> {
    let config = &shard.config.kafka;

    if !config.enabled {
        info!("Kafka server is disabled.");
        return Ok(());
    }

    info!(
        "Starting Kafka server on: {} for shard: {}...",
        config.address, shard.id
    );

    if let Err(e) = kafka_listener::start(shard, shutdown).await {
        error!("Kafka server has failed, error: {e}");
        return Err(e);
    }

    Ok(())
}
