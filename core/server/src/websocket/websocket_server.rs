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
use crate::shard::task_registry::ShutdownToken;
use crate::websocket::websocket_listener;
use crate::websocket::websocket_tls_listener;
use iggy_common::IggyError;
use std::rc::Rc;
use tracing::{error, info};

pub async fn spawn_websocket_server(
    shard: Rc<IggyShard>,
    shutdown: ShutdownToken,
) -> Result<(), IggyError> {
    let config = shard.config.websocket.clone();

    if !config.enabled {
        info!("WebSocket server is disabled.");
        return Ok(());
    }

    let server_name = if config.tls.enabled {
        "WebSocket TLS"
    } else {
        "WebSocket"
    };

    info!(
        "Starting {} server on: {} for shard: {}...",
        server_name, config.address, shard.id
    );

    let result = match config.tls.enabled {
        true => websocket_tls_listener::start(config, shard.clone(), shutdown).await,
        false => websocket_listener::start(config, shard.clone(), shutdown).await,
    };

    if let Err(error) = result {
        error!("{} server has failed to start, error: {error}", server_name);
        return Err(error);
    }

    Ok(())
}
