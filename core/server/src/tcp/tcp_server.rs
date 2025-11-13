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
use crate::tcp::{tcp_listener, tcp_tls_listener};
use iggy_common::IggyError;
use std::net::SocketAddr;
use std::rc::Rc;
use tracing::info;

/// Starts the TCP server.
pub async fn spawn_tcp_server(
    shard: Rc<IggyShard>,
    shutdown: ShutdownToken,
) -> Result<(), IggyError> {
    let server_name = if shard.config.tcp.tls.enabled {
        "Iggy TCP TLS"
    } else {
        "Iggy TCP"
    };
    let socket_config = &shard.config.tcp.socket;
    let addr: SocketAddr = shard
        .config
        .tcp
        .address
        .parse()
        .expect("Failed to parse TCP address");
    info!("Initializing {} server...", server_name);

    match shard.config.tcp.tls.enabled {
        true => {
            tcp_tls_listener::start(server_name, addr, socket_config, shard.clone(), shutdown)
                .await?
        }
        false => {
            tcp_listener::start(server_name, addr, socket_config, shard.clone(), shutdown).await?
        }
    };

    Ok(())
}
