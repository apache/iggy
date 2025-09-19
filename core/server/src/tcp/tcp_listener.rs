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

use crate::binary::sender::SenderKind;
use crate::configs::tcp::TcpSocketConfig;
use crate::shard::IggyShard;
use crate::shard::transmission::event::ShardEvent;
use crate::tcp::connection_handler::{handle_connection, handle_error};
use crate::{shard_error, shard_info};
use compio::net::{TcpListener, TcpOpts};
use error_set::ErrContext;
use futures::FutureExt;
use iggy_common::{IggyError, TransportProtocol};
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

async fn create_listener(
    addr: SocketAddr,
    config: &TcpSocketConfig,
) -> Result<TcpListener, std::io::Error> {
    // Required by the thread-per-core model...
    // We create bunch of sockets on different threads, that bind to exactly the same address and port.
    let opts = TcpOpts::new().reuse_port(true).reuse_port(true);
    let opts = if config.override_defaults {
        let recv_buffer_size = config
            .recv_buffer_size
            .as_bytes_u64()
            .try_into()
            .expect("Failed to parse recv_buffer_size for TCP socket");

        let send_buffer_size = config
            .send_buffer_size
            .as_bytes_u64()
            .try_into()
            .expect("Failed to parse send_buffer_size for TCP socket");

        opts.recv_buffer_size(recv_buffer_size)
            .send_buffer_size(send_buffer_size)
            .keepalive(config.keepalive)
            .linger(config.linger.get_duration())
            .nodelay(config.nodelay)
    } else {
        opts
    };
    TcpListener::bind_with_options(addr, opts).await
}

pub async fn start(
    server_name: &'static str,
    mut addr: SocketAddr,
    config: &TcpSocketConfig,
    shard: Rc<IggyShard>,
) -> Result<(), IggyError> {
    //TODO: Fix me, this needs to take into account that first shard id potentially can be greater than 0.
    if shard.id != 0 && addr.port() == 0 {
        shard_info!(shard.id, "Waiting for TCP address from shard 0...");
        loop {
            if let Some(bound_addr) = shard.tcp_bound_address.get() {
                addr = bound_addr;
                shard_info!(shard.id, "Received TCP address: {}", addr);
                break;
            }
            compio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    let listener = create_listener(addr, config)
        .await
        .map_err(|_| IggyError::CannotBindToSocket(addr.to_string()))
        .with_error_context(|err| {
            format!("Failed to bind {server_name} server to address: {addr}, {err}")
        })?;
    let actual_addr = listener.local_addr().map_err(|e| {
        shard_error!(shard.id, "Failed to get local address: {e}");
        IggyError::CannotBindToSocket(addr.to_string())
    })?;
    shard_info!(
        shard.id,
        "{} server has started on: {:?}",
        server_name,
        actual_addr
    );

    //TODO: Fix me, this needs to take into account that first shard id potentially can be greater than 0.
    if shard.id == 0 {
        if addr.port() == 0 {
            let event = ShardEvent::TcpBound {
                address: actual_addr,
            };
            shard.broadcast_event_to_all_shards(event).await;
        }

        let mut current_config = shard.config.clone();
        current_config.tcp.address = actual_addr.to_string();

        let runtime_path = current_config.system.get_runtime_path();
        let current_config_path = format!("{runtime_path}/current_config.toml");
        let current_config_content =
            toml::to_string(&current_config).expect("Cannot serialize current_config");

        let buf_result = compio::fs::write(&current_config_path, current_config_content).await;
        match buf_result.0 {
            Ok(_) => shard_info!(
                shard.id,
                "Current config written to: {}",
                current_config_path
            ),
            Err(e) => shard_error!(
                shard.id,
                "Failed to write current config to {}: {}",
                current_config_path,
                e
            ),
        }
    }

    accept_loop(server_name, listener, shard).await
}

async fn accept_loop(
    server_name: &'static str,
    listener: TcpListener,
    shard: Rc<IggyShard>,
) -> Result<(), IggyError> {
    loop {
        let shard = shard.clone();
        let shutdown_check = async {
            loop {
                if shard.is_shutting_down() {
                    return;
                }
                compio::time::sleep(Duration::from_millis(100)).await;
            }
        };

        let accept_future = listener.accept();
        futures::select! {
            _ = shutdown_check.fuse() => {
                shard_info!(shard.id, "{} detected shutdown flag, no longer accepting connections", server_name);
                break;
            }
            result = accept_future.fuse() => {
                match result {
                    Ok((stream, address)) => {
                        if shard.is_shutting_down() {
                            shard_info!(shard.id, "Rejecting new connection from {} during shutdown", address);
                            continue;
                        }
                        let shard_clone = shard.clone();
                        shard_info!(shard.id, "Accepted new TCP connection: {}", address);
                        let transport = TransportProtocol::Tcp;
                        let session = shard_clone.add_client(&address, transport);
                        shard_info!(shard.id, "Added {} client with session: {} for IP address: {}", transport, session, address);
                        //TODO: Those can be shared with other shards.
                        shard_clone.add_active_session(session.clone());
                        // Broadcast session to all shards.
                        let event = ShardEvent::NewSession { address, transport };
                        // TODO: Fixme look inside of broadcast_event_to_all_shards method.
                        let _responses = shard_clone.broadcast_event_to_all_shards(event.into()).await;

                        let client_id = session.client_id;
                        shard_info!(shard.id, "Created new session: {}", session);
                        let mut sender = SenderKind::get_tcp_sender(stream);

                        let conn_stop_receiver = shard_clone.task_registry.add_connection(client_id);

                        let shard_for_conn = shard_clone.clone();
                        shard_clone.task_registry.spawn_tracked(async move {
                            if let Err(error) = handle_connection(&session, &mut sender, &shard_for_conn, conn_stop_receiver).await {
                                handle_error(error);
                            }
                            shard_for_conn.task_registry.remove_connection(&client_id);

                            if let Err(error) = sender.shutdown().await {
                                shard_error!(shard.id, "Failed to shutdown TCP stream for client: {}, address: {}. {}", client_id, address, error);
                            } else {
                                shard_info!(shard.id, "Successfully closed TCP stream for client: {}, address: {}.", client_id, address);
                            }
                        });
                    }
                    Err(error) => shard_error!(shard.id, "Unable to accept TCP socket. {}", error),
                }
            }
        }
    }
    Ok(())
}
