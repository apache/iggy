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
use crate::shard::BroadcastResult;
use crate::shard::IggyShard;
use crate::shard::task_registry::ShutdownToken;
use crate::shard::transmission::event::ShardEvent;
use crate::tcp::connection_handler::{handle_connection, handle_error};
use crate::{shard_debug, shard_error, shard_info};
use compio::net::{TcpListener, TcpOpts};
use error_set::ErrContext;
use futures::FutureExt;
use iggy_common::{IggyError, TransportProtocol};
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

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
    shutdown: ShutdownToken,
) -> Result<(), IggyError> {
    if shard.id != 0 && addr.port() == 0 {
        shard_info!(shard.id, "Waiting for TCP address from shard 0...");
        loop {
            if let Some(bound_addr) = shard.tcp_bound_address.get() {
                addr = bound_addr;
                shard_info!(shard.id, "Received TCP address: {}", addr);
                break;
            }
            compio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    let listener = create_listener(addr, config)
        .await
        .map_err(|_| IggyError::CannotBindToSocket(addr.to_string()))
        .with_error_context(|err| {
            format!("Failed to bind {server_name} server to address: {addr}, {err}")
        })?;
    let actual_addr = listener.local_addr().map_err(|e| {
        shard_error!(shard.id, "Failed to get local address: {}", e);
        IggyError::CannotBindToSocket(addr.to_string())
    })?;
    shard_info!(
        shard.id,
        "{} server has started on: {:?}",
        server_name,
        actual_addr
    );

    if shard.id == 0 {
        // Store bound address locally
        shard.tcp_bound_address.set(Some(actual_addr));

        if addr.port() == 0 {
            // Notify config writer on shard 0
            let _ = shard.config_writer_notify.try_send(());

            // Broadcast to other shards for SO_REUSEPORT binding
            let event = ShardEvent::AddressBound {
                protocol: TransportProtocol::Tcp,
                address: actual_addr,
            };
            match shard.broadcast_event_to_all_shards(event).await {
                BroadcastResult::Success(_) => {}
                BroadcastResult::PartialSuccess { errors, .. } => {
                    for (shard_id, error) in errors {
                        shard_info!(
                            shard.id,
                            "Shard {} failed to receive address: {:?}",
                            shard_id,
                            error
                        );
                    }
                }
                BroadcastResult::Failure(err) => {
                    shard_error!(shard.id, "Failed to broadcast address: {:?}", err);
                }
            }
        }
    }

    accept_loop(server_name, listener, shard, shutdown).await
}

async fn accept_loop(
    server_name: &'static str,
    listener: TcpListener,
    shard: Rc<IggyShard>,
    shutdown: ShutdownToken,
) -> Result<(), IggyError> {
    loop {
        let shard = shard.clone();
        let accept_future = listener.accept();
        futures::select! {
            _ = shutdown.wait().fuse() => {
                shard_debug!(shard.id, "{} received shutdown signal, no longer accepting connections", server_name);
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
                        match shard_clone.broadcast_event_to_all_shards(event).await {
                            BroadcastResult::Success(_) => {}
                            BroadcastResult::PartialSuccess { errors, .. } => {
                                for (shard_id, error) in errors {
                                    shard_info!(shard.id, "Shard {} failed NewSession: {:?}", shard_id, error);
                                }
                            }
                            BroadcastResult::Failure(err) => {
                                shard_error!(shard.id, "Failed to broadcast NewSession: {:?}", err);
                            }
                        }

                        let client_id = session.client_id;
                        let user_id = session.get_user_id();
                        shard_info!(shard.id, "Created new session: {}", session);
                        let mut sender = SenderKind::get_tcp_sender(stream);

                        let conn_stop_receiver = shard.task_registry.add_connection(client_id);

                        let shard_for_conn = shard_clone.clone();
                        let registry = shard.task_registry.clone();
                        let registry_clone = registry.clone();
                        registry.spawn_connection(async move {
                            if let Err(error) = handle_connection(&session, &mut sender, &shard_for_conn, conn_stop_receiver).await {
                                shard_for_conn.delete_client(session.client_id);
                                handle_error(error);
                            }
                            registry_clone.remove_connection(&client_id);
                            let event = ShardEvent::ClientDisconnected { client_id, user_id };
                            match shard_for_conn.broadcast_event_to_all_shards(event).await {
                                BroadcastResult::Success(_) => {}
                                BroadcastResult::PartialSuccess { errors, .. } => {
                                    for (shard_id, error) in errors {
                                        shard_info!(shard_for_conn.id, "Shard {} failed ClientDisconnected: {:?}", shard_id, error);
                                    }
                                }
                                BroadcastResult::Failure(err) => {
                                    shard_error!(shard_for_conn.id, "Failed to broadcast ClientDisconnected: {:?}", err);
                                }
                            }

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
