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
use crate::shard::task_registry::ShutdownToken;
use crate::shard::transmission::event::ShardEvent;
use crate::tcp::connection_handler::{handle_connection, handle_error};
use compio::net::{TcpListener, TcpOpts};
use err_trail::ErrContext;
use futures::FutureExt;
use iggy_common::{IggyError, TransportProtocol};
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use tracing::{debug, error, info};

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
        info!("Waiting for TCP address from shard 0...");
        loop {
            if let Some(bound_addr) = shard.tcp_bound_address.get() {
                addr = bound_addr;
                info!("Received TCP address: {}", addr);
                break;
            }
            compio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    let listener = create_listener(addr, config)
        .await
        .map_err(|_| IggyError::CannotBindToSocket(addr.to_string()))
        .with_error(|err| {
            format!("Failed to bind {server_name} server to address: {addr}, {err}")
        })?;
    let actual_addr = listener.local_addr().map_err(|e| {
        error!("Failed to get local address: {}", e);
        IggyError::CannotBindToSocket(addr.to_string())
    })?;
    info!("{} server has started on: {:?}", server_name, actual_addr);

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
            shard.broadcast_event_to_all_shards(event).await?;
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
                debug!("{} received shutdown signal, no longer accepting connections", server_name);
                break;
            }
            result = accept_future.fuse() => {
                match result {
                    Ok((stream, address)) => {
                        if shard.is_shutting_down() {
                            info!("Rejecting new connection from {} during shutdown", address);
                            continue;
                        }
                        let shard_clone = shard.clone();
                        info!("Accepted new TCP connection: {}", address);
                        let transport = TransportProtocol::Tcp;
                        let session = shard_clone.add_client(&address, transport);
                        info!("Added {} client with session: {} for IP address: {}", transport, session, address);

                        let client_id = session.client_id;
                        info!("Created new session: {}", session);
                        let mut sender = SenderKind::get_tcp_sender(stream);

                        let conn_stop_receiver = shard.task_registry.add_connection(client_id);

                        let shard_for_conn = shard_clone.clone();
                        let registry = shard.task_registry.clone();
                        let registry_clone = registry.clone();
                        registry.spawn_connection(async move {
                            if let Err(error) = handle_connection(&session, &mut sender, &shard_for_conn, conn_stop_receiver).await {
                                handle_error(error);
                            }

                            registry_clone.remove_connection(&client_id);
                            shard_for_conn.delete_client(session.client_id);
                            if let Err(error) = sender.shutdown().await {
                                error!("Failed to shutdown TCP stream for client: {}, address: {}. {}", client_id, address, error);
                            } else {
                                info!("Successfully closed TCP stream for client: {}, address: {}.", client_id, address);
                            }
                        });
                    }
                    Err(error) => error!("Unable to accept TCP socket. {}", error),
                }
            }
        }
    }
    Ok(())
}
