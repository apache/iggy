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
use crate::streaming::clients::client_manager::Transport;
use crate::tcp::connection_handler::{handle_connection, handle_error};
use compio::net::{TcpListener, TcpOpts};
use error_set::ErrContext;
use futures::FutureExt;
use iggy_common::IggyError;
use std::net::SocketAddr;
use std::rc::Rc;
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
    addr: SocketAddr,
    config: &TcpSocketConfig,
    shard: Rc<IggyShard>,
) -> Result<(), IggyError> {
    let listener = create_listener(addr, config)
        .await
        .map_err(|_| IggyError::CannotBindToSocket(addr.to_string()))
        .with_error_context(|err| {
            format!("Failed to bind {server_name} server to address: {addr}, {err}")
        })?;
    info!("{server_name} server has started on: {:?}", addr);
    loop {
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
                info!("{server_name} detected shutdown flag, no longer accepting connections");
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
                        info!("Accepted new TCP connection: {address}");
                        let transport = Transport::Tcp;
                        let session = shard_clone.add_client(&address, transport);
                        //TODO: Those can be shared with other shards.
                        shard_clone.add_active_session(session.clone());
                        // Broadcast session to all shards.
                        let event = ShardEvent::NewSession { address, transport };
                        // TODO: Fixme look inside of broadcast_event_to_all_shards method.
                        let _responses = shard_clone.broadcast_event_to_all_shards(event.into());

                        let client_id = session.client_id;
                        info!("Created new session: {session}");
                        let mut sender = SenderKind::get_tcp_sender(stream);

                        let conn_stop_receiver = shard_clone.task_registry.add_connection(client_id);

                        let shard_for_conn = shard_clone.clone();
                        shard_clone.task_registry.spawn_tracked(async move {
                            if let Err(error) = handle_connection(&session, &mut sender, &shard_for_conn, conn_stop_receiver).await {
                                handle_error(error);
                            }
                            shard_for_conn.task_registry.remove_connection(&client_id);

                            if let Err(error) = sender.shutdown().await {
                                error!(
                                    "Failed to shutdown TCP stream for client: {client_id}, address: {address}. {error}"
                                );
                            } else {
                                info!(
                                    "Successfully closed TCP stream for client: {client_id}, address: {address}."
                                );
                            }
                        });
                    }
                    Err(error) => error!("Unable to accept TCP socket. {error}"),
                }
            }
        }
    }
    Ok(())
}
