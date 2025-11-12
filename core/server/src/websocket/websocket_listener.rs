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
use crate::configs::websocket::WebSocketConfig;
use crate::shard::IggyShard;
use crate::shard::task_registry::ShutdownToken;
use crate::shard::transmission::event::ShardEvent;
use crate::websocket::connection_handler::{handle_connection, handle_error};
use crate::websocket::websocket_sender::WebSocketSender;
use compio::net::TcpListener;
use compio_net::TcpOpts;
use compio_ws::accept_async_with_config;
use err_trail::ErrContext;
use futures::FutureExt;
use iggy_common::IggyError;
use iggy_common::TransportProtocol;
use std::net::SocketAddr;
use std::rc::Rc;
use tracing::{debug, error, info};

async fn create_listener(addr: SocketAddr) -> Result<TcpListener, std::io::Error> {
    // Required by the thread-per-core model...
    // We create bunch of sockets on different threads, that bind to exactly the same address and port.
    let opts = TcpOpts::new().reuse_port(true).reuse_address(true);
    TcpListener::bind_with_options(addr, opts).await
}

pub async fn start(
    config: WebSocketConfig,
    shard: Rc<IggyShard>,
    shutdown: ShutdownToken,
) -> Result<(), IggyError> {
    let mut addr: SocketAddr = config
        .address
        .parse()
        .with_error(|error| {
            format!(
                "WebSocket (error: {error}) - failed to parse address: {}",
                config.address
            )
        })
        .map_err(|_| IggyError::InvalidConfiguration)?;

    if shard.id != 0 && addr.port() == 0 {
        info!("Waiting for WebSocket address from shard 0...");
        loop {
            if let Some(bound_addr) = shard.websocket_bound_address.get() {
                addr = bound_addr;
                info!("Received WebSocket address from shard 0: {}", addr);
                break;
            }
            // Small delay to prevent busy waiting
            compio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    let listener = create_listener(addr)
        .await
        .with_error(|error| {
            format!("WebSocket (error: {error}) - failed to bind to address: {addr}")
        })
        .map_err(|_| IggyError::CannotBindToSocket(addr.to_string()))?;

    let local_addr = listener.local_addr().unwrap();
    info!("{} has started on: ws://{}", "WebSocket Server", local_addr);

    // Notify shard about the bound address
    let event = ShardEvent::AddressBound {
        protocol: TransportProtocol::WebSocket,
        address: local_addr,
    };

    if shard.id == 0 {
        // Store bound address locally first
        shard.websocket_bound_address.set(Some(local_addr));

        if addr.port() == 0 {
            // Broadcast to other shards for SO_REUSEPORT binding
            shard.broadcast_event_to_all_shards(event).await?;
        }
    } else {
        // Non-shard0 just handles the event locally
        shard.handle_event(event).await.ok();
    }

    let ws_config = config.to_tungstenite_config();
    info!(
        "WebSocket config: max_message_size: {:?}, max_frame_size: {:?}, accept_unmasked_frames: {}",
        config.max_message_size, config.max_frame_size, config.accept_unmasked_frames
    );

    accept_loop(listener, Some(ws_config), shard, shutdown).await
}

async fn accept_loop(
    listener: TcpListener,
    ws_config: Option<compio_ws::WebSocketConfig>,
    shard: Rc<IggyShard>,
    shutdown: ShutdownToken,
) -> Result<(), IggyError> {
    loop {
        let shard = shard.clone();
        let accept_future = listener.accept();

        futures::select! {
            _ = shutdown.wait().fuse() => {
                debug!("WebSocket Server received shutdown signal, no longer accepting connections");
                break;
            }
            result = accept_future.fuse() => {
                match result {
                    Ok((tcp_stream, remote_addr)) => {
                        if shard.is_shutting_down() {
                            info!("Rejecting new WebSocket connection from {} during shutdown", remote_addr);
                            continue;
                        }
                        info!("Accepted new WebSocket connection from: {}", remote_addr);

                        let shard_clone = shard.clone();
                        let ws_config_clone = ws_config;
                        let registry = shard.task_registry.clone();
                        let registry_clone = registry.clone();

                        registry.spawn_connection(async move {
                            match accept_async_with_config(tcp_stream, ws_config_clone).await {
                                Ok(websocket) => {
                                    info!("WebSocket handshake successful from: {}", remote_addr);

                                    let session = shard_clone.add_client(&remote_addr, TransportProtocol::WebSocket);
                                    let client_id = session.client_id;

                                    let sender = WebSocketSender::new(websocket);
                                    let mut sender_kind = SenderKind::get_websocket_sender(sender);
                                    let client_stop_receiver = registry_clone.add_connection(client_id);

                                    if let Err(error) = handle_connection(&session, &mut sender_kind, &shard_clone, client_stop_receiver).await {
                                        handle_error(error);
                                    }
                                    shard_clone.delete_client(session.client_id);
                                    registry_clone.remove_connection(&client_id);

                                    match sender_kind.shutdown().await {
                                        Ok(_) => {
                                            info!("Successfully closed WebSocket stream for client: {}, address: {}.", client_id, remote_addr);
                                        }
                                        Err(_) => {
                                            // shutdown failures during client disconnect are expected and normal
                                            // real errors would have been caught earlier in handle_connection
                                            debug!("WebSocket shutdown completed with error for client: {} (likely client already disconnected)", client_id);
                                        }
                                    }
                                }
                                Err(error) => {
                                    error!("WebSocket handshake failed from {}: {:?}", remote_addr, error);
                                }
                            }
                        });
                    }
                    Err(error) => {
                        error!("Failed to accept WebSocket connection: {}", error);
                    }
                }
            }
        }
    }

    info!("WebSocket Server listener has stopped");
    Ok(())
}
