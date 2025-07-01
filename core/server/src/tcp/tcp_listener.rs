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
use crate::shard::IggyShard;
use crate::shard::transmission::event::ShardEvent;
use crate::streaming::clients::client_manager::Transport;
use crate::tcp::connection_handler::{handle_connection, handle_error};
use futures::FutureExt;
use iggy_common::IggyError;
use socket2::Socket;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use tracing::{error, info};

pub async fn start(server_name: &'static str, addr: SocketAddr, socket: Socket, shard: Rc<IggyShard>) -> Result<(), IggyError> {
    socket
        .bind(&addr.into())
        .expect("Failed to bind TCP listener");
    socket.listen(1024).unwrap();
    let listener: std::net::TcpListener = socket.into();
    let listener = monoio::net::TcpListener::from_std(listener).unwrap();
    info!("{server_name} server has started on: {:?}", addr);

    loop {
        let shutdown_check = async {
            loop {
                if shard.is_shutting_down() {
                    return;
                }
                monoio::time::sleep(Duration::from_millis(100)).await;
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
