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
use crate::tcp::tcp_socket;
use iggy_common::IggyError;
use std::net::SocketAddr;
use std::rc::Rc;
use tracing::{error, info};

pub async fn start(server_name: &'static str, shard: Rc<IggyShard>) -> Result<(), IggyError> {
    let ip_v6 = shard.config.tcp.ipv6;
    let socket_config = &shard.config.tcp.socket;
    let addr: SocketAddr = shard
        .config
        .tcp
        .address
        .parse()
        .expect("Failed to parse TCP address");

    let socket = tcp_socket::build(ip_v6, socket_config);
    monoio::spawn(async move {
        socket
            .bind(&addr.into())
            .expect("Failed to bind TCP listener");
        socket.listen(1024).unwrap();
        let listener: std::net::TcpListener = socket.into();
        let listener = monoio::net::TcpListener::from_std(listener).unwrap();
        info!("{server_name} server has started on: {:?}", addr);
        loop {
            match listener.accept().await {
                Ok((stream, address)) => {
                    let shard = shard.clone();
                    info!("Accepted new TCP connection: {address}");
                    let transport = Transport::Tcp;
                    let session = shard.add_client(&address, transport);
                    //TODO: Those can be shared with other shards.
                    shard.add_active_session(session.clone());
                    // Broadcast session to all shards.
                    let event = ShardEvent::NewSession { address, transport };
                    // TODO: Fixme look inside of broadcast_event_to_all_shards method.
                    let _responses = shard.broadcast_event_to_all_shards(event.into());

                    let _client_id = session.client_id;
                    info!("Created new session: {session}");
                    let mut sender = SenderKind::get_tcp_sender(stream);
                    monoio::spawn(async move {
                        if let Err(error) = handle_connection(&session, &mut sender, &shard).await {
                            handle_error(error);
                            //TODO: Fixme
                            /*
                            //system.read().await.delete_client(client_id).await;
                            if let Err(error) = sender.shutdown().await {
                                error!(
                                    "Failed to shutdown TCP stream for client: {client_id}, address: {address}. {error}"
                                );
                            } else {
                                info!(
                                    "Successfully closed TCP stream for client: {client_id}, address: {address}."
                                );
                            }
                            */
                        }
                    });
                }
                Err(error) => error!("Unable to accept TCP socket. {error}"),
            }
        }
    })
    .await
}
