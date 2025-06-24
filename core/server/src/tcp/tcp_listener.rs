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
use crate::streaming::clients::client_manager::Transport;
use crate::tcp::connection_handler::{handle_connection, handle_error};
use std::net::SocketAddr;
use std::rc::Rc;
use monoio::net::TcpListener;
use rustls::pki_types::Ipv4Addr;
use tokio::net::TcpSocket;
use tokio::sync::oneshot;
use tracing::{error, info};

pub async fn start(server_name: &str, shard: Rc<IggyShard>) {
    let addr: SocketAddr = if shard.config.tcp.ipv6 {
        shard.config.tcp.address.parse().expect("Unable to parse IPv6 address")
    } else {
        shard.config.tcp.address.parse().expect("Unable to parse IPv4 address")
    };
    monoio::spawn(async move {
        let listener = TcpListener::bind(addr).expect(format!("Unable to start {server_name}.").as_ref());
        loop {
            match listener.accept().await {
                Ok((stream, address)) => {
                    let shard = shard.clone();
                    info!("Accepted new TCP connection: {address}");
                    let session = shard
                        .add_client(&address, Transport::Tcp);

                    let client_id = session.client_id;
                    info!("Created new session: {session}");
                    let mut sender = SenderKind::get_tcp_sender(stream);
                    monoio::spawn(async move {
                        if let Err(error) =
                            handle_connection(session, &mut sender, shard.clone()).await
                        {
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
    });
}
