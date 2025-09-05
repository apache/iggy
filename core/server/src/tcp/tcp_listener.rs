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
use crate::streaming::systems::system::SharedSystem;
use crate::tcp::connection_handler::{handle_connection, handle_error};
use iggy_common::TransportProtocol;
use std::net::SocketAddr;
use tokio::net::TcpSocket;
use tokio::sync::oneshot;
use tracing::{error, info};

pub async fn start(address: &str, socket: TcpSocket, system: SharedSystem) -> SocketAddr {
    let address = address.to_string();
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        let addr = address.parse();
        if addr.is_err() {
            panic!("Unable to parse address {address:?}");
        }

        socket
            .bind(addr.unwrap())
            .expect("Unable to bind socket to address");

        let listener = socket.listen(1024).expect("Unable to start TCP server.");

        let local_addr = listener
            .local_addr()
            .expect("Failed to get local address for TCP listener");

        tx.send(local_addr).unwrap_or_else(|_| {
            panic!("Failed to send the local address {local_addr:?} for TCP listener")
        });

        loop {
            match listener.accept().await {
                Ok((stream, address)) => {
                    info!("Accepted new TCP connection: {address}");
                    let session = system
                        .read()
                        .await
                        .add_client(&address, TransportProtocol::Tcp)
                        .await;

                    let client_id = session.client_id;
                    info!("Created new session: {session}");
                    let system = system.clone();
                    let mut sender = SenderKind::get_tcp_sender(stream);
                    tokio::spawn(async move {
                        if let Err(error) =
                            handle_connection(session, &mut sender, system.clone()).await
                        {
                            handle_error(error);
                            system.read().await.delete_client(client_id).await;
                            if let Err(error) = sender.shutdown().await {
                                error!(
                                    "Failed to shutdown TCP stream for client: {client_id}, address: {address}. {error}"
                                );
                            } else {
                                info!(
                                    "Successfully closed TCP stream for client: {client_id}, address: {address}."
                                );
                            }
                        }
                    });
                }
                Err(error) => error!("Unable to accept TCP socket. {error}"),
            }
        }
    });
    match rx.await {
        Ok(addr) => addr,
        Err(_) => panic!("Failed to get the local address for TCP listener."),
    }
}
