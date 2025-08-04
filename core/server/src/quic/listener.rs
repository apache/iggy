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

use std::rc::Rc;

use crate::binary::command::{ServerCommand, ServerCommandHandler};
use crate::binary::sender::SenderKind;
use crate::server_error::ConnectionError;
use crate::shard::IggyShard;
use crate::streaming::clients::client_manager::Transport;
use crate::streaming::session::Session;
use crate::streaming::utils::random_id;
use anyhow::anyhow;
use compio_quic::{Connection, Endpoint, RecvStream, SendStream};
use iggy_common::IggyError;
use tracing::{debug, error, info, trace};

const LISTENERS_COUNT: u32 = 10;
const INITIAL_BYTES_LENGTH: usize = 4;

pub async fn start(endpoint: Endpoint, shard: Rc<IggyShard>) -> Result<(), IggyError> {
    info!("Starting QUIC listener with {} workers", LISTENERS_COUNT);

    for i in 0..LISTENERS_COUNT {
        let endpoint = endpoint.clone();
        let shard = shard.clone();
        let _ = compio::runtime::spawn(async move {
            info!("Starting QUIC listener worker {}", i);
            loop {
                match endpoint.wait_incoming().await {
                    Some(incoming_conn) => {
                        let shard = shard.clone();
                        info!(
                            "Incoming connection from client: {}",
                            incoming_conn.remote_address()
                        );
                        let connection = match incoming_conn.await {
                            Ok(conn) => conn,
                            Err(error) => {
                                error!(
                                    "QUIC connection acceptance failed on listener {}: {:?}",
                                    i, error
                                );
                                continue;
                            }
                        };
                        let remote_addr = connection.remote_address();
                        let connection_id = random_id::get_ulid();
                        info!(
                            "QUIC connection {} established from {} on listener {}",
                            connection_id, remote_addr, i
                        );
                        // Spawn a task to handle this connection
                        let _ = compio::runtime::spawn(async move {
                            let start_time = std::time::Instant::now();

                            match handle_connection(connection, shard.clone()).await {
                                Ok(_) => {
                                    let duration = start_time.elapsed();
                                    debug!(
                                        "QUIC connection {} completed successfully in {} ms",
                                        connection_id,
                                        duration.as_millis()
                                    );
                                }
                                Err(error) => {
                                    let duration = start_time.elapsed();
                                    error!(
                                        "QUIC connection {} failed after {} ms: {error}",
                                        connection_id,
                                        duration.as_millis()
                                    );
                                }
                            }
                            debug!("QUIC connection {} closed", connection_id);
                        });
                    }
                    None => {
                        // No incoming connection available, wait a bit before checking again
                        compio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                }
            }
        });
    }
    // Keep the main task alive
    loop {
        compio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

async fn handle_connection(
    connection: Connection,
    shard: Rc<IggyShard>,
) -> Result<(), ConnectionError> {
    let address = connection.remote_address();
    info!("Client has connected: {address}");

    let session = shard.add_client(&address, Transport::Quic);
    let client_id = session.client_id;

    while let Some(stream) = accept_stream(&connection, &shard, client_id).await? {
        let shard = shard.clone();
        let session = session.clone();

        let handle_stream_task = async move {
            if let Err(err) = handle_stream(stream, shard, session).await {
                error!("Error when handling QUIC stream: {:?}", err)
            }
        };
        let _ = compio::runtime::spawn(handle_stream_task);
    }
    Ok(())
}

type BiStream = (SendStream, RecvStream);

async fn accept_stream(
    connection: &Connection,
    shard: &Rc<IggyShard>,
    client_id: u32,
) -> Result<Option<BiStream>, ConnectionError> {
    match connection.accept_bi().await {
        Err(compio_quic::ConnectionError::ApplicationClosed { .. }) => {
            info!("Connection closed");
            shard.delete_client(client_id).await;
            Ok(None)
        }
        Err(error) => {
            error!("Error when handling QUIC stream: {:?}", error);
            shard.delete_client(client_id).await;
            Err(error.into())
        }
        Ok(stream) => Ok(Some(stream)),
    }
}

async fn handle_stream(
    stream: BiStream,
    shard: Rc<IggyShard>,
    session: Rc<Session>,
) -> anyhow::Result<()> {
    let (send_stream, mut recv_stream) = stream;
    let request_id = random_id::get_ulid();
    let start_time = std::time::Instant::now();

    let mut length_buffer = [0u8; INITIAL_BYTES_LENGTH];
    let mut code_buffer = [0u8; INITIAL_BYTES_LENGTH];

    recv_stream.read_exact(&mut length_buffer[..]).await?;
    recv_stream.read_exact(&mut code_buffer[..]).await?;

    let length = u32::from_le_bytes(length_buffer);
    let code = u32::from_le_bytes(code_buffer);

    trace!(
        "Processing QUIC request {} with code: {}, length: {}, session: {}",
        request_id, code, length, session.client_id
    );

    let mut sender = SenderKind::get_quic_sender(send_stream, recv_stream);

    let command = match ServerCommand::from_code_and_reader(code, &mut sender, length - 4).await {
        Ok(cmd) => cmd,
        Err(e) => {
            sender.send_error_response(e.clone()).await?;
            return Err(anyhow!("Failed to parse command: {e}"));
        }
    };

    trace!("Received a QUIC command: {command}, payload size: {length}");

    match command.handle(&mut sender, length, &session, &shard).await {
        Ok(_) => {
            let duration = start_time.elapsed();
            trace!(
                "QUIC request {} completed successfully in {} ms (session: {})",
                request_id,
                duration.as_millis(),
                session.client_id
            );
            Ok(())
        }
        Err(e) => {
            let duration = start_time.elapsed();
            error!(
                "QUIC request {} failed after {} ms (session: {}): {e}",
                request_id,
                duration.as_millis(),
                session.client_id
            );

            if let IggyError::ClientNotFound(_) = e {
                sender.send_error_response(e.clone()).await?;
                trace!("QUIC error response sent for request {}", request_id);
                error!(
                    "Session {} will be deleted due to client not found",
                    session.client_id
                );
                Err(anyhow!("Client not found: {e}"))
            } else {
                sender.send_error_response(e).await?;
                trace!("QUIC error response sent for request {}", request_id);
                Ok(())
            }
        }
    }
}
