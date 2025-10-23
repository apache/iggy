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

use crate::binary::command::{ServerCommand, ServerCommandHandler};
use crate::binary::sender::SenderKind;
use crate::server_error::ConnectionError;
use crate::shard::IggyShard;
use crate::shard::task_registry::ShutdownToken;
use crate::streaming::session::Session;
use anyhow::anyhow;
use compio_quic::{Connection, Endpoint, RecvStream, SendStream};
use futures::FutureExt;
use iggy_common::IggyError;
use iggy_common::TransportProtocol;
use std::rc::Rc;
use tracing::{debug, error, info, trace};

const INITIAL_BYTES_LENGTH: usize = 4;

pub async fn start(
    endpoint: Endpoint,
    shard: Rc<IggyShard>,
    shutdown: ShutdownToken,
) -> Result<(), IggyError> {
    loop {
        let accept_future = endpoint.wait_incoming();

        futures::select! {
            _ = shutdown.wait().fuse() => {
                debug!( "QUIC listener received shutdown signal, no longer accepting connections");
                break;
            }
            incoming_conn = accept_future.fuse() => {
                match incoming_conn {
                    Some(incoming_conn) => {
                        let remote_addr = incoming_conn.remote_address();

                        if shard.is_shutting_down() {
                            info!( "Rejecting new QUIC connection from {} during shutdown", remote_addr);
                            continue;
                        }

                        trace!("Incoming connection from client: {}", remote_addr);
                        let shard_for_conn = shard.clone();

                        shard.task_registry.spawn_connection(async move {
                            trace!("Accepting connection from {}", remote_addr);
                            match incoming_conn.await {
                                Ok(connection) => {
                                    trace!("Connection established from {}", remote_addr);
                                    if let Err(error) = handle_connection(connection, shard_for_conn).await {
                                        error!("QUIC connection from {} has failed: {error}", remote_addr);
                                    }
                                }
                                Err(error) => {
                                    error!(
                                        "Error when accepting incoming connection from {}: {:?}",
                                        remote_addr, error
                                    );
                                }
                            }
                        });
                    }
                    None => {
                        info!("QUIC endpoint closed for shard {}", shard.id);
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn handle_connection(
    connection: Connection,
    shard: Rc<IggyShard>,
) -> Result<(), ConnectionError> {
    let address = connection.remote_address();
    info!("Client has connected: {address}");
    let session = Rc::new(shard.add_client(&address, TransportProtocol::Quic));

    let client_id = session.client_id;
    debug!(
        "Added {} client with session: {} for IP address: {}",
        TransportProtocol::Quic,
        session,
        address
    );

    let conn_stop_receiver = shard.task_registry.add_connection(client_id);

    loop {
        let shard = shard.clone();
        futures::select! {
            // Check for shutdown signal
            _ = conn_stop_receiver.recv().fuse() => {
                info!("QUIC connection {} shutting down gracefully", client_id);
                break;
            }
            // Accept new connection
            stream_result = accept_stream(&connection, shard.clone(), client_id).fuse() => {
                match stream_result? {
                    Some(stream) => {
                        let shard_clone = shard.clone();
                        let session_rc = session.clone();

                        shard.task_registry.spawn_connection(async move {
                            if let Err(err) = handle_stream(stream, shard_clone, &session_rc).await {
                                error!("Error when handling QUIC stream: {:?}", err)
                            }
                        });
                    }
                    None => break, // Connection closed
                }
            }
        }
    }

    shard.delete_client(client_id);
    shard.task_registry.remove_connection(&client_id);
    info!("QUIC connection {} closed", client_id);
    Ok(())
}

type BiStream = (SendStream, RecvStream);

async fn accept_stream(
    connection: &Connection,
    _shard: Rc<IggyShard>,
    _client_id: u32,
) -> Result<Option<BiStream>, ConnectionError> {
    match connection.accept_bi().await {
        Err(compio_quic::ConnectionError::ApplicationClosed { .. }) => {
            info!("Connection closed");
            Ok(None)
        }
        Err(error) => {
            error!("Error when accepting QUIC connection: {:?}", error);
            Err(error.into())
        }
        Ok(stream) => Ok(Some(stream)),
    }
}

async fn handle_stream(
    stream: BiStream,
    shard: Rc<IggyShard>,
    session: &Session,
) -> anyhow::Result<()> {
    let (send_stream, mut recv_stream) = stream;

    let mut length_buffer = [0u8; INITIAL_BYTES_LENGTH];
    let mut code_buffer = [0u8; INITIAL_BYTES_LENGTH];

    recv_stream.read_exact(&mut length_buffer[..]).await?;
    recv_stream.read_exact(&mut code_buffer[..]).await?;

    let length = u32::from_le_bytes(length_buffer);
    let code = u32::from_le_bytes(code_buffer);

    trace!("Received a QUIC request, length: {length}, code: {code}");

    let mut sender = SenderKind::get_quic_sender(send_stream, recv_stream);

    let command = match ServerCommand::from_code_and_reader(code, &mut sender, length - 4).await {
        Ok(cmd) => cmd,
        Err(e) => {
            sender.send_error_response(e.clone()).await?;
            return Err(anyhow!("Failed to parse command: {e}"));
        }
    };

    trace!("Received a QUIC command: {command}, payload size: {length}");

    match command.handle(&mut sender, length, session, &shard).await {
        Ok(_) => {
            trace!(
                "Command was handled successfully, session: {:?}. QUIC response was sent.",
                session
            );
            Ok(())
        }
        Err(e) => {
            error!(
                "Command was not handled successfully, session: {:?}, error: {e}.",
                session
            );
            // Only return a connection-terminating error for client not found
            if let IggyError::ClientNotFound(_) = e {
                sender.send_error_response(e.clone()).await?;
                trace!("QUIC error response was sent.");
                error!("Session will be deleted.");
                Err(anyhow!("Client not found: {e}"))
            } else {
                // For all other errors, send response and continue the connection
                sender.send_error_response(e).await?;
                trace!("QUIC error response was sent.");
                Ok(())
            }
        }
    }
}
