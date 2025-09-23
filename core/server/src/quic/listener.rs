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
use crate::shard::transmission::event::ShardEvent;
use crate::streaming::session::Session;
use crate::{shard_debug, shard_info};
use anyhow::anyhow;
use compio_quic::{Connection, Endpoint, RecvStream, SendStream};
use iggy_common::IggyError;
use iggy_common::TransportProtocol;
use tracing::{error, info, trace};

const INITIAL_BYTES_LENGTH: usize = 4;

pub async fn start(endpoint: Endpoint, shard: Rc<IggyShard>) -> Result<(), IggyError> {
    info!("Starting QUIC listener for shard {}", shard.id);

    // Since the QUIC Endpoint is internally Arc-wrapped and can be shared,
    // we only need one worker per shard rather than multiple workers per endpoint.
    // This avoids the N×workers multiplication when multiple shards are used.
    while let Some(incoming_conn) = endpoint.wait_incoming().await {
        let remote_addr = incoming_conn.remote_address();
        trace!("Incoming connection from client: {}", remote_addr);
        let shard = shard.clone();

        // Spawn each connection handler independently to maintain concurrency
        compio::runtime::spawn(async move {
            trace!("Accepting connection from {}", remote_addr);
            match incoming_conn.await {
                Ok(connection) => {
                    trace!("Connection established from {}", remote_addr);
                    if let Err(error) = handle_connection(connection, shard).await {
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
        })
        .detach();
    }

    info!("QUIC listener for shard {} stopped", shard.id);
    Ok(())
}

async fn handle_connection(
    connection: Connection,
    shard: Rc<IggyShard>,
) -> Result<(), ConnectionError> {
    let address = connection.remote_address();
    info!("Client has connected: {address}");
    let session = shard.add_client(&address, TransportProtocol::Quic);

    let session = shard.add_client(&address, TransportProtocol::Quic);
    let client_id = session.client_id;
    shard_debug!(
        shard.id,
        "Added {} client with session: {} for IP address: {}",
        TransportProtocol::Quic,
        session,
        address
    );

    // Add session to active sessions and broadcast to all shards
    shard.add_active_session(session.clone());
    let event = ShardEvent::NewSession {
        address,
        transport: TransportProtocol::Quic,
    };
    let _responses = shard.broadcast_event_to_all_shards(event.into()).await;

    while let Some(stream) = accept_stream(&connection, &shard, client_id).await? {
        let shard = shard.clone();
        let session = session.clone();

        let handle_stream_task = async move {
            if let Err(err) = handle_stream(stream, shard, session).await {
                error!("Error when handling QUIC stream: {:?}", err)
            }
        };
        let _handle = compio::runtime::spawn(handle_stream_task).detach();
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
            shard.delete_client(client_id);
            Ok(None)
        }
        Err(error) => {
            error!("Error when handling QUIC stream: {:?}", error);
            shard.delete_client(client_id);
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

    match command.handle(&mut sender, length, &session, &shard).await {
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
