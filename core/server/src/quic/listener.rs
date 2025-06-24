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
use anyhow::anyhow;
use iggy_common::IggyError;
use quinn::{Connection, Endpoint, RecvStream, SendStream};
use tracing::{error, info, trace};

const LISTENERS_COUNT: u32 = 10;
const INITIAL_BYTES_LENGTH: usize = 4;
//TODO: Fixme
/*

pub fn start(endpoint: Endpoint, system: SharedSystem) {
    for _ in 0..LISTENERS_COUNT {
        let endpoint = endpoint.clone();
        let system = system.clone();
        //TODO: Fixme
        /*
        tokio::spawn(async move {
            while let Some(incoming_connection) = endpoint.accept().await {
                info!(
                    "Incoming connection from client: {}",
                    incoming_connection.remote_address()
                );
                let system = system.clone();
                let incoming_connection = incoming_connection.accept();
                if incoming_connection.is_err() {
                    error!(
                        "Error when accepting incoming connection: {:?}",
                        incoming_connection
                    );
                    continue;
                }
                let incoming_connection = incoming_connection.unwrap();
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(incoming_connection, system).await {
                        error!("Connection has failed: {error}");
                    }
                });
            }
        });
        */
    }
}

async fn handle_connection(
    incoming_connection: quinn::Connecting,
    shard: Rc<IggyShard>,
) -> Result<(), ConnectionError> {
    let connection = incoming_connection.await?;
    let address = connection.remote_address();
    info!("Client has connected: {address}");
    let session = system
        .read()
        .await
        .add_client(&address, Transport::Quic)
        .await;

    let client_id = session.client_id;
    while let Some(stream) = accept_stream(&connection, &system, client_id).await? {
        let system = system.clone();
        let session = session.clone();

        let handle_stream_task = async move {
            if let Err(err) = handle_stream(stream, system, session).await {
                error!("Error when handling QUIC stream: {:?}", err)
            }
        };
        let _handle = tokio::spawn(handle_stream_task);
    }
    Ok(())
}

type BiStream = (SendStream, RecvStream);

async fn accept_stream(
    connection: &Connection,
    system: &SharedSystem,
    client_id: u32,
) -> Result<Option<BiStream>, ConnectionError> {
    match connection.accept_bi().await {
        Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
            info!("Connection closed");
            system.read().await.delete_client(client_id).await;
            Ok(None)
        }
        Err(error) => {
            error!("Error when handling QUIC stream: {:?}", error);
            system.read().await.delete_client(client_id).await;
            Err(error.into())
        }
        Ok(stream) => Ok(Some(stream)),
    }
}

async fn handle_stream(
    stream: BiStream,
    system: SharedSystem,
    session: impl AsRef<Session> + std::fmt::Debug,
) -> anyhow::Result<()> {
    let (send_stream, mut recv_stream) = stream;

    let mut length_buffer = [0u8; INITIAL_BYTES_LENGTH];
    let mut code_buffer = [0u8; INITIAL_BYTES_LENGTH];

    recv_stream.read_exact(&mut length_buffer).await?;
    recv_stream.read_exact(&mut code_buffer).await?;

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

    // if let Err(e) = command.validate() {
    //     sender.send_error_response(e.clone()).await?;
    //     return Err(anyhow!("Command validation failed: {e}"));
    // }

    trace!("Received a QUIC command: {command}, payload size: {length}");

    match command
        .handle(&mut sender, length, session.as_ref(), &system)
        .await
    {
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
*/
