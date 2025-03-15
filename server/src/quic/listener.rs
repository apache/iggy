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

use crate::binary::command;
use crate::binary::sender::SenderKind;
use crate::command::ServerCommand;
use crate::server_error::ConnectionError;
use crate::streaming::clients::client_manager::Transport;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::{anyhow, Context};
use bytes::Bytes;
use iggy::validatable::Validatable;
use iggy::{bytes_serializable::BytesSerializable, messages::MAX_PAYLOAD_SIZE};
use quinn::{Connection, Endpoint, RecvStream, SendStream};
use tracing::{debug, error, info};

const LISTENERS_COUNT: u32 = 10;
const INITIAL_BYTES_LENGTH: usize = 4;

pub fn start(endpoint: Endpoint, system: SharedSystem) {
    for _ in 0..LISTENERS_COUNT {
        let endpoint = endpoint.clone();
        let system = system.clone();
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
    }
}

async fn handle_connection(
    incoming_connection: quinn::Connecting,
    system: SharedSystem,
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
    session: impl AsRef<Session>,
) -> anyhow::Result<()> {
    let (send_stream, mut recv_stream) = stream;
    // TODO: read to BytesMut instead of Vec<u8>
    let request = recv_stream
        .read_to_end(MAX_PAYLOAD_SIZE as usize)
        .await
        .with_context(|| "Error when reading the QUIC request.")?;

    if request.len() < INITIAL_BYTES_LENGTH {
        return Err(anyhow!(
            "Unable to read the QUIC request length, expected: {INITIAL_BYTES_LENGTH} bytes, received: {} bytes.",
            request.len()
        ));
    }

    debug!("Trying to read command...");
    let length = request[..INITIAL_BYTES_LENGTH]
        .try_into()
        .map(u32::from_le_bytes)
        .unwrap_or_default();
    let command =
        ServerCommand::from_bytes(Bytes::copy_from_slice(&request[INITIAL_BYTES_LENGTH..]))
            .with_context(|| "Error when reading the QUIC request command.")?;
    command
        .validate()
        .with_context(|| "Error when validating the QUIC command.")?;

    debug!("Received a QUIC command: {command}, payload size: {length}");

    let mut sender = SenderKind::get_quic_sender(send_stream, recv_stream);
    command::handle(command, &mut sender, session.as_ref(), system.clone())
        .await
        .with_context(|| "Error when handling the QUIC request.")
}
