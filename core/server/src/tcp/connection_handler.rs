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

use crate::binary::command::ServerCommandHandler;
use crate::binary::{command, sender::SenderKind};
use crate::server_error::ConnectionError;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use crate::tcp::connection_handler::command::ServerCommand;
use iggy::error::IggyError;
use std::io::ErrorKind;
use std::sync::Arc;
use tracing::{debug, error, info};

const INITIAL_BYTES_LENGTH: usize = 4;

pub(crate) async fn handle_connection(
    session: Arc<Session>,
    sender: &mut SenderKind,
    system: SharedSystem,
) -> Result<(), ConnectionError> {
    let mut length_buffer = [0u8; INITIAL_BYTES_LENGTH];
    let mut code_buffer = [0u8; INITIAL_BYTES_LENGTH];
    loop {
        let read_length = match sender.read(&mut length_buffer).await {
            Ok(read_length) => read_length,
            Err(error) => {
                if error.as_code() == IggyError::ConnectionClosed.as_code() {
                    return Err(ConnectionError::from(error));
                } else {
                    sender.send_error_response(error).await?;
                    continue;
                }
            }
        };

        if read_length != INITIAL_BYTES_LENGTH {
            sender.send_error_response(IggyError::CommandLengthError(format!(
                "Unable to read the TCP request length, expected: {INITIAL_BYTES_LENGTH} bytes, received: {read_length} bytes."
            ))).await?;
            continue;
        }

        let length = u32::from_le_bytes(length_buffer);
        sender.read(&mut code_buffer).await?;
        let code = u32::from_le_bytes(code_buffer);
        debug!("Received a TCP request, length: {length}, code: {code}");
        let command = ServerCommand::from_code_and_reader(code, sender, length - 4).await?;
        debug!("Received a TCP command: {command}, payload size: {length}");
        match command.handle(sender, length, &session, &system).await {
            Ok(_) => {
                debug!(
                    "Command was handled successfully, session: {session}. TCP response was sent."
                );
            }
            Err(error) => {
                error!("Command was not handled successfully, session: {session}, error: {error}.");
                if let IggyError::ClientNotFound(_) = error {
                    sender.send_error_response(error).await?;
                    debug!("TCP error response was sent to: {session}.");
                    error!("Session: {session} will be deleted.");
                    return Err(ConnectionError::from(IggyError::ClientNotFound(
                        session.client_id,
                    )));
                } else {
                    sender.send_error_response(error).await?;
                    debug!("TCP error response was sent to: {session}.");
                }
            }
        }
    }
}

pub(crate) fn handle_error(error: ConnectionError) {
    match error {
        ConnectionError::IoError(error) => match error.kind() {
            ErrorKind::UnexpectedEof => {
                info!("Connection has been closed.");
            }
            ErrorKind::ConnectionAborted => {
                info!("Connection has been aborted.");
            }
            ErrorKind::ConnectionRefused => {
                info!("Connection has been refused.");
            }
            ErrorKind::ConnectionReset => {
                info!("Connection has been reset.");
            }
            _ => {
                error!("Connection has failed: {error}");
            }
        },
        ConnectionError::SdkError(sdk_error) => match sdk_error {
            IggyError::ConnectionClosed => {
                debug!("Client closed connection.");
            }
            _ => {
                error!("Failure in internal SDK call: {sdk_error}");
            }
        },
        _ => {
            error!("Connection has failed: {error}");
        }
    }
}
