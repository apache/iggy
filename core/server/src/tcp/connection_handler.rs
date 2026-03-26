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

use crate::binary::dispatch::{self, HandlerResult, MAX_CONTROL_FRAME_PAYLOAD};
use crate::server_error::ConnectionError;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use async_channel::Receiver;
use bytes::BytesMut;
use futures::FutureExt;
use iggy_binary_protocol::RequestFrame;
use iggy_binary_protocol::codes::{SEND_MESSAGES_CODE, command_name};
use iggy_common::{GET_CLUSTER_METADATA_CODE, IggyError, SenderKind};
use std::io::ErrorKind;
use std::rc::Rc;
use tracing::{debug, error, info};

/// Connection lifecycle action after command handling.
pub enum ConnectionAction {
    /// Continue handling connection on current shard.
    Finished,

    /// Connection migrated to another shard, exit without cleanup.
    Migrated { to_shard: u16 },
}

pub(crate) async fn handle_connection(
    session: &Session,
    sender: &mut SenderKind,
    shard: &Rc<IggyShard>,
    stop_receiver: Receiver<()>,
) -> Result<ConnectionAction, ConnectionError> {
    let mut header_buffer = BytesMut::with_capacity(RequestFrame::HEADER_SIZE);
    loop {
        let read_future = sender.read(header_buffer);
        // TODO(hubcio): this futures::select! call is translated to epoll_wait syscall for every
        // message, which adds around 100 us median latency. We could instead just call sender.shutdown()
        // if some atomic bool is set, since this is all happenng within single thread.
        let (_, mut header_buf) = futures::select! {
            _ = stop_receiver.recv().fuse() => {
                info!("Connection stop signal received for session: {}", session);
                let _ = sender.send_error_response(IggyError::Disconnected).await;
                return Ok(ConnectionAction::Finished);
            }
            result = read_future.fuse() => {
                match result {
                    (Ok(_), buf) => (Ok::<(), IggyError>(()), buf),
                    (Err(error), buf) => {
                        header_buffer = buf;
                        if error.as_code() == IggyError::ConnectionClosed.as_code() {
                            return Err(ConnectionError::from(error));
                        } else {
                            error!("got error: {:?}", error);
                            sender.send_error_response(error).await?;
                            continue;
                        }
                    }
                }
            }
        };

        let length = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
        let code = u32::from_le_bytes(header_buf[4..8].try_into().unwrap());
        header_buf.clear();
        header_buffer = header_buf;

        let cmd_name = command_name(code).unwrap_or("unknown");
        debug!("Received a TCP request, length: {length}, code: {code} ({cmd_name})");

        let payload_length = match RequestFrame::payload_length(length) {
            Ok(len) => len,
            Err(_) => {
                sender
                    .send_error_response(IggyError::InvalidCommand)
                    .await?;
                continue;
            }
        };

        let result = if code == SEND_MESSAGES_CODE {
            dispatch::dispatch_send_messages(sender, payload_length, session, shard).await
        } else {
            if payload_length > MAX_CONTROL_FRAME_PAYLOAD {
                sender
                    .send_error_response(IggyError::InvalidCommand)
                    .await?;
                continue;
            }
            let payload = dispatch::read_payload(sender, payload_length).await?;
            let frame = RequestFrame::from_parts(code, &payload);
            dispatch::dispatch(frame, sender, session, shard).await
        };

        match result {
            Ok(handler_result) => match handler_result {
                HandlerResult::Finished => {
                    debug!(
                        "Command {code} ({cmd_name}) was handled successfully, session: {session}. TCP response was sent."
                    );
                }
                HandlerResult::Migrated { to_shard } => {
                    info!(
                        "Command {code} ({cmd_name}) was transferred to shard {to_shard}, session: {session}."
                    );

                    return Ok(ConnectionAction::Migrated { to_shard });
                }
            },
            Err(error) => {
                if code == GET_CLUSTER_METADATA_CODE
                    && matches!(error, IggyError::FeatureUnavailable)
                {
                    debug!(
                        "GetClusterMetadata command not available (clustering disabled), session: {session}."
                    );
                    sender.send_error_response(error).await?;
                    debug!("TCP error response was sent to: {session}.");
                } else {
                    error!(
                        "Command with code {code} ({cmd_name}) was not handled successfully, session: {session}, error: {error}."
                    );

                    if matches!(error, IggyError::ClientNotFound(_) | IggyError::StaleClient) {
                        sender.send_error_response(error.clone()).await?;
                        debug!("TCP error response was sent to: {session}.");
                        error!("Session: {session} will be deleted.");
                        return Err(ConnectionError::from(error));
                    } else {
                        sender.send_error_response(error).await?;
                        debug!("TCP error response was sent to: {session}.");
                    }
                }
            }
        }
    }
}

pub(crate) fn handle_error(error: ConnectionError) {
    match error {
        ConnectionError::IoError(e) => match e.kind() {
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
                error!("Connection has failed: {e}");
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
