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

use crate::kafka::handlers;
use crate::kafka::protocol::codec::{decode_request_header, encode_response_frame};
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use crate::{IGGY_ROOT_PASSWORD_ENV, IGGY_ROOT_USERNAME_ENV};
use async_channel::Receiver;
use compio::BufResult;
use compio::io::{AsyncReadExt, AsyncWriteExt};
use compio::net::TcpStream;
use futures::FutureExt;
use iggy_common::IggyError;
use std::rc::Rc;
use tracing::{debug, error, warn};

pub async fn handle_connection(
    session: &Session,
    mut stream: TcpStream,
    shard: &Rc<IggyShard>,
    stop_receiver: Receiver<()>,
) -> Result<(), IggyError> {
    let mut authenticated = false;
    let max_request_size = shard.config.kafka.max_request_size;

    loop {
        let size_buf = vec![0u8; 4];
        let read_future = stream.read_exact(size_buf);

        let BufResult(result, size_buf) = futures::select! {
            _ = stop_receiver.recv().fuse() => {
                debug!("Kafka connection stop signal for client {}", session.client_id);
                return Ok(());
            }
            result = read_future.fuse() => result,
        };

        if let Err(e) = result {
            return if e.kind() == std::io::ErrorKind::UnexpectedEof {
                Ok(())
            } else {
                Err(IggyError::TcpError)
            };
        }

        let request_size = u32::from_be_bytes(size_buf[..4].try_into().unwrap());
        if request_size > max_request_size {
            warn!(
                "Kafka request too large ({request_size} bytes) from client {}",
                session.client_id
            );
            return Err(IggyError::TcpError);
        }

        let payload_buf = vec![0u8; request_size as usize];
        let BufResult(result, payload_buf) = stream.read_exact(payload_buf).await;
        if let Err(e) = result {
            return if e.kind() == std::io::ErrorKind::UnexpectedEof {
                Ok(())
            } else {
                Err(IggyError::TcpError)
            };
        }

        let (header, body_offset) = match decode_request_header(&payload_buf) {
            Ok(h) => h,
            Err(e) => {
                error!(
                    "Failed to decode Kafka request header: {e} (payload_len={})",
                    payload_buf.len()
                );
                return Err(IggyError::TcpError);
            }
        };

        if !authenticated {
            let username =
                std::env::var(IGGY_ROOT_USERNAME_ENV).unwrap_or_else(|_| "iggy".to_string());
            let password =
                std::env::var(IGGY_ROOT_PASSWORD_ENV).unwrap_or_else(|_| "iggy".to_string());
            if let Err(e) = shard.login_user(&username, &password, Some(session)) {
                error!("Kafka auto-login failed: {e}");
                return Err(IggyError::Unauthenticated);
            }
            authenticated = true;
        }

        let body = &payload_buf[body_offset..];

        let response_body =
            handlers::dispatch(shard, session, &shard.config.kafka, &header, body).await?;

        // ApiVersions always uses response header v0 (KIP-511).
        let frame = encode_response_frame(header.correlation_id, &response_body);
        let BufResult(result, _) = stream.write_all(frame).await;
        if let Err(e) = result {
            debug!("Kafka write failed for client {}: {e}", session.client_id);
            return Err(IggyError::TcpError);
        }
    }
}
