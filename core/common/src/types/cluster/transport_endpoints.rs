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

use crate::{BytesSerializable, IggyError};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TransportEndpoints {
    pub tcp: u16,
    pub quic: u16,
    pub http: u16,
    pub websocket: u16,
}

impl TransportEndpoints {
    pub fn new(tcp: u16, quic: u16, http: u16, websocket: u16) -> Self {
        Self {
            tcp,
            quic,
            http,
            websocket,
        }
    }
}

impl BytesSerializable for TransportEndpoints {
    fn to_bytes(&self) -> Bytes {
        let size = self.get_buffer_size();
        let mut bytes = BytesMut::with_capacity(size);
        self.write_to_buffer(&mut bytes);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError> {
        if bytes.len() < 8 {
            // Minimum: 4 ports * 2 bytes each
            return Err(IggyError::InvalidCommand);
        }

        let mut position = 0;

        // Read TCP port
        let tcp = u16::from_le_bytes(
            bytes[position..position + 2]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        position += 2;

        // Read QUIC port
        let quic = u16::from_le_bytes(
            bytes[position..position + 2]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        position += 2;

        // Read HTTP port
        let http = u16::from_le_bytes(
            bytes[position..position + 2]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        position += 2;

        // Read WebSocket port
        let websocket = u16::from_le_bytes(
            bytes[position..position + 2]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        Ok(TransportEndpoints {
            tcp,
            quic,
            http,
            websocket,
        })
    }

    fn write_to_buffer(&self, buf: &mut BytesMut) {
        buf.put_u16_le(self.tcp);
        buf.put_u16_le(self.quic);
        buf.put_u16_le(self.http);
        buf.put_u16_le(self.websocket);
    }

    fn get_buffer_size(&self) -> usize {
        8 // 4 ports * 2 bytes each
    }
}

impl Display for TransportEndpoints {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tcp: {}, quic: {}, http: {}, websocket: {}",
            self.tcp, self.quic, self.http, self.websocket
        )
    }
}
