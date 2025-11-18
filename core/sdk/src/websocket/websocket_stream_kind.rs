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

use crate::websocket::websocket_connection_stream::WebSocketConnectionStream;
use crate::websocket::websocket_stream::ConnectionStream;
use crate::websocket::websocket_tls_connection_stream::WebSocketTlsConnectionStream;
use iggy_common::IggyError;

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum WebSocketStreamKind {
    Plain(WebSocketConnectionStream),
    Tls(WebSocketTlsConnectionStream),
}

impl WebSocketStreamKind {
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize, IggyError> {
        match self {
            Self::Plain(stream) => stream.read(buf).await,
            Self::Tls(stream) => stream.read(buf).await,
        }
    }

    pub async fn write(&mut self, buf: &[u8]) -> Result<(), IggyError> {
        match self {
            Self::Plain(stream) => stream.write(buf).await,
            Self::Tls(stream) => stream.write(buf).await,
        }
    }

    pub async fn flush(&mut self) -> Result<(), IggyError> {
        match self {
            Self::Plain(stream) => stream.flush().await,
            Self::Tls(stream) => stream.flush().await,
        }
    }

    pub async fn shutdown(&mut self) -> Result<(), IggyError> {
        match self {
            Self::Plain(stream) => stream.shutdown().await,
            Self::Tls(stream) => stream.shutdown().await,
        }
    }
}
