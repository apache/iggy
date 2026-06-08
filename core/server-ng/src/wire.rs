// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Leaf wire helpers shared by the request-handling modules.
//!
//! Request-body slicing, the `usize -> u32` wire conversion, and the
//! transport-kind discriminant mapping.

use iggy_binary_protocol::RequestHeader;
use iggy_common::IggyError;
use message_bus::installer::conn_info::ClientTransportKind;
use server_common::Message;

pub(crate) fn request_body(request: &Message<RequestHeader>) -> &[u8] {
    &request.as_slice()[std::mem::size_of::<RequestHeader>()..request.header().size as usize]
}

/// Map the transport kind to the legacy wire discriminant
/// (`1=TCP, 2=QUIC, 4=WebSocket`); TLS variants report their base
/// transport. `ClientTransportKind` is `#[non_exhaustive]`, so any other
/// (TCP, TCP-TLS, or a future) variant falls back to TCP.
pub(crate) const fn transport_kind_to_wire(kind: ClientTransportKind) -> u8 {
    match kind {
        ClientTransportKind::Quic => 2,
        ClientTransportKind::Ws | ClientTransportKind::Wss => 4,
        _ => 1,
    }
}

pub(crate) fn usize_to_u32(value: usize) -> Result<u32, IggyError> {
    u32::try_from(value).map_err(|_| IggyError::InvalidIdentifier)
}
