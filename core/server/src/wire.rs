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

use bytes::Bytes;
use iggy_binary_protocol::RoutedRequestHeader;
use iggy_common::IggyError;
use message_bus::installer::conn_info::ClientTransportKind;
use server_common::Message;

pub(crate) fn request_body(request: &Message<RoutedRequestHeader>) -> &[u8] {
    &request.as_slice()[std::mem::size_of::<RoutedRequestHeader>()..request.header().size as usize]
}

/// Check a client's `request_checksum` against the body it stamps.
///
/// Must run BEFORE any body rewrite: PAT / password / consumer-group paths
/// substitute server-chosen bytes. Zero is "unstamped" and skips the check, so an
/// SDK predating the stamp still works.
///
/// # Errors
/// [`IggyError::InvalidFormat`] when the stamp disagrees with the body.
pub(crate) fn verify_request_checksum(
    request: &Message<RoutedRequestHeader>,
) -> Result<(), IggyError> {
    let stamped = request.header().request_checksum;
    if stamped == 0 || u128::from(iggy_common::calculate_checksum(request_body(request))) == stamped
    {
        return Ok(());
    }
    Err(IggyError::InvalidFormat)
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

/// Rebuild a request message with `body` replacing the original payload,
/// preserving the header (and fixing `size`). Used by the primary-side
/// request rewrites that swap a secret-bearing wire body for the
/// hash-carrying replicated body before consensus.
pub(crate) fn rewrite_request_body(
    request: &Message<RoutedRequestHeader>,
    body: &Bytes,
) -> Result<Message<RoutedRequestHeader>, IggyError> {
    let total_size = std::mem::size_of::<RoutedRequestHeader>()
        .checked_add(body.len())
        .ok_or(IggyError::InvalidConfiguration)?;
    let size = u32::try_from(total_size).map_err(|_| IggyError::InvalidConfiguration)?;
    let mut rewritten = Message::<RoutedRequestHeader>::new(total_size);
    let header = bytemuck::checked::try_from_bytes_mut::<RoutedRequestHeader>(
        &mut rewritten.as_mut_slice()[..std::mem::size_of::<RoutedRequestHeader>()],
    )
    .expect("zeroed bytes are a valid request header");
    *header = *request.header();
    header.size = size;
    // Both describe the body just replaced, and nothing recomputes them for a
    // `RoutedRequestHeader` -- the prepare projection derives its own `checksum_body`
    // downstream. Clear rather than recompute; carrying them forward is a stale claim.
    header.checksum = 0;
    header.checksum_body = 0;
    // `request_checksum` is deliberately NOT touched: it stamps what the CLIENT sent,
    // already validated at admission. Re-stamping it over the substituted body would
    // make the client-table reuse check compare a value no client ever produced.
    rewritten.as_mut_slice()[std::mem::size_of::<RoutedRequestHeader>()..].copy_from_slice(body);
    Ok(rewritten)
}

#[cfg(test)]
mod tests {
    use super::{request_body, rewrite_request_body};
    use bytes::Bytes;
    use iggy_binary_protocol::{Command, Operation, RoutedRequestHeader};
    use server_common::Message;
    use std::mem::size_of;

    fn request(body: &[u8], request_checksum: u128) -> Message<RoutedRequestHeader> {
        let total_size = size_of::<RoutedRequestHeader>() + body.len();
        let mut message = Message::<RoutedRequestHeader>::new(total_size).transmute_header(
            |_, header: &mut RoutedRequestHeader| {
                header.command = Command::Request;
                header.operation = Operation::CreateStream;
                header.client = 1;
                header.session = 1;
                header.request = 9;
                header.size = u32::try_from(total_size).expect("fits u32");
                header.request_checksum = request_checksum;
                header.checksum = 0xdead;
                header.checksum_body = 0xbeef;
            },
        );
        message.as_mut_slice()[size_of::<RoutedRequestHeader>()..].copy_from_slice(body);
        message
    }

    #[test]
    fn given_a_body_rewrite_should_keep_the_client_stamp_and_clear_the_stale_seals() {
        // The secret-bearing wire body is swapped for the hash-carrying replicated
        // one. `request_checksum` describes what the client sent and admission has
        // already checked it, so it must survive; the other two describe the body
        // that just went away.
        let original = request(b"plaintext-secret", 0x1234);
        let rewritten = rewrite_request_body(&original, &Bytes::from_static(b"argon2-hash"))
            .expect("the rewritten body fits a request message");

        assert_eq!(
            rewritten.header().request_checksum,
            0x1234,
            "the client's stamp must not be re-signed over server-substituted bytes"
        );
        assert_eq!(rewritten.header().checksum, 0);
        assert_eq!(rewritten.header().checksum_body, 0);
        assert_eq!(request_body(&rewritten), b"argon2-hash");
        assert_eq!(
            rewritten.header().size as usize,
            size_of::<RoutedRequestHeader>() + b"argon2-hash".len(),
            "`size` follows the new body, so `request_body` bounds it correctly"
        );
    }

    #[test]
    fn given_an_unstamped_request_when_rewriting_should_stay_unstamped() {
        // Zero means "unstamped" all the way through the client table, so a rewrite
        // must not manufacture a stamp for a client that sent none.
        let original = request(b"plaintext-secret", 0);
        let rewritten = rewrite_request_body(&original, &Bytes::from_static(b"argon2-hash"))
            .expect("the rewritten body fits a request message");

        assert_eq!(rewritten.header().request_checksum, 0);
    }
}
