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

//! Consensus wire framing for reading/writing `Message<GenericHeader>` over TCP.
//!
//! Wire format: `[256-byte header][optional body]`
//! where `body_len = header.size - HEADER_SIZE`.
//! Header-only messages have `header.size == HEADER_SIZE` (no body).

use compio::BufResult;
use compio::buf::{IntoInner, IoBuf, IoBufMut};
use compio::io::{AsyncReadExt, AsyncWriteExt};
use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
use iggy_binary_protocol::consensus::iobuf::Owned;
use iggy_binary_protocol::{GenericHeader, HEADER_SIZE, Message};
use iggy_common::IggyError;

/// Default hard ceiling on a single wire frame. Frames above this are
/// almost certainly a protocol violation or a malicious peer; we drop
/// the connection rather than try to allocate a massive body buffer.
///
/// Equivalent to `MessageBusConfig::default().max_message_size`; kept in
/// sync with the [`crate::MessageBusConfig::default`] impl. Retained as
/// a named const for test ergonomics and for callers that have no
/// `IggyMessageBus` in scope (e.g. standalone handshake helpers).
pub const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

// Guards the `header_buf[48..52]` size-field decode below. If `GenericHeader`'s
// layout ever shifts, this trips at compile time before the runtime read
// parses garbage.
const _: () = {
    assert!(
        std::mem::offset_of!(GenericHeader, size) == 48,
        "framing::read_message hardcodes size offset 48; GenericHeader layout changed",
    );
};

/// Write a consensus message to a stream. Zero-copy: the message's owned
/// buffer is handed straight to `io_uring` as a `Frozen`.
///
/// Used by the handshake and framing test paths. Hot-path bus traffic goes
/// through [`crate::writer_task`] which batches many messages into a single
/// `writev` instead.
///
/// # Errors
///
/// Returns `IggyError::TcpError` if the write fails.
#[allow(clippy::future_not_send)]
pub async fn write_message<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    message: Message<GenericHeader>,
) -> Result<(), IggyError> {
    let buf = message.into_frozen();
    stream
        .write_all(buf)
        .await
        .0
        .map_err(|_| IggyError::TcpError)
}

/// Read a consensus message from a stream, rejecting frames whose
/// reported size field exceeds `max_message_size`.
///
/// Reads the 256-byte fixed header first, inspects `header.size` to
/// determine if there's a body, reads the body if present, and
/// constructs a validated `Message<GenericHeader>`.
///
/// Allocation: one `Owned<MESSAGE_ALIGN>` per frame. The header is read
/// into the first `HEADER_SIZE` bytes, then the body is read into the tail
/// of the same buffer. No intermediate reassembly copy.
///
/// # Errors
///
/// Returns `IggyError::ConnectionClosed` on EOF.
/// Returns `IggyError::TcpError` on I/O errors.
/// Returns `IggyError::InvalidCommand` if the header fails validation.
#[allow(clippy::future_not_send)]
pub async fn read_message<S: AsyncReadExt + Unpin>(
    stream: &mut S,
    max_message_size: usize,
) -> Result<Message<GenericHeader>, IggyError> {
    // Stage 1: allocate one `Owned<MESSAGE_ALIGN>` sized for just the
    // header. `Owned` implements `IoBufMut`, so compio writes directly
    // into its backing AVec and advances the length via `SetLen::set_len`.
    let owned = Owned::<MESSAGE_ALIGN>::with_capacity(HEADER_SIZE);
    let BufResult(result, owned) = stream.read_exact(owned).await;
    result.map_err(|e| to_read_error(&e))?;

    let total_size = u32::from_le_bytes(
        owned.as_slice()[48..52]
            .try_into()
            .map_err(|_| IggyError::InvalidCommand)?,
    ) as usize;

    if !(HEADER_SIZE..=max_message_size).contains(&total_size) {
        return Err(IggyError::InvalidCommand);
    }

    if total_size == HEADER_SIZE {
        return Message::<GenericHeader>::try_from(owned).map_err(|_| IggyError::InvalidCommand);
    }

    let body_size = total_size - HEADER_SIZE;

    // Stage 2: grow the same `Owned` in place and fill the tail via a
    // slice read. Total allocations for a body frame: ONE
    // (`Owned::with_capacity(HEADER_SIZE)` plus one in-place realloc of
    // the backing AVec). Zero memcpys of the data.
    let mut owned = owned;
    // Propagate any underlying reservation failure. `Owned::reserve_exact`
    // is infallible today, but the `IoBufMut` contract allows an `Err`
    // (e.g. capacity overflow, unsupported buffer kind) and silently
    // ignoring it would leave `owned` at header-only capacity, causing the
    // subsequent `read_exact` to read into an ungrown buffer.
    IoBufMut::reserve_exact(&mut owned, body_size).map_err(|_| IggyError::TcpError)?;
    let BufResult(result, slice) = stream
        .read_exact(owned.slice(HEADER_SIZE..total_size))
        .await;
    result.map_err(|e| to_read_error(&e))?;
    let owned = slice.into_inner();

    Message::<GenericHeader>::try_from(owned).map_err(|_| IggyError::InvalidCommand)
}

fn to_read_error(e: &std::io::Error) -> IggyError {
    if e.kind() == std::io::ErrorKind::UnexpectedEof {
        IggyError::ConnectionClosed
    } else {
        IggyError::TcpError
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use compio::net::{TcpListener, TcpStream};
    use iggy_binary_protocol::Command2;

    #[allow(clippy::cast_possible_truncation)]
    fn make_header_only(command: Command2) -> Message<GenericHeader> {
        Message::<GenericHeader>::new(HEADER_SIZE).transmute_header(|_, h: &mut GenericHeader| {
            h.command = command;
            h.size = HEADER_SIZE as u32;
        })
    }

    #[allow(clippy::future_not_send)]
    async fn local_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connect = TcpStream::connect(addr);
        let accept = listener.accept();
        let (client_res, accept_res) = futures::join!(connect, accept);
        let (server, _) = accept_res.unwrap();
        (client_res.unwrap(), server)
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn write_then_read_header_only() {
        let (mut a, mut b) = local_pair().await;
        let msg = make_header_only(Command2::Ping);
        write_message(&mut a, msg).await.unwrap();
        let read = read_message(&mut b, MAX_MESSAGE_SIZE).await.unwrap();
        assert_eq!(read.header().command, Command2::Ping);
        assert_eq!(read.header().size as usize, HEADER_SIZE);
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn read_rejects_oversize_size_field() {
        use compio::io::AsyncWriteExt;
        let (mut a, mut b) = local_pair().await;

        let mut buf = vec![0u8; HEADER_SIZE];
        let bogus = u32::try_from(MAX_MESSAGE_SIZE + 1)
            .unwrap_or(u32::MAX)
            .to_le_bytes();
        buf[48..52].copy_from_slice(&bogus);
        a.write_all(buf).await.0.unwrap();

        let res = read_message(&mut b, MAX_MESSAGE_SIZE).await;
        assert!(matches!(res, Err(IggyError::InvalidCommand)));
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn read_rejects_undersize_size_field() {
        use compio::io::AsyncWriteExt;
        let (mut a, mut b) = local_pair().await;

        let mut buf = vec![0u8; HEADER_SIZE];
        let undersize = u32::try_from(HEADER_SIZE - 1).unwrap();
        buf[48..52].copy_from_slice(&undersize.to_le_bytes());
        a.write_all(buf).await.0.unwrap();

        let res = read_message(&mut b, MAX_MESSAGE_SIZE).await;
        assert!(matches!(res, Err(IggyError::InvalidCommand)));
    }
}
