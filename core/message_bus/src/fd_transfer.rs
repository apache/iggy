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

//! File descriptor transfer between shards.
//!
//! After shard 0 accepts or connects a TCP socket and completes the
//! handshake, it calls [`dup_fd`] to create a second kernel reference
//! to the same socket. The duplicated fd (a plain `i32`) is sent to
//! the target shard via the inter-shard channel. The target shard
//! calls [`wrap_duped_fd`] to construct a compio `TcpStream` on its
//! own runtime.
//!
//! Shard 0 then drops its original `TcpStream`, closing its fd. The
//! socket stays alive because the duplicated fd still references it
//! in the kernel's file table.

use compio::net::TcpStream;
use std::io;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use tracing::warn;

/// Duplicate the underlying file descriptor of a TCP stream with
/// `FD_CLOEXEC` set atomically.
///
/// Returns the new fd number. The caller must arrange for the new fd
/// to be consumed by [`wrap_duped_fd`] on the target shard; leaking
/// it is a resource leak.
///
/// Uses `fcntl(F_DUPFD_CLOEXEC)` rather than `dup(2)` so that any
/// future `fork`+`exec` child cannot inherit this socket.
///
/// # Errors
///
/// Returns `io::Error` if the `fcntl(2)` syscall fails.
pub fn dup_fd(stream: &TcpStream) -> io::Result<RawFd> {
    let original = stream.as_raw_fd();
    // SAFETY: F_DUPFD_CLOEXEC allocates the lowest-numbered free fd >= 0
    // referring to the same open file description as `original`, with
    // FD_CLOEXEC set atomically. Safe to call on any valid fd.
    let duped = unsafe { libc::fcntl(original, libc::F_DUPFD_CLOEXEC, 0) };
    if duped == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(duped)
}

/// Wrap a previously duplicated raw fd into a compio `TcpStream`.
///
/// Must be called on the target shard's compio runtime so the
/// `TcpStream` is registered with the correct `io_uring` instance.
///
/// # Safety
///
/// - `fd` must be a valid, open file descriptor referring to a TCP socket.
/// - `fd` must not be owned by any other `TcpStream` or resource that
///   will close it.
/// - The caller takes ownership: the returned `TcpStream` will close
///   `fd` on drop.
#[must_use]
pub unsafe fn wrap_duped_fd(fd: RawFd) -> TcpStream {
    unsafe { TcpStream::from_raw_fd(fd) }
}

/// Close a raw fd without wrapping it in a `TcpStream`.
///
/// Used for cleanup on error paths when the fd was duped but the
/// target shard setup failed. Logs a warning if `close(2)` fails
/// with anything other than `EINTR`, since that signals a resource
/// accounting issue (wrong fd, double-close, or kernel fd table
/// corruption) that callers cannot recover from here.
pub fn close_fd(fd: RawFd) {
    // SAFETY: We own this fd and are explicitly closing it.
    let rc = unsafe { libc::close(fd) };
    if rc == -1 {
        let err = io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EINTR) {
            warn!(fd, error = %err, "close(2) failed on duped fd");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use compio::net::{TcpListener, TcpStream};

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn dup_fd_sets_fd_cloexec() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connect = TcpStream::connect(addr);
        let accept = listener.accept();
        let (client_res, accept_res) = futures::join!(connect, accept);
        let (_server, _) = accept_res.unwrap();
        let client = client_res.unwrap();

        let duped = dup_fd(&client).expect("dup_fd failed");

        // SAFETY: F_GETFD is safe on any valid fd; duped was just allocated.
        let flags = unsafe { libc::fcntl(duped, libc::F_GETFD) };
        assert!(flags >= 0, "F_GETFD failed: {}", io::Error::last_os_error());
        assert_ne!(
            flags & libc::FD_CLOEXEC,
            0,
            "duped fd must have FD_CLOEXEC set"
        );

        close_fd(duped);
    }
}
