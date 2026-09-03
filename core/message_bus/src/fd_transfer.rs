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

//! TCP socket transfer between shards.
//!
//! After shard 0 accepts or connects a TCP socket and completes the
//! handshake, it calls [`dup_fd`] to create a second kernel reference
//! to the same socket. The duplicated socket is wrapped in an owning
//! [`DupedFd`] and sent to the target shard via the inter-shard channel.
//! The target shard calls [`wrap_duped_fd`] to construct a compio
//! `TcpStream` on its own runtime.
//!
//! Shard 0 then drops its original `TcpStream`. The socket stays alive
//! because the duplicated handle still references it in the kernel.
//!
//! [`DupedFd`] closes the underlying socket on drop, so a `ShardFrame`
//! discarded mid-flight (shutdown, pump drain abort, router panic
//! before `install_*_fd`) does not leak the duplicate.

use compio::net::TcpStream;
use std::io;
#[cfg(unix)]
use std::os::fd::{AsRawFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, RawSocket};

use socket2::{SockRef, Socket};

/// Owning handle for a duplicated TCP socket.
///
/// Produced by [`dup_fd`] and consumed by [`wrap_duped_fd`] on the
/// target shard. If neither happens (frame dropped unprocessed), the
/// socket is closed on drop so duped-but-unused sockets cannot accumulate.
///
/// The type is deliberately opaque: there is no public constructor
/// from a raw socket. Call sites wishing to transfer ownership out
/// (into a `TcpStream` wrapper) must go through [`wrap_duped_fd`],
/// which consumes `self` by value.
#[derive(Debug)]
pub struct DupedFd(Socket);

impl DupedFd {
    /// Expose the raw fd for logging purposes only. The fd remains
    /// owned by `self` and is still closed on drop; callers must not
    /// pass this value to `close(2)`, `from_raw_fd`, or similar.
    #[cfg(unix)]
    #[must_use]
    pub fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }

    /// Expose the raw socket for logging purposes only. Kept under the
    /// historical `as_raw_fd` name because shard routing logs are fd-oriented.
    #[cfg(windows)]
    #[must_use]
    pub fn as_raw_fd(&self) -> RawSocket {
        self.0.as_raw_socket()
    }

    fn into_socket(self) -> Socket {
        self.0
    }
}

/// Duplicate the underlying socket of a TCP stream.
///
/// Returns an owning [`DupedFd`]. The caller must arrange for it to
/// be consumed by [`wrap_duped_fd`] on the target shard; otherwise
/// the socket is closed when the holder (typically a `ShardFrame`) is
/// discarded.
///
/// Uses `socket2::Socket::try_clone`, which maps to `F_DUPFD_CLOEXEC`
/// on Unix and a non-inheritable duplicated Winsock socket on Windows.
///
/// # Errors
///
/// Returns `io::Error` if duplicating the socket fails.
pub fn dup_fd(stream: &TcpStream) -> io::Result<DupedFd> {
    SockRef::from(stream).try_clone().map(DupedFd)
}

/// Wrap a previously duplicated socket into a compio `TcpStream`.
///
/// Must be called on the target shard's compio runtime so the
/// `TcpStream` is registered with the correct `io_uring` instance.
///
/// Takes `DupedFd` by value, so the type-system guarantees that (a) the
/// socket originated from [`dup_fd`] and (b) ownership is transferred
/// into the returned `TcpStream`, which will close the fd on drop.
///
/// # Errors
///
/// Returns `io::Error` if compio cannot adopt the duplicated socket on
/// the current runtime.
pub fn wrap_duped_fd(fd: DupedFd) -> io::Result<TcpStream> {
    TcpStream::from_std(fd.into_socket().into())
}

#[cfg(unix)]
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

        // SAFETY: F_GETFD is safe on any valid fd; duped still owns it.
        let flags = unsafe { libc::fcntl(duped.as_raw_fd(), libc::F_GETFD) };
        assert!(flags >= 0, "F_GETFD failed: {}", io::Error::last_os_error());
        assert_ne!(
            flags & libc::FD_CLOEXEC,
            0,
            "duped fd must have FD_CLOEXEC set"
        );
        // Drop `duped` closes the fd via Drop impl.
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn duped_fd_drops_close_underlying_fd() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connect = TcpStream::connect(addr);
        let accept = listener.accept();
        let (client_res, accept_res) = futures::join!(connect, accept);
        let (_server, _) = accept_res.unwrap();
        let client = client_res.unwrap();

        let duped = dup_fd(&client).expect("dup_fd failed");
        let raw = duped.as_raw_fd();
        drop(duped);

        // After drop the fd must be gone from this process' fd table.
        // SAFETY: F_GETFD on a closed fd is defined and returns -1/EBADF.
        let flags = unsafe { libc::fcntl(raw, libc::F_GETFD) };
        assert_eq!(flags, -1, "fd must be closed after DupedFd drop");
        assert_eq!(
            io::Error::last_os_error().raw_os_error(),
            Some(libc::EBADF),
            "closed fd must report EBADF"
        );
    }
}
