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
//! to the same socket. The duplicated fd is wrapped in an owning
//! [`DupedFd`] and sent to the target shard via the inter-shard channel.
//! The target shard calls [`wrap_duped_fd`] to construct a compio
//! `TcpStream` on its own runtime.
//!
//! Shard 0 then drops its original `TcpStream`, closing its fd. The
//! socket stays alive because the duplicated fd still references it
//! in the kernel's file table.
//!
//! [`DupedFd`] closes the underlying fd on drop, so a `ShardFrame`
//! discarded mid-flight (shutdown, pump drain abort, router panic
//! before `install_*_fd`) does not leak the dup.

use compio::net::TcpStream;
use std::io;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use tracing::warn;

/// Owning handle for a duplicated TCP socket fd.
///
/// Produced by [`dup_fd`] and consumed by [`wrap_duped_fd`] on the
/// target shard. If neither happens (frame dropped unprocessed), the
/// fd is closed on drop so duped-but-unused sockets cannot accumulate.
///
/// The type is deliberately opaque: there is no public constructor
/// from a raw fd. Call sites wishing to transfer ownership out (into
/// a `TcpStream` wrapper) must go through [`wrap_duped_fd`], which
/// consumes `self` by value.
#[derive(Debug)]
pub struct DupedFd(RawFd);

impl DupedFd {
    /// Expose the raw fd for logging purposes only. The fd remains
    /// owned by `self` and is still closed on drop; callers must not
    /// pass this value to `close(2)`, `from_raw_fd`, or similar.
    #[must_use]
    pub const fn as_raw_fd(&self) -> RawFd {
        self.0
    }

    /// Release ownership of the raw fd. The returned value becomes the
    /// caller's responsibility to close. Used internally by
    /// [`wrap_duped_fd`].
    const fn into_raw(self) -> RawFd {
        let fd = self.0;
        std::mem::forget(self);
        fd
    }
}

impl Drop for DupedFd {
    fn drop(&mut self) {
        if self.0 >= 0 {
            close_fd(self.0);
        }
    }
}

/// Duplicate the underlying file descriptor of a TCP stream with
/// `FD_CLOEXEC` set atomically.
///
/// Returns an owning [`DupedFd`]. The caller must arrange for it to
/// be consumed by [`wrap_duped_fd`] on the target shard; otherwise
/// the `Drop` impl closes the dup when the holder (typically a
/// `ShardFrame`) is discarded.
///
/// Uses `fcntl(F_DUPFD_CLOEXEC)` rather than `dup(2)` so that any
/// future `fork`+`exec` child cannot inherit this socket.
///
/// # Errors
///
/// Returns `io::Error` if the `fcntl(2)` syscall fails.
pub fn dup_fd(stream: &TcpStream) -> io::Result<DupedFd> {
    let original = stream.as_raw_fd();
    // SAFETY: F_DUPFD_CLOEXEC allocates the lowest-numbered free fd >= 0
    // referring to the same open file description as `original`, with
    // FD_CLOEXEC set atomically. Safe to call on any valid fd.
    let duped = unsafe { libc::fcntl(original, libc::F_DUPFD_CLOEXEC, 0) };
    if duped == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(DupedFd(duped))
}

/// Wrap a previously duplicated fd into a compio `TcpStream`.
///
/// Must be called on the target shard's compio runtime so the
/// `TcpStream` is registered with the correct `io_uring` instance.
///
/// Takes `DupedFd` by value, so the type-system guarantees that (a)
/// the fd originated from [`dup_fd`] and (b) ownership is transferred
/// into the returned `TcpStream`, which will close the fd on drop.
#[must_use]
pub fn wrap_duped_fd(fd: DupedFd) -> TcpStream {
    let raw = fd.into_raw();
    // SAFETY: `DupedFd` guarantees `raw` is an open TCP fd whose
    // ownership is being transferred here. No other resource holds it.
    unsafe { TcpStream::from_raw_fd(raw) }
}

/// Close a raw fd. Internal helper used by [`DupedFd::drop`].
///
/// Logs a warning on anything other than `EINTR` since that signals a
/// resource accounting issue (wrong fd, double-close, or kernel fd
/// table corruption) that callers cannot recover from here.
fn close_fd(fd: RawFd) {
    // SAFETY: caller (DupedFd::drop) owns this fd and is closing it.
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
