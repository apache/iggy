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

//! Hand-rolled rustls `UnbufferedConnection` driver for the in-process
//! TCP-TLS and WSS transports.
//!
//! # Why an unbuffered driver
//!
//! Compio is single-threaded per shard. The buffered rustls
//! `ConnectionCommon` API requires `Read + Write` adapters that
//! coordinate read and write directions through internal buffers, and
//! the third-party `compio-tls` crate (the buffered-API adapter) does
//! not expose split read/write halves — it lives behind a monolithic
//! `TlsStream<S>` that cannot be safely shared between two compio
//! tasks.
//!
//! The unbuffered API exposes a state machine
//! ([`UnbufferedStatus`](rustls::unbuffered::UnbufferedStatus) /
//! [`ConnectionState`](rustls::unbuffered::ConnectionState)) that we
//! drive ourselves with caller-owned ciphertext buffers. Per the
//! [rustls maintainer endorsement](https://github.com/rustls/rustls/issues/288#issuecomment-2692731297)
//! a split reader / writer over `Rc<RefCell<UnbufferedConnection>>`
//! is safe so long as the borrow never spans an `.await`. This module
//! enforces that constraint by structure: every rustls call lives
//! inside a synchronous helper that drops its `RefCell` borrow before
//! any async I/O.
//!
//! # Phase 9 staging
//!
//! This file lands in two commits:
//!
//! - **P9-T2a (this commit)**: driver core + [`TlsDriver::drive_handshake`].
//!   The handshake state machine is the most error-prone part of the
//!   plan; landing it in isolation lets it ride a focused review and
//!   carry its own targeted unit tests before any reader / writer
//!   plumbing piles on top.
//! - **P9-T2b (next commit)**: `WriterEvent`, `spawn_tasks`,
//!   reader / writer task bodies, and the orchestrator-side
//!   `shutdown` helper backed by `libc::shutdown(SHUT_RD)` for
//!   reader wake (see
//!   `Documents/silverhand/iggy/message_bus/transport-plan/designs/tls-shutdown-protocol.md`).
//!
//! # Invariant cross-references
//!
//! - **No `RefCell` borrow across `.await`**: enforced by the
//!   `process_step` / `flush_outgoing` helper signatures (no
//!   `&Rc<TlsState>` parameters that imply a held borrow during
//!   suspension).
//! - **Buffer caps** at [`TLS_BUFFER_CAP`] per direction; an attacker
//!   cannot drive unbounded growth via fragmented or oversized
//!   handshake records.
//! - **0-RTT off** (I7): `ConnectionState::ReadEarlyData` is treated
//!   as a protocol violation; the driver returns
//!   [`DriveError::EarlyDataNotPermitted`] on receipt.

use crate::framing::MAX_MESSAGE_SIZE;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::{OwnedReadHalf, OwnedWriteHalf, TcpStream};
use rustls::client::UnbufferedClientConnection;
use rustls::pki_types::ServerName;
use rustls::server::UnbufferedServerConnection;
use rustls::unbuffered::{
    ConnectionState, EncodeError, EncodeTlsData, EncryptError, InsufficientSizeError,
    UnbufferedStatus,
};
use std::cell::{Cell, RefCell};
use std::io;
use std::rc::Rc;
use std::sync::Arc;

/// 16 KiB record-layer headroom on top of [`MAX_MESSAGE_SIZE`].
///
/// TLS 1.3 caps a single record at 16 KiB; the headroom covers one
/// in-flight record on top of an in-progress application frame so the
/// buffer never has to grow mid-decrypt.
pub(super) const TLS_RECORD_HEADROOM: usize = 16 * 1024;

/// Capacity bound for `incoming_tls` and `outgoing_tls` per direction.
///
/// Rejecting a peer that drives growth past this bound is a defensive
/// posture against handshake-layer denial-of-service; legitimate
/// traffic never approaches it (a single TLS record is ≤ 16 KiB, and
/// our application frames are capped at [`MAX_MESSAGE_SIZE`] before
/// encryption).
pub(super) const TLS_BUFFER_CAP: usize = MAX_MESSAGE_SIZE + TLS_RECORD_HEADROOM;

/// Read chunk size for ciphertext I/O during the handshake.
///
/// 8 KiB is large enough to fit most TLS handshake records in one
/// `read` and small enough to keep the per-iteration buffer cheap.
const HANDSHAKE_READ_CHUNK: usize = 8 * 1024;

/// Initial capacity for `outgoing_tls`. Grown on demand; capped at
/// [`TLS_BUFFER_CAP`].
const OUTGOING_TLS_INITIAL: usize = 4 * 1024;

/// Encode-step growth increment when the encoder reports
/// `InsufficientSize` without a `required_size` hint we can use as-is.
/// The next iteration will land on the precise required size if this
/// value is still short.
const ENCODE_GROW_HINT: usize = 2 * 1024;

/// Errors that can arise while driving a rustls `UnbufferedConnection`.
///
/// Variants name the rustls subsystem that produced the error so
/// integration tests can assert on specific failure modes.
#[derive(Debug, thiserror::Error)]
pub(super) enum DriveError {
    /// The peer's TLS implementation rejected our handshake or sent
    /// malformed records. Wraps the original `rustls::Error`.
    #[error("rustls: {0}")]
    Rustls(#[from] rustls::Error),

    /// `EncodeTlsData::encode` reported an unrecoverable error
    /// (already encoded, etc.).
    #[error("rustls encode: {0:?}")]
    Encode(EncodeError),

    /// `WriteTraffic::encrypt` or `WriteTraffic::queue_close_notify`
    /// reported an unrecoverable error (encryptor exhausted, etc.).
    #[allow(dead_code)] // Consumed by P9-T2b reader / writer paths.
    #[error("rustls encrypt: {0:?}")]
    Encrypt(EncryptError),

    /// Outbound buffer would have grown past [`TLS_BUFFER_CAP`].
    /// Treated as a hostile peer or a protocol bug.
    #[error("TLS buffer cap exceeded: required {required}, max {max}")]
    BufferCapExceeded { required: usize, max: usize },

    /// TCP read returned 0 bytes (EOF) before the handshake completed.
    #[error("connection closed during handshake")]
    HandshakeEof,

    /// 0-RTT data appeared on the wire. Policy says off; treat as a
    /// protocol violation (I7).
    #[error("0-RTT early data not permitted (I7)")]
    EarlyDataNotPermitted,

    /// `ConnectionState::Closed` or `PeerClosed` arrived during the
    /// handshake.
    #[error("peer closed connection during handshake")]
    PeerClosedDuringHandshake,

    /// Pre-handshake `ConnectionState::ReadTraffic` (should not
    /// happen — protocol violation).
    #[error("unexpected ReadTraffic during handshake")]
    UnexpectedReadTrafficDuringHandshake,

    /// Rustls reported `discard` larger than the incoming buffer.
    /// Defensive; would indicate a rustls invariant break.
    #[error("rustls reported invalid discard: {discard} > {available}")]
    InvalidDiscard { discard: usize, available: usize },

    /// I/O error from the underlying TCP halves.
    #[error("io: {0}")]
    Io(#[from] io::Error),
}

/// Polymorphism over `UnbufferedServerConnection` and
/// `UnbufferedClientConnection`.
///
/// The two types differ only in the phantom `Data` parameter on
/// their inner `UnbufferedConnectionCommon`. We dispatch on the
/// variant in a single helper ([`process_step`]) so the per-state
/// match logic stays shared.
pub(super) enum UnbufferedConnection {
    Server(UnbufferedServerConnection),
    Client(UnbufferedClientConnection),
}

/// Per-connection state shared between the reader and writer tasks.
///
/// `Rc<TlsState>` is cloned into both tasks; every rustls operation
/// acquires its `RefCell` borrow inside a synchronous helper and
/// drops it before any `.await`. Compile-time enforced by the helper
/// signatures: [`flush_outgoing`] takes `&TlsState`, not a held
/// borrow, and the caller may never hold `outgoing_tls.borrow_mut()`
/// across the await inside it.
pub(super) struct TlsState {
    pub(super) conn: RefCell<UnbufferedConnection>,
    pub(super) incoming_tls: RefCell<Vec<u8>>,
    pub(super) outgoing_tls: RefCell<Vec<u8>>,
    /// Set by the reader once it observes `ConnectionState::PeerClosed`.
    /// Reader exits the next loop iteration; writer drains its own
    /// queue then sends our `close_notify` (P9-T2b).
    #[allow(dead_code)] // Consumed by P9-T2b reader / writer paths.
    pub(super) peer_closed: Cell<bool>,
    /// Set by the orchestrator's shutdown helper before signalling the
    /// halves. Causes the reader to exit on the next iteration even
    /// if no peer EOF arrives (P9-T2b).
    #[allow(dead_code)] // Consumed by P9-T2b reader / writer paths.
    pub(super) shutdown: Cell<bool>,
}

impl TlsState {
    fn new(conn: UnbufferedConnection) -> Self {
        Self {
            conn: RefCell::new(conn),
            incoming_tls: RefCell::new(Vec::with_capacity(HANDSHAKE_READ_CHUNK)),
            outgoing_tls: RefCell::new(Vec::with_capacity(OUTGOING_TLS_INITIAL)),
            peer_closed: Cell::new(false),
            shutdown: Cell::new(false),
        }
    }
}

/// The driver itself: a thin handle around `Rc<TlsState>`.
///
/// `Clone` is `Rc`-cheap; the handshake driver and the future
/// reader / writer tasks all hold their own clone.
#[derive(Clone)]
pub(super) struct TlsDriver {
    pub(super) state: Rc<TlsState>,
}

impl TlsDriver {
    /// Build a server-side driver from a `rustls::ServerConfig`.
    ///
    /// # Errors
    ///
    /// Returns [`DriveError::Rustls`] if rustls rejects the config
    /// (e.g. no resolver, no cipher suites, etc.).
    pub(super) fn new_server(config: Arc<rustls::ServerConfig>) -> Result<Self, DriveError> {
        let conn = UnbufferedServerConnection::new(config)?;
        Ok(Self {
            state: Rc::new(TlsState::new(UnbufferedConnection::Server(conn))),
        })
    }

    /// Build a client-side driver from a `rustls::ClientConfig` and a
    /// validated server name.
    ///
    /// # Errors
    ///
    /// Returns [`DriveError::Rustls`] if rustls rejects the config or
    /// server name.
    pub(super) fn new_client(
        config: Arc<rustls::ClientConfig>,
        server_name: ServerName<'static>,
    ) -> Result<Self, DriveError> {
        let conn = UnbufferedClientConnection::new(config, server_name)?;
        Ok(Self {
            state: Rc::new(TlsState::new(UnbufferedConnection::Client(conn))),
        })
    }

    /// Drive the TLS handshake to completion.
    ///
    /// Alternates between consuming inbound ciphertext
    /// (`BlockedHandshake`), encoding our own handshake records
    /// (`EncodeTlsData`), transmitting them (`TransmitTlsData`), and
    /// finally reaching the steady-state `WriteTraffic`.
    ///
    /// # Errors
    ///
    /// - [`DriveError::HandshakeEof`] if the TCP read returns 0
    ///   bytes before completion.
    /// - [`DriveError::EarlyDataNotPermitted`] on a `ReadEarlyData`
    ///   transition (I7 violation).
    /// - [`DriveError::PeerClosedDuringHandshake`] on `PeerClosed` /
    ///   `Closed`.
    /// - [`DriveError::BufferCapExceeded`] if a malicious peer drives
    ///   buffer growth past [`TLS_BUFFER_CAP`].
    /// - [`DriveError::Rustls`], [`DriveError::Encode`], or
    ///   [`DriveError::Io`] for record-layer / I/O faults.
    #[allow(clippy::future_not_send)] // single-threaded compio runtime
    pub(super) async fn drive_handshake(
        &self,
        read_half: &mut OwnedReadHalf<TcpStream>,
        write_half: &mut OwnedWriteHalf<TcpStream>,
    ) -> Result<(), DriveError> {
        let mut read_chunk: Vec<u8> = vec![0u8; HANDSHAKE_READ_CHUNK];

        loop {
            match self.process_step()? {
                StepOutcome::HandshakeComplete => {
                    // The transition to `WriteTraffic` may leave the
                    // last encoded handshake record (TLS 1.3 client
                    // Finished, server NewSessionTicket, etc.) in the
                    // outgoing buffer. Flush it before returning so
                    // the peer's handshake driver also reaches
                    // completion.
                    flush_outgoing(&self.state, write_half).await?;
                    return Ok(());
                }
                StepOutcome::NeedFlush => {
                    flush_outgoing(&self.state, write_half).await?;
                }
                StepOutcome::NeedReadMore => {
                    let buf = std::mem::take(&mut read_chunk);
                    let compio::buf::BufResult(n_res, buf) = read_half.read(buf).await;
                    read_chunk = buf;
                    let n = n_res.map_err(DriveError::Io)?;
                    if n == 0 {
                        return Err(DriveError::HandshakeEof);
                    }
                    append_incoming(&self.state, &read_chunk[..n])?;
                }
            }
        }
    }

    /// Drive the synchronous portion of one handshake turn.
    ///
    /// Drains every state-machine transition that does not require
    /// new I/O. Returns the next async step the caller must perform
    /// (read more bytes, flush outgoing, or stop because the
    /// handshake is complete).
    ///
    /// All `RefCell` borrows are local to this method; they drop
    /// before return so the caller is free to `.await` on I/O.
    fn process_step(&self) -> Result<StepOutcome, DriveError> {
        let mut conn = self.state.conn.borrow_mut();
        let mut incoming = self.state.incoming_tls.borrow_mut();
        let mut outgoing = self.state.outgoing_tls.borrow_mut();

        let mut needs_flush = false;
        let mut needs_read = false;
        let mut handshake_done = false;

        loop {
            let result = match &mut *conn {
                UnbufferedConnection::Server(s) => {
                    let UnbufferedStatus { discard, state } = s.process_tls_records(&mut incoming);
                    classify(state, discard, &mut outgoing, &mut needs_flush)?
                }
                UnbufferedConnection::Client(c) => {
                    let UnbufferedStatus { discard, state } = c.process_tls_records(&mut incoming);
                    classify(state, discard, &mut outgoing, &mut needs_flush)?
                }
            };

            // Apply discard after the per-side dispatch returned.
            if result.discard > 0 {
                if result.discard > incoming.len() {
                    return Err(DriveError::InvalidDiscard {
                        discard: result.discard,
                        available: incoming.len(),
                    });
                }
                incoming.drain(..result.discard);
            }

            match result.transition {
                Transition::HandshakeDone => {
                    handshake_done = true;
                    break;
                }
                Transition::Progressed => {
                    // Loop again synchronously to drain more state.
                }
                Transition::Stalled => {
                    needs_read = true;
                    break;
                }
            }
        }

        drop(outgoing);
        drop(incoming);
        drop(conn);

        if handshake_done {
            Ok(StepOutcome::HandshakeComplete)
        } else if needs_flush {
            Ok(StepOutcome::NeedFlush)
        } else if needs_read {
            Ok(StepOutcome::NeedReadMore)
        } else {
            // No flush, no read needed, no handshake complete: rustls
            // wants us to come back. Defer to a read so we don't spin.
            Ok(StepOutcome::NeedReadMore)
        }
    }
}

/// One step outcome of the synchronous handshake state machine pass.
enum StepOutcome {
    HandshakeComplete,
    NeedFlush,
    NeedReadMore,
}

/// Inner-loop transition flag from a single `process_tls_records` call.
enum Transition {
    HandshakeDone,
    Progressed,
    Stalled,
}

/// Result returned from [`classify`] for one `process_tls_records`
/// call.
struct ClassifyResult {
    discard: usize,
    transition: Transition,
}

/// Generic over `Data`: handle one [`ConnectionState`] returned from
/// [`UnbufferedConnectionCommon::process_tls_records`].
///
/// Encodes outbound handshake records into `outgoing` and reports
/// whether the state machine made progress, stalled (need more
/// inbound), or completed (`WriteTraffic`).
fn classify<Data>(
    state: Result<ConnectionState<'_, '_, Data>, rustls::Error>,
    discard: usize,
    outgoing: &mut Vec<u8>,
    needs_flush: &mut bool,
) -> Result<ClassifyResult, DriveError> {
    let cs = state?;
    let transition = match cs {
        ConnectionState::EncodeTlsData(mut e) => {
            encode_into_buf(&mut e, outgoing)?;
            *needs_flush = true;
            Transition::Progressed
        }
        ConnectionState::TransmitTlsData(t) => {
            *needs_flush = true;
            t.done();
            Transition::Progressed
        }
        ConnectionState::BlockedHandshake => Transition::Stalled,
        ConnectionState::WriteTraffic(_) => Transition::HandshakeDone,
        ConnectionState::ReadEarlyData(_) => return Err(DriveError::EarlyDataNotPermitted),
        ConnectionState::PeerClosed | ConnectionState::Closed => {
            return Err(DriveError::PeerClosedDuringHandshake);
        }
        ConnectionState::ReadTraffic(_) => {
            return Err(DriveError::UnexpectedReadTrafficDuringHandshake);
        }
        // The `ConnectionState` enum is `non_exhaustive` upstream;
        // future variants must surface as a protocol violation rather
        // than silently fall through.
        _ => return Err(DriveError::PeerClosedDuringHandshake),
    };
    Ok(ClassifyResult {
        discard,
        transition,
    })
}

/// Encode one handshake record into `outgoing`, growing the buffer
/// up to [`TLS_BUFFER_CAP`] on `InsufficientSize`.
fn encode_into_buf<Data>(
    encoder: &mut EncodeTlsData<'_, Data>,
    outgoing: &mut Vec<u8>,
) -> Result<(), DriveError> {
    let start = outgoing.len();
    let mut grow_to = start.saturating_add(ENCODE_GROW_HINT).min(TLS_BUFFER_CAP);
    if grow_to <= start {
        return Err(DriveError::BufferCapExceeded {
            required: start.saturating_add(1),
            max: TLS_BUFFER_CAP,
        });
    }
    outgoing.resize(grow_to, 0);

    loop {
        match encoder.encode(&mut outgoing[start..]) {
            Ok(written) => {
                outgoing.truncate(start + written);
                return Ok(());
            }
            Err(EncodeError::InsufficientSize(InsufficientSizeError { required_size })) => {
                let needed = start.saturating_add(required_size);
                if needed > TLS_BUFFER_CAP {
                    outgoing.truncate(start);
                    return Err(DriveError::BufferCapExceeded {
                        required: needed,
                        max: TLS_BUFFER_CAP,
                    });
                }
                grow_to = needed;
                outgoing.resize(grow_to, 0);
            }
            Err(other) => {
                outgoing.truncate(start);
                return Err(DriveError::Encode(other));
            }
        }
    }
}

/// Append peer-supplied bytes into `incoming_tls`, rejecting any
/// growth past [`TLS_BUFFER_CAP`].
fn append_incoming(state: &TlsState, bytes: &[u8]) -> Result<(), DriveError> {
    let mut incoming = state.incoming_tls.borrow_mut();
    let new_len = incoming.len().saturating_add(bytes.len());
    if new_len > TLS_BUFFER_CAP {
        return Err(DriveError::BufferCapExceeded {
            required: new_len,
            max: TLS_BUFFER_CAP,
        });
    }
    incoming.extend_from_slice(bytes);
    Ok(())
}

/// Drain `outgoing_tls` onto `write_half`. Synchronous take-into-Vec
/// brackets a single `.await` on the wire write, then the empty Vec
/// is returned to the state for re-use.
#[allow(clippy::future_not_send)] // single-threaded compio runtime
async fn flush_outgoing(
    state: &TlsState,
    write_half: &mut OwnedWriteHalf<TcpStream>,
) -> Result<(), DriveError> {
    let bytes = {
        let mut outgoing = state.outgoing_tls.borrow_mut();
        if outgoing.is_empty() {
            return Ok(());
        }
        std::mem::take(&mut *outgoing)
    };

    let compio::buf::BufResult(write_res, mut bytes) = write_half.write_all(bytes).await;
    write_res.map_err(DriveError::Io)?;

    bytes.clear();
    let mut outgoing = state.outgoing_tls.borrow_mut();
    if outgoing.is_empty() {
        // Common case in the single-threaded handshake path: nobody
        // pushed new bytes while we were awaiting. Reuse the
        // allocation.
        *outgoing = bytes;
    }
    // Otherwise the state already has fresh outgoing bytes; drop the
    // empty Vec (cannot happen during single-task handshake; defensive
    // for the steady-state reader / writer split landing in P9-T2b).
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transports::tls::{install_default_crypto_provider, self_signed_for_loopback};
    use compio::net::{TcpListener, TcpStream as CompioTcpStream};
    use rustls::RootCertStore;
    use std::net::SocketAddr;
    use std::sync::OnceLock;

    fn ensure_provider() {
        static GUARD: OnceLock<()> = OnceLock::new();
        GUARD.get_or_init(install_default_crypto_provider);
    }

    fn server_config() -> (
        Arc<rustls::ServerConfig>,
        Vec<rustls::pki_types::CertificateDer<'static>>,
    ) {
        ensure_provider();
        let creds = self_signed_for_loopback();
        let cert_chain = creds.cert_chain.clone();
        let mut cfg = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain.clone(), creds.key_der)
            .expect("server config");
        cfg.max_early_data_size = 0;
        (Arc::new(cfg), cert_chain)
    }

    fn client_config_trusting(
        server_cert_chain: &[rustls::pki_types::CertificateDer<'static>],
    ) -> Arc<rustls::ClientConfig> {
        ensure_provider();
        let mut roots = RootCertStore::empty();
        for cert in server_cert_chain {
            roots.add(cert.clone()).expect("add cert");
        }
        let cfg = rustls::ClientConfig::builder()
            .with_root_certificates(Arc::new(roots))
            .with_no_client_auth();
        Arc::new(cfg)
    }

    #[allow(clippy::future_not_send)]
    async fn connected_pair() -> (CompioTcpStream, CompioTcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let server_addr: SocketAddr = listener.local_addr().expect("local_addr");
        let client_fut = CompioTcpStream::connect(server_addr);
        let accept_fut = listener.accept();
        let (client_res, accept_res) = futures::join!(client_fut, accept_fut);
        let client = client_res.expect("connect");
        let (server, _peer) = accept_res.expect("accept");
        (client, server)
    }

    #[compio::test]
    async fn handshake_round_trip_via_tcp_pair() {
        let (server_cfg, cert_chain) = server_config();
        let client_cfg = client_config_trusting(&cert_chain);

        let (client_stream, server_stream) = connected_pair().await;
        let (mut client_read, mut client_write) = client_stream.into_split();
        let (mut server_read, mut server_write) = server_stream.into_split();

        let server_driver = TlsDriver::new_server(server_cfg).expect("server driver");
        let server_name: ServerName<'static> = "localhost".try_into().expect("server name");
        let client_driver = TlsDriver::new_client(client_cfg, server_name).expect("client driver");

        let server_fut = async move {
            server_driver
                .drive_handshake(&mut server_read, &mut server_write)
                .await
        };
        let client_fut = async move {
            client_driver
                .drive_handshake(&mut client_read, &mut client_write)
                .await
        };

        let (server_res, client_res) = futures::join!(server_fut, client_fut);
        server_res.expect("server handshake");
        client_res.expect("client handshake");
    }

    #[compio::test]
    async fn handshake_with_wrong_cert_fails_cleanly() {
        let (server_cfg, _cert_chain) = server_config();
        // Client trusts an unrelated CA; handshake must fail without
        // panic and with a rustls error.
        let stranger_creds = self_signed_for_loopback();
        let client_cfg = client_config_trusting(&stranger_creds.cert_chain);

        let (client_stream, server_stream) = connected_pair().await;
        let (mut client_read, mut client_write) = client_stream.into_split();
        let (mut server_read, mut server_write) = server_stream.into_split();

        let server_driver = TlsDriver::new_server(server_cfg).expect("server driver");
        let server_name: ServerName<'static> = "localhost".try_into().expect("server name");
        let client_driver = TlsDriver::new_client(client_cfg, server_name).expect("client driver");

        let server_fut = async move {
            server_driver
                .drive_handshake(&mut server_read, &mut server_write)
                .await
        };
        let client_fut = async move {
            client_driver
                .drive_handshake(&mut client_read, &mut client_write)
                .await
        };

        let (server_res, client_res) = futures::join!(server_fut, client_fut);
        // Client triggers the failure (cert verify); server observes
        // EOF or alert and surfaces an error too.
        assert!(
            client_res.is_err(),
            "client handshake must fail with stranger cert"
        );
        assert!(
            server_res.is_err(),
            "server handshake must fail when client aborts"
        );
    }

    #[compio::test]
    async fn handshake_with_wrong_server_name_fails_cleanly() {
        let (server_cfg, cert_chain) = server_config();
        let client_cfg = client_config_trusting(&cert_chain);

        let (client_stream, server_stream) = connected_pair().await;
        let (mut client_read, mut client_write) = client_stream.into_split();
        let (mut server_read, mut server_write) = server_stream.into_split();

        let server_driver = TlsDriver::new_server(server_cfg).expect("server driver");
        // Cert is for "localhost"; client attempts SNI "evil.example".
        let bad_name: ServerName<'static> = "evil.example".try_into().expect("server name");
        let client_driver = TlsDriver::new_client(client_cfg, bad_name).expect("client driver");

        let server_fut = async move {
            server_driver
                .drive_handshake(&mut server_read, &mut server_write)
                .await
        };
        let client_fut = async move {
            client_driver
                .drive_handshake(&mut client_read, &mut client_write)
                .await
        };

        let (server_res, client_res) = futures::join!(server_fut, client_fut);
        assert!(
            client_res.is_err(),
            "client must reject cert for unrelated SNI"
        );
        assert!(server_res.is_err(), "server must surface the abort");
    }
}
