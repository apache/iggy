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
//! - **P9-T2a**: driver core + [`TlsDriver::drive_handshake`].
//! - **P9-T2b** (this commit): [`WriterEvent`], [`reader_task`],
//!   [`writer_task`], [`TlsDriver::spawn_tasks`], [`TlsConnHandles`],
//!   and [`shutdown`]. The cooperative shutdown sequence (libc
//!   `SHUT_RD` reader wake -> drop writer channel -> drain budget ->
//!   reap reader) is the design from
//!   `Documents/silverhand/iggy/message_bus/transport-plan/designs/tls-shutdown-protocol.md`.
//!
//! # Invariant cross-references
//!
//! - **No `RefCell` borrow across `.await`**: enforced by structure.
//!   Every helper that touches `state.conn` /
//!   `state.incoming_tls` / `state.outgoing_tls` is synchronous and
//!   drops its borrows before returning. The async
//!   [`reader_task`] and [`writer_task`] only `.await` on the wire
//!   (`read` / `write_all` / `shutdown`) and on cancel-safe channel
//!   futures (`Sender::send` / `Receiver::recv`).
//! - **No `select!` in [`writer_task`]**: locked in by the shutdown
//!   protocol design. The writer's only `.await` per iteration is
//!   `rx.recv()`, and channel close is the cooperative shutdown
//!   signal.
//! - **Buffer caps** at [`TLS_BUFFER_CAP`] per direction; an attacker
//!   cannot drive unbounded growth via fragmented or oversized
//!   handshake records.
//! - **0-RTT off** (I7): `ConnectionState::ReadEarlyData` is treated
//!   as a protocol violation; the driver returns
//!   [`DriveError::EarlyDataNotPermitted`] on receipt.

use crate::framing::MAX_MESSAGE_SIZE;
use async_channel::{Receiver, Sender};
use compio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use compio::net::{OwnedReadHalf, OwnedWriteHalf, TcpStream};
use compio::runtime::JoinHandle;
use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
use iggy_binary_protocol::consensus::iobuf::{Frozen, Owned};
use iggy_binary_protocol::{GenericHeader, HEADER_SIZE, Message};
use rustls::client::UnbufferedClientConnection;
use rustls::pki_types::ServerName;
use rustls::server::UnbufferedServerConnection;
use rustls::unbuffered::{
    ConnectionState, EncodeError, EncodeTlsData, EncryptError, InsufficientSizeError,
    UnbufferedStatus, WriteTraffic,
};
use std::cell::{Cell, RefCell};
use std::io;
use std::os::fd::RawFd;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

/// `JoinHandle` returned for both reader and writer tasks. Aliased so
/// the `spawn_tasks` and `TlsConnHandles` signatures stay readable.
pub(in crate::transports) type TaskHandle = JoinHandle<Result<(), DriveError>>;

/// Maximum TLS-record overhead per direction.
///
/// At [`MAX_MESSAGE_SIZE`] (64 MiB) of plaintext, rustls fragments into
/// `MAX_MESSAGE_SIZE / 16 KiB = 4096` TLS 1.3 records, each carrying a
/// 5 B record header + 1 B content type + 16 B AEAD tag = 22 B of
/// overhead, totalling ~88 KiB. 128 KiB rounds up with margin for any
/// concurrent handshake-layer record (`KeyUpdate` request / response,
/// alert) buffered alongside an in-flight application frame.
pub(in crate::transports) const TLS_RECORD_HEADROOM: usize = 128 * 1024;

/// Capacity bound for `incoming_tls` and `outgoing_tls` per direction.
///
/// Rejecting a peer that drives growth past this bound is a defensive
/// posture against handshake-layer denial-of-service; legitimate
/// traffic never approaches it. The outgoing half also has to fit the
/// encrypted form of one in-flight [`MAX_MESSAGE_SIZE`] application
/// frame plus the per-record overhead captured by
/// [`TLS_RECORD_HEADROOM`].
pub(in crate::transports) const TLS_BUFFER_CAP: usize = MAX_MESSAGE_SIZE + TLS_RECORD_HEADROOM;

/// Read chunk size for ciphertext I/O during the handshake.
///
/// 8 KiB is large enough to fit most TLS handshake records in one
/// `read` and small enough to keep the per-iteration buffer cheap.
const HANDSHAKE_READ_CHUNK: usize = 8 * 1024;

/// Read chunk size for steady-state ciphertext I/O on the reader task.
///
/// Matches the TLS 1.3 single-record plaintext cap so each socket read
/// nominally lands at one record boundary; rustls fragments larger
/// application frames itself so the reader still drains fragments
/// across multiple iterations.
const STEADY_READ_CHUNK: usize = 16 * 1024;

/// TLS 1.3 single-record plaintext cap. `WriteTraffic::encrypt`
/// fragments inputs larger than this internally.
const TLS_RECORD_PLAINTEXT_CAP: usize = 16 * 1024;

/// Per-record encryption overhead estimate used when sizing
/// `outgoing_tls` for one `WriteTraffic::encrypt` call. 32 B is a
/// safety-margin round-up over the actual 22 B (5 record header + 1
/// content type + 16 AEAD tag) for TLS 1.3 records.
const ENCRYPT_OVERHEAD_PER_RECORD: usize = 32;

/// Initial capacity for `outgoing_tls`. Grown on demand; capped at
/// [`TLS_BUFFER_CAP`].
const OUTGOING_TLS_INITIAL: usize = 4 * 1024;

/// Initial reservation for the `close_notify` alert record. The alert
/// itself is at most ~30 B encrypted; the reservation never grows in
/// practice but the loop tolerates `InsufficientSize` regardless.
const CLOSE_NOTIFY_INITIAL: usize = 64;

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
pub(in crate::transports) enum DriveError {
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

    /// Writer attempted to encrypt application data while the rustls
    /// state machine was outside `WriteTraffic` after exhausting
    /// pending transitions (e.g. peer-initiated close). Surfaces a
    /// drop-the-message-and-bail decision to the caller.
    #[error("rustls connection not in WriteTraffic state")]
    WriterNotInWriteTraffic,

    /// Decrypted plaintext violated the consensus framing contract
    /// (256 B header expected, `header.size` outside
    /// `HEADER_SIZE..=MAX_MESSAGE_SIZE`, or `Message::try_from`
    /// rejected the buffer).
    #[error("invalid consensus frame in decrypted plaintext")]
    InvalidFrame,

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
pub(in crate::transports) enum UnbufferedConnection {
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
pub(in crate::transports) struct TlsState {
    pub(in crate::transports) conn: RefCell<UnbufferedConnection>,
    pub(in crate::transports) incoming_tls: RefCell<Vec<u8>>,
    pub(in crate::transports) outgoing_tls: RefCell<Vec<u8>>,
    /// Set by the reader once it observes `ConnectionState::PeerClosed`
    /// or `ConnectionState::Closed`. Reader exits the next loop
    /// iteration; writer drains its own queue then sends our
    /// `close_notify` (per shutdown-protocol design doc).
    pub(in crate::transports) peer_closed: Cell<bool>,
    /// Set by the orchestrator's shutdown helper before signalling the
    /// halves. Causes the reader to exit on the next iteration even
    /// if no peer EOF arrives.
    pub(in crate::transports) shutdown: Cell<bool>,
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
pub(in crate::transports) struct TlsDriver {
    pub(in crate::transports) state: Rc<TlsState>,
}

impl TlsDriver {
    /// Build a server-side driver from a `rustls::ServerConfig`.
    ///
    /// # Errors
    ///
    /// Returns [`DriveError::Rustls`] if rustls rejects the config
    /// (e.g. no resolver, no cipher suites, etc.).
    pub(in crate::transports) fn new_server(
        config: Arc<rustls::ServerConfig>,
    ) -> Result<Self, DriveError> {
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
    pub(in crate::transports) fn new_client(
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
    pub(in crate::transports) async fn drive_handshake(
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

    /// Spawn the steady-state reader and writer tasks.
    ///
    /// Caller wires the channels: `in_tx` is the inbound consensus
    /// frame queue (reader pushes; orchestrator drains via the paired
    /// receiver). `rx` is the merged-channel writer event queue
    /// (orchestrator pushes outbound [`WriterEvent::Out`] via its own
    /// [`Sender`] clone; reader pushes [`WriterEvent::FlushTls`] via
    /// `reader_writer_tx`). The reader's clone of the `Sender` is
    /// dropped on reader exit, halving cooperative-shutdown wakeup
    /// latency once the orchestrator also drops its clone.
    ///
    /// Returns the two `JoinHandle`s so the orchestrator can install
    /// them on a [`TlsConnHandles`] alongside the captured `RawFd` and
    /// the `Sender` clone it retained.
    pub(in crate::transports) fn spawn_tasks(
        self,
        read_half: OwnedReadHalf<TcpStream>,
        write_half: OwnedWriteHalf<TcpStream>,
        in_tx: Sender<Message<GenericHeader>>,
        rx: Receiver<WriterEvent>,
        reader_writer_tx: Sender<WriterEvent>,
    ) -> (TaskHandle, TaskHandle) {
        let reader_state = Rc::clone(&self.state);
        let writer_state = self.state;
        let reader_handle = compio::runtime::spawn(reader_task(
            read_half,
            in_tx,
            reader_state,
            reader_writer_tx,
        ));
        let writer_handle = compio::runtime::spawn(writer_task(write_half, rx, writer_state));
        (reader_handle, writer_handle)
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
        // Nobody pushed new bytes while we were awaiting; reuse the
        // allocation for the next flush.
        *outgoing = bytes;
    }
    // Otherwise the state already has fresh outgoing bytes (reader
    // encoded a handshake response while we were mid-write). Drop the
    // empty Vec; the next flush call will pick those bytes up.
    Ok(())
}

// =====================================================================
// P9-T2b: steady-state reader / writer + spawn / shutdown plumbing
// =====================================================================

/// Event consumed by [`writer_task`]. The merged channel pattern keeps
/// the writer body free of `select!`: closing the channel (orchestrator
/// drops its [`Sender`] and reader drops its clone on exit) is the sole
/// cooperative-shutdown signal.
pub(in crate::transports) enum WriterEvent {
    /// Outbound application-layer message to encrypt and send.
    Out(Frozen<MESSAGE_ALIGN>),
    /// Reader signalled `outgoing_tls` has bytes to flush — handshake
    /// response, alert, or `KeyUpdate` response. Sent via `try_send`
    /// from the reader so reader progress is never blocked on writer
    /// queue pressure.
    FlushTls,
}

/// Reader-side task. Reads ciphertext from `read_half`, drains the
/// rustls state machine, dispatches decrypted plaintext records into a
/// streaming consensus framer, and signals the writer when handshake
/// bytes need flushing (`KeyUpdate` response, alert).
///
/// Exit conditions:
/// - `read` returns `Ok(0)` (peer FIN or `libc::shutdown(SHUT_RD)`).
/// - `state.shutdown.get()` becomes `true`.
/// - rustls reports `PeerClosed` / `Closed`; sets
///   `state.peer_closed.set(true)`.
/// - `in_tx` send fails (orchestrator dropped the inbound receiver —
///   client gone).
///
/// On exit, the local `writer_tx` clone drops, halving the writer's
/// cooperative-shutdown wakeup latency once the orchestrator also
/// drops its clone.
#[allow(clippy::future_not_send)] // single-threaded compio runtime
pub(in crate::transports) async fn reader_task(
    mut read_half: OwnedReadHalf<TcpStream>,
    in_tx: Sender<Message<GenericHeader>>,
    state: Rc<TlsState>,
    writer_tx: Sender<WriterEvent>,
) -> Result<(), DriveError> {
    let mut buf: Vec<u8> = vec![0u8; STEADY_READ_CHUNK];
    let mut plaintext: Vec<u8> = Vec::new();

    loop {
        if state.shutdown.get() {
            return Ok(());
        }

        let owned = std::mem::take(&mut buf);
        let compio::buf::BufResult(n_res, owned) = read_half.read(owned).await;
        buf = owned;
        let n = n_res.map_err(DriveError::Io)?;
        if n == 0 {
            return Ok(());
        }

        append_incoming(&state, &buf[..n])?;

        let outcome = drain_inbound(&state, &mut plaintext)?;
        if outcome.flush_needed {
            // Best effort: writer's queue full means it will see the
            // bytes the moment it processes the next event. Channel
            // closed means writer is on its way out.
            let _ = writer_tx.try_send(WriterEvent::FlushTls);
        }

        while let Some(msg) = try_take_frame(&mut plaintext)? {
            if in_tx.send(msg).await.is_err() {
                // Inbound consumer gone; signal upstream `client gone`
                // to writer / orchestrator by exiting.
                return Ok(());
            }
        }

        if outcome.peer_closed {
            state.peer_closed.set(true);
            return Ok(());
        }
    }
}

/// Writer-side task. Single-await receive loop on the merged
/// [`WriterEvent`] channel; channel close is the cooperative shutdown
/// signal.
///
/// Steady state:
/// - `Out(frozen)`: encrypt the application bytes via
///   `WriteTraffic::encrypt` into `outgoing_tls`, then flush.
/// - `FlushTls`: flush any handshake / alert bytes the reader encoded.
///
/// Cooperative shutdown (channel closed):
/// 1. Synchronously queue a TLS `close_notify` alert (best-effort —
///    skipped if the conn is already in a non-`WriteTraffic` state).
/// 2. Flush remaining `outgoing_tls` bytes (best-effort).
/// 3. Half-close the TCP write side via `OwnedWriteHalf::shutdown`
///    (best-effort).
/// 4. Return.
///
/// No `select!`. Each step is sequential; `RefCell` borrows are
/// confined to synchronous helpers.
#[allow(clippy::future_not_send)] // single-threaded compio runtime
pub(in crate::transports) async fn writer_task(
    mut write_half: OwnedWriteHalf<TcpStream>,
    rx: Receiver<WriterEvent>,
    state: Rc<TlsState>,
) -> Result<(), DriveError> {
    while let Ok(event) = rx.recv().await {
        match event {
            WriterEvent::Out(frozen) => {
                encrypt_app_data(&state, frozen.as_slice())?;
                flush_outgoing(&state, &mut write_half).await?;
            }
            WriterEvent::FlushTls => {
                flush_outgoing(&state, &mut write_half).await?;
            }
        }
    }

    // Cooperative shutdown: best-effort close_notify + half-close.
    // The shutdown-protocol design doc treats failures here as benign;
    // log and move on.
    if let Err(e) = queue_close_notify(&state) {
        warn!(error = ?e, "TLS writer: queue_close_notify failed during shutdown");
    }
    if let Err(e) = flush_outgoing(&state, &mut write_half).await {
        warn!(error = ?e, "TLS writer: flush_outgoing failed during shutdown");
    }
    if let Err(e) = write_half.shutdown().await {
        warn!(error = ?e, "TLS writer: TCP SHUT_WR failed during shutdown");
    }
    Ok(())
}

/// Per-iteration outcome from [`drain_inbound`].
struct DrainOutcome {
    /// True if rustls produced bytes that need to leave the wire (a
    /// handshake response, alert, or `KeyUpdate` response). The reader
    /// signals the writer rather than touching `write_half` itself.
    flush_needed: bool,
    /// True if rustls reached `PeerClosed` / `Closed`.
    peer_closed: bool,
}

/// Drain post-handshake state-machine progress: copy plaintext from
/// `ReadTraffic` records into `plaintext`, encode any
/// handshake-response records into `outgoing_tls`, and report whether
/// the writer needs to flush.
///
/// Synchronous. Holds `RefCell` borrows on `state.conn`,
/// `state.incoming_tls`, and `state.outgoing_tls`; drops them all
/// before returning.
fn drain_inbound(state: &TlsState, plaintext: &mut Vec<u8>) -> Result<DrainOutcome, DriveError> {
    let mut conn = state.conn.borrow_mut();
    let mut incoming = state.incoming_tls.borrow_mut();
    let mut outgoing = state.outgoing_tls.borrow_mut();
    let mut flush_needed = false;
    let mut peer_closed = false;

    loop {
        let result = match &mut *conn {
            UnbufferedConnection::Server(s) => {
                let UnbufferedStatus { discard, state: cs } = s.process_tls_records(&mut incoming);
                drain_step(cs, discard, plaintext, &mut outgoing, &mut flush_needed)?
            }
            UnbufferedConnection::Client(c) => {
                let UnbufferedStatus { discard, state: cs } = c.process_tls_records(&mut incoming);
                drain_step(cs, discard, plaintext, &mut outgoing, &mut flush_needed)?
            }
        };

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
            DrainTransition::Stalled => break,
            DrainTransition::Progressed => {}
            DrainTransition::PeerClosed => {
                peer_closed = true;
                break;
            }
        }
    }

    drop(outgoing);
    drop(incoming);
    drop(conn);

    Ok(DrainOutcome {
        flush_needed,
        peer_closed,
    })
}

/// Inner-loop transition flag for [`drain_inbound`].
enum DrainTransition {
    Progressed,
    Stalled,
    PeerClosed,
}

/// Result returned from [`drain_step`] for one `process_tls_records`
/// call in the steady-state path.
struct DrainResult {
    discard: usize,
    transition: DrainTransition,
}

/// Generic over `Data`: classify a [`ConnectionState`] in the
/// post-handshake / steady-state path. `ReadTraffic` drains decrypted
/// records into `plaintext`; `EncodeTlsData` /  `TransmitTlsData`
/// drive the handshake-response sub-state-machine (`KeyUpdate` response,
/// alert).
fn drain_step<Data>(
    state: Result<ConnectionState<'_, '_, Data>, rustls::Error>,
    discard: usize,
    plaintext: &mut Vec<u8>,
    outgoing: &mut Vec<u8>,
    flush_needed: &mut bool,
) -> Result<DrainResult, DriveError> {
    let cs = state?;
    let transition = match cs {
        ConnectionState::ReadTraffic(mut rt) => {
            while let Some(record) = rt.next_record() {
                let rec = record?;
                plaintext.extend_from_slice(rec.payload);
            }
            DrainTransition::Progressed
        }
        ConnectionState::EncodeTlsData(mut e) => {
            encode_into_buf(&mut e, outgoing)?;
            *flush_needed = true;
            DrainTransition::Progressed
        }
        ConnectionState::TransmitTlsData(t) => {
            *flush_needed = true;
            t.done();
            DrainTransition::Progressed
        }
        ConnectionState::BlockedHandshake | ConnectionState::WriteTraffic(_) => {
            DrainTransition::Stalled
        }
        ConnectionState::ReadEarlyData(_) => return Err(DriveError::EarlyDataNotPermitted),
        // `ConnectionState` is `non_exhaustive`. `PeerClosed` /
        // `Closed` plus any future close-shaped variant collapse to a
        // peer-initiated close so the reader exits cleanly rather
        // than busy-looping.
        _ => DrainTransition::PeerClosed,
    };
    Ok(DrainResult {
        discard,
        transition,
    })
}

/// Encrypt one application-data buffer into `outgoing_tls`. Drains
/// any pending handshake transitions (e.g. a pending `KeyUpdate`
/// `EncodeTlsData` injected by `WriteTraffic::refresh_traffic_keys`)
/// before reaching `WriteTraffic` and calling `encrypt`.
///
/// rustls fragments inputs larger than the TLS 1.3 single-record
/// plaintext cap (16 KiB) internally, so a `MAX_MESSAGE_SIZE` payload
/// produces ~4096 records in one call. The output is bounded by
/// [`TLS_BUFFER_CAP`].
fn encrypt_app_data(state: &TlsState, app_data: &[u8]) -> Result<(), DriveError> {
    let mut conn = state.conn.borrow_mut();
    let mut outgoing = state.outgoing_tls.borrow_mut();

    loop {
        let done = match &mut *conn {
            UnbufferedConnection::Server(s) => {
                let UnbufferedStatus {
                    discard: _,
                    state: cs,
                } = s.process_tls_records(&mut []);
                step_to_encrypt(cs, app_data, &mut outgoing)?
            }
            UnbufferedConnection::Client(c) => {
                let UnbufferedStatus {
                    discard: _,
                    state: cs,
                } = c.process_tls_records(&mut []);
                step_to_encrypt(cs, app_data, &mut outgoing)?
            }
        };
        if done {
            return Ok(());
        }
    }
}

/// Generic over `Data`: handle one transition while driving toward
/// `WriteTraffic` for an application-data encrypt. Returns `true` once
/// the encrypt has actually run.
fn step_to_encrypt<Data>(
    cs: Result<ConnectionState<'_, '_, Data>, rustls::Error>,
    app_data: &[u8],
    outgoing: &mut Vec<u8>,
) -> Result<bool, DriveError> {
    let cs = cs?;
    match cs {
        ConnectionState::WriteTraffic(mut wt) => {
            do_encrypt_into_buf(&mut wt, app_data, outgoing)?;
            Ok(true)
        }
        ConnectionState::EncodeTlsData(mut e) => {
            encode_into_buf(&mut e, outgoing)?;
            Ok(false)
        }
        ConnectionState::TransmitTlsData(t) => {
            t.done();
            Ok(false)
        }
        ConnectionState::ReadEarlyData(_) => Err(DriveError::EarlyDataNotPermitted),
        // `ConnectionState` is `non_exhaustive`. `PeerClosed`,
        // `Closed`, `ReadTraffic`, `BlockedHandshake`, and any future
        // variant collapse to "writer can't encrypt right now"; the
        // writer surfaces this as an error and exits.
        _ => Err(DriveError::WriterNotInWriteTraffic),
    }
}

/// Encrypt `app_data` into `outgoing[start..]`, growing the buffer up
/// to [`TLS_BUFFER_CAP`] on `InsufficientSize`.
fn do_encrypt_into_buf<Data>(
    wt: &mut WriteTraffic<'_, Data>,
    app_data: &[u8],
    outgoing: &mut Vec<u8>,
) -> Result<(), DriveError> {
    let start = outgoing.len();
    let records = app_data.len().div_ceil(TLS_RECORD_PLAINTEXT_CAP).max(1);
    let initial_estimate = app_data
        .len()
        .saturating_add(records.saturating_mul(ENCRYPT_OVERHEAD_PER_RECORD));
    let mut grow_to = start.saturating_add(initial_estimate).min(TLS_BUFFER_CAP);
    if grow_to <= start {
        return Err(DriveError::BufferCapExceeded {
            required: start.saturating_add(1),
            max: TLS_BUFFER_CAP,
        });
    }
    outgoing.resize(grow_to, 0);

    loop {
        match wt.encrypt(app_data, &mut outgoing[start..]) {
            Ok(written) => {
                outgoing.truncate(start + written);
                return Ok(());
            }
            Err(EncryptError::InsufficientSize(InsufficientSizeError { required_size })) => {
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
                return Err(DriveError::Encrypt(other));
            }
        }
    }
}

/// Synchronously queue a TLS `close_notify` alert into `outgoing_tls`.
///
/// Best-effort: if the connection is no longer in `WriteTraffic` (peer
/// already closed, encryptor exhausted, etc.) the call is a noop per
/// the shutdown-protocol design.
fn queue_close_notify(state: &TlsState) -> Result<(), DriveError> {
    let mut conn = state.conn.borrow_mut();
    let mut outgoing = state.outgoing_tls.borrow_mut();

    match &mut *conn {
        UnbufferedConnection::Server(s) => {
            let UnbufferedStatus {
                discard: _,
                state: cs,
            } = s.process_tls_records(&mut []);
            close_notify_with_state(cs, &mut outgoing)
        }
        UnbufferedConnection::Client(c) => {
            let UnbufferedStatus {
                discard: _,
                state: cs,
            } = c.process_tls_records(&mut []);
            close_notify_with_state(cs, &mut outgoing)
        }
    }
}

/// Generic over `Data`: queue `close_notify` if the rustls state is
/// `WriteTraffic`; ignore otherwise.
fn close_notify_with_state<Data>(
    cs: Result<ConnectionState<'_, '_, Data>, rustls::Error>,
    outgoing: &mut Vec<u8>,
) -> Result<(), DriveError> {
    match cs {
        Ok(ConnectionState::WriteTraffic(mut wt)) => {
            let start = outgoing.len();
            let mut grow_to = start
                .saturating_add(CLOSE_NOTIFY_INITIAL)
                .min(TLS_BUFFER_CAP);
            if grow_to <= start {
                return Err(DriveError::BufferCapExceeded {
                    required: start.saturating_add(1),
                    max: TLS_BUFFER_CAP,
                });
            }
            outgoing.resize(grow_to, 0);
            loop {
                match wt.queue_close_notify(&mut outgoing[start..]) {
                    Ok(written) => {
                        outgoing.truncate(start + written);
                        return Ok(());
                    }
                    Err(EncryptError::InsufficientSize(InsufficientSizeError {
                        required_size,
                    })) => {
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
                        return Err(DriveError::Encrypt(other));
                    }
                }
            }
        }
        // Already closed, in error, or in some other transient state:
        // skip the alert. Per shutdown-protocol design.
        _ => Ok(()),
    }
}

/// Try to drain one complete consensus frame from the front of
/// `plaintext`. Returns `Ok(Some(msg))` once both the 256 B header and
/// the body are present; `Ok(None)` if more bytes are needed;
/// `Err(InvalidFrame)` if the header reports a `size` field outside
/// `HEADER_SIZE..=MAX_MESSAGE_SIZE`.
///
/// The size-field offset (48) is the same compile-time-asserted layout
/// the TCP framer uses (`framing.rs:46`); a layout drift trips both
/// sites simultaneously.
fn try_take_frame(plaintext: &mut Vec<u8>) -> Result<Option<Message<GenericHeader>>, DriveError> {
    if plaintext.len() < HEADER_SIZE {
        return Ok(None);
    }
    let total_size = u32::from_le_bytes(
        plaintext[48..52]
            .try_into()
            .map_err(|_| DriveError::InvalidFrame)?,
    ) as usize;
    if !(HEADER_SIZE..=MAX_MESSAGE_SIZE).contains(&total_size) {
        return Err(DriveError::InvalidFrame);
    }
    if plaintext.len() < total_size {
        return Ok(None);
    }
    let owned = Owned::<MESSAGE_ALIGN>::copy_from_slice(&plaintext[..total_size]);
    plaintext.drain(..total_size);
    Message::<GenericHeader>::try_from(owned)
        .map(Some)
        .map_err(|_| DriveError::InvalidFrame)
}

/// Bundle of per-connection control surfaces returned by the
/// orchestrator-side spawn path. `raw_fd` is captured pre-`into_split`
/// (`RawFd` is `Copy`, no ownership tangle) and is the only mechanism to
/// wake a reader blocked on `read.await` without reaching into the
/// task's owned read half.
pub(in crate::transports) struct TlsConnHandles {
    pub(in crate::transports) reader: TaskHandle,
    pub(in crate::transports) writer: TaskHandle,
    pub(in crate::transports) writer_tx: Sender<WriterEvent>,
    pub(in crate::transports) raw_fd: RawFd,
}

/// Cooperative shutdown of a `(reader, writer)` task pair.
///
/// Sequence (from `designs/tls-shutdown-protocol.md`):
/// 1. `libc::shutdown(raw_fd, SHUT_RD)` wakes the reader's pending
///    `read` SQE with `Ok(0)`. Reader exits and drops its
///    `writer_tx` clone.
/// 2. Drop the orchestrator's `writer_tx` clone. Once the reader's
///    clone is also gone, the writer's `recv` returns `Err`, falling
///    into the cooperative shutdown body (`close_notify` + flush +
///    `SHUT_WR`).
/// 3. `compio::time::timeout(drain_budget, handles.writer)` bounds the
///    drain. On timeout the `JoinHandle` is dropped which cancels the
///    spawned task at its next `.await` (`async_task::Task::Drop`
///    sets the canceled flag); compio's leak-on-cancel handles any
///    in-flight `io_uring` SQE.
/// 4. `handles.reader.await` reaps the reader (already exited from
///    EOF in step 1 unless it raced ahead).
///
/// # Safety
///
/// `libc::shutdown` is `unsafe`. The fd is guaranteed open while
/// either task holds its owned half (`compio_net::TcpStream::into_split`
/// distributes a `SharedFd<socket2::Socket>` whose refcount keeps the
/// kernel fd alive). Both `JoinHandle`s are still live at call time,
/// so at least one half — and therefore the fd — survives.
#[allow(clippy::future_not_send)] // single-threaded compio runtime
pub(in crate::transports) async fn shutdown(handles: TlsConnHandles, drain_budget: Duration) {
    // Step 1: wake reader.
    // SAFETY: `raw_fd` was captured from a still-live `TcpStream`
    // before `into_split`. The `SharedFd` in both owned halves keeps
    // the kernel fd open at least until both tasks complete; both
    // `JoinHandle`s are held in `handles`, so the fd is still valid.
    unsafe {
        libc::shutdown(handles.raw_fd, libc::SHUT_RD);
    }

    // Step 2: signal cooperative writer shutdown by dropping the
    // orchestrator's `Sender` clone. The reader will drop its clone
    // on exit; once both are gone the writer's `recv` returns `Err`.
    drop(handles.writer_tx);

    // Step 3: bounded wait for the writer to finish its
    // close_notify + SHUT_WR sequence.
    match compio::time::timeout(drain_budget, handles.writer).await {
        Ok(Ok(Ok(()))) => {
            // Clean exit.
        }
        Ok(Ok(Err(e))) => {
            warn!(error = ?e, "TLS writer task exited with error during shutdown");
        }
        Ok(Err(panic_payload)) => {
            warn!(?panic_payload, "TLS writer task panicked during shutdown");
        }
        Err(_) => {
            warn!(
                ?drain_budget,
                "TLS writer drain budget exceeded; aborting writer task"
            );
            // The JoinHandle is already dropped by the failed
            // `timeout` future; `async_task::Task::Drop` marked the
            // task canceled. compio leak-on-cancel handles any
            // in-flight write SQE.
        }
    }

    // Step 4: reap reader. Already exited from SHUT_RD wake unless it
    // races ahead (peer FIN before our `libc::shutdown` returns).
    match handles.reader.await {
        Ok(Ok(())) => {
            // Clean exit.
        }
        Ok(Err(e)) => {
            warn!(error = ?e, "TLS reader task exited with error during shutdown");
        }
        Err(panic_payload) => {
            warn!(?panic_payload, "TLS reader task panicked during shutdown");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transports::tls::{install_default_crypto_provider, self_signed_for_loopback};
    use async_channel::bounded;
    use compio::net::{TcpListener, TcpStream as CompioTcpStream};
    use iggy_binary_protocol::Command2;
    use rustls::RootCertStore;
    use std::net::SocketAddr;
    use std::os::fd::AsRawFd;
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

    /// Drive the handshake on both ends of a TCP loopback pair and
    /// return the post-handshake drivers + owned halves + captured
    /// raw fds for `libc::shutdown` experiments.
    #[allow(clippy::future_not_send)]
    #[allow(clippy::too_many_arguments)]
    async fn handshaken_pair() -> HandshakenPair {
        let (server_cfg, cert_chain) = server_config();
        let client_cfg = client_config_trusting(&cert_chain);

        let (client_stream, server_stream) = connected_pair().await;
        let client_fd = client_stream.as_raw_fd();
        let server_fd = server_stream.as_raw_fd();
        let (mut client_read, mut client_write) = client_stream.into_split();
        let (mut server_read, mut server_write) = server_stream.into_split();

        let server_driver = TlsDriver::new_server(server_cfg).expect("server driver");
        let server_name: ServerName<'static> = "localhost".try_into().expect("server name");
        let client_driver = TlsDriver::new_client(client_cfg, server_name).expect("client driver");

        let server_handshake = {
            let driver = server_driver.clone();
            async move {
                driver
                    .drive_handshake(&mut server_read, &mut server_write)
                    .await
                    .expect("server handshake");
                (server_read, server_write)
            }
        };
        let client_handshake = {
            let driver = client_driver.clone();
            async move {
                driver
                    .drive_handshake(&mut client_read, &mut client_write)
                    .await
                    .expect("client handshake");
                (client_read, client_write)
            }
        };

        let ((server_read, server_write), (client_read, client_write)) =
            futures::join!(server_handshake, client_handshake);

        HandshakenPair {
            server: PostHandshake {
                driver: server_driver,
                read_half: server_read,
                write_half: server_write,
                raw_fd: server_fd,
            },
            client: PostHandshake {
                driver: client_driver,
                read_half: client_read,
                write_half: client_write,
                raw_fd: client_fd,
            },
        }
    }

    struct HandshakenPair {
        server: PostHandshake,
        client: PostHandshake,
    }

    struct PostHandshake {
        driver: TlsDriver,
        read_half: OwnedReadHalf<CompioTcpStream>,
        write_half: OwnedWriteHalf<CompioTcpStream>,
        raw_fd: RawFd,
    }

    #[allow(clippy::cast_possible_truncation)]
    fn header_only_frozen(command: Command2) -> Frozen<MESSAGE_ALIGN> {
        Message::<GenericHeader>::new(HEADER_SIZE)
            .transmute_header(|_, h: &mut GenericHeader| {
                h.command = command;
                h.size = HEADER_SIZE as u32;
            })
            .into_frozen()
    }

    #[allow(clippy::cast_possible_truncation)]
    fn padded_frozen(command: Command2, total_size: usize) -> Frozen<MESSAGE_ALIGN> {
        Message::<GenericHeader>::new(total_size)
            .transmute_header(|_, h: &mut GenericHeader| {
                h.command = command;
                h.size = total_size as u32;
            })
            .into_frozen()
    }

    /// Bundle returned from [`spawn_pair`] so tests can dispose of
    /// the outbound sender explicitly before calling [`shutdown`].
    /// Cooperative-shutdown semantics demand that *all* orchestrator
    /// senders drop before the writer's `recv` returns `Err`; holding
    /// `outbound` past `shutdown` would block the writer until the
    /// drain budget elapses.
    struct SpawnedConn {
        outbound: Sender<WriterEvent>,
        inbound: Receiver<Message<GenericHeader>>,
        handles: TlsConnHandles,
    }

    /// Spawn a `(reader, writer)` task pair atop a post-handshake
    /// driver. The original `writer_tx` is moved into
    /// `handles.writer_tx` so the test holds only one outbound clone;
    /// the reader holds another internally.
    fn spawn_pair(post: PostHandshake) -> SpawnedConn {
        let (writer_tx, writer_rx) = bounded::<WriterEvent>(64);
        let (in_tx, in_rx) = bounded::<Message<GenericHeader>>(64);
        let outbound = writer_tx.clone();
        let reader_writer_tx = writer_tx.clone();
        let (reader, writer) = post.driver.spawn_tasks(
            post.read_half,
            post.write_half,
            in_tx,
            writer_rx,
            reader_writer_tx,
        );
        SpawnedConn {
            outbound,
            inbound: in_rx,
            handles: TlsConnHandles {
                reader,
                writer,
                writer_tx,
                raw_fd: post.raw_fd,
            },
        }
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

    /// End-to-end app-data round trip: client encrypts a Ping frame,
    /// server decrypts it through the steady-state reader / writer.
    #[compio::test]
    async fn app_data_round_trip_after_handshake() {
        let pair = handshaken_pair().await;
        let client = spawn_pair(pair.client);
        let server = spawn_pair(pair.server);

        client
            .outbound
            .send(WriterEvent::Out(header_only_frozen(Command2::Ping)))
            .await
            .expect("queue outbound");

        let received = server.inbound.recv().await.expect("inbound");
        assert_eq!(received.header().command, Command2::Ping);
        assert_eq!(received.header().size as usize, HEADER_SIZE);

        drop(client.outbound);
        shutdown(client.handles, Duration::from_secs(2)).await;
        drop(server.outbound);
        shutdown(server.handles, Duration::from_secs(2)).await;
    }

    /// 1 MiB body stresses TLS 1.3 record fragmentation: rustls
    /// fragments the input across ~64 records (16 KiB plaintext per
    /// record cap). The streaming framer on the receiver side must
    /// reassemble across record boundaries.
    #[compio::test]
    async fn large_frame_round_trip_through_tls() {
        const BODY_SIZE: usize = 1024 * 1024;

        let pair = handshaken_pair().await;
        let client = spawn_pair(pair.client);
        let server = spawn_pair(pair.server);

        client
            .outbound
            .send(WriterEvent::Out(padded_frozen(Command2::Ping, BODY_SIZE)))
            .await
            .expect("queue outbound");

        let received = server.inbound.recv().await.expect("inbound");
        assert_eq!(received.header().size as usize, BODY_SIZE);

        drop(client.outbound);
        shutdown(client.handles, Duration::from_secs(5)).await;
        drop(server.outbound);
        shutdown(server.handles, Duration::from_secs(5)).await;
    }

    /// Pipeline several frames in a single batch: the writer encrypts
    /// each in turn; the reader's streaming framer must decode them
    /// from the plaintext accumulator without losing bytes between
    /// records.
    #[compio::test]
    async fn pipelined_frames_round_trip() {
        let pair = handshaken_pair().await;
        let client = spawn_pair(pair.client);
        let server = spawn_pair(pair.server);

        for _ in 0..8 {
            client
                .outbound
                .send(WriterEvent::Out(header_only_frozen(Command2::Ping)))
                .await
                .expect("queue outbound");
        }

        for _ in 0..8 {
            let msg = server.inbound.recv().await.expect("inbound");
            assert_eq!(msg.header().command, Command2::Ping);
        }

        drop(client.outbound);
        shutdown(client.handles, Duration::from_secs(2)).await;
        drop(server.outbound);
        shutdown(server.handles, Duration::from_secs(2)).await;
    }

    /// Cooperative shutdown: drop the orchestrator's `writer_tx` after
    /// queueing N messages; assert all messages reach the peer before
    /// the writer queues `close_notify` and exits.
    #[compio::test]
    async fn cooperative_shutdown_drains_queue_then_close_notify() {
        const N: usize = 4;

        let pair = handshaken_pair().await;
        let client = spawn_pair(pair.client);
        let server = spawn_pair(pair.server);

        for _ in 0..N {
            client
                .outbound
                .send(WriterEvent::Out(header_only_frozen(Command2::Ping)))
                .await
                .expect("queue outbound");
        }
        drop(client.outbound);

        for _ in 0..N {
            let msg = server.inbound.recv().await.expect("inbound");
            assert_eq!(msg.header().command, Command2::Ping);
        }

        // Server-side reader must observe peer close_notify and exit.
        shutdown(client.handles, Duration::from_secs(5)).await;
        drop(server.outbound);
        shutdown(server.handles, Duration::from_secs(5)).await;
    }

    /// Peer queues `close_notify`; reader sees `PeerClosed`, sets the
    /// flag, and exits.
    #[compio::test]
    async fn peer_close_notify_observed() {
        let pair = handshaken_pair().await;
        let server_state = Rc::clone(&pair.server.driver.state);
        let client = spawn_pair(pair.client);
        let server = spawn_pair(pair.server);

        // Initiate cooperative shutdown on the client side: writer
        // sends close_notify when its channel closes.
        drop(client.outbound);
        shutdown(client.handles, Duration::from_secs(5)).await;

        // Server reader must drain the channel close: in_rx returns
        // Err once the reader exits and drops in_tx.
        let drained = server.inbound.recv().await;
        assert!(drained.is_err(), "inbound channel must close on peer close");
        assert!(
            server_state.peer_closed.get(),
            "server state must flag peer_closed after client close_notify"
        );

        drop(server.outbound);
        shutdown(server.handles, Duration::from_secs(5)).await;
    }

    /// `libc::shutdown(fd, SHUT_RD)` wakes a reader blocked on a
    /// pending `io_uring` `read` SQE with `Ok(0)`. This is the
    /// orchestrator's mechanism for unblocking the reader without a
    /// `select!` over a shutdown channel.
    #[compio::test]
    async fn libc_shutdown_shut_rd_wakes_reader() {
        let pair = handshaken_pair().await;
        let _client = spawn_pair(pair.client); // keep peer alive
        let server = spawn_pair(pair.server);

        // Reader is blocked on read.await with no peer traffic. SHUT_RD
        // wakes it. shutdown() invokes SHUT_RD and reaps the reader.
        drop(server.outbound);
        let started = std::time::Instant::now();
        shutdown(server.handles, Duration::from_secs(2)).await;
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "shutdown should complete promptly via SHUT_RD"
        );

        // After reader exits, in_rx returns Err.
        assert!(server.inbound.recv().await.is_err());
    }

    /// Reader exits on TCP EOF (peer drops the stream without a TLS
    /// `close_notify` alert). `DriveError::Io` may or may not surface
    /// depending on whether rustls already had bytes buffered; either
    /// way the reader returns rather than panicking.
    #[compio::test]
    async fn reader_exits_on_tcp_eof() {
        let pair = handshaken_pair().await;

        // Drop the client side entirely BEFORE spawning server tasks
        // so the FIN reaches the server reader's first `read.await`.
        let PostHandshake {
            read_half,
            write_half,
            ..
        } = pair.client;
        drop(read_half);
        drop(write_half);

        let server = spawn_pair(pair.server);

        // Server reader observes EOF; in_rx closes once reader exits.
        let drained = server.inbound.recv().await;
        assert!(drained.is_err(), "inbound channel must close on peer EOF");

        drop(server.outbound);
        shutdown(server.handles, Duration::from_secs(2)).await;
    }

    /// Header carrying a `size` field outside the legal range produces
    /// `DriveError::InvalidFrame` and the reader exits with that
    /// error. Driven by encrypting hand-built bytes with the wrong
    /// size field through a real TLS channel.
    /// Synchronous round trip through `try_take_frame` with a
    /// declared `size` field outside the legal range produces
    /// `DriveError::InvalidFrame`. Forging an oversized frame
    /// through the safe `Message<GenericHeader>` constructor is
    /// rejected upstream, so this exercises the framer directly
    /// rather than via the network path.
    #[test]
    fn try_take_frame_rejects_oversized_header() {
        let mut plaintext: Vec<u8> = vec![0u8; HEADER_SIZE];
        let bogus = u32::try_from(MAX_MESSAGE_SIZE + 1)
            .unwrap_or(u32::MAX)
            .to_le_bytes();
        plaintext[48..52].copy_from_slice(&bogus);
        let res = try_take_frame(&mut plaintext);
        assert!(matches!(res, Err(DriveError::InvalidFrame)));
    }

    /// Header carrying a `size` field below `HEADER_SIZE` also
    /// triggers `DriveError::InvalidFrame`.
    #[test]
    fn try_take_frame_rejects_undersized_header() {
        let mut plaintext: Vec<u8> = vec![0u8; HEADER_SIZE];
        let bad = u32::try_from(HEADER_SIZE - 1).unwrap().to_le_bytes();
        plaintext[48..52].copy_from_slice(&bad);
        let res = try_take_frame(&mut plaintext);
        assert!(matches!(res, Err(DriveError::InvalidFrame)));
    }

    /// Synchronous round trip through the streaming consensus framer:
    /// header + body assembled from multiple appends (mimicking
    /// rustls record-by-record decryption).
    #[test]
    fn try_take_frame_handles_partial_appends() {
        let frame = padded_frozen(Command2::Ping, HEADER_SIZE + 32);
        let bytes = frame.as_slice().to_vec();
        let mut accum = Vec::new();

        // Feed in 8 KiB-style chunks; here just two halves.
        let mid = bytes.len() / 2;
        accum.extend_from_slice(&bytes[..mid]);
        assert!(matches!(try_take_frame(&mut accum), Ok(None)));

        accum.extend_from_slice(&bytes[mid..]);
        let msg = try_take_frame(&mut accum)
            .expect("ok")
            .expect("complete frame");
        assert_eq!(msg.header().command, Command2::Ping);
        assert!(accum.is_empty());
    }

    /// Multiple frames packed back-to-back in the plaintext
    /// accumulator are emitted in order without losing bytes.
    #[test]
    fn try_take_frame_drains_back_to_back_frames() {
        let mut accum: Vec<u8> = Vec::new();
        for _ in 0..3 {
            let f = header_only_frozen(Command2::Ping);
            accum.extend_from_slice(f.as_slice());
        }
        for _ in 0..3 {
            let m = try_take_frame(&mut accum).expect("ok").expect("frame");
            assert_eq!(m.header().command, Command2::Ping);
        }
        assert!(accum.is_empty());
        assert!(matches!(try_take_frame(&mut accum), Ok(None)));
    }
}
