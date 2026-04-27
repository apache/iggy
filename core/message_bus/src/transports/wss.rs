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

//! In-process WSS transport: WebSocket framing stacked on top of the
//! `transports::tls::driver` rustls `UnbufferedConnection` driver.
//!
//! Two record layers ride a single TCP stream: rustls frames each
//! plaintext byte stream into TLS records, and the bytes inside that
//! TLS-protected channel are themselves framed as WebSocket frames per
//! RFC 6455. The HTTP-Upgrade handshake runs INSIDE the TLS-protected
//! channel; nothing on the wire before TLS handshake completion is
//! HTTP.
//!
//! # Lifecycle
//!
//! [`WssTransportConn::run`] runs in five phases:
//!
//! 1. Capture `raw_fd`, split TCP into owned read / write halves.
//! 2. Build a `transports::tls::TlsDriver` (server or client by role)
//!    and drive the rustls handshake to completion.
//! 3. Run the WebSocket HTTP-Upgrade exchange over the TLS plaintext
//!    stream. Server: read request, validate
//!    `Sec-WebSocket-Protocol = iggy.consensus.v1` + key + version,
//!    write 101 with `Sec-WebSocket-Accept` derived via
//!    [`tungstenite::handshake::derive_accept_key`]; reject with HTTP
//!    400 on missing / wrong subprotocol. Client: send request with
//!    fresh nonce and Upgrade headers, validate the 101 response's
//!    `Sec-WebSocket-Accept`.
//! 4. Spawn the steady-state WS reader / writer task pair. Reader:
//!    decrypt TLS records, parse WS frames, decode consensus
//!    messages, forward inbound Pings to the writer for Pong replies.
//!    Writer: drain consensus / control events, build WS frame
//!    plaintext (server unmasked, client masked per RFC 6455 §5.3),
//!    encrypt via the shared TLS state, flush.
//! 5. Race [`ActorContext::shutdown`] against natural reader / writer
//!    exits via the same internal-Shutdown bridge pattern as
//!    [`super::tcp_tls`]. On internal shutdown the writer emits a
//!    clean WS Close frame (TLS-encrypted) before the cooperative
//!    TLS-shutdown sequence drives `close_notify` + `SHUT_WR`.
//!
//! # `Frozen` ownership on the WSS plane
//!
//! Plaintext TCP preserves `Frozen<MESSAGE_ALIGN>` ownership end-to-end
//! (invariant I8). The TLS plane structurally cannot: rustls's
//! `WriteTraffic::encrypt` copies plaintext into the outbound
//! ciphertext buffer (see [`super::tcp_tls`] rustdoc). The WS plane on
//! top of TLS pays a second copy: the WS header (≤ 14 B) is prepended
//! to the consensus body in a per-frame scratch buffer before it
//! reaches `encrypt`. On the client side the body is also masked
//! in-place inside that scratch. Two structural copies on the WSS
//! plane; the formal I8 amendment lands in P9-T13.
//!
//! # Cancellation safety
//!
//! All `RefCell` borrows on the driver's shared state live inside
//! synchronous helpers in `transports::tls`; the orchestrator-level
//! awaits here only touch cancel-safe channel operations and
//! `ShutdownToken::wait`.

use super::tls::{
    DriveError, TlsConnHandles, TlsDriver, TlsRole, TlsState, append_incoming, drain_inbound,
    encrypt_app_data, flush_outgoing, shutdown as tls_shutdown,
};
use super::ws::{
    InboundFrame, Role, WS_HEADER_MAX, WS_SUBPROTOCOL, apply_mask, build_header,
    decode_consensus_frame, format_header, parse_one_frame,
};
use super::{ActorContext, TransportConn};
use crate::lifecycle::{BusReceiver, Shutdown, ShutdownToken};
use async_channel::{Receiver, Sender, bounded};
use compio::buf::BufResult;
use compio::io::{AsyncRead, AsyncWrite};
use compio::net::{OwnedReadHalf, OwnedWriteHalf, TcpStream};
use futures::FutureExt;
use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
use iggy_binary_protocol::consensus::iobuf::Frozen;
use iggy_binary_protocol::{GenericHeader, Message};
use rustls::pki_types::ServerName;
use std::os::fd::AsRawFd;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};
use tungstenite::handshake::client::generate_key;
use tungstenite::handshake::derive_accept_key;
use tungstenite::protocol::frame::coding::{Control, Data, OpCode};

/// Default cooperative-shutdown drain budget, mirrors
/// [`super::tcp_tls::DEFAULT_DRAIN_BUDGET`]. Threaded through to
/// [`super::tls::shutdown`].
const DEFAULT_DRAIN_BUDGET: Duration = Duration::from_secs(5);

/// Read chunk size for the steady-state ciphertext reader. Matches the
/// TLS 1.3 single-record plaintext cap so each socket read nominally
/// lands at one record boundary; rustls fragments larger payloads
/// itself.
const STEADY_READ_CHUNK: usize = 16 * 1024;

/// Read chunk size during the HTTP-Upgrade exchange. 8 KiB is large
/// enough to hold any reasonable HTTP request / response in one
/// `read`.
const UPGRADE_READ_CHUNK: usize = 8 * 1024;

/// Hard cap on the buffered HTTP-Upgrade headers (request or response)
/// before the orchestrator gives up. Prevents a hostile peer from
/// forcing unbounded plaintext-buffer growth via never-terminating
/// header lines.
const HTTP_UPGRADE_MAX: usize = 16 * 1024;

/// In-process WSS transport: a [`TcpStream`] paired with a role-specific
/// rustls configuration, on top of which the WebSocket framing layer is
/// stacked.
///
/// Construct with [`Self::new_server`] or [`Self::new_client`]; drive
/// with [`TransportConn::run`]. The default cooperative-shutdown drain
/// budget is 5 s; override per connection via
/// [`Self::with_drain_budget`].
pub struct WssTransportConn {
    stream: TcpStream,
    role: TlsRole,
    drain_budget: Duration,
}

impl WssTransportConn {
    /// Server-side connection: drive the rustls server state machine
    /// against `config` and then accept an inbound HTTP-Upgrade
    /// handshake on the TLS-protected channel.
    #[must_use]
    pub const fn new_server(stream: TcpStream, config: Arc<rustls::ServerConfig>) -> Self {
        Self {
            stream,
            role: TlsRole::Server(config),
            drain_budget: DEFAULT_DRAIN_BUDGET,
        }
    }

    /// Client-side connection: drive the rustls client state machine
    /// against `config`, presenting `server_name` in SNI, and then
    /// initiate an HTTP-Upgrade handshake on the TLS-protected channel.
    #[must_use]
    pub const fn new_client(
        stream: TcpStream,
        config: Arc<rustls::ClientConfig>,
        server_name: ServerName<'static>,
    ) -> Self {
        Self {
            stream,
            role: TlsRole::Client {
                config,
                server_name,
            },
            drain_budget: DEFAULT_DRAIN_BUDGET,
        }
    }

    /// Override the cooperative-shutdown drain budget. Intended for
    /// tests and dev-loops.
    #[must_use]
    pub const fn with_drain_budget(mut self, drain_budget: Duration) -> Self {
        self.drain_budget = drain_budget;
        self
    }
}

impl TransportConn for WssTransportConn {
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn run(self, ctx: ActorContext) {
        let raw_fd = self.stream.as_raw_fd();
        let drain_budget = self.drain_budget;
        let role = self.role;
        let max_batch = ctx.max_batch;
        let label = ctx.label;
        let peer_for_log = ctx.peer.clone();
        let (mut read_half, mut write_half) = self.stream.into_split();

        let (driver, ws_role) = match build_driver(&role) {
            Ok(d) => d,
            Err(e) => {
                warn!(
                    %label,
                    peer = %peer_for_log,
                    error = ?e,
                    "WSS driver setup failed; closing connection"
                );
                return;
            }
        };

        if let Err(e) = driver
            .drive_handshake(&mut read_half, &mut write_half)
            .await
        {
            warn!(
                %label,
                peer = %peer_for_log,
                error = ?e,
                "TLS handshake failed; closing connection"
            );
            return;
        }

        let state = Rc::clone(&driver.state);

        let leftover = match ws_role {
            Role::Server => upgrade_server(&state, &mut read_half, &mut write_half).await,
            Role::Client => match &role {
                TlsRole::Client { server_name, .. } => {
                    upgrade_client(
                        &state,
                        &mut read_half,
                        &mut write_half,
                        &server_name_host(server_name),
                    )
                    .await
                }
                TlsRole::Server(_) => unreachable!("client TLS role implies client WS role"),
            },
        };
        let leftover = match leftover {
            Ok(buf) => buf,
            Err(e) => {
                warn!(
                    %label,
                    peer = %peer_for_log,
                    error = ?e,
                    "WS HTTP-Upgrade failed; closing connection"
                );
                return;
            }
        };

        // One merged event channel. Capacity is `max_batch` so the
        // reader's `try_send` of Pong / Close / FlushTls competes with
        // outbound consensus traffic at the same bound the rest of the
        // bus uses; on `Full` the reader logs and continues (matches
        // `transports::ws`), on `Closed` the writer is on its way out.
        let (writer_tx, writer_rx) = bounded::<WssWriterEvent>(max_batch);

        let reader_state = Rc::clone(&state);
        let reader_writer_tx = writer_tx.clone();
        let reader_handle = compio::runtime::spawn(reader_task(
            read_half,
            leftover,
            ctx.in_tx,
            reader_writer_tx,
            reader_state,
            ws_role,
        ));
        let writer_state = Rc::clone(&state);
        let writer_handle =
            compio::runtime::spawn(writer_task(write_half, writer_rx, writer_state, ws_role));

        // Internal shutdown: any of (external ctx.shutdown / reader exit /
        // writer exit) triggers a single fused token the orchestrator awaits
        // before invoking the cooperative shutdown helper.
        let (internal, internal_token) = Shutdown::new();
        let internal = Rc::new(internal);

        let ext_token = ctx.shutdown.clone();
        let i_ext = Rc::clone(&internal);
        compio::runtime::spawn(async move {
            ext_token.wait().await;
            i_ext.trigger();
        })
        .detach();

        let i_reader = Rc::clone(&internal);
        let reader_watch = compio::runtime::spawn(async move {
            let res = reader_handle.await;
            i_reader.trigger();
            match res {
                Ok(inner) => inner,
                Err(_panic) => Ok(()),
            }
        });
        let i_writer = Rc::clone(&internal);
        let writer_watch = compio::runtime::spawn(async move {
            let res = writer_handle.await;
            i_writer.trigger();
            match res {
                Ok(inner) => inner,
                Err(_panic) => Ok(()),
            }
        });

        let pump_writer_tx = writer_tx.clone();
        let pump_token = internal_token.clone();
        let pump_handle = compio::runtime::spawn(pump_outbound(ctx.rx, pump_writer_tx, pump_token));

        internal_token.wait().await;

        // Pump observes the same internal token; awaiting its handle here
        // ensures its `Sender` clone is dropped before the writer's
        // cooperative drain starts.
        let _ = pump_handle.await;

        // Politely emit a WS Close frame (TLS-encrypted) before the
        // cooperative TLS shutdown, so the peer's WS layer sees a
        // protocol-level closure before the TCP half-close. Best-effort:
        // a writer that already exited makes this a no-op via channel
        // close.
        let _ = writer_tx.try_send(WssWriterEvent::Close);

        let handles = TlsConnHandles {
            reader: reader_watch,
            writer: writer_watch,
            writer_tx: dummy_writer_event_sender(),
            raw_fd,
        };
        // Drop our own writer_tx so the writer's `recv` returns Err once
        // the reader's clone is also gone. The dummy channel inside
        // `handles` is what `tls_shutdown` drops; our real writer_tx is
        // dropped here.
        drop(writer_tx);
        tls_shutdown(handles, drain_budget).await;
    }
}

/// Build the rustls driver and infer the WS role from the TLS role.
fn build_driver(role: &TlsRole) -> Result<(TlsDriver, Role), DriveError> {
    match role {
        TlsRole::Server(cfg) => {
            let driver = TlsDriver::new_server(Arc::clone(cfg))?;
            Ok((driver, Role::Server))
        }
        TlsRole::Client {
            config,
            server_name,
        } => {
            let driver = TlsDriver::new_client(Arc::clone(config), server_name.clone())?;
            Ok((driver, Role::Client))
        }
    }
}

/// Render a [`ServerName`] as a `Host:` header value.
fn server_name_host(server_name: &ServerName<'_>) -> String {
    match server_name {
        ServerName::DnsName(dns) => dns.as_ref().to_owned(),
        ServerName::IpAddress(ip) => format!("{ip:?}"),
        _ => "localhost".to_owned(),
    }
}

/// Forward outbound `Frozen` frames from the bus's per-peer queue into
/// the writer's merged event channel as
/// [`WssWriterEvent::Out`]. Mirrors [`super::tcp_tls::pump_outbound`].
#[allow(clippy::future_not_send)]
async fn pump_outbound(
    rx: BusReceiver,
    writer_tx: Sender<WssWriterEvent>,
    shutdown: ShutdownToken,
) {
    let mut shutdown_fut = Box::pin(shutdown.wait().fuse());
    loop {
        futures::select_biased! {
            () = shutdown_fut.as_mut() => return,
            msg = rx.recv().fuse() => {
                let Ok(frozen) = msg else { return; };
                if writer_tx.send(WssWriterEvent::Out(frozen)).await.is_err() {
                    return;
                }
            }
        }
    }
}

// =====================================================================
// HTTP-Upgrade dance over the TLS plaintext channel.
// =====================================================================

/// Errors that can arise driving the HTTP-Upgrade exchange.
#[derive(Debug, thiserror::Error)]
enum UpgradeError {
    #[error("TLS driver: {0:?}")]
    Tls(DriveError),
    #[error("connection closed mid-upgrade")]
    Eof,
    #[error("HTTP header buffer cap exceeded")]
    HeaderTooLarge,
    #[error("malformed HTTP request line")]
    BadRequestLine,
    #[error("malformed HTTP response line")]
    BadResponseLine,
    #[error("missing or invalid Upgrade header")]
    MissingUpgrade,
    #[error("missing or invalid Connection header")]
    MissingConnection,
    #[error("missing or invalid Sec-WebSocket-Version header (must be 13)")]
    BadVersion,
    #[error("missing Sec-WebSocket-Key header")]
    MissingKey,
    #[error("missing or wrong Sec-WebSocket-Protocol header (expected {0})")]
    BadSubprotocol(&'static str),
    #[error("response status was not 101 Switching Protocols")]
    NotSwitchingProtocols,
    #[error("Sec-WebSocket-Accept did not match")]
    BadAcceptKey,
}

impl From<DriveError> for UpgradeError {
    fn from(e: DriveError) -> Self {
        Self::Tls(e)
    }
}

/// Server-side HTTP-Upgrade. Read the inbound request from the
/// TLS-protected stream, validate it, and write a 101 response (or 400
/// on a subprotocol violation). Returns any leftover plaintext bytes
/// the WS reader must seed its accumulator with (normally empty; a
/// well-behaved client does not pipeline before 101).
#[allow(clippy::future_not_send)]
async fn upgrade_server(
    state: &TlsState,
    read_half: &mut OwnedReadHalf<TcpStream>,
    write_half: &mut OwnedWriteHalf<TcpStream>,
) -> Result<Vec<u8>, UpgradeError> {
    let mut accum: Vec<u8> = Vec::new();
    loop {
        if let Some(consumed) = find_headers_end(&accum) {
            let header_bytes = accum[..consumed].to_vec();
            match parse_request_headers(&header_bytes) {
                Ok(req) => {
                    let response = build_101_response(&req.key);
                    tls_send_plaintext(state, write_half, response.as_bytes()).await?;
                    accum.drain(..consumed);
                    return Ok(accum);
                }
                Err(e) => {
                    let resp = build_400_response(&e);
                    let _ = tls_send_plaintext(state, write_half, resp.as_bytes()).await;
                    return Err(e);
                }
            }
        }
        if accum.len() > HTTP_UPGRADE_MAX {
            return Err(UpgradeError::HeaderTooLarge);
        }
        tls_recv_into(state, read_half, write_half, &mut accum).await?;
    }
}

/// Client-side HTTP-Upgrade. Send an Upgrade request over the
/// TLS-protected stream, validate the 101 response, return any
/// post-101 plaintext bytes for the WS reader's accumulator.
#[allow(clippy::future_not_send)]
async fn upgrade_client(
    state: &TlsState,
    read_half: &mut OwnedReadHalf<TcpStream>,
    write_half: &mut OwnedWriteHalf<TcpStream>,
    host: &str,
) -> Result<Vec<u8>, UpgradeError> {
    let key = generate_key();
    let request = format!(
        "GET / HTTP/1.1\r\n\
         Host: {host}\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Key: {key}\r\n\
         Sec-WebSocket-Version: 13\r\n\
         Sec-WebSocket-Protocol: {WS_SUBPROTOCOL}\r\n\
         \r\n"
    );
    tls_send_plaintext(state, write_half, request.as_bytes()).await?;
    let expected_accept = derive_accept_key(key.as_bytes());

    let mut accum: Vec<u8> = Vec::new();
    loop {
        if let Some(consumed) = find_headers_end(&accum) {
            let header_bytes = accum[..consumed].to_vec();
            validate_response_headers(&header_bytes, &expected_accept)?;
            accum.drain(..consumed);
            return Ok(accum);
        }
        if accum.len() > HTTP_UPGRADE_MAX {
            return Err(UpgradeError::HeaderTooLarge);
        }
        tls_recv_into(state, read_half, write_half, &mut accum).await?;
    }
}

/// Encrypt `plaintext` into the TLS outbound buffer and flush it onto
/// `write_half`.
#[allow(clippy::future_not_send)]
async fn tls_send_plaintext(
    state: &TlsState,
    write_half: &mut OwnedWriteHalf<TcpStream>,
    plaintext: &[u8],
) -> Result<(), DriveError> {
    encrypt_app_data(state, plaintext)?;
    flush_outgoing(state, write_half).await
}

/// Read one chunk of ciphertext from `read_half`, drain decrypted
/// bytes into `accum`, and flush any handshake-response bytes the
/// drain produced.
#[allow(clippy::future_not_send)]
async fn tls_recv_into(
    state: &TlsState,
    read_half: &mut OwnedReadHalf<TcpStream>,
    write_half: &mut OwnedWriteHalf<TcpStream>,
    accum: &mut Vec<u8>,
) -> Result<(), UpgradeError> {
    let buf = vec![0u8; UPGRADE_READ_CHUNK];
    let BufResult(n_res, buf) = read_half.read(buf).await;
    let n = n_res.map_err(|e| UpgradeError::Tls(DriveError::Io(e)))?;
    if n == 0 {
        return Err(UpgradeError::Eof);
    }
    append_incoming(state, &buf[..n])?;
    let outcome = drain_inbound(state, accum)?;
    if outcome.flush_needed {
        flush_outgoing(state, write_half).await?;
    }
    if outcome.peer_closed {
        return Err(UpgradeError::Eof);
    }
    Ok(())
}

/// Locate the `\r\n\r\n` header terminator. Returns the byte offset
/// past the terminator (i.e. the position where the body starts /
/// where leftover plaintext begins).
fn find_headers_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
}

/// Subset of the HTTP request headers the upgrade dance cares about.
struct RequestSummary {
    key: String,
}

/// Parse and validate the inbound HTTP request headers. Returns the
/// extracted `Sec-WebSocket-Key` on success.
fn parse_request_headers(buf: &[u8]) -> Result<RequestSummary, UpgradeError> {
    let text = std::str::from_utf8(buf).map_err(|_| UpgradeError::BadRequestLine)?;
    let mut lines = text.split("\r\n");
    let request_line = lines.next().ok_or(UpgradeError::BadRequestLine)?;
    let mut parts = request_line.split_ascii_whitespace();
    let method = parts.next().ok_or(UpgradeError::BadRequestLine)?;
    let _path = parts.next().ok_or(UpgradeError::BadRequestLine)?;
    let version = parts.next().ok_or(UpgradeError::BadRequestLine)?;
    if !method.eq_ignore_ascii_case("GET") {
        return Err(UpgradeError::BadRequestLine);
    }
    if version != "HTTP/1.1" {
        return Err(UpgradeError::BadRequestLine);
    }

    let mut upgrade_ok = false;
    let mut connection_ok = false;
    let mut version_ok = false;
    let mut key: Option<String> = None;
    let mut subprotocol_ok = false;
    let mut subprotocol_seen = false;

    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':').ok_or(UpgradeError::BadRequestLine)?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("Upgrade") {
            if value
                .split(',')
                .any(|t| t.trim().eq_ignore_ascii_case("websocket"))
            {
                upgrade_ok = true;
            }
        } else if name.eq_ignore_ascii_case("Connection") {
            if value
                .split(',')
                .any(|t| t.trim().eq_ignore_ascii_case("Upgrade"))
            {
                connection_ok = true;
            }
        } else if name.eq_ignore_ascii_case("Sec-WebSocket-Version") {
            if value.trim() == "13" {
                version_ok = true;
            }
        } else if name.eq_ignore_ascii_case("Sec-WebSocket-Key") {
            key = Some(value.trim().to_owned());
        } else if name.eq_ignore_ascii_case("Sec-WebSocket-Protocol") {
            subprotocol_seen = true;
            if value
                .split(',')
                .any(|t| t.trim().eq_ignore_ascii_case(WS_SUBPROTOCOL))
            {
                subprotocol_ok = true;
            }
        }
    }

    if !upgrade_ok {
        return Err(UpgradeError::MissingUpgrade);
    }
    if !connection_ok {
        return Err(UpgradeError::MissingConnection);
    }
    if !version_ok {
        return Err(UpgradeError::BadVersion);
    }
    let key = key.ok_or(UpgradeError::MissingKey)?;
    if !subprotocol_seen || !subprotocol_ok {
        return Err(UpgradeError::BadSubprotocol(WS_SUBPROTOCOL));
    }
    Ok(RequestSummary { key })
}

/// Validate the inbound 101 response headers against `expected_accept`.
fn validate_response_headers(buf: &[u8], expected_accept: &str) -> Result<(), UpgradeError> {
    let text = std::str::from_utf8(buf).map_err(|_| UpgradeError::BadResponseLine)?;
    let mut lines = text.split("\r\n");
    let status_line = lines.next().ok_or(UpgradeError::BadResponseLine)?;
    let mut parts = status_line.splitn(3, ' ');
    let version = parts.next().ok_or(UpgradeError::BadResponseLine)?;
    let status = parts.next().ok_or(UpgradeError::BadResponseLine)?;
    if !version.eq_ignore_ascii_case("HTTP/1.1") {
        return Err(UpgradeError::BadResponseLine);
    }
    if status != "101" {
        return Err(UpgradeError::NotSwitchingProtocols);
    }

    let mut accept_ok = false;
    let mut subprotocol_ok = false;

    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':').ok_or(UpgradeError::BadResponseLine)?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("Sec-WebSocket-Accept") && value == expected_accept {
            accept_ok = true;
        } else if name.eq_ignore_ascii_case("Sec-WebSocket-Protocol")
            && value.eq_ignore_ascii_case(WS_SUBPROTOCOL)
        {
            subprotocol_ok = true;
        }
    }
    if !accept_ok {
        return Err(UpgradeError::BadAcceptKey);
    }
    if !subprotocol_ok {
        return Err(UpgradeError::BadSubprotocol(WS_SUBPROTOCOL));
    }
    Ok(())
}

/// Build the 101 Switching Protocols response.
fn build_101_response(client_key: &str) -> String {
    let accept = derive_accept_key(client_key.as_bytes());
    format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {accept}\r\n\
         Sec-WebSocket-Protocol: {WS_SUBPROTOCOL}\r\n\
         \r\n"
    )
}

/// Build a 400 Bad Request response naming the expected subprotocol so
/// the rejected client has something diagnostic to log.
fn build_400_response(reason: &UpgradeError) -> String {
    let body = format!("WSS upgrade rejected: {reason}. Expected subprotocol: {WS_SUBPROTOCOL}.\n");
    format!(
        "HTTP/1.1 400 Bad Request\r\n\
         Connection: close\r\n\
         Content-Type: text/plain; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         \r\n\
         {body}",
        body.len()
    )
}

// =====================================================================
// Steady-state reader / writer task pair.
// =====================================================================

/// Outbound work the writer task drains.
enum WssWriterEvent {
    /// Outbound application-layer consensus frame.
    Out(Frozen<MESSAGE_ALIGN>),
    /// Reply to an inbound Ping with a Pong carrying the same payload
    /// (RFC 6455 §5.5.2).
    Pong(Vec<u8>),
    /// Emit a graceful WS Close frame and exit.
    Close,
    /// Reader signalled `outgoing_tls` has bytes to flush — handshake
    /// response, alert, or `KeyUpdate` response. Best-effort
    /// `try_send` from the reader so reader progress is never blocked
    /// on writer queue pressure.
    FlushTls,
}

/// WSS reader task: read TLS ciphertext, drain decrypted plaintext into
/// a streaming WS-frame parser, decode consensus binaries, forward
/// inbound Pings as Pongs to the writer.
#[allow(clippy::future_not_send)]
async fn reader_task(
    mut read_half: OwnedReadHalf<TcpStream>,
    leftover: Vec<u8>,
    in_tx: Sender<Message<GenericHeader>>,
    writer_tx: Sender<WssWriterEvent>,
    state: Rc<TlsState>,
    role: Role,
) -> Result<(), DriveError> {
    let mut buf: Vec<u8> = vec![0u8; STEADY_READ_CHUNK];
    let mut plaintext: Vec<u8> = leftover;

    loop {
        if state.shutdown.get() {
            return Ok(());
        }

        // Drain any complete WS frames already buffered in `plaintext`.
        loop {
            match parse_one_frame(&mut plaintext, role) {
                Ok(Some(InboundFrame::Binary(payload))) => match decode_consensus_frame(&payload) {
                    Ok(msg) => {
                        if in_tx.send(msg).await.is_err() {
                            debug!("WSS reader: inbound queue dropped, exiting");
                            return Ok(());
                        }
                    }
                    Err(e) => {
                        warn!("WSS reader: consensus frame decode failed: {e:?}");
                        let _ = writer_tx.try_send(WssWriterEvent::Close);
                        return Ok(());
                    }
                },
                Ok(Some(InboundFrame::Ping(payload))) => {
                    if writer_tx.try_send(WssWriterEvent::Pong(payload)).is_err() {
                        warn!("WSS reader: writer queue full / closed, dropping inbound Ping");
                    }
                }
                Ok(Some(InboundFrame::Pong)) => { /* server keepalive reply; ignore */ }
                Ok(Some(InboundFrame::Close)) => {
                    debug!("WSS reader: peer initiated close");
                    let _ = writer_tx.try_send(WssWriterEvent::Close);
                    return Ok(());
                }
                Ok(None) => break,
                Err(e) => {
                    warn!("WSS reader: framing violation: {e:?}");
                    let _ = writer_tx.try_send(WssWriterEvent::Close);
                    return Ok(());
                }
            }
        }

        // Need more bytes from the wire.
        let owned = std::mem::take(&mut buf);
        let BufResult(n_res, owned) = read_half.read(owned).await;
        buf = owned;
        let n = n_res.map_err(DriveError::Io)?;
        if n == 0 {
            return Ok(());
        }
        append_incoming(&state, &buf[..n])?;
        let outcome = drain_inbound(&state, &mut plaintext)?;
        if outcome.flush_needed {
            // Reader cannot touch write_half; signal writer. On full /
            // closed: full means the writer's next iteration will
            // observe the fresh `outgoing_tls` regardless via
            // `flush_outgoing`, closed means the writer is on its way
            // out — both safe to ignore.
            let _ = writer_tx.try_send(WssWriterEvent::FlushTls);
        }
        if outcome.peer_closed {
            return Ok(());
        }
    }
}

/// WSS writer task: drain consensus + control events from the merged
/// channel, build WS frames (server unmasked, client masked), encrypt
/// + flush via the shared TLS state.
///
/// No `select!`: channel close is the cooperative shutdown signal,
/// matching the [`super::tls::driver`] pattern.
#[allow(clippy::future_not_send)]
async fn writer_task(
    mut write_half: OwnedWriteHalf<TcpStream>,
    rx: Receiver<WssWriterEvent>,
    state: Rc<TlsState>,
    role: Role,
) -> Result<(), DriveError> {
    let mut frame_scratch: Vec<u8> = Vec::with_capacity(WS_HEADER_MAX);

    while let Ok(event) = rx.recv().await {
        match event {
            WssWriterEvent::Out(frozen) => {
                build_ws_frame(
                    &mut frame_scratch,
                    OpCode::Data(Data::Binary),
                    frozen.as_slice(),
                    role,
                );
                encrypt_app_data(&state, &frame_scratch)?;
                flush_outgoing(&state, &mut write_half).await?;
            }
            WssWriterEvent::Pong(payload) => {
                build_ws_frame(
                    &mut frame_scratch,
                    OpCode::Control(Control::Pong),
                    &payload,
                    role,
                );
                encrypt_app_data(&state, &frame_scratch)?;
                flush_outgoing(&state, &mut write_half).await?;
            }
            WssWriterEvent::Close => {
                build_ws_frame(
                    &mut frame_scratch,
                    OpCode::Control(Control::Close),
                    &[],
                    role,
                );
                let _ = encrypt_app_data(&state, &frame_scratch);
                let _ = flush_outgoing(&state, &mut write_half).await;
                break;
            }
            WssWriterEvent::FlushTls => {
                flush_outgoing(&state, &mut write_half).await?;
            }
        }
    }

    // Cooperative shutdown body inherited from `super::tls::driver`:
    // queue close_notify, flush, half-close.
    if let Err(e) = super::tls::queue_close_notify(&state) {
        warn!(error = ?e, "WSS writer: queue_close_notify failed during shutdown");
    }
    if let Err(e) = flush_outgoing(&state, &mut write_half).await {
        warn!(error = ?e, "WSS writer: flush_outgoing failed during shutdown");
    }
    if let Err(e) = write_half.shutdown().await {
        warn!(error = ?e, "WSS writer: TCP SHUT_WR failed during shutdown");
    }
    Ok(())
}

/// Build one WS frame into `scratch`. Server role writes unmasked,
/// client role applies a fresh per-frame mask per RFC 6455 §5.3.
fn build_ws_frame(scratch: &mut Vec<u8>, opcode: OpCode, payload: &[u8], role: Role) {
    scratch.clear();
    let mask = match role {
        Role::Server => None,
        Role::Client => Some(rand::random::<u32>().to_ne_bytes()),
    };
    let header = build_header(opcode, mask);
    #[allow(clippy::cast_possible_truncation)]
    format_header(&header, payload.len() as u64, scratch);
    let header_len = scratch.len();
    scratch.extend_from_slice(payload);
    if let Some(m) = mask {
        apply_mask(&mut scratch[header_len..], m);
    }
}

/// Build a placeholder `Sender<WriterEvent>` to satisfy the
/// [`TlsConnHandles`] field. The shutdown helper drops this sender
/// before awaiting the writer; for WSS the real shutdown signal goes
/// through our own [`WssWriterEvent`] channel which is dropped before
/// `tls_shutdown` is called. The dummy is never written to or
/// observed.
fn dummy_writer_event_sender() -> Sender<super::tls::WriterEvent> {
    let (tx, _rx) = bounded::<super::tls::WriterEvent>(1);
    tx
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framing;
    use crate::lifecycle::Shutdown;
    use crate::transports::tls::{install_default_crypto_provider, self_signed_for_loopback};
    use async_channel::{Receiver, bounded};
    use compio::net::TcpListener;
    use iggy_binary_protocol::consensus::iobuf::Frozen;
    use iggy_binary_protocol::{Command2, GenericHeader, HEADER_SIZE, Message};
    use rustls::RootCertStore;
    use std::net::SocketAddr;
    use std::sync::OnceLock;

    fn ensure_provider() {
        static GUARD: OnceLock<()> = OnceLock::new();
        GUARD.get_or_init(install_default_crypto_provider);
    }

    fn server_cfg() -> (
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

    fn client_cfg_trusting(
        server_cert_chain: &[rustls::pki_types::CertificateDer<'static>],
    ) -> Arc<rustls::ClientConfig> {
        ensure_provider();
        let mut roots = RootCertStore::empty();
        for cert in server_cert_chain {
            roots.add(cert.clone()).expect("add cert");
        }
        Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(Arc::new(roots))
                .with_no_client_auth(),
        )
    }

    #[allow(clippy::future_not_send)]
    async fn connected_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let server_addr: SocketAddr = listener.local_addr().expect("local_addr");
        let connect = TcpStream::connect(server_addr);
        let accept = listener.accept();
        let (client_res, accept_res) = futures::join!(connect, accept);
        let (server, _peer) = accept_res.expect("accept");
        (client_res.expect("connect"), server)
    }

    #[allow(clippy::cast_possible_truncation)]
    fn header_only(command: Command2) -> Frozen<MESSAGE_ALIGN> {
        Message::<GenericHeader>::new(HEADER_SIZE)
            .transmute_header(|_, h: &mut GenericHeader| {
                h.command = command;
                h.size = HEADER_SIZE as u32;
            })
            .into_frozen()
    }

    #[allow(clippy::cast_possible_truncation)]
    fn padded(command: Command2, total_size: usize) -> Frozen<MESSAGE_ALIGN> {
        Message::<GenericHeader>::new(total_size)
            .transmute_header(|_, h: &mut GenericHeader| {
                h.command = command;
                h.size = total_size as u32;
            })
            .into_frozen()
    }

    #[allow(clippy::future_not_send)]
    fn drive(
        conn: WssTransportConn,
    ) -> (
        Sender<Frozen<MESSAGE_ALIGN>>,
        Receiver<Message<GenericHeader>>,
        Shutdown,
        compio::runtime::JoinHandle<()>,
    ) {
        let (out_tx, out_rx) = bounded::<Frozen<MESSAGE_ALIGN>>(16);
        let (in_tx, in_rx) = bounded::<Message<GenericHeader>>(16);
        let (shutdown, token) = Shutdown::new();
        let ctx = ActorContext {
            in_tx,
            rx: out_rx,
            shutdown: token,
            max_batch: 16,
            max_message_size: framing::MAX_MESSAGE_SIZE,
            label: "test",
            peer: "test".to_owned(),
        };
        let handle = compio::runtime::spawn(async move { conn.run(ctx).await });
        (out_tx, in_rx, shutdown, handle)
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn wss_loopback_round_trip_with_self_signed_cert() {
        let (server_config, cert_chain) = server_cfg();
        let client_config = client_cfg_trusting(&cert_chain);
        let server_name: ServerName<'static> = "localhost".try_into().expect("server name");

        let (client_stream, server_stream) = connected_pair().await;
        let server_conn = WssTransportConn::new_server(server_stream, server_config);
        let client_conn = WssTransportConn::new_client(client_stream, client_config, server_name);

        let (server_out, server_in, server_shutdown, server_handle) = drive(server_conn);
        let (client_out, client_in, client_shutdown, client_handle) = drive(client_conn);

        client_out
            .send(header_only(Command2::Request))
            .await
            .expect("client send");
        let received = compio::time::timeout(Duration::from_secs(5), server_in.recv())
            .await
            .expect("server recv within 5 s")
            .expect("server frame");
        assert_eq!(received.header().command, Command2::Request);

        server_out
            .send(header_only(Command2::Reply))
            .await
            .expect("server send");
        let reply = compio::time::timeout(Duration::from_secs(5), client_in.recv())
            .await
            .expect("client recv within 5 s")
            .expect("client frame");
        assert_eq!(reply.header().command, Command2::Reply);

        server_shutdown.trigger();
        client_shutdown.trigger();
        let _ = compio::time::timeout(Duration::from_secs(5), server_handle).await;
        let _ = compio::time::timeout(Duration::from_secs(5), client_handle).await;
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn wss_handshake_rejects_missing_subprotocol() {
        // Hand-rolled client that drives TLS itself but sends an
        // Upgrade request without `Sec-WebSocket-Protocol`. The server
        // must reject; the server-side `WssTransportConn::run` must
        // exit cleanly within 5 s with `in_tx` closed.
        let (server_config, cert_chain) = server_cfg();
        let client_config = client_cfg_trusting(&cert_chain);
        let server_name: ServerName<'static> = "localhost".try_into().expect("server name");

        let (client_stream, server_stream) = connected_pair().await;
        let server_conn = WssTransportConn::new_server(server_stream, server_config);
        let (_server_out, server_in, _server_shutdown, server_handle) = drive(server_conn);

        // Drive the client side: TLS handshake, then a malformed
        // Upgrade request.
        let bad_client = compio::runtime::spawn(async move {
            let driver = TlsDriver::new_client(client_config, server_name).expect("driver");
            let (mut read_half, mut write_half) = client_stream.into_split();
            driver
                .drive_handshake(&mut read_half, &mut write_half)
                .await
                .expect("client handshake");
            let state = Rc::clone(&driver.state);
            // Missing Sec-WebSocket-Protocol header.
            let req = "GET / HTTP/1.1\r\n\
                       Host: localhost\r\n\
                       Upgrade: websocket\r\n\
                       Connection: Upgrade\r\n\
                       Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                       Sec-WebSocket-Version: 13\r\n\
                       \r\n";
            tls_send_plaintext(&state, &mut write_half, req.as_bytes())
                .await
                .expect("tls send");
            // Drain whatever the server sent so the orchestrator can
            // observe the close.
            let mut accum: Vec<u8> = Vec::new();
            for _ in 0..16 {
                if tls_recv_into(&state, &mut read_half, &mut write_half, &mut accum)
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        let server_recv = compio::time::timeout(Duration::from_secs(5), server_in.recv()).await;
        assert!(
            matches!(server_recv, Ok(Err(_))),
            "server in_rx must close cleanly on subprotocol reject, got {server_recv:?}"
        );
        let _ = compio::time::timeout(Duration::from_secs(5), server_handle).await;
        let _ = compio::time::timeout(Duration::from_secs(5), bad_client).await;
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn wss_large_frame_round_trip() {
        // 1 MiB body. Stresses both rustls record fragmentation
        // (~64 records) AND WS frame extended-length encoding (8-byte
        // length prefix).
        const BODY_SIZE: usize = 1024 * 1024;
        let total = HEADER_SIZE + BODY_SIZE;

        let (server_config, cert_chain) = server_cfg();
        let client_config = client_cfg_trusting(&cert_chain);
        let server_name: ServerName<'static> = "localhost".try_into().expect("server name");

        let (client_stream, server_stream) = connected_pair().await;
        let server_conn = WssTransportConn::new_server(server_stream, server_config);
        let client_conn = WssTransportConn::new_client(client_stream, client_config, server_name);

        let (_server_out, server_in, server_shutdown, server_handle) = drive(server_conn);
        let (client_out, _client_in, client_shutdown, client_handle) = drive(client_conn);

        client_out
            .send(padded(Command2::Request, total))
            .await
            .expect("client send 1 MiB");
        let received = compio::time::timeout(Duration::from_secs(15), server_in.recv())
            .await
            .expect("server recv within 15 s")
            .expect("server frame");
        assert_eq!(received.header().command, Command2::Request);
        assert_eq!(received.header().size as usize, total);

        server_shutdown.trigger();
        client_shutdown.trigger();
        let _ = compio::time::timeout(Duration::from_secs(10), server_handle).await;
        let _ = compio::time::timeout(Duration::from_secs(10), client_handle).await;
    }
}
