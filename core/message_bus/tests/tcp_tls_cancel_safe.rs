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

//! Counterpart to `cancel_unsafe.rs` for the TCP-TLS plane.
//!
//! ## Why this test must never be deleted
//!
//! `core/message_bus/src/transports/tcp_tls.rs` `select!`s a single
//! `tls.read` future against the bus shutdown token and the framing
//! reserve step. The pump is only sound because
//! `compio_tls::TlsStream::read` resolves through `futures-rustls`'s
//! `Stream::poll_read`. On a poll that returns `Pending`, that adapter
//! registers wakers and returns without copying bytes into the
//! caller-supplied buffer. A `Pending`-state cancel therefore leaves
//! the underlying TLS session intact: the next `tls.read` on the same
//! stream observes whatever ciphertext subsequently arrives, fully
//! decrypts it, and delivers the plaintext.
//!
//! That property is library-version-sensitive. This test pins it in
//! CI: if a `compio` / `futures-rustls` bump introduces a code path
//! where a cancelled-while-Pending `tls.read` poisons the stream
//! (state corrupted, bytes silently dropped, future bytes misdecoded),
//! the assertion below trips and the `tcp_tls` pump must be re-audited
//! before the bump is merged.
//!
//! Mechanics, mirroring the pump's `select!`-over-`read` shape:
//!   1. Loopback TCP pair, real rustls handshake on top.
//!   2. Sender holds back; no plaintext on the wire yet.
//!   3. Receiver issues `tls.read(buf_400)` racing a 30 ms timer.
//!      The read is Pending (no ciphertext); the timer wins; the
//!      read future is dropped while in the Pending state.
//!   4. Sender writes 200 'A' plaintext and flushes.
//!   5. Receiver re-issues `tls.read(buf_400)` on the same stream.
//!   6. Assert: phase 2 returns `Ok(200)` and the buffer head is the
//!      full ordered 200 'A' bytes. The Pending-state cancel did not
//!      poison the TLS session.

use std::time::Duration;

use compio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use compio::net::{TcpListener, TcpStream};
use compio::tls::{TlsAcceptor, TlsConnector};
use futures::FutureExt;
use message_bus::transports::tls::{install_default_crypto_provider, self_signed_for_loopback};
use rustls::RootCertStore;
use std::sync::Arc;

const N: usize = 200;
const BUF_CAP: usize = 400;
const PHASE1_GRACE: Duration = Duration::from_millis(30);
const SENDER_DELAY: Duration = Duration::from_millis(120);
const PHASE2_TIMEOUT: Duration = Duration::from_secs(2);

#[compio::test]
async fn tls_read_pending_cancel_does_not_poison_stream() {
    install_default_crypto_provider();

    let creds = self_signed_for_loopback();
    let cert_chain = creds.cert_chain.clone();
    let server_cfg: Arc<rustls::ServerConfig> = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(creds.cert_chain, creds.key_der)
            .expect("ServerConfig::with_single_cert"),
    );
    let mut roots = RootCertStore::empty();
    for cert in cert_chain {
        roots.add(cert).expect("trust self-signed cert");
    }
    let client_cfg: Arc<rustls::ClientConfig> = Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(Arc::new(roots))
            .with_no_client_auth(),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let connect_tcp = TcpStream::connect(addr);
    let accept_tcp = listener.accept();
    let (client_res, accept_res) = futures::join!(connect_tcp, accept_tcp);
    let client_tcp = client_res.unwrap();
    let (server_tcp, _) = accept_res.unwrap();

    let acceptor = TlsAcceptor::from(server_cfg);
    let connector = TlsConnector::from(client_cfg);
    let server_handshake = acceptor.accept(server_tcp);
    let client_handshake = connector.connect("localhost", client_tcp);
    let (server_tls_res, client_tls_res) = futures::join!(server_handshake, client_handshake);
    let mut server_tls = server_tls_res.expect("server tls handshake");
    let mut client_tls = client_tls_res.expect("client tls handshake");

    // Sender intentionally delays past the phase 1 grace so the
    // receiver's first read is guaranteed to be Pending when the timer
    // fires.
    let sender = compio::runtime::spawn(async move {
        compio::time::sleep(SENDER_DELAY).await;
        let payload = vec![b'A'; N];
        client_tls.write_all(payload).await.0.unwrap();
        client_tls.flush().await.unwrap();
        client_tls
    });

    // Phase 1: race a Pending tls.read against the grace timer; timer
    // wins; the read future drops while still Pending.
    let phase1_buf = vec![0u8; BUF_CAP];
    let phase1_timer_won = {
        let read_fut = server_tls.read(phase1_buf);
        let timer = compio::time::sleep(PHASE1_GRACE);
        futures::select! {
            res = read_fut.fuse() => {
                let (n_or_err, _b) = (res.0, res.1);
                let _ = n_or_err;
                false
            }
            () = timer.fuse() => true,
        }
    };
    assert!(
        phase1_timer_won,
        "test premise broken: tls.read returned before the {PHASE1_GRACE:?} grace; \
         expected the timer to fire while no ciphertext was on the wire"
    );

    // Phase 2: re-issue read on the same stream. If the Pending-state
    // cancel poisoned the TLS session, this will hang, error, or
    // return misdecoded bytes.
    let phase2_buf = vec![0u8; BUF_CAP];
    let phase2 = compio::time::timeout(PHASE2_TIMEOUT, server_tls.read(phase2_buf)).await;
    let phase2_inner = phase2.expect(
        "phase 2 timed out: TLS read hung after a Pending-state cancel. \
         compio_tls / futures-rustls cancel semantics changed; re-audit the \
         tcp_tls pump's `select!`-over-read invariant before relaxing the \
         exact-pin on `compio` in the workspace Cargo.toml.",
    );
    let (read_res, buf) = (phase2_inner.0, phase2_inner.1);
    let n_read = read_res.expect(
        "phase 2 returned an error: TLS session was poisoned by the Pending-state \
         cancel. compio_tls / futures-rustls cancel semantics changed; re-audit \
         the tcp_tls pump's `select!`-over-read invariant before relaxing the \
         exact-pin on `compio` in the workspace Cargo.toml.",
    );
    assert_eq!(
        n_read, N,
        "phase 2 read {n_read} plaintext bytes; expected exactly {N} 'A' bytes",
    );
    assert!(
        buf.as_slice()[..N].iter().all(|&b| b == b'A'),
        "phase 2 plaintext misdecoded; expected {N} 'A' bytes, got {:?}",
        &buf.as_slice()[..N.min(buf.as_slice().len())]
    );

    let _ = sender.await;
}
