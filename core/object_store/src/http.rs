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

//! The HTTP client that every remote provider and credential source uses.

use std::cell::RefCell;
use std::error::Error;
use std::fmt::Write as _;
use std::io;
use std::rc::Rc;
use std::sync::Arc;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use send_wrapper::SendWrapper;

use crate::error::ObjectStoreError;
use crate::resolver::ShardResolver;

/// A client for a shard: host lookups off the shard thread, no redirects,
/// and the system's certificate authorities only for `https`.
///
/// A redirect would resend a body that is already sent, under a signature
/// or a token meant for another host.
pub fn client(scheme: &str) -> Result<cyper::Client, ObjectStoreError> {
    // Without cyper's `http2` feature the client offers only `http/1.1` in
    // ALPN and never sends a request on a connection that has one in flight.
    // So a second attempt at a slow put always goes out on another
    // connection than the first one, which is what lets it pass a stalled
    // connection.
    let builder = cyper::Client::builder()
        .custom_resolver(ShardResolver::default())
        .redirect(cyper::redirect::Policy::none());
    let builder = if scheme == "https" {
        builder.use_rustls_default()
    } else {
        // Without a config, the client loads the system's certificate
        // authorities when it is built, and fails on a host without them,
        // although it never dials https.
        builder.use_rustls(Arc::new(plain_http_tls_config()?))
    };
    builder.build().map_err(transport)
}

/// `pieces` as a request body. A failure of a piece is left in `failure`,
/// so the put returns the body's own error, not the one that the client
/// makes of it.
pub fn streamed(
    pieces: impl Stream<Item = io::Result<Bytes>> + 'static,
    failure: Rc<RefCell<Option<io::Error>>>,
) -> cyper::Body {
    let pieces = pieces.map(move |piece| {
        piece.map_err(|error| {
            let kind = error.kind();
            *failure.borrow_mut() = Some(error);
            cyper::Error::System(io::Error::from(kind))
        })
    });
    // The client polls the body on the shard thread that sends it.
    cyper::Body::stream(SendWrapper::new(pieces))
}

/// The error of a request that the client failed. A failure to connect, in
/// the name lookup, the TCP connect or the TLS handshake, comes before any
/// byte of the request is written, so the request was never sent.
#[allow(clippy::needless_pass_by_value)]
pub fn transport(error: cyper::Error) -> ObjectStoreError {
    match &error {
        cyper::Error::HyperClient(client) if client.is_connect() => {
            ObjectStoreError::NotSent(with_sources(&error))
        }
        _ => ObjectStoreError::Transport(error.to_string()),
    }
}

/// `error` and the errors it wraps, which name what failed to connect.
fn with_sources(error: &dyn Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let _ = write!(text, ": {cause}");
        source = cause.source();
    }
    text
}

/// A TLS config that trusts no authority, for a client that dials only plain
/// http.
fn plain_http_tls_config() -> Result<rustls::ClientConfig, ObjectStoreError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    Ok(rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| ObjectStoreError::Transport(error.to_string()))?
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth())
}

/// Endpoints for the tests of the providers that tell a request that never
/// left from one whose answer was lost, and that drop a request.
#[cfg(test)]
pub mod test_endpoints {
    use std::io::Read;
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc;
    use std::time::Duration;

    /// An `http` endpoint on this host where nothing listens, so a connect
    /// is refused.
    pub fn refusing() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        format!("http://{}", listener.local_addr().unwrap())
    }

    /// An `http` endpoint that takes one request whole, body included, and
    /// closes the connection without an answer.
    pub fn hanging_up() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        std::thread::spawn(move || {
            if let Ok((mut connection, _)) = listener.accept() {
                read_request(&mut connection, true);
            }
        });
        endpoint
    }

    /// An endpoint that takes one request and never answers it. With
    /// `take_body` it reads the whole body first, and otherwise it stops
    /// reading after the head, as a store that stalls does.
    pub struct Silent {
        pub endpoint: String,
        look: mpsc::Sender<Duration>,
        closed: mpsc::Receiver<bool>,
    }

    impl Silent {
        pub fn start(take_body: bool) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let (look, looked) = mpsc::channel::<Duration>();
            let (report, closed) = mpsc::channel();
            std::thread::spawn(move || {
                let Ok((mut connection, _)) = listener.accept() else {
                    return;
                };
                read_request(&mut connection, take_body);
                let Ok(wait) = looked.recv() else {
                    return;
                };
                let _ = report.send(reaches_end(&mut connection, wait));
            });
            Self {
                endpoint,
                look,
                closed,
            }
        }

        /// Whether the client closed the connection: the endpoint reads what
        /// is left of it, and waits up to `wait` for its end. The client's
        /// runtime keeps running meanwhile, so it can flush and close.
        pub async fn client_closed(&self, wait: Duration) -> bool {
            self.look.send(wait).unwrap();
            for _ in 0..wait.as_millis().saturating_mul(4) / 10 {
                if let Ok(closed) = self.closed.try_recv() {
                    return closed;
                }
                compio::time::sleep(Duration::from_millis(10)).await;
            }
            false
        }
    }

    /// Reads the head of one request, and with `take_body` its declared body.
    fn read_request(connection: &mut TcpStream, take_body: bool) {
        let mut received = Vec::new();
        let mut buffer = [0_u8; 8192];
        loop {
            if let Some(end) = received.windows(4).position(|window| window == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&received[..end]).to_ascii_lowercase();
                let declared: usize = head
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length:"))
                    .and_then(|length| length.trim().parse().ok())
                    .unwrap_or(0);
                if !take_body || received.len() >= end + 4 + declared {
                    return;
                }
            }
            match connection.read(&mut buffer) {
                Ok(0) | Err(_) => return,
                Ok(read) => received.extend_from_slice(&buffer[..read]),
            }
        }
    }

    /// Whether `connection` ends within `wait` of its last byte.
    fn reaches_end(connection: &mut TcpStream, wait: Duration) -> bool {
        connection.set_read_timeout(Some(wait)).unwrap();
        let mut buffer = vec![0_u8; 1 << 20];
        loop {
            match connection.read(&mut buffer) {
                Ok(0) => return true,
                Ok(_) => {}
                Err(error) => return error.kind() == std::io::ErrorKind::ConnectionReset,
            }
        }
    }
}
