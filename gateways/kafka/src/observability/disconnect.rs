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

//! Why a Kafka connection closed, and the log line that says so.
//!
//! A cause that someone has to act on logs at warn: a misconfigured client, a slow network, a
//! refused login. A cause that is part of normal work logs at debug: a client that closed, an idle
//! connection, a shutdown. Warnings are rate limited per cause, so a flood of bad connections
//! costs a few lines, and the line after a quiet spell says how many it left out.

use std::fmt::Display;
use std::io;
use std::net::SocketAddr;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use iggy_kafka_wire::KafkaWireError;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

use crate::error::KafkaProtocolError;
use crate::listener::SecurityProtocol;

/// Warnings of one cause that go to the log in each [`LOG_WINDOW`].
const LOG_BURST: u32 = 5;

/// The window of [`LOG_BURST`].
const LOG_WINDOW: Duration = Duration::from_secs(10);

/// Why a Kafka connection closed: the `cause` label of `connections_closed_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CloseCause {
    /// The client closed the connection, or reset it.
    ClientClosed,
    /// No request came within `IGGY_KAFKA_IDLE_TIMEOUT_SECS`, or within
    /// `IGGY_KAFKA_PRE_AUTH_TIMEOUT_SECS` before the client authenticated.
    IdleTimeout,
    /// The gateway shut down.
    Shutdown,
    /// The Iggy user of the connection was deleted, deactivated or renamed.
    UserRevoked,
    /// A request did not arrive in full within `IGGY_KAFKA_READ_TIMEOUT_SECS`.
    ReadTimeout,
    /// The client did not take a response within `IGGY_KAFKA_WRITE_TIMEOUT_SECS`.
    WriteTimeout,
    /// A frame was larger than `IGGY_KAFKA_MAX_FRAME_SIZE`.
    FrameTooLarge,
    /// A frame named a length of zero or less.
    InvalidFrame,
    /// A request header did not decode.
    InvalidRequest,
    /// No response could answer a request: an API key or version the gateway does not serve, or
    /// a response that would not encode.
    RequestRefused,
    /// A Produce with `acks=0` failed. Kafka closes such a connection, so the client refreshes
    /// its metadata.
    UnacknowledgedProduceFailed,
    /// A request came before SASL authentication on a SASL listener.
    IllegalSaslState,
    /// The client asked for a SASL mechanism the gateway does not offer.
    UnsupportedSaslMechanism,
    /// Iggy rejected the credentials, or the token was malformed.
    AuthenticationFailed,
    /// The credentials, or the user a client certificate names, could not be checked: the peer
    /// was throttled after a rejected login, no check slot came free, the check timed out or Iggy
    /// was unreachable. The log line says which.
    AuthenticationDropped,
    /// The TLS handshake failed or did not finish in time.
    TlsHandshakeFailed,
    /// The client certificate names no active Iggy user, or no mapping rule matched it.
    CertificateRejected,
    /// The listener held `IGGY_KAFKA_MAX_CONNECTIONS` and refused the connection.
    ConnectionLimit,
    /// Any other socket error.
    IoError,
    /// A state the gateway's checks rule out.
    InternalError,
}

impl CloseCause {
    /// Every cause, in the order of [`Self::index`].
    const ALL: [Self; 20] = [
        Self::ClientClosed,
        Self::IdleTimeout,
        Self::Shutdown,
        Self::UserRevoked,
        Self::ReadTimeout,
        Self::WriteTimeout,
        Self::FrameTooLarge,
        Self::InvalidFrame,
        Self::InvalidRequest,
        Self::RequestRefused,
        Self::UnacknowledgedProduceFailed,
        Self::IllegalSaslState,
        Self::UnsupportedSaslMechanism,
        Self::AuthenticationFailed,
        Self::AuthenticationDropped,
        Self::TlsHandshakeFailed,
        Self::CertificateRejected,
        Self::ConnectionLimit,
        Self::IoError,
        Self::InternalError,
    ];

    /// The value of the `cause` label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::ClientClosed => "client_closed",
            Self::IdleTimeout => "idle_timeout",
            Self::Shutdown => "shutdown",
            Self::UserRevoked => "user_revoked",
            Self::ReadTimeout => "read_timeout",
            Self::WriteTimeout => "write_timeout",
            Self::FrameTooLarge => "frame_too_large",
            Self::InvalidFrame => "invalid_frame",
            Self::InvalidRequest => "invalid_request",
            Self::RequestRefused => "request_refused",
            Self::UnacknowledgedProduceFailed => "unacknowledged_produce_failed",
            Self::IllegalSaslState => "illegal_sasl_state",
            Self::UnsupportedSaslMechanism => "unsupported_sasl_mechanism",
            Self::AuthenticationFailed => "authentication_failed",
            Self::AuthenticationDropped => "authentication_dropped",
            Self::TlsHandshakeFailed => "tls_handshake_failed",
            Self::CertificateRejected => "certificate_rejected",
            Self::ConnectionLimit => "connection_limit",
            Self::IoError => "io_error",
            Self::InternalError => "internal_error",
        }
    }

    /// What the log line says, with what to check where the cause points at a setting.
    const fn summary(self) -> &'static str {
        match self {
            Self::ClientClosed => "Kafka connection closed by the client",
            Self::IdleTimeout => "Kafka connection closed: no request within the idle timeout",
            Self::Shutdown => "Kafka connection closed by the gateway's shutdown",
            Self::UserRevoked => {
                "Kafka connection closed: its Iggy user was deleted, deactivated or renamed"
            }
            Self::ReadTimeout => {
                "Kafka connection closed: a request did not arrive in full within \
                 IGGY_KAFKA_READ_TIMEOUT_SECS"
            }
            Self::WriteTimeout => {
                "Kafka connection closed: the client did not take a response within \
                 IGGY_KAFKA_WRITE_TIMEOUT_SECS"
            }
            Self::FrameTooLarge => {
                "Kafka connection closed: a frame is larger than IGGY_KAFKA_MAX_FRAME_SIZE. A \
                 TLS or HTTP client on a plaintext listener looks the same"
            }
            Self::InvalidFrame => "Kafka connection closed: a frame has an invalid length",
            Self::InvalidRequest => "Kafka connection closed: a request header does not decode",
            Self::RequestRefused => {
                "Kafka connection closed: no response can answer a request, such as an API key \
                 or version the gateway does not serve"
            }
            Self::UnacknowledgedProduceFailed => {
                "Kafka connection closed after a Produce with acks=0 failed, so the client \
                 refreshes its metadata"
            }
            Self::IllegalSaslState => {
                "Kafka connection closed: a request came before SASL authentication. Check that \
                 the client uses SASL on this listener"
            }
            Self::UnsupportedSaslMechanism => {
                "Kafka connection closed: the client asked for a SASL mechanism other than PLAIN"
            }
            Self::AuthenticationFailed => "Kafka connection closed: SASL authentication failed",
            Self::AuthenticationDropped => {
                "Kafka connection closed: the credentials could not be checked"
            }
            Self::TlsHandshakeFailed => "Kafka connection closed: the TLS handshake failed",
            Self::CertificateRejected => {
                "Kafka connection closed: the client certificate names no active Iggy user"
            }
            Self::ConnectionLimit => {
                "Kafka connection refused: the listener holds its maximum of connections"
            }
            Self::IoError => "Kafka connection closed by a socket error",
            Self::InternalError => "Kafka connection closed by a gateway fault",
        }
    }

    /// The level of the log line. Warn and error lines are rate limited.
    const fn level(self) -> Level {
        match self {
            Self::ClientClosed | Self::IdleTimeout | Self::Shutdown => Level::Debug,
            Self::UserRevoked => Level::Info,
            Self::InternalError => Level::Error,
            Self::ReadTimeout
            | Self::WriteTimeout
            | Self::FrameTooLarge
            | Self::InvalidFrame
            | Self::InvalidRequest
            | Self::RequestRefused
            | Self::UnacknowledgedProduceFailed
            | Self::IllegalSaslState
            | Self::UnsupportedSaslMechanism
            | Self::AuthenticationFailed
            | Self::AuthenticationDropped
            | Self::TlsHandshakeFailed
            | Self::CertificateRejected
            | Self::ConnectionLimit
            | Self::IoError => Level::Warn,
        }
    }

    const fn index(self) -> usize {
        self as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Level {
    Debug,
    Info,
    Warn,
    Error,
}

/// One closed connection: its cause, and what the log line adds to it.
#[derive(Debug)]
pub struct Disconnect {
    cause: CloseCause,
    detail: Option<String>,
}

impl Disconnect {
    #[must_use]
    pub const fn new(cause: CloseCause) -> Self {
        Self {
            cause,
            detail: None,
        }
    }

    /// `cause`, with `detail` in the log line: the error, the request, or the setting involved.
    #[must_use]
    pub fn with_detail(cause: CloseCause, detail: impl Display) -> Self {
        Self {
            cause,
            detail: Some(detail.to_string()),
        }
    }

    #[must_use]
    pub const fn cause(&self) -> CloseCause {
        self.cause
    }

    #[must_use]
    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }

    /// The disconnect that `error` ends a connection with.
    #[must_use]
    pub fn from_error(error: &KafkaProtocolError) -> Self {
        match error {
            KafkaProtocolError::Io(error) => Self::from_io(error),
            KafkaProtocolError::Wire(KafkaWireError::FrameTooLarge { .. }) => {
                Self::with_detail(CloseCause::FrameTooLarge, error)
            }
            KafkaProtocolError::Wire(KafkaWireError::InvalidFrameLength(_)) => {
                Self::with_detail(CloseCause::InvalidFrame, error)
            }
            KafkaProtocolError::Wire(_) => Self::with_detail(CloseCause::InvalidRequest, error),
            KafkaProtocolError::InvalidConfig(_) | KafkaProtocolError::Bridge(_) => {
                Self::with_detail(CloseCause::InternalError, error)
            }
        }
    }

    fn from_io(error: &io::Error) -> Self {
        if let Some(timeout) = FrameTimeout::of(error) {
            return Self::new(match timeout {
                FrameTimeout::Idle => CloseCause::IdleTimeout,
                FrameTimeout::Read => CloseCause::ReadTimeout,
                FrameTimeout::Write => CloseCause::WriteTimeout,
            });
        }
        match error.kind() {
            io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe => Self::new(CloseCause::ClientClosed),
            _ => Self::with_detail(CloseCause::IoError, error),
        }
    }
}

/// Which timeout of a connection ran out. The inner error of the `TimedOut` I/O error that ends
/// the read or the write, so the disconnect names the right setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FrameTimeout {
    #[error("idle timeout")]
    Idle,
    #[error("read timeout")]
    Read,
    #[error("write timeout")]
    Write,
}

impl FrameTimeout {
    /// The `TimedOut` I/O error of this timeout.
    #[must_use]
    pub fn into_io(self) -> io::Error {
        io::Error::new(io::ErrorKind::TimedOut, self)
    }

    fn of(error: &io::Error) -> Option<Self> {
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<Self>())
            .copied()
    }
}

/// The log of closed connections, with the rate limit of its warnings.
#[derive(Debug, Default)]
pub(super) struct DisconnectLog {
    windows: Mutex<[Window; CloseCause::ALL.len()]>,
}

/// The warnings of one cause in the current [`LOG_WINDOW`].
#[derive(Debug, Default, Clone, Copy)]
struct Window {
    started: Option<Instant>,
    logged: u32,
    /// Warnings left out since the last one logged.
    left_out: u64,
}

impl DisconnectLog {
    pub(super) fn log(
        &self,
        peer: SocketAddr,
        listener: SecurityProtocol,
        disconnect: &Disconnect,
    ) {
        let cause = disconnect.cause;
        let detail = disconnect.detail.as_deref().unwrap_or("");
        let summary = cause.summary();
        match cause.level() {
            Level::Debug => {
                debug!(%peer, %listener, cause = cause.label(), detail, "{summary}");
            }
            Level::Info => info!(%peer, %listener, cause = cause.label(), detail, "{summary}"),
            Level::Warn => {
                if let Some(left_out) = self.admit(cause, Instant::now()) {
                    warn!(%peer, %listener, cause = cause.label(), detail, left_out, "{summary}");
                }
            }
            Level::Error => {
                if let Some(left_out) = self.admit(cause, Instant::now()) {
                    error!(%peer, %listener, cause = cause.label(), detail, left_out, "{summary}");
                }
            }
        }
    }

    /// Whether a line of `cause` may go to the log at `now`, and how many lines of it were left
    /// out since the last one. `None` leaves this one out.
    fn admit(&self, cause: CloseCause, now: Instant) -> Option<u64> {
        let mut windows = self.windows.lock().unwrap_or_else(PoisonError::into_inner);
        let admitted = windows[cause.index()].admit(now);
        drop(windows);
        admitted
    }
}

impl Window {
    /// See [`DisconnectLog::admit`].
    fn admit(&mut self, now: Instant) -> Option<u64> {
        let expired = self
            .started
            .is_none_or(|started| now.duration_since(started) >= LOG_WINDOW);
        if expired {
            self.started = Some(now);
            self.logged = 0;
        }
        if self.logged >= LOG_BURST {
            self.left_out += 1;
            return None;
        }
        self.logged += 1;
        Some(std::mem::take(&mut self.left_out))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tracing::field::{Field, Visit};
    use tracing::{Event, Subscriber};
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

    use super::*;

    /// One log line: its level, its `cause` and its `left_out`.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Line {
        level: tracing::Level,
        cause: String,
        left_out: Option<u64>,
    }

    /// Collects the log lines written while it is the default subscriber.
    #[derive(Clone, Default)]
    struct Lines(Arc<Mutex<Vec<Line>>>);

    impl Lines {
        fn take(&self) -> Vec<Line> {
            std::mem::take(&mut self.0.lock().unwrap())
        }
    }

    impl<S: Subscriber> Layer<S> for Lines {
        fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
            let mut fields = Fields::default();
            event.record(&mut fields);
            self.0.lock().unwrap().push(Line {
                level: *event.metadata().level(),
                cause: fields.cause,
                left_out: fields.left_out,
            });
        }
    }

    #[derive(Default)]
    struct Fields {
        cause: String,
        left_out: Option<u64>,
    }

    impl Visit for Fields {
        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "cause" {
                self.cause = value.to_string();
            }
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            if field.name() == "left_out" {
                self.left_out = Some(value);
            }
        }

        fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
    }

    /// Runs `log` with `lines` collecting what it writes.
    fn logged(lines: &Lines, log: impl FnOnce()) {
        let subscriber = tracing_subscriber::registry().with(lines.clone());
        tracing::subscriber::with_default(subscriber, log);
    }

    fn peer() -> SocketAddr {
        "10.0.0.7:50000".parse().unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn given_each_cause_should_log_it_at_the_level_of_who_acts_on_it() {
        let lines = Lines::default();
        logged(&lines, || {
            let log = DisconnectLog::default();
            for cause in CloseCause::ALL {
                log.log(peer(), SecurityProtocol::Plaintext, &Disconnect::new(cause));
            }
        });

        let levels: Vec<(String, tracing::Level)> = lines
            .take()
            .into_iter()
            .map(|line| (line.cause, line.level))
            .collect();
        let expected: Vec<(String, tracing::Level)> = CloseCause::ALL
            .iter()
            .map(|cause| {
                let level = match cause {
                    CloseCause::ClientClosed | CloseCause::IdleTimeout | CloseCause::Shutdown => {
                        tracing::Level::DEBUG
                    }
                    CloseCause::UserRevoked => tracing::Level::INFO,
                    CloseCause::InternalError => tracing::Level::ERROR,
                    _ => tracing::Level::WARN,
                };
                (cause.label().to_string(), level)
            })
            .collect();
        assert_eq!(levels, expected);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_flood_of_warnings_should_log_five_each_ten_seconds() {
        let lines = Lines::default();
        let log = DisconnectLog::default();
        let flood = |cause: CloseCause, times: usize| {
            logged(&lines, || {
                for _ in 0..times {
                    log.log(peer(), SecurityProtocol::Plaintext, &Disconnect::new(cause));
                }
            });
        };

        flood(CloseCause::FrameTooLarge, 8);
        flood(CloseCause::InternalError, 8);
        let first = lines.take();
        assert_eq!(
            first
                .iter()
                .filter(|line| line.cause == "frame_too_large")
                .count(),
            5,
            "{first:?}"
        );
        assert_eq!(
            first
                .iter()
                .filter(|line| line.cause == "internal_error")
                .count(),
            5,
            "errors are limited too: {first:?}"
        );

        tokio::time::advance(Duration::from_millis(9_999)).await;
        flood(CloseCause::FrameTooLarge, 1);
        assert_eq!(lines.take(), [], "the window lasts 10 s");

        tokio::time::advance(Duration::from_millis(1)).await;
        flood(CloseCause::FrameTooLarge, 1);
        assert_eq!(
            lines.take(),
            [Line {
                level: tracing::Level::WARN,
                cause: "frame_too_large".to_string(),
                left_out: Some(4),
            }],
            "the first line of a new window says how many were left out"
        );

        flood(CloseCause::ClientClosed, 8);
        assert_eq!(lines.take().len(), 8, "debug lines are not limited");
    }

    #[test]
    fn given_the_causes_should_index_each_one_at_its_place() {
        for (index, cause) in CloseCause::ALL.iter().enumerate() {
            assert_eq!(cause.index(), index, "{cause:?}");
        }
    }

    #[test]
    fn given_a_flood_of_one_cause_should_log_a_burst_then_say_how_many_it_left_out() {
        let log = DisconnectLog::default();
        let start = Instant::now();
        let admitted: Vec<Option<u64>> = (0..8)
            .map(|_| log.admit(CloseCause::FrameTooLarge, start))
            .collect();

        assert_eq!(
            admitted,
            [
                Some(0),
                Some(0),
                Some(0),
                Some(0),
                Some(0),
                None,
                None,
                None
            ]
        );
        assert_eq!(
            log.admit(CloseCause::IdleTimeout, start),
            Some(0),
            "every cause has a window of its own"
        );
        assert_eq!(
            log.admit(CloseCause::FrameTooLarge, start + LOG_WINDOW),
            Some(3),
            "the first line of the next window names the lines left out"
        );
        assert_eq!(
            log.admit(CloseCause::FrameTooLarge, start + LOG_WINDOW),
            Some(0)
        );
    }

    #[test]
    fn given_connection_errors_should_name_the_cause_an_operator_acts_on() {
        let cases = [
            (
                KafkaProtocolError::Io(FrameTimeout::Read.into_io()),
                CloseCause::ReadTimeout,
            ),
            (
                KafkaProtocolError::Io(FrameTimeout::Idle.into_io()),
                CloseCause::IdleTimeout,
            ),
            (
                KafkaProtocolError::Io(FrameTimeout::Write.into_io()),
                CloseCause::WriteTimeout,
            ),
            (
                KafkaProtocolError::Io(io::Error::from(io::ErrorKind::UnexpectedEof)),
                CloseCause::ClientClosed,
            ),
            (
                KafkaProtocolError::Io(io::Error::from(io::ErrorKind::ConnectionReset)),
                CloseCause::ClientClosed,
            ),
            (
                KafkaProtocolError::Io(io::Error::from(io::ErrorKind::TimedOut)),
                CloseCause::IoError,
            ),
            (
                KafkaWireError::FrameTooLarge {
                    max_bytes: 8,
                    actual_bytes: 9,
                }
                .into(),
                CloseCause::FrameTooLarge,
            ),
            (
                KafkaWireError::InvalidFrameLength(-1).into(),
                CloseCause::InvalidFrame,
            ),
            (
                KafkaWireError::Malformed("header".into()).into(),
                CloseCause::InvalidRequest,
            ),
        ];
        for (error, cause) in cases {
            assert_eq!(Disconnect::from_error(&error).cause(), cause, "{error}");
        }
    }

    #[test]
    fn given_a_timeout_should_keep_the_message_of_its_io_error() {
        assert_eq!(FrameTimeout::Read.into_io().to_string(), "read timeout");
        assert_eq!(FrameTimeout::Idle.into_io().to_string(), "idle timeout");
    }
}
