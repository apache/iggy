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

use iggy::prelude::{
    AutoLogin as RustAutoLogin, Credentials as RustCredentials,
    QuicClientConfig as RustQuicClientConfig, QuicClientConfigBuilder,
    QuicClientReconnectionConfig as RustQuicClientReconnectionConfig,
    TcpClientConfig as RustTcpClientConfig, TcpClientConfigBuilder,
    TcpClientReconnectionConfig as RustTcpClientReconnectionConfig,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDelta;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use pyo3_stub_gen::impl_stub_type;
use secrecy::SecretString;
use std::sync::Arc;

use crate::duration::{
    duration_repr, iggy_duration_to_py_delta, millis_repr, millis_to_py_delta,
    py_delta_to_iggy_duration, py_delta_to_millis, reject_zero,
};

/// The credentials replayed by the client every time it (re)connects.
///
/// `IggyClient` only recovers a lost session when it has credentials to replay,
/// so a long-running consumer should pass one of the enabled variants.
#[gen_stub_pyclass]
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct AutoLogin {
    pub(crate) inner: RustAutoLogin,
}

#[gen_stub_pymethods]
#[pymethods]
impl AutoLogin {
    /// No automatic login. `login_user()` must be called by hand after every connect.
    #[staticmethod]
    fn disabled() -> Self {
        Self {
            inner: RustAutoLogin::Disabled,
        }
    }

    /// Log in with the given username and password on every connect.
    #[staticmethod]
    fn username_password(username: String, password: String) -> Self {
        Self {
            inner: RustAutoLogin::Enabled(RustCredentials::UsernamePassword(
                username,
                SecretString::from(password),
            )),
        }
    }

    /// Log in with the given personal access token on every connect.
    #[staticmethod]
    fn personal_access_token(token: String) -> Self {
        Self {
            inner: RustAutoLogin::Enabled(RustCredentials::PersonalAccessToken(
                SecretString::from(token),
            )),
        }
    }

    /// Whether automatic login is enabled.
    #[getter]
    fn enabled(&self) -> bool {
        matches!(self.inner, RustAutoLogin::Enabled(_))
    }

    /// The username to log in with, or `None` for the disabled and token variants.
    #[gen_stub(override_return_type(type_repr = "builtins.str | None"))]
    #[getter]
    fn username(&self) -> Option<String> {
        match &self.inner {
            RustAutoLogin::Enabled(RustCredentials::UsernamePassword(username, _)) => {
                Some(username.clone())
            }
            _ => None,
        }
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            RustAutoLogin::Disabled => "AutoLogin.disabled()".to_owned(),
            RustAutoLogin::Enabled(RustCredentials::UsernamePassword(username, _)) => {
                format!("AutoLogin.username_password({username:?}, ...)")
            }
            RustAutoLogin::Enabled(RustCredentials::PersonalAccessToken(_)) => {
                "AutoLogin.personal_access_token(...)".to_owned()
            }
        }
    }
}

/// How the TCP client reconnects after the connection to the server is lost.
#[gen_stub_pyclass]
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct TcpReconnectionConfig {
    pub(crate) inner: RustTcpClientReconnectionConfig,
}

#[gen_stub_pymethods]
#[pymethods]
impl TcpReconnectionConfig {
    /// Constructs a reconnection policy.
    ///
    /// Args:
    ///     enabled: Whether to reconnect at all. Defaults to enabled.
    ///     max_retries: Passes over the known endpoints after the first, or
    ///         `None` for unlimited; `0` still makes that first pass. One pass
    ///         tries the endpoint the client is on, the address it was
    ///         configured with, and every node the roster named, so this counts
    ///         passes rather than dials. Defaults
    ///         to unlimited, which means a call awaited while the server is
    ///         down never returns: `connect()`, `send_messages()` and
    ///         `poll_messages()` all wait inside the retry loop. Set a finite
    ///         number for request/reply style usage, so a call fails instead.
    ///     interval: Delay between passes. Defaults to 1 second. The first pass
    ///         runs at once when more than one endpoint is known.
    ///     reestablish_after: Cooldown before redialing the endpoint of the last
    ///         successful connection, measured from when it was established, so
    ///         a session that outlived the interval is redialed at once. Owed to
    ///         that endpoint alone. Defaults to 5 seconds.
    ///
    /// Raises:
    ///     ValueError: If a duration is negative, if `max_retries` is outside the
    ///         range of an unsigned 32-bit integer, or if `interval` is zero.
    #[new]
    #[pyo3(signature = (*, enabled=None, max_retries=None, interval=None, reestablish_after=None))]
    fn new(
        #[gen_stub(override_type(type_repr = "builtins.bool | None"))] enabled: Option<bool>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] max_retries: Option<i64>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        interval: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        reestablish_after: Option<Py<PyDelta>>,
    ) -> PyResult<Self> {
        let defaults = RustTcpClientReconnectionConfig::default();
        let enabled = enabled.unwrap_or(defaults.enabled);
        let max_retries = max_retries
            .map(|max_retries| {
                u32::try_from(max_retries).map_err(|_| {
                    PyValueError::new_err(format!(
                        "'max_retries' must be between 0 and {}",
                        u32::MAX
                    ))
                })
            })
            .transpose()?;
        let interval = interval
            .as_ref()
            .map(py_delta_to_iggy_duration)
            .transpose()?
            .map(|interval| reject_zero(interval, "interval"))
            .transpose()?
            .unwrap_or(defaults.interval);
        Ok(Self {
            inner: RustTcpClientReconnectionConfig {
                enabled,
                max_retries,
                interval,
                reestablish_after: reestablish_after
                    .as_ref()
                    .map(py_delta_to_iggy_duration)
                    .transpose()?
                    .unwrap_or(defaults.reestablish_after),
            },
        })
    }

    #[getter]
    fn enabled(&self) -> bool {
        self.inner.enabled
    }

    #[gen_stub(override_return_type(type_repr = "builtins.int | None"))]
    #[getter]
    fn max_retries(&self) -> Option<u32> {
        self.inner.max_retries
    }

    #[gen_stub(override_return_type(type_repr = "datetime.timedelta", imports=("datetime")))]
    #[getter]
    fn interval<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyDelta>> {
        iggy_duration_to_py_delta(py, self.inner.interval.get())
    }

    #[gen_stub(override_return_type(type_repr = "datetime.timedelta", imports=("datetime")))]
    #[getter]
    fn reestablish_after<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyDelta>> {
        iggy_duration_to_py_delta(py, self.inner.reestablish_after)
    }

    fn __repr__(&self) -> String {
        let max_retries = match self.inner.max_retries {
            Some(max_retries) => max_retries.to_string(),
            None => "None".to_owned(),
        };
        format!(
            "TcpReconnectionConfig(enabled={}, max_retries={max_retries}, interval={}, reestablish_after={})",
            python_bool(self.inner.enabled),
            duration_repr(self.inner.interval.get()),
            duration_repr(self.inner.reestablish_after),
        )
    }
}

/// Configuration for the TCP transport, accepted by `IggyClient(...)`.
///
/// Every field is keyword-only and optional.
#[gen_stub_pyclass]
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct TcpConfig {
    inner: Arc<RustTcpClientConfig>,
}

impl TcpConfig {
    /// The configuration in the shape `TcpClient::create` expects.
    pub(crate) fn client_config(&self) -> Arc<RustTcpClientConfig> {
        self.inner.clone()
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl TcpConfig {
    /// Constructs a TCP configuration.
    ///
    /// Args:
    ///     server_address: `host:port` of the Iggy server. Defaults to `127.0.0.1:8090`.
    ///     auto_login: Credentials replayed on every connect. Defaults to `AutoLogin.disabled()`.
    ///     reconnection: Reconnection policy. Defaults to `TcpReconnectionConfig()`.
    ///     heartbeat_interval: Interval of heartbeats sent by the client. Defaults to 5 seconds.
    ///     tls_enabled: Whether to connect over TLS. Defaults to disabled.
    ///     tls_domain: Domain to validate the certificate against. Empty means it is
    ///         taken from `server_address`.
    ///     tls_ca_file: Path to the CA file for TLS. Read only when `tls_enabled`
    ///         and `tls_validate_certificate` are both on; with either one off it
    ///         is kept but never consulted, so pairing it with
    ///         `tls_validate_certificate=False` pins nothing.
    ///     tls_validate_certificate: Whether to validate the server certificate.
    ///         Defaults to validating. Disabling this accepts any certificate the
    ///         server presents, including self-signed and mismatched ones, and
    ///         takes precedence over `tls_ca_file`; intended for local development
    ///         only.
    ///     nodelay: Disable the Nagle algorithm for the TCP socket. Defaults to
    ///         leaving it on.
    ///
    /// Raises:
    ///     ValueError: If `server_address` is not a valid `host:port` pair, if a
    ///         duration is negative, or if `heartbeat_interval` is zero.
    #[new]
    #[pyo3(signature = (
        *,
        server_address=None,
        auto_login=None,
        reconnection=None,
        heartbeat_interval=None,
        tls_enabled=None,
        tls_domain=None,
        tls_ca_file=None,
        tls_validate_certificate=None,
        nodelay=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        #[gen_stub(override_type(type_repr = "builtins.str | None"))] server_address: Option<
            String,
        >,
        #[gen_stub(override_type(type_repr = "AutoLogin | None"))] auto_login: Option<AutoLogin>,
        #[gen_stub(override_type(type_repr = "TcpReconnectionConfig | None"))] reconnection: Option<
            TcpReconnectionConfig,
        >,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        heartbeat_interval: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "builtins.bool | None"))] tls_enabled: Option<bool>,
        #[gen_stub(override_type(type_repr = "builtins.str | None"))] tls_domain: Option<String>,
        #[gen_stub(override_type(type_repr = "builtins.str | None"))] tls_ca_file: Option<String>,
        #[gen_stub(override_type(type_repr = "builtins.bool | None"))]
        tls_validate_certificate: Option<bool>,
        #[gen_stub(override_type(type_repr = "builtins.bool | None"))] nodelay: Option<bool>,
    ) -> PyResult<Self> {
        // The builder starts from `TcpClientConfig::default()`, and its `build()`
        // trims and validates the address whether or not one was set here.
        let mut builder = TcpClientConfigBuilder::new();
        if let Some(server_address) = server_address {
            builder = builder.with_server_address(server_address);
        }
        let mut inner = builder
            .build()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        if let Some(auto_login) = auto_login {
            inner.auto_login = auto_login.inner;
        }
        if let Some(reconnection) = reconnection {
            inner.reconnection = reconnection.inner;
        }
        if let Some(heartbeat_interval) = heartbeat_interval {
            inner.heartbeat_interval = reject_zero(
                py_delta_to_iggy_duration(&heartbeat_interval)?,
                "heartbeat_interval",
            )?;
        }
        if let Some(tls_enabled) = tls_enabled {
            inner.tls_enabled = tls_enabled;
        }
        if let Some(tls_domain) = tls_domain {
            inner.tls_domain = tls_domain;
        }
        if tls_ca_file.is_some() {
            inner.tls_ca_file = tls_ca_file;
        }
        if let Some(tls_validate_certificate) = tls_validate_certificate {
            inner.tls_validate_certificate = tls_validate_certificate;
        }
        if let Some(nodelay) = nodelay {
            inner.nodelay = nodelay;
        }

        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    #[getter]
    fn server_address(&self) -> String {
        self.inner.server_address.clone()
    }

    #[getter]
    fn auto_login(&self) -> AutoLogin {
        AutoLogin {
            inner: self.inner.auto_login.clone(),
        }
    }

    #[getter]
    fn reconnection(&self) -> TcpReconnectionConfig {
        TcpReconnectionConfig {
            inner: self.inner.reconnection.clone(),
        }
    }

    #[gen_stub(override_return_type(type_repr = "datetime.timedelta", imports=("datetime")))]
    #[getter]
    fn heartbeat_interval<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyDelta>> {
        iggy_duration_to_py_delta(py, self.inner.heartbeat_interval.get())
    }

    #[getter]
    fn tls_enabled(&self) -> bool {
        self.inner.tls_enabled
    }

    #[getter]
    fn tls_domain(&self) -> String {
        self.inner.tls_domain.clone()
    }

    #[gen_stub(override_return_type(type_repr = "builtins.str | None"))]
    #[getter]
    fn tls_ca_file(&self) -> Option<String> {
        self.inner.tls_ca_file.clone()
    }

    #[getter]
    fn tls_validate_certificate(&self) -> bool {
        self.inner.tls_validate_certificate
    }

    #[getter]
    fn nodelay(&self) -> bool {
        self.inner.nodelay
    }

    fn __repr__(&self) -> String {
        let tls_ca_file = match &self.inner.tls_ca_file {
            Some(tls_ca_file) => format!("{tls_ca_file:?}"),
            None => "None".to_owned(),
        };
        format!(
            "TcpConfig(server_address={:?}, auto_login={}, reconnection={}, heartbeat_interval={}, tls_enabled={}, tls_domain={:?}, tls_ca_file={tls_ca_file}, tls_validate_certificate={}, nodelay={})",
            self.inner.server_address,
            self.auto_login().__repr__(),
            self.reconnection().__repr__(),
            duration_repr(self.inner.heartbeat_interval.get()),
            python_bool(self.inner.tls_enabled),
            self.inner.tls_domain,
            python_bool(self.inner.tls_validate_certificate),
            python_bool(self.inner.nodelay),
        )
    }
}

/// How the QUIC client reconnects after the connection to the server is lost.
#[gen_stub_pyclass]
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct QuicReconnectionConfig {
    pub(crate) inner: RustQuicClientReconnectionConfig,
}

#[gen_stub_pymethods]
#[pymethods]
impl QuicReconnectionConfig {
    /// Constructs a reconnection policy.
    ///
    /// Args:
    ///     enabled: Whether to reconnect at all. Defaults to enabled.
    ///     max_retries: Passes over the known endpoints after the first, or
    ///         `None` for unlimited; `0` still makes that first pass. One pass
    ///         tries the endpoint the client is on, the address it was
    ///         configured with, and every node the roster named, so this counts
    ///         passes rather than dials. Defaults
    ///         to unlimited, which means a call awaited while the server is
    ///         down never returns: `connect()`, `send_messages()` and
    ///         `poll_messages()` all wait inside the retry loop. Set a finite
    ///         number for request/reply style usage, so a call fails instead.
    ///     interval: Delay between passes. Defaults to 1 second. The first pass
    ///         runs at once when more than one endpoint is known.
    ///     reestablish_after: Cooldown before redialing the endpoint of the last
    ///         successful connection, measured from when it was established, so
    ///         a session that outlived the interval is redialed at once. Owed to
    ///         that endpoint alone. Defaults to 5 seconds.
    ///
    /// Raises:
    ///     ValueError: If a duration is negative, if `max_retries` is outside the
    ///         range of an unsigned 32-bit integer, or if `interval` is zero.
    #[new]
    #[pyo3(signature = (*, enabled=None, max_retries=None, interval=None, reestablish_after=None))]
    fn new(
        #[gen_stub(override_type(type_repr = "builtins.bool | None"))] enabled: Option<bool>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] max_retries: Option<i64>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        interval: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        reestablish_after: Option<Py<PyDelta>>,
    ) -> PyResult<Self> {
        let defaults = RustQuicClientReconnectionConfig::default();
        let enabled = enabled.unwrap_or(defaults.enabled);
        let max_retries = max_retries
            .map(|max_retries| {
                u32::try_from(max_retries).map_err(|_| {
                    PyValueError::new_err(format!(
                        "'max_retries' must be between 0 and {}",
                        u32::MAX
                    ))
                })
            })
            .transpose()?;
        let interval = interval
            .as_ref()
            .map(py_delta_to_iggy_duration)
            .transpose()?
            .map(|interval| reject_zero(interval, "interval"))
            .transpose()?
            .unwrap_or(defaults.interval);
        Ok(Self {
            inner: RustQuicClientReconnectionConfig {
                enabled,
                max_retries,
                interval,
                reestablish_after: reestablish_after
                    .as_ref()
                    .map(py_delta_to_iggy_duration)
                    .transpose()?
                    .unwrap_or(defaults.reestablish_after),
            },
        })
    }

    #[getter]
    fn enabled(&self) -> bool {
        self.inner.enabled
    }

    #[gen_stub(override_return_type(type_repr = "builtins.int | None"))]
    #[getter]
    fn max_retries(&self) -> Option<u32> {
        self.inner.max_retries
    }

    #[gen_stub(override_return_type(type_repr = "datetime.timedelta", imports=("datetime")))]
    #[getter]
    fn interval<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyDelta>> {
        iggy_duration_to_py_delta(py, self.inner.interval.get())
    }

    #[gen_stub(override_return_type(type_repr = "datetime.timedelta", imports=("datetime")))]
    #[getter]
    fn reestablish_after<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyDelta>> {
        iggy_duration_to_py_delta(py, self.inner.reestablish_after)
    }

    fn __repr__(&self) -> String {
        let max_retries = match self.inner.max_retries {
            Some(max_retries) => max_retries.to_string(),
            None => "None".to_owned(),
        };
        format!(
            "QuicReconnectionConfig(enabled={}, max_retries={max_retries}, interval={}, reestablish_after={})",
            python_bool(self.inner.enabled),
            duration_repr(self.inner.interval.get()),
            duration_repr(self.inner.reestablish_after),
        )
    }
}

/// Configuration for the QUIC transport, accepted by `IggyClient(...)`.
///
/// Every field is keyword-only and optional.
#[gen_stub_pyclass]
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct QuicConfig {
    inner: Arc<RustQuicClientConfig>,
}

impl QuicConfig {
    /// The configuration in the shape `QuicClient::create` expects.
    pub(crate) fn client_config(&self) -> Arc<RustQuicClientConfig> {
        self.inner.clone()
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl QuicConfig {
    /// Constructs a QUIC configuration.
    ///
    /// Args:
    ///     server_address: `host:port` of the Iggy server. Defaults to `127.0.0.1:8080`.
    ///     client_address: `host:port` to bind the local UDP socket to. Defaults to
    ///         `127.0.0.1:0`, which binds to any available port.
    ///     server_name: Server name used for the QUIC/TLS handshake. Defaults to
    ///         `localhost`.
    ///     auto_login: Credentials replayed on every connect. Defaults to `AutoLogin.disabled()`.
    ///     reconnection: Reconnection policy. Defaults to `QuicReconnectionConfig()`.
    ///     heartbeat_interval: Interval of heartbeats sent by the client. Defaults to 5 seconds.
    ///     response_buffer_size: Size of the response buffer in bytes. Defaults to 10 MB.
    ///     max_concurrent_bidi_streams: Maximum number of concurrent bidirectional
    ///         streams. Defaults to 10,000.
    ///     datagram_send_buffer_size: Size of the datagram send buffer in bytes.
    ///         Defaults to 100,000.
    ///     initial_mtu: Initial MTU in bytes. Defaults to 1200.
    ///     send_window: Send window size in bytes. Defaults to 100,000.
    ///     receive_window: Receive window size in bytes. Defaults to 100,000.
    ///     keep_alive_interval: Interval between QUIC keep-alive pings, or a zero
    ///         duration to disable them. Defaults to 5 seconds.
    ///     max_idle_timeout: How long the connection tolerates silence before it is
    ///         considered dead, or a zero duration to use quinn's own default (30
    ///         seconds) instead, since `configure()` skips the setter entirely when
    ///         zero. Defaults to 10 seconds.
    ///     validate_certificate: Whether to validate the server certificate. Defaults
    ///         to disabled, unlike the TCP and WebSocket transports.
    ///
    /// Raises:
    ///     ValueError: If `server_address` is not a valid `host:port` pair, if a
    ///         duration is negative, if `heartbeat_interval` is zero, if
    ///         `keep_alive_interval` or `max_idle_timeout` is non-zero but rounds
    ///         down to 0ms, if `initial_mtu` is below quinn's minimum of 1200, or if
    ///         a numeric field is outside the range of its underlying wire type.
    #[new]
    #[pyo3(signature = (
        *,
        server_address=None,
        client_address=None,
        server_name=None,
        auto_login=None,
        reconnection=None,
        heartbeat_interval=None,
        response_buffer_size=None,
        max_concurrent_bidi_streams=None,
        datagram_send_buffer_size=None,
        initial_mtu=None,
        send_window=None,
        receive_window=None,
        keep_alive_interval=None,
        max_idle_timeout=None,
        validate_certificate=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        #[gen_stub(override_type(type_repr = "builtins.str | None"))] server_address: Option<
            String,
        >,
        #[gen_stub(override_type(type_repr = "builtins.str | None"))] client_address: Option<
            String,
        >,
        #[gen_stub(override_type(type_repr = "builtins.str | None"))] server_name: Option<String>,
        #[gen_stub(override_type(type_repr = "AutoLogin | None"))] auto_login: Option<AutoLogin>,
        #[gen_stub(override_type(type_repr = "QuicReconnectionConfig | None"))]
        reconnection: Option<QuicReconnectionConfig>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        heartbeat_interval: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] response_buffer_size: Option<
            i64,
        >,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))]
        max_concurrent_bidi_streams: Option<i64>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))]
        datagram_send_buffer_size: Option<i64>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] initial_mtu: Option<i64>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] send_window: Option<i64>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] receive_window: Option<i64>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        keep_alive_interval: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        max_idle_timeout: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "builtins.bool | None"))] validate_certificate: Option<
            bool,
        >,
    ) -> PyResult<Self> {
        // The builder starts from `QuicClientConfig::default()`, and its `build()`
        // trims and validates the server address whether or not one was set here.
        let mut builder = QuicClientConfigBuilder::new();
        if let Some(server_address) = server_address {
            builder = builder.with_server_address(server_address);
        }
        let mut inner = builder
            .build()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        if let Some(client_address) = client_address {
            inner.client_address = client_address;
        }
        if let Some(server_name) = server_name {
            inner.server_name = server_name;
        }
        if let Some(auto_login) = auto_login {
            inner.auto_login = auto_login.inner;
        }
        if let Some(reconnection) = reconnection {
            inner.reconnection = reconnection.inner;
        }
        if let Some(heartbeat_interval) = heartbeat_interval {
            inner.heartbeat_interval = reject_zero(
                py_delta_to_iggy_duration(&heartbeat_interval)?,
                "heartbeat_interval",
            )?;
        }
        if let Some(response_buffer_size) = response_buffer_size {
            inner.response_buffer_size = u64_param(response_buffer_size, "response_buffer_size")?;
        }
        if let Some(max_concurrent_bidi_streams) = max_concurrent_bidi_streams {
            inner.max_concurrent_bidi_streams =
                varint_param(max_concurrent_bidi_streams, "max_concurrent_bidi_streams")?;
        }
        if let Some(datagram_send_buffer_size) = datagram_send_buffer_size {
            inner.datagram_send_buffer_size =
                u64_param(datagram_send_buffer_size, "datagram_send_buffer_size")?;
        }
        if let Some(initial_mtu) = initial_mtu {
            let initial_mtu = u16_param(initial_mtu, "initial_mtu")?;
            if initial_mtu < QUINN_MIN_INITIAL_MTU {
                return Err(PyValueError::new_err(format!(
                    "'initial_mtu' must be at least {QUINN_MIN_INITIAL_MTU}; quinn silently \
                     raises anything smaller to that floor, so the getter would no longer \
                     match the value actually in effect"
                )));
            }
            inner.initial_mtu = initial_mtu;
        }
        if let Some(send_window) = send_window {
            inner.send_window = u64_param(send_window, "send_window")?;
        }
        if let Some(receive_window) = receive_window {
            inner.receive_window = varint_param(receive_window, "receive_window")?;
        }
        if let Some(keep_alive_interval) = keep_alive_interval {
            inner.keep_alive_interval =
                py_delta_to_millis(&keep_alive_interval, "keep_alive_interval")?;
        }
        if let Some(max_idle_timeout) = max_idle_timeout {
            inner.max_idle_timeout = py_delta_to_millis(&max_idle_timeout, "max_idle_timeout")?;
        }
        if let Some(validate_certificate) = validate_certificate {
            inner.validate_certificate = validate_certificate;
        }

        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    #[getter]
    fn server_address(&self) -> String {
        self.inner.server_address.clone()
    }

    #[getter]
    fn client_address(&self) -> String {
        self.inner.client_address.clone()
    }

    #[getter]
    fn server_name(&self) -> String {
        self.inner.server_name.clone()
    }

    #[getter]
    fn auto_login(&self) -> AutoLogin {
        AutoLogin {
            inner: self.inner.auto_login.clone(),
        }
    }

    #[getter]
    fn reconnection(&self) -> QuicReconnectionConfig {
        QuicReconnectionConfig {
            inner: self.inner.reconnection.clone(),
        }
    }

    #[gen_stub(override_return_type(type_repr = "datetime.timedelta", imports=("datetime")))]
    #[getter]
    fn heartbeat_interval<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyDelta>> {
        iggy_duration_to_py_delta(py, self.inner.heartbeat_interval.get())
    }

    #[gen_stub(override_return_type(type_repr = "builtins.int"))]
    #[getter]
    fn response_buffer_size(&self) -> u64 {
        self.inner.response_buffer_size
    }

    #[gen_stub(override_return_type(type_repr = "builtins.int"))]
    #[getter]
    fn max_concurrent_bidi_streams(&self) -> u64 {
        self.inner.max_concurrent_bidi_streams
    }

    #[gen_stub(override_return_type(type_repr = "builtins.int"))]
    #[getter]
    fn datagram_send_buffer_size(&self) -> u64 {
        self.inner.datagram_send_buffer_size
    }

    #[gen_stub(override_return_type(type_repr = "builtins.int"))]
    #[getter]
    fn initial_mtu(&self) -> u16 {
        self.inner.initial_mtu
    }

    #[gen_stub(override_return_type(type_repr = "builtins.int"))]
    #[getter]
    fn send_window(&self) -> u64 {
        self.inner.send_window
    }

    #[gen_stub(override_return_type(type_repr = "builtins.int"))]
    #[getter]
    fn receive_window(&self) -> u64 {
        self.inner.receive_window
    }

    #[gen_stub(override_return_type(type_repr = "datetime.timedelta", imports=("datetime")))]
    #[getter]
    fn keep_alive_interval<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyDelta>> {
        millis_to_py_delta(py, self.inner.keep_alive_interval)
    }

    #[gen_stub(override_return_type(type_repr = "datetime.timedelta", imports=("datetime")))]
    #[getter]
    fn max_idle_timeout<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyDelta>> {
        millis_to_py_delta(py, self.inner.max_idle_timeout)
    }

    #[getter]
    fn validate_certificate(&self) -> bool {
        self.inner.validate_certificate
    }

    fn __repr__(&self) -> String {
        format!(
            "QuicConfig(server_address={:?}, client_address={:?}, server_name={:?}, auto_login={}, reconnection={}, heartbeat_interval={}, response_buffer_size={}, max_concurrent_bidi_streams={}, datagram_send_buffer_size={}, initial_mtu={}, send_window={}, receive_window={}, keep_alive_interval={}, max_idle_timeout={}, validate_certificate={})",
            self.inner.server_address,
            self.inner.client_address,
            self.inner.server_name,
            self.auto_login().__repr__(),
            self.reconnection().__repr__(),
            duration_repr(self.inner.heartbeat_interval.get()),
            self.inner.response_buffer_size,
            self.inner.max_concurrent_bidi_streams,
            self.inner.datagram_send_buffer_size,
            self.inner.initial_mtu,
            self.inner.send_window,
            self.inner.receive_window,
            millis_repr(self.inner.keep_alive_interval),
            millis_repr(self.inner.max_idle_timeout),
            python_bool(self.inner.validate_certificate),
        )
    }
}

fn python_bool(value: bool) -> &'static str {
    if value { "True" } else { "False" }
}

/// Converts a Python int to the unsigned 64-bit integer a QUIC transport
/// field expects, naming the parameter in the error so a caller can tell
/// which argument was out of range.
fn u64_param(value: i64, parameter: &str) -> PyResult<u64> {
    u64::try_from(value).map_err(|_| {
        PyValueError::new_err(format!("'{parameter}' must be between 0 and {}", u64::MAX))
    })
}

/// Converts a Python int to the unsigned 16-bit integer `initial_mtu` expects.
fn u16_param(value: i64, parameter: &str) -> PyResult<u16> {
    u16::try_from(value).map_err(|_| {
        PyValueError::new_err(format!("'{parameter}' must be between 0 and {}", u16::MAX))
    })
}

/// quinn clamps `TransportConfig::initial_mtu` up to this floor rather than
/// rejecting a smaller value, so `QuicConfig` rejects it instead: otherwise the
/// getter would read back a value that is not the one actually in effect.
const QUINN_MIN_INITIAL_MTU: u16 = 1200;

/// Converts a Python int to a `u64` that also fits `quinn::VarInt` (max
/// `2^62 - 1`), which `max_concurrent_bidi_streams` and `receive_window` are
/// narrowed into when the connection is configured. A `u64` in range for
/// `u64::MAX` but not `VarInt::MAX` would otherwise only fail there, as an
/// opaque `RuntimeError` instead of a `ValueError` naming the argument.
fn varint_param(value: i64, parameter: &str) -> PyResult<u64> {
    const VARINT_MAX: u64 = (1u64 << 62) - 1;
    let value = u64_param(value, parameter)?;
    if value > VARINT_MAX {
        return Err(PyValueError::new_err(format!(
            "'{parameter}' must be between 0 and {VARINT_MAX}"
        )));
    }
    Ok(value)
}

/// What `IggyClient(...)` accepts: a bare `host:port`, a full `TcpConfig`, or a
/// `QuicConfig` for the QUIC transport.
#[derive(FromPyObject)]
pub enum PyClientConfig {
    #[pyo3(transparent)]
    Tcp(TcpConfig),
    #[pyo3(transparent)]
    Quic(QuicConfig),
    #[pyo3(transparent, annotation = "str")]
    ServerAddress(String),
}
impl_stub_type!(PyClientConfig = TcpConfig | QuicConfig | String);
