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
    duration_repr, iggy_duration_to_py_delta, py_delta_to_iggy_duration, reject_zero,
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
    ///     max_retries: Attempts before giving up, or `None` for unlimited.
    ///         Defaults to unlimited, which means a call awaited while the server
    ///         is down never returns: `connect()`, `send_messages()` and
    ///         `poll_messages()` all wait inside the retry loop. Set a finite
    ///         number for request/reply style usage, so a call fails instead.
    ///     interval: Delay between attempts. Defaults to 1 second.
    ///     reestablish_after: Cooldown before reconnecting after a previously
    ///         successful connection. Defaults to 5 seconds.
    ///
    /// Raises:
    ///     ValueError: If a duration is negative, or if `interval` is zero while
    ///         reconnection is enabled and `max_retries` is unlimited.
    #[new]
    #[pyo3(signature = (*, enabled=None, max_retries=None, interval=None, reestablish_after=None))]
    fn new(
        #[gen_stub(override_type(type_repr = "builtins.bool | None"))] enabled: Option<bool>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] max_retries: Option<u32>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        interval: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        reestablish_after: Option<Py<PyDelta>>,
    ) -> PyResult<Self> {
        let defaults = RustTcpClientReconnectionConfig::default();
        let enabled = enabled.unwrap_or(defaults.enabled);
        let interval = interval
            .as_ref()
            .map(py_delta_to_iggy_duration)
            .transpose()?
            .unwrap_or(defaults.interval);
        // Unlimited retries at a zero interval reconnect in a continuous loop;
        // a zero interval with a retry cap is a legitimate fast-retry policy, and
        // with reconnection off the interval is never read at all.
        if enabled && interval.is_zero() && max_retries.is_none() {
            return Err(PyValueError::new_err(
                "'interval' must not be zero unless 'max_retries' is set",
            ));
        }
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
        iggy_duration_to_py_delta(py, self.inner.interval)
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
            duration_repr(self.inner.interval),
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
        iggy_duration_to_py_delta(py, self.inner.heartbeat_interval)
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
            duration_repr(self.inner.heartbeat_interval),
            python_bool(self.inner.tls_enabled),
            self.inner.tls_domain,
            python_bool(self.inner.tls_validate_certificate),
            python_bool(self.inner.nodelay),
        )
    }
}

fn python_bool(value: bool) -> &'static str {
    if value { "True" } else { "False" }
}

/// What `IggyClient(...)` accepts: a bare `host:port` or a full `TcpConfig`.
#[derive(FromPyObject)]
pub enum PyClientConfig {
    #[pyo3(transparent)]
    Config(TcpConfig),
    #[pyo3(transparent, annotation = "str")]
    ServerAddress(String),
}
impl_stub_type!(PyClientConfig = TcpConfig | String);
