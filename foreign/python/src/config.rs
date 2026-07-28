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
use pyo3::prelude::*;
use pyo3::types::PyDelta;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use pyo3_stub_gen::impl_stub_type;
use secrecy::SecretString;
use std::sync::Arc;

use crate::duration::{iggy_duration_to_py_delta, py_delta_to_iggy_duration};

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

impl Default for AutoLogin {
    fn default() -> Self {
        Self::disabled()
    }
}

/// How the TCP client reconnects after the connection to the server is lost.
#[gen_stub_pyclass]
#[pyclass(from_py_object)]
#[derive(Clone, Default)]
pub struct TcpReconnectionConfig {
    pub(crate) inner: RustTcpClientReconnectionConfig,
}

#[gen_stub_pymethods]
#[pymethods]
impl TcpReconnectionConfig {
    /// Constructs a reconnection policy, defaulting every unset field to the
    /// value the Rust SDK uses.
    ///
    /// Args:
    ///     enabled: Whether to reconnect at all.
    ///     max_retries: Attempts before giving up, or `None` for unlimited.
    ///     interval: Delay between attempts. Defaults to 1 second.
    ///     reestablish_after: Cooldown before reconnecting after a previously
    ///         successful connection. Defaults to 5 seconds.
    #[new]
    #[pyo3(signature = (*, enabled=true, max_retries=None, interval=None, reestablish_after=None))]
    fn new(
        enabled: bool,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] max_retries: Option<u32>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        interval: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        reestablish_after: Option<Py<PyDelta>>,
    ) -> Self {
        let defaults = RustTcpClientReconnectionConfig::default();
        Self {
            inner: RustTcpClientReconnectionConfig {
                enabled,
                max_retries,
                interval: interval
                    .as_ref()
                    .map_or(defaults.interval, py_delta_to_iggy_duration),
                reestablish_after: reestablish_after
                    .as_ref()
                    .map_or(defaults.reestablish_after, py_delta_to_iggy_duration),
            },
        }
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
            if self.inner.enabled { "True" } else { "False" },
            self.inner.interval.as_human_time_string(),
            self.inner.reestablish_after.as_human_time_string(),
        )
    }
}

/// Configuration for the TCP transport, accepted by `IggyClient(...)`.
///
/// Mirrors `TcpClientConfig` in the Rust SDK. Every field is keyword-only and
/// falls back to the same default the Rust SDK uses.
#[gen_stub_pyclass]
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct TcpConfig {
    auto_login: AutoLogin,
    reconnection: TcpReconnectionConfig,
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
    /// Constructs a TCP configuration, defaulting every unset field to the value
    /// the Rust SDK uses.
    ///
    /// Args:
    ///     server_address: `host:port` of the Iggy server. Defaults to `127.0.0.1:8090`.
    ///     auto_login: Credentials replayed on every connect. Defaults to `AutoLogin.disabled()`.
    ///     reconnection: Reconnection policy. Defaults to `TcpReconnectionConfig()`.
    ///     heartbeat_interval: Interval of heartbeats sent by the client. Defaults to 5 seconds.
    ///     tls_enabled: Whether to connect over TLS.
    ///     tls_domain: Domain to validate the certificate against. Empty means it is
    ///         taken from `server_address`.
    ///     tls_ca_file: Path to the CA file for TLS.
    ///     tls_validate_certificate: Whether to validate the server certificate.
    ///     nodelay: Disable the Nagle algorithm for the TCP socket.
    ///
    /// Raises:
    ///     PyValueError: If `server_address` is not a valid `host:port` pair.
    #[new]
    #[pyo3(signature = (
        *,
        server_address=None,
        auto_login=None,
        reconnection=None,
        heartbeat_interval=None,
        tls_enabled=false,
        tls_domain=None,
        tls_ca_file=None,
        tls_validate_certificate=true,
        nodelay=false,
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
        tls_enabled: bool,
        #[gen_stub(override_type(type_repr = "builtins.str | None"))] tls_domain: Option<String>,
        #[gen_stub(override_type(type_repr = "builtins.str | None"))] tls_ca_file: Option<String>,
        tls_validate_certificate: bool,
        nodelay: bool,
    ) -> PyResult<Self> {
        let defaults = RustTcpClientConfig::default();
        let auto_login = auto_login.unwrap_or_default();
        let reconnection = reconnection.unwrap_or_default();

        let mut builder = TcpClientConfigBuilder::new()
            .with_server_address(server_address.unwrap_or(defaults.server_address))
            .with_auto_sign_in(auto_login.inner.clone())
            .with_tls_enabled(tls_enabled)
            .with_tls_domain(tls_domain.unwrap_or(defaults.tls_domain))
            .with_tls_validate_certificate(tls_validate_certificate);
        if let Some(tls_ca_file) = tls_ca_file {
            builder = builder.with_tls_ca_file(tls_ca_file);
        }
        if nodelay {
            builder = builder.with_no_delay();
        }

        let mut inner = builder
            .build()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        // TcpClientConfigBuilder exposes no setter for either of these.
        inner.heartbeat_interval = heartbeat_interval
            .as_ref()
            .map_or(defaults.heartbeat_interval, py_delta_to_iggy_duration);
        inner.reconnection = reconnection.inner.clone();

        Ok(Self {
            auto_login,
            reconnection,
            inner: Arc::new(inner),
        })
    }

    #[getter]
    fn server_address(&self) -> String {
        self.inner.server_address.clone()
    }

    #[getter]
    fn auto_login(&self) -> AutoLogin {
        self.auto_login.clone()
    }

    #[getter]
    fn reconnection(&self) -> TcpReconnectionConfig {
        self.reconnection.clone()
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
        format!(
            "TcpConfig(server_address={:?}, auto_login={}, reconnection={}, heartbeat_interval={}, tls_enabled={})",
            self.inner.server_address,
            self.auto_login.__repr__(),
            self.reconnection.__repr__(),
            self.inner.heartbeat_interval.as_human_time_string(),
            if self.inner.tls_enabled {
                "True"
            } else {
                "False"
            },
        )
    }
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
