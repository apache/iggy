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

use bytes::Bytes;
use iggy::prelude::{
    Consumer as RustConsumer, IggyClient as RustIggyClient, IggyExpiry as RustIggyExpiry,
    IggyMessage as RustMessage, MaxTopicSize as RustMaxTopicSize, OptionKey as RustOptionKey,
    Partitioning as RustPartitioning, PollOptions as RustPollOptions,
    PollingStrategy as RustPollingStrategy, ReadError as RustReadError, *,
};
use pyo3::PyRef;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDelta, PyList, PyType};
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::define_stub_info_gatherer;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::PyClientConfig;
use crate::consumer::{
    ClientCommitPolicy, Consumer as PyConsumer, ConsumerGroup as PyConsumerGroup,
    ConsumerGroupDetails as PyConsumerGroupDetails, IggyConsumer,
};
use crate::duration::{py_delta_to_iggy_duration, reject_zero};
use crate::identifier::PyIdentifier;
use crate::metadata_changes::{
    MetadataChangeGroup, MetadataChangeStart, MetadataChanges, validated_query,
};
use crate::options::OptionSpec as PyOptionSpec;
use crate::partitioning::PyPartitioning;
use crate::permissions::Permissions as PyPermissions;
use crate::poll_options::PollOptions;
use crate::producer::{IggyProducer, ProducerMode, u32_param as producer_u32_param};
use crate::reader::PartitionReader;
use crate::receive_message::{PolledBatch, PollingStrategy};
use crate::resource_options::{OptionKey, StreamOptions, TopicOptions, UserOptions};
use crate::send_message::{SendMessage, SendMessagesResponse as PySendMessagesResponse};
use crate::stats::Stats as PyStats;
use crate::stream::{Stream, StreamDetails};
use crate::topic::{IggyExpiry, MaxTopicSize, Topic, TopicDetails};
use crate::user::{
    IdentityInfo as PyIdentityInfo, UserInfo as PyUserInfo, UserInfoDetails as PyUserInfoDetails,
    UserStatus as PyUserStatus,
};
use tokio::sync::Mutex;

const DEFAULT_RESOLVE_TIMEOUT: Duration = Duration::from_secs(30);

/// A Python class representing the Iggy client.
/// It provides asynchronous functionality through the contained runtime.
#[gen_stub_pyclass]
#[pyclass]
pub struct IggyClient {
    inner: Arc<RustIggyClient>,
}

/// An error of the Iggy SDK. `code` is the numeric Iggy error code, the same
/// code that every other Iggy SDK reports for it.
#[gen_stub_pyclass]
#[pyclass(frozen, subclass, name = "IggyError", extends=PyRuntimeError)]
pub struct PyIggyError {
    #[pyo3(get)]
    code: u32,
}

impl From<&IggyError> for PyIggyError {
    fn from(error: &IggyError) -> Self {
        Self {
            code: error.as_code(),
        }
    }
}

/// An operation whose retained identity is needed to recover or release its lane.
#[gen_stub_pyclass]
#[pyclass(frozen, extends=PyIggyError)]
pub struct PendingOperationError {
    #[pyo3(get)]
    operation_id: u64,
    #[pyo3(get)]
    kind: &'static str,
    #[pyo3(get)]
    partition_id: Option<u32>,
}

/// An option refusal, with the server error code and its snake_case name as `kind`.
#[gen_stub_pyclass]
#[pyclass(frozen, extends=PyIggyError)]
pub struct OptionError {
    #[pyo3(get)]
    kind: &'static str,
}

/// A failed read of a `PartitionReader` or an `IggyConsumer`. The reader stays usable after
/// it and retries by itself. `code` is the code of `error`.
#[gen_stub_pyclass]
#[pyclass(frozen, extends=PyIggyError)]
pub struct ReadError {
    /// The partition of the error, or `None` for an error of the whole reader.
    #[pyo3(get)]
    partition_id: Option<u32>,
    error: Py<PyAny>,
    skip: Option<RustPollingStrategy>,
}

#[gen_stub_pymethods]
#[pymethods]
impl ReadError {
    /// The error as the client raises it for any other call: an `IggyError`, or a
    /// `PendingOperationError` with the operation to abandon.
    #[gen_stub(override_return_type(type_repr = "IggyError"))]
    #[getter]
    fn error(&self, py: Python<'_>) -> Py<PyAny> {
        self.error.clone_ref(py)
    }

    /// For a message that does not decode: the continuation after it. The messages before it
    /// arrive in a batch before this error, and the reader reads the message again until
    /// `seek()` to this continuation skips it.
    #[gen_stub(override_return_type(type_repr = "PollingStrategy | None"))]
    #[getter]
    fn skip(&self) -> Option<PollingStrategy> {
        self.skip.map(PollingStrategy::from)
    }
}

/// The JSON error body the HTTP API returns.
#[derive(Deserialize)]
struct HttpErrorBody {
    id: u32,
    reason: String,
}

/// Raises every SDK error with its code, and an operation with its recovery
/// identity, rather than requiring callers to parse an error message.
pub(crate) fn to_runtime_error(error: IggyError) -> PyErr {
    let pending = match error {
        IggyError::OperationUncertain(operation_id) => Some((operation_id, "uncertain", None)),
        IggyError::FetchProgressUncertain(operation_id) => {
            Some((operation_id, "fetch_progress_uncertain", None))
        }
        IggyError::LaneBlocked(operation_id) => Some((operation_id, "lane_blocked", None)),
        IggyError::PartitionLaneBlocked(partition_id, operation_id) => {
            Some((operation_id, "lane_blocked", Some(partition_id)))
        }
        IggyError::OperationNotRetained(operation_id) => Some((operation_id, "not_retained", None)),
        _ => {
            if let Some(refusal) = to_option_error(&error) {
                return refusal;
            }
            None
        }
    };
    Python::attach(|py| {
        let coded = PyIggyError::from(&error);
        let instance = match pending {
            Some((operation_id, kind, partition_id)) => Bound::new(
                py,
                PyClassInitializer::from(coded).add_subclass(PendingOperationError {
                    operation_id,
                    kind,
                    partition_id,
                }),
            )
            .map(Bound::into_any),
            None => Bound::new(py, coded).map(Bound::into_any),
        };
        let instance = match instance {
            Ok(instance) => instance,
            Err(error) => return error,
        };
        if let Err(error) = instance.setattr("args", (error.to_string(),)) {
            return error;
        }
        PyErr::from_value(instance)
    })
}

/// Raises a read error with its partition and skip, and its cause as `to_runtime_error` raises it.
pub(crate) fn to_read_error(error: RustReadError) -> PyErr {
    let message = error.to_string();
    let coded = PyIggyError::from(&error.error);
    let cause = to_runtime_error(error.error);
    Python::attach(|py| {
        let read_error = ReadError {
            partition_id: error.partition_id,
            error: cause.into_value(py).into_any(),
            skip: error.skip,
        };
        let instance =
            match Bound::new(py, PyClassInitializer::from(coded).add_subclass(read_error)) {
                Ok(instance) => instance,
                Err(error) => return error,
            };
        if let Err(error) = instance.setattr("args", (message,)) {
            return error;
        }
        PyErr::from_value(instance.into_any())
    })
}

/// Every transport reports an option refusal with the same code: binary by the error itself,
/// HTTP by the `id` of its JSON error body.
fn to_option_error(error: &IggyError) -> Option<PyErr> {
    let (code, message) = match error {
        IggyError::HttpResponseError(_, body) => {
            let body = serde_json::from_str::<HttpErrorBody>(body).ok()?;
            (body.id, body.reason)
        }
        _ => (error.as_code(), error.to_string()),
    };
    if !is_option_error(&IggyError::from_code(code)) {
        return None;
    }
    let kind = IggyError::from_code_as_string(code);
    Some(Python::attach(|py| {
        let instance = match Bound::new(
            py,
            PyClassInitializer::from(PyIggyError { code }).add_subclass(OptionError { kind }),
        ) {
            Ok(instance) => instance,
            Err(error) => return error,
        };
        if let Err(error) = instance.setattr("args", (message,)) {
            return error;
        }
        PyErr::from_value(instance.into_any())
    }))
}

fn is_option_error(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::UnsupportedOptionKey(_)
            | IggyError::InvalidOptionValue(_)
            | IggyError::OptionsBlockTooLarge(_)
            | IggyError::OptionNotUpdatable(_)
            | IggyError::OptionNotResettable(_)
            | IggyError::ReadOnlyOption(_)
            | IggyError::StreamEncryptionUnavailable
            | IggyError::UnsupportedCriticalOption(_)
            | IggyError::CannotModifyRootUser
    )
}

fn to_value_error<E: Display>(error: E) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyValueError, _>(error.to_string())
}

fn reset_keys(reset: Option<Vec<OptionKey>>) -> BTreeSet<RustOptionKey> {
    reset
        .into_iter()
        .flatten()
        .map(RustOptionKey::from)
        .collect()
}

#[gen_stub_pymethods]
#[pymethods]
impl IggyClient {
    /// Constructs a new IggyClient from a TCP server address, a `TcpConfig`, a
    /// `QuicConfig`, an `HttpConfig`, or a `WebSocketConfig`. Construction is
    /// synchronous; async methods return asyncio awaitables backed by the shared
    /// Tokio runtime.
    ///
    /// Args:
    ///     conn: A `host:port` address, a `TcpConfig`, a `QuicConfig`, an
    ///         `HttpConfig`, or a `WebSocketConfig`. Defaults to `127.0.0.1:8090`
    ///         over TCP with auto-login disabled. A malformed address is reported
    ///         differently depending on the form: the string form raises
    ///         `RuntimeError` here, while every config type raises `ValueError`
    ///         when it is constructed, before any of them reaches this call. Neither
    ///         exception is a subclass of the other.
    ///
    /// Raises:
    ///     RuntimeError: If the address passed as a string is not a valid
    ///         `host:port` pair. QUIC binds its local UDP socket during
    ///         `connect()`, which reports a port already in use.
    #[new]
    #[pyo3(signature = (conn=None))]
    fn new(
        #[gen_stub(override_type(
            type_repr = "TcpConfig | QuicConfig | HttpConfig | WebSocketConfig | builtins.str | None"
        ))]
        conn: Option<PyClientConfig>,
    ) -> PyResult<Self> {
        let wrapper = match conn {
            Some(PyClientConfig::Tcp(config)) => ClientWrapper::Tcp(
                TcpClient::create(config.client_config()).map_err(to_runtime_error)?,
            ),
            Some(PyClientConfig::ServerAddress(server_address)) => {
                let config = Arc::new(
                    TcpClientConfigBuilder::new()
                        .with_server_address(server_address)
                        .build()
                        .map_err(to_runtime_error)?,
                );
                ClientWrapper::Tcp(TcpClient::create(config).map_err(to_runtime_error)?)
            }
            Some(PyClientConfig::Quic(config)) => ClientWrapper::Quic(
                QuicClient::create(config.client_config()).map_err(to_runtime_error)?,
            ),
            Some(PyClientConfig::Http(config)) => ClientWrapper::Http(
                HttpClient::create(config.client_config()).map_err(to_runtime_error)?,
            ),
            Some(PyClientConfig::WebSocket(config)) => ClientWrapper::WebSocket(
                WebSocketClient::create(config.client_config()).map_err(to_runtime_error)?,
            ),
            None => ClientWrapper::Tcp(
                TcpClient::create(Arc::new(TcpClientConfig::default()))
                    .map_err(to_runtime_error)?,
            ),
        };
        Ok(Self {
            inner: Arc::new(RustIggyClient::new(wrapper)),
        })
    }

    /// Constructs a new IggyClient from a connection string.
    /// Returns an error if the connection string provided is invalid.
    // TODO: add examples for connection strings or at least a link to the doc page where
    // connection strings are explained.
    #[classmethod]
    #[pyo3(signature = (connection_string))]
    fn from_connection_string(
        _cls: &Bound<'_, PyType>,
        connection_string: String,
    ) -> PyResult<Self> {
        let client =
            RustIggyClient::from_connection_string(&connection_string).map_err(to_runtime_error)?;
        Ok(Self {
            inner: Arc::new(client),
        })
    }

    /// Sends a ping request to the server to check connectivity.
    /// Raises `RuntimeError` if the connection fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn ping<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(
            py,
            async move { inner.ping().await.map_err(to_runtime_error) },
        )
    }

    /// Get the statistics and details of the server and its running process.
    ///
    /// Requires an authenticated session whose user holds the `read_servers`
    /// or `manage_servers` global permission.
    ///
    /// Returns:
    ///     An awaitable that resolves to `Stats`.
    ///
    /// Raises:
    ///     RuntimeError: If the client is not connected, the session is not
    ///         authenticated, the user lacks the permission, or the request
    ///         fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[Stats]", imports=("collections.abc")))]
    fn get_stats<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let stats = inner.get_stats().await.map_err(to_runtime_error)?;
            Ok(PyStats::from(stats))
        })
    }

    /// Describe the option catalog for a resource scope.
    ///
    /// This is the discovery surface for the `options` argument on
    /// `create_topic`/`update_topic`: a key outside the catalog is refused at
    /// create, and the binary transports carry only the error code back.
    ///
    /// Args:
    ///     scope: One of `"topic"`, `"stream"`, `"user"`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `list[OptionSpec]`, empty for a scope
    ///     with no keys yet.
    ///
    /// Raises:
    ///     ValueError: If the scope name is not one of the three above.
    ///     RuntimeError: If the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[list[OptionSpec]]", imports=("collections.abc")))]
    fn describe_options<'a>(&self, py: Python<'a>, scope: &str) -> PyResult<Bound<'a, PyAny>> {
        let scope = crate::options::options_scope_from_str(scope)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let specs = inner
                .describe_options(scope)
                .await
                .map_err(to_runtime_error)?;
            Ok(specs
                .into_iter()
                .map(PyOptionSpec::from)
                .collect::<Vec<_>>())
        })
    }

    /// Reads one page of the metadata change log, or of the current state.
    ///
    /// Requires root or the global `manage_servers` permission. Keep the
    /// page's `next` cursor and send it back in `MetadataChangeStart.After`.
    ///
    /// Args:
    ///     start: Where to start, as `MetadataChangeStart`.
    ///     groups: The groups to read as `list[MetadataChangeGroup] | None`.
    ///         `None` or an empty list reads every group. A current state scan
    ///         needs the parent groups of each group, for example `Topics`
    ///         with `Streams`.
    ///     since: Keep records prepared at or after this Unix time in
    ///         microseconds. Only with `Earliest` and `After`.
    ///     until: Keep records prepared at or before this Unix time in
    ///         microseconds. Only with `Earliest` and `After`.
    ///     limit: Records or items per page, 1 to 1000. Defaults to 100.
    ///
    /// Returns:
    ///     An awaitable that resolves to `MetadataChanges`.
    ///
    /// Raises:
    ///     IggyError: With the invalid command code before anything is sent
    ///         when the query is refused, or when the request fails. A user
    ///         without the permission gets the unauthorized code.
    #[pyo3(signature = (start, *, groups=None, since=None, until=None, limit=None))]
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[MetadataChanges]", imports=("collections.abc")))]
    fn get_metadata_changes<'a>(
        &self,
        py: Python<'a>,
        start: &MetadataChangeStart,
        #[gen_stub(override_type(type_repr = "builtins.list[MetadataChangeGroup] | None"))]
        groups: Option<Vec<MetadataChangeGroup>>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] since: Option<u64>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] until: Option<u64>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] limit: Option<u32>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let query = validated_query(start, groups, since, until, limit)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let changes = inner
                .get_metadata_changes(&query)
                .await
                .map_err(to_runtime_error)?;
            MetadataChanges::try_from(changes)
        })
    }

    /// Logs in the user with the given credentials.
    ///
    /// Returns:
    ///     An awaitable that resolves to `IdentityInfo`.
    ///     `access_token` is set only on HTTP; binary transports leave it `None`.
    ///
    /// Raises:
    ///     RuntimeError: If the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[IdentityInfo]", imports=("collections.abc")))]
    fn login_user<'a>(
        &self,
        py: Python<'a>,
        username: String,
        password: String,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let identity = inner
                .login_user(&username, &password)
                .await
                .map_err(to_runtime_error)?;
            Ok(PyIdentityInfo::from(identity))
        })
    }

    /// Logs in with a personal access token created separately.
    ///
    /// Args:
    ///     token: Raw PAT string.
    ///
    /// Returns:
    ///     An awaitable that resolves to `IdentityInfo`.
    ///
    /// Raises:
    ///     RuntimeError: If the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[IdentityInfo]", imports=("collections.abc")))]
    fn login_with_personal_access_token<'a>(
        &self,
        py: Python<'a>,
        token: String,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let identity = inner
                .login_with_personal_access_token(&token)
                .await
                .map_err(to_runtime_error)?;
            Ok(PyIdentityInfo::from(identity))
        })
    }

    /// Get the info about a specific user by unique ID or username.
    ///
    /// Args:
    ///     user_id: User identifier as `str | int`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `UserInfoDetails` if the user exists,
    ///     or `None` otherwise.
    ///
    /// Raises:
    ///     ValueError: If a string identifier is invalid.
    ///     RuntimeError: If the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[UserInfoDetails | None]", imports=("collections.abc")))]
    fn get_user<'a>(&self, py: Python<'a>, user_id: PyIdentifier) -> PyResult<Bound<'a, PyAny>> {
        let user_id = Identifier::try_from(user_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let user = inner.get_user(&user_id).await.map_err(to_runtime_error)?;
            Ok(user.map(PyUserInfoDetails::from))
        })
    }

    /// Get the info about all the users.
    ///
    /// Returns:
    ///     An awaitable that resolves to `list[UserInfo]`.
    ///
    /// Raises:
    ///     RuntimeError: If the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[list[UserInfo]]", imports=("collections.abc")))]
    fn get_users<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let users = inner.get_users().await.map_err(to_runtime_error)?;
            Ok(users.into_iter().map(PyUserInfo::from).collect::<Vec<_>>())
        })
    }

    /// Create a new user.
    ///
    /// Args:
    ///     username: Username as `str`.
    ///     password: Password as `str`.
    ///     status: User status as `UserStatus | None`; defaults to `UserStatus.Active`.
    ///     permissions: Permissions as `Permissions | None`; the user has none when `None`.
    ///     options: User options as `UserOptions | None`.
    ///
    /// Returns:
    ///     An awaitable that resolves to the created `UserInfoDetails`.
    ///
    /// Raises:
    ///     RuntimeError: If an argument is invalid or the request fails.
    #[pyo3(signature = (username, password, status=None, permissions=None, options=None))]
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[UserInfoDetails]", imports=("collections.abc")))]
    fn create_user<'a>(
        &self,
        py: Python<'a>,
        username: String,
        password: String,
        #[gen_stub(override_type(type_repr = "UserStatus | None"))] status: Option<PyUserStatus>,
        #[gen_stub(override_type(type_repr = "Permissions | None"))] permissions: Option<
            PyPermissions,
        >,
        #[gen_stub(override_type(type_repr = "UserOptions | None"))] options: Option<UserOptions>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let status = status.map_or(UserStatus::Active, UserStatus::from);
        let permissions = permissions.map(|permissions| permissions.inner);
        let options = options.map(|options| options.inner).unwrap_or_default();
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let user = inner
                .create_user(&username, &password, status, permissions, &options)
                .await
                .map_err(to_runtime_error)?;
            Ok(PyUserInfoDetails::from(user))
        })
    }

    /// Update a user by unique ID or username.
    ///
    /// A patch: only the fields passed change, every other setting keeps its
    /// value. Renaming the user this client is signed in as also updates the
    /// credentials a reconnect replays.
    ///
    /// Args:
    ///     user_id: User identifier as `str | int`.
    ///     username: New username as `str | None`; unchanged when `None`.
    ///     status: New status as `UserStatus | None`; unchanged when `None`.
    ///         Deactivating a user ends its sessions.
    ///     reset: Keys to return to their defaults as `list[OptionKey] | None`.
    ///         No user key is resettable yet, so any key is refused.
    ///     options: Additional option keys as `dict[str, str] | None`,
    ///         forwarded to the server. No other user key exists yet, so a
    ///         current server rejects every key.
    ///     raw_reset: Additional reset names as `list[str] | None`, sent
    ///         verbatim so a key newer than this build can be reset. The
    ///         server decides them.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the user is updated.
    ///
    /// Raises:
    ///     ValueError: If a string identifier is invalid.
    ///     OptionError: A `RuntimeError` carrying the error `code` and
    ///         `kind` when the client or the server refuses an option.
    ///     RuntimeError: Before anything is sent, if the update changes
    ///         nothing, names a key both set and reset, or resets a key that
    ///         cannot be reset. Otherwise if the request fails.
    #[pyo3(signature = (user_id, username=None, status=None, reset=None, options=None, raw_reset=None))]
    #[allow(clippy::too_many_arguments)]
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn update_user<'a>(
        &self,
        py: Python<'a>,
        user_id: PyIdentifier,
        #[gen_stub(override_type(type_repr = "builtins.str | None"))] username: Option<String>,
        #[gen_stub(override_type(type_repr = "UserStatus | None"))] status: Option<PyUserStatus>,
        #[gen_stub(override_type(type_repr = "builtins.list[OptionKey] | None"))] reset: Option<
            Vec<OptionKey>,
        >,
        #[gen_stub(override_type(type_repr = "builtins.dict[builtins.str, builtins.str] | None"))]
        options: Option<BTreeMap<String, String>>,
        #[gen_stub(override_type(type_repr = "builtins.list[builtins.str] | None"))]
        raw_reset: Option<Vec<String>>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let user_id = Identifier::try_from(user_id)?;
        let update_options = UserUpdateOptions {
            username,
            status: status.map(UserStatus::from),
            reset: reset_keys(reset),
            raw_reset: raw_reset.into_iter().flatten().collect(),
            raw: options.unwrap_or_default(),
        };
        update_options.to_patch().map_err(to_runtime_error)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .update_user(&user_id, &update_options)
                .await
                .map_err(to_runtime_error)?;
            Ok(())
        })
    }

    /// Delete a user by unique ID or username.
    ///
    /// Args:
    ///     user_id: User identifier as `str | int`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the user is deleted.
    ///
    /// Raises:
    ///     ValueError: If a string identifier is invalid.
    ///     RuntimeError: If the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn delete_user<'a>(&self, py: Python<'a>, user_id: PyIdentifier) -> PyResult<Bound<'a, PyAny>> {
        let user_id = Identifier::try_from(user_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .delete_user(&user_id)
                .await
                .map_err(to_runtime_error)?;
            Ok(())
        })
    }

    /// Update the permissions of a user by unique ID or username.
    ///
    /// This is a full replacement: the given permissions overwrite the previous
    /// ones, and `None` removes them entirely.
    ///
    /// Args:
    ///     user_id: User identifier as `str | int`.
    ///     permissions: New permissions as `Permissions | None`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the permissions are updated.
    ///
    /// Raises:
    ///     ValueError: If a string identifier is invalid.
    ///     RuntimeError: If the request fails.
    #[pyo3(signature = (user_id, permissions))]
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn update_permissions<'a>(
        &self,
        py: Python<'a>,
        user_id: PyIdentifier,
        #[gen_stub(override_type(type_repr = "Permissions | None"))] permissions: Option<
            PyPermissions,
        >,
    ) -> PyResult<Bound<'a, PyAny>> {
        let user_id = Identifier::try_from(user_id)?;
        let permissions = permissions.map(|permissions| permissions.inner);
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .update_permissions(&user_id, permissions)
                .await
                .map_err(to_runtime_error)?;
            Ok(())
        })
    }

    /// Change the password of a user by unique ID or username.
    ///
    /// Args:
    ///     user_id: User identifier as `str | int`.
    ///     current_password: Current password as `str`.
    ///     new_password: New password as `str`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the password is changed.
    ///
    /// Raises:
    ///     ValueError: If a string identifier is invalid.
    ///     RuntimeError: If the current password is wrong or the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn change_password<'a>(
        &self,
        py: Python<'a>,
        user_id: PyIdentifier,
        current_password: String,
        new_password: String,
    ) -> PyResult<Bound<'a, PyAny>> {
        let user_id = Identifier::try_from(user_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .change_password(&user_id, &current_password, &new_password)
                .await
                .map_err(to_runtime_error)?;
            Ok(())
        })
    }

    /// Log out the currently authenticated user.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the user is logged out.
    ///
    /// Raises:
    ///     RuntimeError: If the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn logout_user<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner.logout_user().await.map_err(to_runtime_error)?;
            Ok(())
        })
    }

    /// Connects the IggyClient to its service and starts the heartbeat task.
    /// Raises `RuntimeError` if the connection fails. Over HTTP there is no
    /// connection to establish, so only the heartbeat starts and this call
    /// succeeds even against an unreachable server.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn connect<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner.connect().await.map_err(to_runtime_error)?;
            Ok(())
        })
    }

    /// Closes the current connection. Repeated calls are safe. Call `connect`
    /// to use the client again. Over TCP and QUIC, that `connect` first waits
    /// for the rest of `reestablish_after` (5 s by default). The sign-in made
    /// with `login_user` is dropped, so it must be repeated after reconnecting.
    /// A client configured with auto-login credentials signs in again on
    /// `connect`. Until then, requests fail with `RuntimeError`, and nothing
    /// connects the client again in the background, even with auto-login
    /// credentials. Over HTTP there is no connection to close and this call
    /// does nothing.
    ///
    /// Raises:
    ///     RuntimeError: If the connection cannot be closed.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn disconnect<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner.disconnect().await.map_err(to_runtime_error)?;
            Ok(())
        })
    }

    /// Replays the exact retained operation until its final result or the deadline.
    /// A committed rejection raises its original error and frees the lane.
    /// Uncertainty keeps the operation retained. HTTP does not support this operation.
    /// A fetch progress block raises code 14010 without replaying; only explicit
    /// abandon_operation releases it, accepting the unknown server outcome.
    /// `timeout` must be a positive timedelta; omission or None uses 30 seconds.
    #[pyo3(signature = (operation_id, *, timeout=None))]
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn resolve_operation<'a>(
        &self,
        py: Python<'a>,
        operation_id: u64,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        timeout: Option<Py<PyDelta>>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let timeout = timeout
            .as_ref()
            .map(py_delta_to_iggy_duration)
            .transpose()?
            .map(|timeout| reject_zero(timeout, "timeout"))
            .transpose()?
            .map_or(DEFAULT_RESOLVE_TIMEOUT, |timeout| {
                timeout.get().get_duration()
            });
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| PyValueError::new_err("'timeout' is too large"))?;
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .resolve_operation(operation_id, deadline)
                .await
                .map_err(to_runtime_error)
        })
    }

    /// Releases a retained operation without claiming whether it committed.
    /// For a fetch, this releases every affected partition without stopping the old request.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn abandon_operation<'a>(
        &self,
        py: Python<'a>,
        operation_id: u64,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .abandon_operation(operation_id)
                .await
                .map_err(to_runtime_error)
        })
    }

    /// Closes the connection. Shut down background producers with
    /// `IggyProducer.shutdown()` and stop iterating consumers before this call,
    /// because they share the connection. Otherwise background producers drop
    /// queued messages and consumer iterators hang. Later requests fail with
    /// `RuntimeError`. Repeated calls are safe. Over HTTP there is no
    /// connection to close, but the heartbeat that `connect` started keeps
    /// sending pings until the client is dropped.
    ///
    /// Raises:
    ///     RuntimeError: If the client cannot be shut down.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn shutdown<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner.shutdown().await.map_err(to_runtime_error)?;
            Ok(())
        })
    }

    /// Create a new stream.
    ///
    /// On a server with encryption enabled every stream is stored under its
    /// own key: the one `options` carries, or one the server generates. The
    /// returned details report only the key's source and fingerprints.
    ///
    /// Args:
    ///     name: Stream name as `str`, between 1 and 255 UTF-8 bytes.
    ///     options: Stream options as `StreamOptions | None`.
    ///
    /// Returns:
    ///     An awaitable that resolves to the created `StreamDetails`.
    ///
    /// Raises:
    ///     RuntimeError: If the stream cannot be created, for example when
    ///         `options` carries a key and the server has no encryption key.
    #[pyo3(signature = (name, options=None))]
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[StreamDetails]", imports=("collections.abc")))]
    fn create_stream<'a>(
        &self,
        py: Python<'a>,
        name: String,
        #[gen_stub(override_type(type_repr = "StreamOptions | None"))] options: Option<
            StreamOptions,
        >,
    ) -> PyResult<Bound<'a, PyAny>> {
        let options = options.map(|options| options.inner).unwrap_or_default();
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let stream = inner
                .create_stream(&name, &options)
                .await
                .map_err(to_runtime_error)?;
            Ok(StreamDetails::from(stream))
        })
    }

    /// Gets stream by id.
    /// Returns the stream details, or `None` if the stream does not exist.
    /// Raises `RuntimeError` on failure.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[StreamDetails | None]", imports=("collections.abc")))]
    fn get_stream<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let stream = inner
                .get_stream(&stream_id)
                .await
                .map_err(to_runtime_error)?;
            Ok(stream.map(StreamDetails::from))
        })
    }

    /// Return all streams.
    ///
    /// Returns:
    ///     A list of `Stream` summaries.
    ///
    /// Raises:
    ///     RuntimeError: If the client is not authenticated, the user lacks global
    ///         `read_streams` or `manage_streams` permission, or the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[list[Stream]]", imports=("collections.abc")))]
    fn get_streams<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let streams = inner.get_streams().await.map_err(to_runtime_error)?;
            Ok(streams.into_iter().map(Stream::from).collect::<Vec<_>>())
        })
    }

    /// Update a stream selected by name or numeric ID.
    ///
    /// A patch: only the fields passed change. The stream keeps its id, its
    /// encryption key and its data. `stream_id` accepts a stream name as `str`
    /// or numeric ID as `int`. A decimal-only string is interpreted as a
    /// numeric ID. Renaming a stream to its current name succeeds without
    /// changing it.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///     name: New stream name as `str | None`, unique and between 1 and 255
    ///         UTF-8 bytes, unchanged when `None`.
    ///     reset: Keys to return to their defaults as `list[OptionKey] | None`.
    ///         No stream key is resettable, so any key is refused.
    ///     options: Additional option keys as `dict[str, str] | None`, forwarded
    ///         to the server. Current server versions reject all stream update
    ///         option keys.
    ///     raw_reset: Additional reset names as `list[str] | None`, sent
    ///         verbatim so a key newer than this build can be reset. The
    ///         server decides them.
    ///
    /// Returns:
    ///     None.
    ///
    /// Raises:
    ///     TypeError: If `stream_id` is not `str` or an integer in
    ///         `0..=2**32 - 1`, or `name` is not `str`.
    ///     ValueError: If a string identifier is empty or exceeds 255 UTF-8 bytes.
    ///     OptionError: A `RuntimeError` carrying the error `code` and
    ///         `kind` when the client or the server refuses an option.
    ///     RuntimeError: Before anything is sent, if the update changes
    ///         nothing, the name does not fit, or a reset key cannot be reset.
    ///         Otherwise if the client is not authenticated, the user lacks
    ///         global `manage_streams` or per-stream `manage_stream` permission,
    ///         the stream does not exist, the new name is already used, or the
    ///         request fails.
    #[pyo3(signature = (stream_id, name=None, reset=None, options=None, raw_reset=None))]
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn update_stream<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        #[gen_stub(override_type(type_repr = "builtins.str | None"))] name: Option<String>,
        #[gen_stub(override_type(type_repr = "builtins.list[OptionKey] | None"))] reset: Option<
            Vec<OptionKey>,
        >,
        #[gen_stub(override_type(type_repr = "builtins.dict[builtins.str, builtins.str] | None"))]
        options: Option<BTreeMap<String, String>>,
        #[gen_stub(override_type(type_repr = "builtins.list[builtins.str] | None"))]
        raw_reset: Option<Vec<String>>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let update_options = StreamUpdateOptions {
            name,
            reset: reset_keys(reset),
            raw_reset: raw_reset.into_iter().flatten().collect(),
            raw: options.unwrap_or_default(),
        };
        update_options.to_patch().map_err(to_runtime_error)?;
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .update_stream(&stream_id, &update_options)
                .await
                .map_err(to_runtime_error)?;
            Ok(())
        })
    }

    /// Delete a stream selected by name or numeric ID.
    ///
    /// Deletion removes the stream and all of its topics, partitions, and messages.
    /// `stream_id` accepts a stream name as `str` or numeric ID as `int`. A
    /// decimal-only string is interpreted as a numeric ID.
    ///
    /// Returns:
    ///     None.
    ///
    /// Raises:
    ///     TypeError: If `stream_id` is not `str` or an integer in
    ///         `0..=2**32 - 1`.
    ///     ValueError: If a string identifier is empty or exceeds 255 UTF-8 bytes.
    ///     RuntimeError: If the client is not authenticated, the user lacks global
    ///         `manage_streams` or per-stream `manage_stream` permission, the
    ///         stream does not exist, or the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn delete_stream<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .delete_stream(&stream_id)
                .await
                .map_err(to_runtime_error)?;
            Ok(())
        })
    }

    /// Creates a new topic.
    ///
    /// Args:
    ///     stream: Stream identifier as `str | int`.
    ///     name: Topic name as `str`.
    ///     options: Topic options as `TopicOptions | None`. Without options the
    ///         topic gets one partition and every setting resolves against the
    ///         server default at admission.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the topic is created.
    ///
    /// Raises:
    ///     RuntimeError: If an argument is invalid or the request fails.
    #[pyo3(signature = (stream, name, options=None))]
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn create_topic<'a>(
        &self,
        py: Python<'a>,
        stream: PyIdentifier,
        name: String,
        #[gen_stub(override_type(type_repr = "TopicOptions | None"))] options: Option<TopicOptions>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let topic_options = options.map(|options| options.inner).unwrap_or_default();
        let stream = Identifier::try_from(stream)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .create_topic(&stream, &name, &topic_options)
                .await
                .map_err(to_runtime_error)?;
            Ok(())
        })
    }

    /// Gets topic by stream and id.
    /// Returns the topic details, or `None` if the topic does not exist.
    /// Raises `RuntimeError` on failure.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[TopicDetails | None]", imports=("collections.abc")))]
    fn get_topic<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let topic_id = Identifier::try_from(topic_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let topic = inner
                .get_topic(&stream_id, &topic_id)
                .await
                .map_err(to_runtime_error)?;
            Ok(topic.map(TopicDetails::from))
        })
    }

    /// Get all topics in a stream.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `list[Topic]`.
    ///
    /// Raises:
    ///     RuntimeError: If the identifier is invalid or the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[list[Topic]]", imports=("collections.abc")))]
    fn get_topics<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let topics = inner
                .get_topics(&stream_id)
                .await
                .map_err(to_runtime_error)?;
            Ok(topics.into_iter().map(Topic::from).collect::<Vec<_>>())
        })
    }

    /// Update an existing topic.
    ///
    /// A patch, not a replacement: only the fields passed change, every other
    /// setting keeps its value. Settings fixed at creation, such as the
    /// compression algorithm, have no field here.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///     topic_id: Topic identifier as `str | int`.
    ///     name: New topic name as `str | None`, unchanged when `None`.
    ///     message_expiry: Message expiry as `IggyExpiry | None`, unchanged
    ///         when `None`. `IggyExpiry.ServerDefault()` changes nothing either,
    ///         reset `OptionKey.MessageExpiry` to return to the default.
    ///     max_topic_size: Maximum topic size as `MaxTopicSize | None`,
    ///         unchanged when `None`. `MaxTopicSize.ServerDefault()` changes
    ///         nothing either, reset `OptionKey.MaxTopicSize` to return to the
    ///         default.
    ///     reset: Keys to return to their defaults as `list[OptionKey] | None`:
    ///         `OptionKey.MessageExpiry` or `OptionKey.MaxTopicSize`.
    ///     options: Additional option keys as `dict[str, str] | None`, sent
    ///         verbatim so an updatable server key can be set from this build.
    ///         A create-only key is refused by name.
    ///     raw_reset: Additional reset names as `list[str] | None`, sent
    ///         verbatim so a key newer than this build can be reset. The
    ///         server decides them.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the topic is updated.
    ///
    /// Raises:
    ///     ValueError: If `message_expiry` or `max_topic_size` is out of range.
    ///     OptionError: A `RuntimeError` carrying the error `code` and
    ///         `kind` when the client or the server refuses an option.
    ///     RuntimeError: Before anything is sent, if the update changes
    ///         nothing, names a key both set and reset, the name does not fit,
    ///         or a reset key cannot be reset. Otherwise if the request fails.
    #[pyo3(
        signature = (stream_id, topic_id, name = None, message_expiry = None, max_topic_size = None, reset = None, options = None, raw_reset = None)
    )]
    #[allow(clippy::too_many_arguments)]
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn update_topic<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
        #[gen_stub(override_type(type_repr = "builtins.str | None"))] name: Option<String>,
        #[gen_stub(override_type(type_repr = "IggyExpiry | None"))] message_expiry: Option<
            &IggyExpiry,
        >,
        #[gen_stub(override_type(type_repr = "MaxTopicSize | None"))] max_topic_size: Option<
            &MaxTopicSize,
        >,
        #[gen_stub(override_type(type_repr = "builtins.list[OptionKey] | None"))] reset: Option<
            Vec<OptionKey>,
        >,
        #[gen_stub(override_type(type_repr = "builtins.dict[builtins.str, builtins.str] | None"))]
        options: Option<BTreeMap<String, String>>,
        #[gen_stub(override_type(type_repr = "builtins.list[builtins.str] | None"))]
        raw_reset: Option<Vec<String>>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let update_options = TopicUpdateOptions {
            name,
            message_expiry: message_expiry.map(RustIggyExpiry::try_from).transpose()?,
            max_topic_size: max_topic_size.map(RustMaxTopicSize::try_from).transpose()?,
            reset: reset_keys(reset),
            raw_reset: raw_reset.into_iter().flatten().collect(),
            raw: options.unwrap_or_default(),
        };
        update_options.to_patch().map_err(to_runtime_error)?;

        let stream_id = Identifier::try_from(stream_id)?;
        let topic_id = Identifier::try_from(topic_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .update_topic(&stream_id, &topic_id, &update_options)
                .await
                .map_err(to_runtime_error)?;
            Ok(())
        })
    }

    /// Delete a topic from a stream.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///     topic_id: Topic identifier as `str | int`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the topic is deleted.
    ///
    /// Raises:
    ///     RuntimeError: If an identifier is invalid or the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn delete_topic<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let topic_id = Identifier::try_from(topic_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .delete_topic(&stream_id, &topic_id)
                .await
                .map_err(to_runtime_error)?;
            Ok(())
        })
    }

    /// Create partitions for a topic. New partition IDs continue from one past the
    /// current highest ID; IDs removed by deletion can be reused. Existing consumer
    /// groups are immediately rebalanced across all partitions, advancing their
    /// generation and dropping pending revocations.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///     topic_id: Topic identifier as `str | int`.
    ///     partitions_count: Number of partitions to create as `int`, between 1 and
    ///         1000 inclusive.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the partitions are committed;
    ///     storage materialization completes asynchronously.
    ///
    /// Raises:
    ///     ValueError: If an identifier is invalid.
    ///     OverflowError: If `partitions_count` is outside the unsigned 32-bit range.
    ///     RuntimeError: If the client is not authenticated, lacks global
    ///         `manage_streams` or `manage_topics`, per-stream `manage_stream` or
    ///         `manage_topics`, or per-topic `manage_topic` permission, or the
    ///         request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn create_partitions<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
        partitions_count: u32,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let topic_id = Identifier::try_from(topic_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .create_partitions(&stream_id, &topic_id, partitions_count)
                .await
                .map_err(to_runtime_error)?;
            Ok(())
        })
    }

    /// Delete the last partitions from a topic, including all messages stored in them.
    /// Existing consumer groups are immediately rebalanced across the remaining
    /// partitions, advancing their generation and dropping pending revocations.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///     topic_id: Topic identifier as `str | int`.
    ///     partitions_count: Number of partitions to delete as `int` from the end of
    ///         the topic; must be between 1 and 1000 inclusive and no greater than
    ///         its current count.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when deletion is accepted; storage
    ///     teardown completes asynchronously.
    ///
    /// Raises:
    ///     ValueError: If an identifier is invalid.
    ///     OverflowError: If `partitions_count` is outside the unsigned 32-bit range.
    ///     RuntimeError: If the client is not authenticated, lacks global
    ///         `manage_streams` or `manage_topics`, per-stream `manage_stream` or
    ///         `manage_topics`, or per-topic `manage_topic` permission, or the
    ///         request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn delete_partitions<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
        partitions_count: u32,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let topic_id = Identifier::try_from(topic_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .delete_partitions(&stream_id, &topic_id, partitions_count)
                .await
                .map_err(to_runtime_error)?;
            Ok(())
        })
    }

    /// Delete the oldest sealed segments of a partition, including all messages
    /// stored in them. The active segment is never deleted, and deletion stops at
    /// the lowest offset any consumer or consumer group has committed on the
    /// partition, so fewer than `segments_count` segments may be removed at first.
    /// The server keeps the request and removes the remaining segments later, up
    /// to the requested count, as consumers commit past them.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///     topic_id: Topic identifier as `str | int`.
    ///     partition_id: Partition ID as `int`.
    ///     segments_count: Maximum number of segments to delete as `int`, starting
    ///         from the oldest; `0` deletes nothing.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when deletion is accepted; segment
    ///     files are removed asynchronously.
    ///
    /// Raises:
    ///     ValueError: If an identifier is invalid.
    ///     TypeError: If an integer identifier is outside the unsigned 32-bit range.
    ///     OverflowError: If `partition_id` or `segments_count` is outside the
    ///         unsigned 32-bit range.
    ///     RuntimeError: If the client is not authenticated, lacks global
    ///         `manage_streams` or `manage_topics`, per-stream `manage_stream` or
    ///         `manage_topics`, or per-topic `manage_topic` permission, the
    ///         stream, topic, or partition does not exist, or the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn delete_segments<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
        partition_id: u32,
        segments_count: u32,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let topic_id = Identifier::try_from(topic_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .delete_segments(&stream_id, &topic_id, partition_id, segments_count)
                .await
                .map_err(to_runtime_error)?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    /// Create a consumer group for a stream and topic.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///     topic_id: Topic identifier as `str | int`.
    ///     name: Consumer group name as `str`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the consumer group is created.
    ///
    /// Raises:
    ///     ValueError: If an identifier is invalid.
    ///     RuntimeError: If the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn create_consumer_group<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
        name: String,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let topic_id = Identifier::try_from(topic_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .create_consumer_group(&stream_id, &topic_id, &name)
                .await
                .map_err(to_runtime_error)?;
            Ok(())
        })
    }

    /// Retrieve details for a consumer group from the specified stream and topic.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///     topic_id: Topic identifier as `str | int`.
    ///     group_id: Consumer group identifier as `str | int`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `ConsumerGroupDetails` if the consumer group exists,
    ///     or `None` otherwise.
    ///
    /// Raises:
    ///     ValueError: If an identifier is invalid.
    ///     RuntimeError: If the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[ConsumerGroupDetails | None]", imports=("collections.abc")))]
    fn get_consumer_group<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
        group_id: PyIdentifier,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let topic_id = Identifier::try_from(topic_id)?;
        let group_id = Identifier::try_from(group_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let group = inner
                .get_consumer_group(&stream_id, &topic_id, &group_id)
                .await
                .map_err(to_runtime_error)?;
            Ok(group.map(PyConsumerGroupDetails::from))
        })
    }

    /// Get all consumer groups for the specified stream and topic.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///     topic_id: Topic identifier as `str | int`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `list[ConsumerGroup]`.
    ///
    /// Raises:
    ///     ValueError: If an identifier is invalid.
    ///     RuntimeError: If the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[list[ConsumerGroup]]", imports=("collections.abc")))]
    fn get_consumer_groups<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let topic_id = Identifier::try_from(topic_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let groups = inner
                .get_consumer_groups(&stream_id, &topic_id)
                .await
                .map_err(to_runtime_error)?;
            Ok(groups
                .into_iter()
                .map(PyConsumerGroup::from)
                .collect::<Vec<_>>())
        })
    }

    /// Delete a consumer group for a stream and topic.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///     topic_id: Topic identifier as `str | int`.
    ///     group_id: Consumer group identifier as `str | int`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the consumer group is deleted.
    ///
    /// Raises:
    ///     ValueError: If a string identifier is invalid.
    ///     RuntimeError: If the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn delete_consumer_group<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
        group_id: PyIdentifier,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let topic_id = Identifier::try_from(topic_id)?;
        let group_id = Identifier::try_from(group_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .delete_consumer_group(&stream_id, &topic_id, &group_id)
                .await
                .map_err(to_runtime_error)?;
            Ok(())
        })
    }

    /// Join a consumer group for a stream and topic.
    ///
    /// This method only registers the current client as a group member. To consume messages
    /// as a group, use `consumer_group()`, which enables auto-join by default.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///     topic_id: Topic identifier as `str | int`.
    ///     group_id: Consumer group identifier as `str | int`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the client joins the consumer group.
    ///
    /// Raises:
    ///     ValueError: If a string identifier is invalid.
    ///     RuntimeError: If the request fails, including `Feature is unavailable` on HTTP transport.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn join_consumer_group<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
        group_id: PyIdentifier,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let topic_id = Identifier::try_from(topic_id)?;
        let group_id = Identifier::try_from(group_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .join_consumer_group(&stream_id, &topic_id, &group_id)
                .await
                .map_err(to_runtime_error)?;
            Ok(())
        })
    }

    /// Leave a consumer group for a stream and topic.
    ///
    /// Args:
    ///     stream_id: Stream identifier as `str | int`.
    ///     topic_id: Topic identifier as `str | int`.
    ///     group_id: Consumer group identifier as `str | int`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `None` when the client leaves the consumer group.
    ///
    /// Note:
    ///     Consumers created from this client for the same group share one server-side
    ///     membership. Leaving revokes that membership. Consumers with auto-join enabled
    ///     rejoin on their next poll.
    ///
    /// Raises:
    ///     ValueError: If a string identifier is invalid.
    ///     RuntimeError: If the request fails, including `Feature is unavailable` on HTTP transport.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn leave_consumer_group<'a>(
        &self,
        py: Python<'a>,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
        group_id: PyIdentifier,
    ) -> PyResult<Bound<'a, PyAny>> {
        let stream_id = Identifier::try_from(stream_id)?;
        let topic_id = Identifier::try_from(topic_id)?;
        let group_id = Identifier::try_from(group_id)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .leave_consumer_group(&stream_id, &topic_id, &group_id)
                .await
                .map_err(to_runtime_error)?;
            Ok(())
        })
    }

    /// Sends a batch of messages to a topic using the selected partitioning strategy.
    ///
    /// Args:
    ///     stream: Stream identifier as `str | int`.
    ///     topic: Topic identifier as `str | int`.
    ///     partitioning: A `Partitioning` strategy or an integer partition ID.
    ///         Use `Partitioning.balanced()`, `Partitioning.partition_id(id)`, or
    ///         `Partitioning.messages_key(key)`. An integer is shorthand for
    ///         `Partitioning.partition_id(id)`.
    ///     messages: Messages to send as `list[SendMessage]`.
    ///
    /// Returns:
    ///     An awaitable that resolves to `SendMessagesResponse`. Its confirmations
    ///     report the committed partition and batch base offset. The list is empty
    ///     when the server reports no offsets.
    ///
    /// Raises:
    ///     ValueError: If a string stream or topic identifier is invalid.
    ///     TypeError: If `partitioning` or `messages` has an unsupported type.
    ///     OverflowError: If a numeric stream, topic, or partition ID is outside
    ///         the supported unsigned 32-bit range.
    ///     RuntimeError: If the request fails.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[SendMessagesResponse]", imports=("collections.abc")))]
    fn send_messages<'a>(
        &self,
        py: Python<'a>,
        stream: PyIdentifier,
        topic: PyIdentifier,
        #[gen_stub(override_type(type_repr = "Partitioning | builtins.int"))]
        partitioning: PyPartitioning,
        #[gen_stub(override_type(type_repr = "list[SendMessage]"))] messages: &Bound<'_, PyList>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let messages: Vec<SendMessage> = messages
            .iter()
            .map(|item| {
                let msg: PyRef<'_, SendMessage> = item.extract()?;
                Ok::<_, PyErr>(msg.clone())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut messages: Vec<RustMessage> = messages
            .into_iter()
            .map(|message| message.inner)
            .collect::<Vec<_>>();

        let stream = Identifier::try_from(stream)?;
        let topic = Identifier::try_from(topic)?;
        let partitioning = RustPartitioning::from(partitioning);
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let response = inner
                .send_messages(&stream, &topic, &partitioning, messages.as_mut())
                .await
                .map_err(to_runtime_error)?;
            Ok(PySendMessagesResponse::from(response))
        })
    }

    /// Creates and initializes a high-level producer bound to a stream and topic.
    ///
    /// This is a Python port of the Rust high-level producer API. For detailed
    /// producer semantics, see https://iggy.apache.org/docs/sdk/rust/high-level-sdk/.
    /// `None` selects direct mode. `BackgroundProducerConfig` starts background
    /// workers and makes successful sends mean queue acceptance rather than a
    /// server commit. Replicated topics accept producer writes. Set
    /// `topic_durability=Durability.PERSISTED` for an automatically created topic
    /// when sends require crash-safe retry receipts.
    ///
    /// Raises `ValueError` for invalid names or numeric ranges and `RuntimeError`
    /// when stream/topic initialization fails.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        stream,
        topic,
        partitioning=None,
        mode=None,
        create_stream_if_not_exists=true,
        create_topic_if_not_exists=true,
        topic_partitions_count=1,
        topic_message_expiry=None,
        topic_max_size=None,
        topic_durability=None,
    ))]
    #[gen_stub(override_return_type(type_repr = "collections.abc.Awaitable[IggyProducer]", imports=("collections.abc")))]
    fn producer<'a>(
        &self,
        py: Python<'a>,
        stream: &str,
        topic: &str,
        #[gen_stub(override_type(type_repr = "Partitioning | None"))] partitioning: Option<
            &crate::partitioning::Partitioning,
        >,
        #[gen_stub(override_type(
            type_repr = "DirectProducerConfig | BackgroundProducerConfig | None"
        ))]
        mode: Option<ProducerMode>,
        create_stream_if_not_exists: bool,
        create_topic_if_not_exists: bool,
        topic_partitions_count: i64,
        #[gen_stub(override_type(type_repr = "IggyExpiry | None"))] topic_message_expiry: Option<
            &IggyExpiry,
        >,
        #[gen_stub(override_type(type_repr = "MaxTopicSize | None"))] topic_max_size: Option<
            &MaxTopicSize,
        >,
        #[gen_stub(override_type(type_repr = "Durability | None"))] topic_durability: Option<
            &Bound<'_, PyAny>,
        >,
    ) -> PyResult<Bound<'a, PyAny>> {
        let mode = mode.unwrap_or_default();

        let topic_partitions_count =
            producer_u32_param(topic_partitions_count, "topic_partitions_count")?;
        let topic_message_expiry = topic_message_expiry
            .map(RustIggyExpiry::try_from)
            .transpose()?
            .unwrap_or(RustIggyExpiry::ServerDefault);
        let topic_max_size = topic_max_size
            .map(RustMaxTopicSize::try_from)
            .transpose()?
            .unwrap_or(RustMaxTopicSize::ServerDefault);
        let mut builder = self
            .inner
            .producer(stream, topic)
            .map_err(to_value_error)?
            .topic_durability(crate::durability::Durability::try_from(topic_durability)?.0);

        builder = match mode {
            ProducerMode::Direct(config) => builder.direct((&config).into()),
            ProducerMode::Background(config) => builder.background((&config).try_into()?),
        };

        if let Some(partitioning) = partitioning {
            builder = builder.partitioning(partitioning.inner.as_ref().clone());
        }
        if create_stream_if_not_exists {
            builder = builder.create_stream_if_not_exists();
        } else {
            builder = builder.do_not_create_stream_if_not_exists();
        }
        if create_topic_if_not_exists {
            builder = builder.create_topic_if_not_exists(
                topic_partitions_count,
                topic_message_expiry,
                topic_max_size,
            );
        } else {
            builder = builder.do_not_create_topic_if_not_exists();
        }

        future_into_py(py, async move {
            // A background build starts Tokio worker tasks, so it must happen
            // while this future is executing on the Rust runtime.
            let producer = builder.build();
            producer.init().await.map_err(to_runtime_error)?;
            Ok(IggyProducer::new(producer))
        })
    }

    /// Polls one batch of messages of a partition on behalf of the given consumer.
    ///
    /// `options` defaults to `PollOptions()`: at most 1000 messages and 1 MiB, answered at
    /// once, and no offset stored. With `ServerCommitPolicy.OnRead`, the server stores the
    /// offset of the last message as part of the reply. If that reply is lost, the poll raises
    /// `PendingOperationError` of kind `fetch_progress_uncertain`, and every later poll of the
    /// partition raises it of kind `lane_blocked` until `abandon_operation()` releases it.
    ///
    /// To read on, pass `polled.continuation()` as the next `polling_strategy`. The poll is
    /// then refused with `HistoryUnavailable` (87) if the partition was deleted and created
    /// again, or if its consumer group owner changed, instead of reading the new partition at
    /// that offset. A continuation of another partition is refused with `InvalidCommand` (3).
    ///
    /// A consumer group member reads only the partitions assigned to it: another partition
    /// raises `ConsumerGroupPartitionNotOwned` (5009).
    /// Returns the batch, or raises `IggyError` on failure.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (stream, topic, *, partition_id, consumer, polling_strategy, options=None))]
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[PolledMessages]", imports=("collections.abc")))]
    fn poll_messages<'a>(
        &self,
        py: Python<'a>,
        stream: PyIdentifier,
        topic: PyIdentifier,
        partition_id: u32,
        consumer: &PyConsumer,
        polling_strategy: &PollingStrategy,
        #[gen_stub(override_type(type_repr = "PollOptions | None"))] options: Option<PollOptions>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let consumer = RustConsumer::try_from(consumer)?;
        let stream = Identifier::try_from(stream)?;
        let topic = Identifier::try_from(topic)?;
        let strategy = polling_strategy.inner;
        let options = options.map_or_else(RustPollOptions::default, |options| options.inner);
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let polled = inner
                .poll_messages(&stream, &topic, partition_id, &consumer, &strategy, options)
                .await
                .map_err(to_runtime_error)?;
            Ok(PolledBatch(polled))
        })
    }

    /// Builds a `PartitionReader` of `topic` in `stream` that reads as `consumer`.
    ///
    /// A `Consumer.Single` reads `partitions`, which it requires. A `Consumer.Group` reads the
    /// assignment of its member, so `partitions` must be `None`: the reader joins the group
    /// here. Consumer groups are not available over HTTP.
    ///
    /// `start` is where a partition without a stored position starts, `PollingStrategy.Next()`
    /// by default. A continuation is refused, because it belongs to one partition. `options`
    /// sets each request, and defaults to `PollOptions(max_wait=timedelta(seconds=1))`: a
    /// reader without a wait polls an idle topic in a busy loop. `prefetch_bytes` (16 MiB by
    /// default) and `prefetch_messages` (the larger of 16,000 and `options.count` by default)
    /// bound the requests in flight and the queued batches.
    ///
    /// Raises `IggyError` with `InvalidCommand` (3) for partitions of a group, a
    /// `Consumer.Single` without partitions or a continuation as `start`,
    /// `FeatureUnavailable` for a consumer group over HTTP, the errors of `options`, and the
    /// error of the group join. A join with an unknown outcome raises `PendingOperationError`:
    /// the client keeps it, so a new `reader()` raises `lane_blocked` until
    /// `resolve_operation()` or `abandon_operation()` settles it.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        stream,
        topic,
        *,
        consumer,
        partitions=None,
        start=None,
        options=None,
        prefetch_bytes=None,
        prefetch_messages=None,
    ))]
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[PartitionReader]", imports=("collections.abc")))]
    fn reader<'a>(
        &self,
        py: Python<'a>,
        stream: PyIdentifier,
        topic: PyIdentifier,
        consumer: &PyConsumer,
        #[gen_stub(override_type(type_repr = "list[builtins.int] | None"))] partitions: Option<
            Vec<u32>,
        >,
        #[gen_stub(override_type(type_repr = "PollingStrategy | None"))] start: Option<
            PollingStrategy,
        >,
        #[gen_stub(override_type(type_repr = "PollOptions | None"))] options: Option<PollOptions>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] prefetch_bytes: Option<u32>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] prefetch_messages: Option<
            u32,
        >,
    ) -> PyResult<Bound<'a, PyAny>> {
        let consumer = RustConsumer::try_from(consumer)?;
        let stream = Identifier::try_from(stream)?;
        let topic = Identifier::try_from(topic)?;
        let mut builder = self.inner.reader(&stream, &topic, &consumer);
        if let Some(partitions) = partitions {
            builder = builder.partitions(partitions);
        }
        if let Some(start) = start {
            builder = builder.start(start.inner);
        }
        if let Some(options) = options {
            builder = builder.options(options.inner);
        }
        if let Some(prefetch_bytes) = prefetch_bytes {
            builder = builder.prefetch_bytes(prefetch_bytes);
        }
        if let Some(prefetch_messages) = prefetch_messages {
            builder = builder.prefetch_messages(prefetch_messages);
        }

        future_into_py(py, async move {
            // The reader starts Tokio tasks, so it must be built on the Rust runtime.
            let reader = builder.build().await.map_err(to_runtime_error)?;
            Ok(PartitionReader::new(reader))
        })
    }

    /// Creates a new consumer group consumer, which joins the group before it returns.
    /// The member reads the partitions the server assigns to it.
    ///
    /// `polling_strategy` is where a partition without a stored offset starts,
    /// `PollingStrategy.Next()` by default. `poll_options` sets each request, and defaults to
    /// `PollOptions(max_wait=timedelta(seconds=1))`: a consumer without a wait polls an idle
    /// topic in a busy loop. `ServerCommitPolicy.OnRead` makes the server store the offset of
    /// each reply, and it needs the default `client_commit`.
    ///
    /// `client_commit` defaults to `ClientCommitPolicy()`: the consumer stores no offset, so
    /// commit checkpoints after processing, or each restart reads the same messages again.
    /// `prefetch_bytes` (16 MiB by default) and `prefetch_messages` (the larger of 16,000 and
    /// the poll count by default) bound the requests in flight and the queued batches.
    /// Call `shutdown()` before dropping the consumer.
    /// Returns the consumer or raises `IggyError` on failure. Raises `ValueError` if
    /// `polling_retry_interval` or `init_retry_interval` is negative or zero.
    ///
    /// Consumer groups are not available over HTTP: this call raises `FeatureUnavailable`.
    /// Use `Consumer.Single(...)` with `poll_messages(...)` or `reader(...)` there.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        name,
        stream,
        topic,
        *,
        polling_strategy=None,
        poll_options=None,
        client_commit=None,
        prefetch_bytes=None,
        prefetch_messages=None,
        create_consumer_group_if_not_exists=true,
        polling_retry_interval=None,
        init_retries=None,
        init_retry_interval=None,
        allow_replay=false,
    ))]
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[IggyConsumer]", imports=("collections.abc")))]
    fn consumer_group<'a>(
        &self,
        py: Python<'a>,
        name: &str,
        stream: &str,
        topic: &str,
        #[gen_stub(override_type(type_repr = "PollingStrategy | None"))] polling_strategy: Option<
            &PollingStrategy,
        >,
        #[gen_stub(override_type(type_repr = "PollOptions | None"))] poll_options: Option<
            PollOptions,
        >,
        #[gen_stub(override_type(type_repr = "ClientCommitPolicy | None"))] client_commit: Option<
            &ClientCommitPolicy,
        >,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] prefetch_bytes: Option<u32>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] prefetch_messages: Option<
            u32,
        >,
        create_consumer_group_if_not_exists: bool,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        polling_retry_interval: Option<Py<PyDelta>>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] init_retries: Option<u32>,
        #[gen_stub(override_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
        init_retry_interval: Option<Py<PyDelta>>,
        allow_replay: bool,
    ) -> PyResult<Bound<'a, PyAny>> {
        let mut builder = self
            .inner
            .consumer_group(name, stream, topic)
            .map_err(to_runtime_error)?
            .without_encryptor();

        if create_consumer_group_if_not_exists {
            builder = builder.create_consumer_group_if_not_exists()
        } else {
            builder = builder.do_not_create_consumer_group_if_not_exists()
        };
        if let Some(polling_strategy) = polling_strategy {
            builder = builder.polling_strategy(polling_strategy.inner)
        };
        if let Some(poll_options) = poll_options {
            builder = builder.poll_options(poll_options.inner)
        };
        if let Some(client_commit) = client_commit {
            builder = builder.client_commit(client_commit.inner)
        };
        if let Some(prefetch_bytes) = prefetch_bytes {
            builder = builder.prefetch_bytes(prefetch_bytes)
        };
        if let Some(prefetch_messages) = prefetch_messages {
            builder = builder.prefetch_messages(prefetch_messages)
        };
        if let Some(polling_retry_interval) = polling_retry_interval {
            builder = builder.polling_retry_interval(reject_zero(
                py_delta_to_iggy_duration(&polling_retry_interval)?,
                "polling_retry_interval",
            )?)
        }
        if init_retries.is_some() && init_retry_interval.is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "'init_retry_interval' is required if 'init_retries' is set",
            ));
        }
        if init_retries.is_none() && init_retry_interval.is_some() {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "'init_retries' is required if 'init_retry_interval' is set",
            ));
        }
        if let (Some(init_retries), Some(init_retry_interval)) = (init_retries, init_retry_interval)
        {
            builder = builder.init_retries(
                init_retries,
                reject_zero(
                    py_delta_to_iggy_duration(&init_retry_interval)?,
                    "init_retry_interval",
                )?,
            );
        }
        if allow_replay {
            builder = builder.allow_replay()
        }
        let mut consumer = builder.build();

        future_into_py(py, async move {
            consumer.init().await.map_err(to_runtime_error)?;
            let state = consumer.state();
            let name = consumer.name().to_string();
            let stream = PyIdentifier::try_from(consumer.stream())?;
            let topic = PyIdentifier::try_from(consumer.topic())?;
            Ok(IggyConsumer {
                inner: Arc::new(Mutex::new(consumer)),
                state,
                name,
                stream,
                topic,
                consuming: Arc::default(),
            })
        })
    }

    /// Send a command code with a payload and return the raw response bytes.
    ///
    /// Session-control codes are rejected client-side. HTTP transport does not
    /// support raw binary commands.
    ///
    /// Args:
    ///     code: Command code as `int`.
    ///     payload: Request payload as `bytes`.
    ///
    /// Returns:
    ///     An awaitable that resolves to the raw response `bytes`.
    ///
    /// Raises:
    ///     RuntimeError: If the command cannot be sent or the server returns an error.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[bytes]", imports=("collections.abc")))]
    fn send_binary_request<'a>(
        &self,
        py: Python<'a>,
        code: u32,
        #[gen_stub(override_type(type_repr = "builtins.bytes"))] payload: Vec<u8>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let response = inner
                .send_binary_request(code, Bytes::from(payload))
                .await
                .map_err(to_runtime_error)?;
            Ok(Python::attach(|py| PyBytes::new(py, &response).unbind()))
        })
    }

    /// Send a command code with a payload to the replica with the given id and
    /// return the raw response bytes.
    ///
    /// The replica id is the one cluster metadata names for the node. Every
    /// node answers non-replicated commands such as GetStats from its own
    /// state. Replicated commands, polls, consumer-offset reads and
    /// session-control codes are rejected client-side, and a replica id that
    /// cluster metadata does not name fails. HTTP transport does not support
    /// raw binary commands.
    ///
    /// Args:
    ///     code: Command code as `int`.
    ///     payload: Request payload as `bytes`.
    ///     replica_id: Replica id as `int`, as cluster metadata names it.
    ///
    /// Returns:
    ///     An awaitable that resolves to the raw response `bytes`.
    ///
    /// Raises:
    ///     RuntimeError: If the command cannot be sent or the replica returns an error.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[bytes]", imports=("collections.abc")))]
    fn send_binary_request_to_replica<'a>(
        &self,
        py: Python<'a>,
        code: u32,
        #[gen_stub(override_type(type_repr = "builtins.bytes"))] payload: Vec<u8>,
        replica_id: u8,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let response = inner
                .send_binary_request_to_replica(code, Bytes::from(payload), replica_id)
                .await
                .map_err(to_runtime_error)?;
            Ok(Python::attach(|py| PyBytes::new(py, &response).unbind()))
        })
    }
}

define_stub_info_gatherer!(stub_info);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_root_policy_refusal_when_mapping_should_preserve_code_and_kind() {
        Python::initialize();
        Python::attach(|py| {
            let error = to_runtime_error(IggyError::CannotModifyRootUser);
            assert!(error.is_instance_of::<OptionError>(py));
            let value = error.value(py);
            assert_eq!(
                value.getattr("code").unwrap().extract::<u32>().unwrap(),
                4108
            );
            assert_eq!(
                value.getattr("kind").unwrap().extract::<String>().unwrap(),
                "cannot_modify_root_user"
            );
        });
    }

    #[test]
    fn pending_operation_errors_preserve_exact_identity_and_kind() {
        Python::initialize();
        Python::attach(|py| {
            for operation_id in [(1u64 << 53) + 17, u64::MAX] {
                for (error, kind, partition_id) in [
                    (
                        IggyError::OperationUncertain(operation_id),
                        "uncertain",
                        None,
                    ),
                    (IggyError::LaneBlocked(operation_id), "lane_blocked", None),
                    (
                        IggyError::FetchProgressUncertain(operation_id),
                        "fetch_progress_uncertain",
                        None,
                    ),
                    (
                        IggyError::PartitionLaneBlocked(u32::MAX, operation_id),
                        "lane_blocked",
                        Some(u32::MAX),
                    ),
                    (
                        IggyError::OperationNotRetained(operation_id),
                        "not_retained",
                        None,
                    ),
                ] {
                    let code = error.as_code();
                    let error = to_runtime_error(error);
                    assert!(error.is_instance_of::<PendingOperationError>(py));
                    assert!(error.is_instance_of::<PyIggyError>(py));
                    assert!(error.is_instance_of::<PyRuntimeError>(py));
                    let value = error.value(py);
                    assert_eq!(
                        value
                            .getattr("operation_id")
                            .unwrap()
                            .extract::<u64>()
                            .unwrap(),
                        operation_id
                    );
                    assert_eq!(
                        value.getattr("code").unwrap().extract::<u32>().unwrap(),
                        code
                    );
                    assert_eq!(
                        value.getattr("kind").unwrap().extract::<String>().unwrap(),
                        kind
                    );
                    assert_eq!(
                        value
                            .getattr("partition_id")
                            .unwrap()
                            .extract::<Option<u32>>()
                            .unwrap(),
                        partition_id
                    );
                    assert!(value.setattr("operation_id", 0).is_err());
                    assert!(value.setattr("partition_id", 0).is_err());
                }
            }
        });
    }

    #[test]
    fn given_read_error_when_mapping_should_keep_partition_skip_and_cause() {
        const PARTITION_ID: u32 = 7;
        const OPERATION_ID: u64 = 42;
        const SKIP_OFFSET: u64 = 11;
        Python::initialize();
        Python::attach(|py| {
            let cause = IggyError::FetchProgressUncertain(OPERATION_ID);
            let code = cause.as_code();
            let read_error = RustReadError {
                partition_id: Some(PARTITION_ID),
                error: cause,
                skip: Some(RustPollingStrategy::offset(SKIP_OFFSET)),
            };
            let message = read_error.to_string();

            let error = to_read_error(read_error);

            assert!(error.is_instance_of::<ReadError>(py));
            assert!(error.is_instance_of::<PyIggyError>(py));
            assert!(error.is_instance_of::<PyRuntimeError>(py));
            let value = error.value(py);
            assert_eq!(
                value.getattr("code").unwrap().extract::<u32>().unwrap(),
                code
            );
            assert_eq!(
                value
                    .getattr("partition_id")
                    .unwrap()
                    .extract::<Option<u32>>()
                    .unwrap(),
                Some(PARTITION_ID)
            );
            assert_eq!(value.str().unwrap().to_str().unwrap(), message);
            let skip = value.getattr("skip").unwrap();
            assert_eq!(
                skip.extract::<PollingStrategy>().unwrap().inner,
                RustPollingStrategy::offset(SKIP_OFFSET)
            );
            let cause = value.getattr("error").unwrap();
            assert!(cause.is_instance_of::<PendingOperationError>());
            assert_eq!(
                cause
                    .getattr("operation_id")
                    .unwrap()
                    .extract::<u64>()
                    .unwrap(),
                OPERATION_ID
            );
            assert_eq!(
                cause.getattr("code").unwrap().extract::<u32>().unwrap(),
                code
            );
        });
    }

    #[test]
    fn given_read_error_of_the_whole_reader_when_mapping_should_have_no_partition_or_skip() {
        Python::initialize();
        Python::attach(|py| {
            let error = to_read_error(RustReadError {
                partition_id: None,
                error: IggyError::Disconnected,
                skip: None,
            });

            let value = error.value(py);
            assert!(value.getattr("partition_id").unwrap().is_none());
            assert!(value.getattr("skip").unwrap().is_none());
            let cause = value.getattr("error").unwrap();
            assert!(cause.is_instance_of::<PyIggyError>());
            assert!(!cause.is_instance_of::<PendingOperationError>());
        });
    }

    #[test]
    fn business_rejection_keeps_original_error_without_retained_identity() {
        Python::initialize();
        Python::attach(|py| {
            let rejection = IggyError::UserAlreadyExists;
            let code = rejection.as_code();
            let message = rejection.to_string();
            let error = to_runtime_error(rejection);
            assert!(error.is_instance_of::<PyIggyError>(py));
            assert!(error.is_instance_of::<PyRuntimeError>(py));
            assert!(!error.is_instance_of::<PendingOperationError>(py));
            let value = error.value(py);
            assert_eq!(
                value.getattr("code").unwrap().extract::<u32>().unwrap(),
                code
            );
            assert_eq!(value.str().unwrap().to_str().unwrap(), message);
        });
    }
}
