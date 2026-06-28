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

use std::str::FromStr;

use iggy::prelude::{IdKind, Identifier, IdentityInfo as RustIdentityInfo};
use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use pyo3_stub_gen::impl_stub_type;

#[derive(FromPyObject, IntoPyObject)]
pub(crate) enum PyIdentifier {
    #[pyo3(transparent, annotation = "str")]
    String(String),
    #[pyo3(transparent, annotation = "int")]
    Int(u32),
}
impl_stub_type!(PyIdentifier = String | isize);

impl TryFrom<PyIdentifier> for Identifier {
    type Error = PyErr;

    fn try_from(py_identifier: PyIdentifier) -> Result<Self, Self::Error> {
        match py_identifier {
            PyIdentifier::String(s) => {
                Identifier::from_str(&s).map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))
            }
            PyIdentifier::Int(i) => {
                Identifier::numeric(i).map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))
            }
        }
    }
}

impl TryFrom<&Identifier> for PyIdentifier {
    type Error = PyErr;

    fn try_from(val: &Identifier) -> Result<Self, Self::Error> {
        match val.kind {
            IdKind::String => val
                .get_string_value()
                .map(PyIdentifier::String)
                .map_err(|e| PyErr::new::<PyRuntimeError, _>(e.to_string())),
            IdKind::Numeric => val
                .get_u32_value()
                .map(PyIdentifier::Int)
                .map_err(|e| PyErr::new::<PyRuntimeError, _>(e.to_string())),
        }
    }
}

/// Authentication details returned by `IggyClient.login_user()`.
///
/// This object contains the authenticated user id and any access token details
/// returned by the server.
#[gen_stub_pyclass]
#[pyclass]
pub struct IdentityInfo {
    user_id: u32,
    access_token_value: Option<String>,
    access_token_expiry: Option<u64>,
}

impl From<RustIdentityInfo> for IdentityInfo {
    fn from(identity_info: RustIdentityInfo) -> Self {
        let (access_token_value, access_token_expiry) = match identity_info.access_token {
            Some(token_info) => (Some(token_info.token), Some(token_info.expiry)),
            None => (None, None),
        };
        Self {
            user_id: identity_info.user_id,
            access_token_value,
            access_token_expiry,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl IdentityInfo {
    /// Get the authenticated user id.
    ///
    /// Returns:
    ///     The user id as `int`.
    #[getter]
    pub fn user_id(&self) -> u32 {
        self.user_id
    }

    /// Get the access token value.
    ///
    /// Returns:
    ///     The access token as `str`, or `None` if no access token was returned.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "builtins.str | None"))]
    pub fn access_token_value(&self) -> Option<String> {
        self.access_token_value.clone()
    }

    /// Get the access token expiry value.
    ///
    /// Returns:
    ///     The encoded expiry value as `int`, or `None` if no access token expiry
    ///     was returned.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "builtins.int | None"))]
    pub fn access_token_expiry(&self) -> Option<u64> {
        self.access_token_expiry
    }
}
