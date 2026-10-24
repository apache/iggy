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
    HeaderValue as RustHeaderValue, OptionSpec as RustOptionSpec, OptionsScope as RustOptionsScope,
    ResourceOptions,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::str::FromStr;

use crate::user_headers::{
    HeaderValue, UserHeaders, rust_header_value_to_py, rust_user_headers_to_py,
};

/// One entry of a resource's option catalog, as served by `describe_options`.
#[gen_stub_pyclass]
#[pyclass]
pub struct OptionSpec {
    inner: RustOptionSpec,
}

impl From<RustOptionSpec> for OptionSpec {
    fn from(spec: RustOptionSpec) -> Self {
        Self { inner: spec }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl OptionSpec {
    /// The option key a create command accepts.
    #[getter]
    pub fn key(&self) -> String {
        self.inner.key.clone()
    }

    /// Name of this key's canonical kind: what the server encodes its default
    /// under, and what a value set by a create or an update is stored as
    /// whatever kind it was sent in, since admission re-encodes the block from
    /// its own parse.
    #[getter]
    pub fn kind(&self) -> String {
        self.inner.kind.to_string()
    }

    /// The key's default as a `HeaderValue`, or `None` when the key has no
    /// default.
    ///
    /// The same type message user headers use, so the usual accessors read it;
    /// options ride that codec.
    #[gen_stub(override_return_type(type_repr = "HeaderValue | None"))]
    #[getter]
    #[gen_stub(override_return_type(type_repr = "HeaderValue | None"))]
    pub fn default_value<'a>(&self, py: Python<'a>) -> PyResult<Option<Bound<'a, HeaderValue>>> {
        if self.inner.default_value.is_empty() {
            return Ok(None);
        }
        let value = RustHeaderValue::from_raw(self.inner.kind, &self.inner.default_value).map_err(
            |error| {
                PyValueError::new_err(format!(
                    "option '{}' has a default this build cannot read: {error}",
                    self.inner.key
                ))
            },
        )?;
        rust_header_value_to_py(py, &value).map(Some)
    }

    /// What the option does, including the bounds its value is checked against.
    #[getter]
    pub fn description(&self) -> String {
        self.inner.description.clone()
    }

    /// Whether an update may set the key.
    #[getter]
    pub fn updatable(&self) -> bool {
        self.inner.updatable
    }

    /// Whether an update may reset the key to its default.
    #[getter]
    pub fn resettable(&self) -> bool {
        self.inner.resettable
    }

    /// Whether the key is write-only: accepted on create, never returned.
    #[getter]
    pub fn secret(&self) -> bool {
        self.inner.secret
    }

    /// Whether the server computes the key and returns it on reads, never
    /// accepting it.
    #[getter]
    pub fn read_only(&self) -> bool {
        self.inner.read_only
    }

    /// Whether a node that does not understand the key must refuse to serve
    /// the resource.
    #[getter]
    pub fn critical(&self) -> bool {
        self.inner.critical
    }

    fn __repr__(&self) -> String {
        format!(
            "OptionSpec(key='{}', kind='{}')",
            self.inner.key, self.inner.kind
        )
    }
}

/// Resolve the scope a `describe_options` call names.
///
/// Takes the same names the CLI and the REST path take (`topic`, `stream`,
/// `user`) rather than an enum class, so a scope is one string at the call site.
///
/// # Errors
///
/// Raises `ValueError` for a name outside the three scopes.
pub fn options_scope_from_str(scope: &str) -> PyResult<RustOptionsScope> {
    RustOptionsScope::from_str(scope).map_err(|_| {
        PyValueError::new_err(format!(
            "unknown options scope '{scope}', expected one of: topic, stream, user"
        ))
    })
}

/// The entries of one provenance, as the dictionary message user headers come
/// back as.
///
/// Options ride the user-headers codec, so they are handed back through the
/// same `HeaderKey`/`HeaderValue` types rather than a second shape meaning the
/// same thing: `to_scalar_dict()` works on the result exactly as it does on
/// `ReceiveMessage.user_headers`.
pub(crate) fn options_by_provenance<'a>(
    py: Python<'a>,
    options: &ResourceOptions,
    explicit: bool,
) -> PyResult<Bound<'a, UserHeaders>> {
    let selected = options
        .iter()
        .filter(|(_, option)| option.explicit == explicit)
        .map(|(key, option)| (key.clone(), option.value.clone()))
        .collect();
    rust_user_headers_to_py(py, selected)
}
