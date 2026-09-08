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

use iggy::prelude::Partitioning as RustPartitioning;
use pyo3::{exceptions::PyValueError, prelude::*, types::PyBytes};
use pyo3_stub_gen::{
    derive::{gen_stub_pyclass, gen_stub_pymethods},
    impl_stub_type,
};

/// Defines how a batch of messages is assigned to a topic partition.
#[derive(Clone)]
#[pyclass(from_py_object)]
#[gen_stub_pyclass]
pub struct Partitioning {
    pub(crate) inner: RustPartitioning,
}

#[gen_stub_pymethods]
#[pymethods]
impl Partitioning {
    /// Routes the batch to one partition selected by round-robin.
    #[staticmethod]
    pub fn balanced() -> Self {
        Self {
            inner: RustPartitioning::balanced(),
        }
    }

    /// Routes the batch to the specified partition.
    ///
    /// `partition_id` must be between 0 and `2**32 - 1`. The topic must contain
    /// that partition when the batch is sent.
    ///
    /// Raises:
    ///     TypeError: If `partition_id` is not an integer.
    ///     OverflowError: If `partition_id` is outside the supported unsigned
    ///         32-bit range.
    #[staticmethod]
    pub fn partition_id(partition_id: u32) -> Self {
        Self {
            inner: RustPartitioning::partition_id(partition_id),
        }
    }

    /// Routes the batch to one partition selected by hashing `key`.
    ///
    /// `key` may be `str` or `bytes`. Strings are encoded as UTF-8; the encoded
    /// key must contain between 1 and 255 bytes.
    ///
    /// Raises:
    ///     ValueError: If the encoded key is empty or exceeds 255 bytes.
    ///     TypeError: If `key` is not `str` or `bytes`.
    #[staticmethod]
    pub fn messages_key(py: Python<'_>, key: PyMessagesKey) -> PyResult<Self> {
        let key = match key {
            PyMessagesKey::String(key) => key.into_bytes(),
            PyMessagesKey::Bytes(key) => key.extract::<Vec<u8>>(py)?,
        };
        let inner = RustPartitioning::messages_key(&key)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        Ok(Self { inner })
    }
}

#[derive(FromPyObject)]
pub enum PyMessagesKey {
    #[pyo3(transparent, annotation = "str")]
    String(String),
    #[pyo3(transparent, annotation = "bytes")]
    Bytes(Py<PyBytes>),
}
impl_stub_type!(PyMessagesKey = String | PyBytes);

#[derive(FromPyObject)]
pub(crate) enum PyPartitioning {
    #[pyo3(transparent, annotation = "Partitioning")]
    Strategy(Partitioning),
    #[pyo3(transparent, annotation = "int")]
    PartitionId(u32),
}
impl_stub_type!(PyPartitioning = Partitioning | isize);

impl From<PyPartitioning> for RustPartitioning {
    fn from(partitioning: PyPartitioning) -> Self {
        match partitioning {
            PyPartitioning::Strategy(partitioning) => partitioning.inner,
            PyPartitioning::PartitionId(partition_id) => Self::partition_id(partition_id),
        }
    }
}
