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

use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use iggy::prelude::{
    HeaderField, HeaderKey as RustHeaderKey, HeaderKind, HeaderValue as RustHeaderValue,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pyclass::CompareOp;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyString};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_complex_enum, gen_stub_pymethods};

type RustUserHeaders = BTreeMap<RustHeaderKey, RustHeaderValue>;

#[gen_stub_pyclass_complex_enum]
#[pyclass]
/// Typed key for an Iggy user header.
///
/// Use these constructors when the header key must preserve an explicit
/// wire type instead of using the common string-key dictionary form.
pub enum HeaderKey {
    /// Raw bytes key. The byte length must be 1..=255.
    Raw { value: Py<PyBytes> },
    /// UTF-8 string key. The encoded byte length must be 1..=255.
    String { value: String },
    /// Boolean key.
    Bool { value: bool },
    /// Signed 8-bit integer key.
    Int8 { value: i8 },
    /// Signed 16-bit integer key.
    Int16 { value: i16 },
    /// Signed 32-bit integer key.
    Int32 { value: i32 },
    /// Signed 64-bit integer key.
    Int64 { value: i64 },
    /// Signed 128-bit integer key.
    Int128 { value: i128 },
    /// Unsigned 8-bit integer key.
    UnsignedInt8 { value: u8 },
    /// Unsigned 16-bit integer key.
    UnsignedInt16 { value: u16 },
    /// Unsigned 32-bit integer key.
    UnsignedInt32 { value: u32 },
    /// Unsigned 64-bit integer key.
    UnsignedInt64 { value: u64 },
    /// Unsigned 128-bit integer key.
    UnsignedInt128 { value: u128 },
    /// 32-bit floating point key.
    Float32 { value: f32 },
    /// 64-bit floating point key.
    Float64 { value: f64 },
}

#[gen_stub_pyclass_complex_enum]
#[pyclass]
/// Typed value for an Iggy user header.
///
/// Use these constructors when the header value must preserve an explicit
/// wire type instead of using the common Python scalar dictionary form.
pub enum HeaderValue {
    /// Raw bytes value. The byte length must be 1..=255.
    Raw { value: Py<PyBytes> },
    /// UTF-8 string value. The encoded byte length must be 1..=255.
    String { value: String },
    /// Boolean value.
    Bool { value: bool },
    /// Signed 8-bit integer value.
    Int8 { value: i8 },
    /// Signed 16-bit integer value.
    Int16 { value: i16 },
    /// Signed 32-bit integer value.
    Int32 { value: i32 },
    /// Signed 64-bit integer value.
    Int64 { value: i64 },
    /// Signed 128-bit integer value.
    Int128 { value: i128 },
    /// Unsigned 8-bit integer value.
    UnsignedInt8 { value: u8 },
    /// Unsigned 16-bit integer value.
    UnsignedInt16 { value: u16 },
    /// Unsigned 32-bit integer value.
    UnsignedInt32 { value: u32 },
    /// Unsigned 64-bit integer value.
    UnsignedInt64 { value: u64 },
    /// Unsigned 128-bit integer value.
    UnsignedInt128 { value: u128 },
    /// 32-bit floating point value.
    Float32 { value: f32 },
    /// 64-bit floating point value.
    Float64 { value: f64 },
}

#[gen_stub_pymethods]
#[pymethods]
impl HeaderKey {
    pub fn __hash__(&self, py: Python<'_>) -> PyResult<isize> {
        Ok(py_hash(header_identity_hash(self.identity(py)?)))
    }

    pub fn __richcmp__(
        &self,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
        op: CompareOp,
    ) -> PyResult<Py<PyAny>> {
        header_key_richcmp(py, self.identity(py)?, other, op)
    }

    pub fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        self.repr(py)
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl HeaderValue {
    pub fn __hash__(&self, py: Python<'_>) -> PyResult<isize> {
        Ok(py_hash(header_identity_hash(self.identity(py)?)))
    }

    pub fn __richcmp__(
        &self,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
        op: CompareOp,
    ) -> PyResult<Py<PyAny>> {
        header_value_richcmp(py, self.identity(py)?, other, op)
    }

    pub fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        self.repr(py)
    }
}

impl HeaderKey {
    fn to_rust(&self, py: Python<'_>) -> PyResult<RustHeaderKey> {
        match self {
            HeaderKey::Raw { value } => {
                RustHeaderKey::try_from(value.extract::<Vec<u8>>(py)?).map_err(to_value_error)
            }
            HeaderKey::String { value } => {
                RustHeaderKey::try_from(value.as_str()).map_err(to_value_error)
            }
            HeaderKey::Bool { value } => Ok((*value).into()),
            HeaderKey::Int8 { value } => Ok((*value).into()),
            HeaderKey::Int16 { value } => Ok((*value).into()),
            HeaderKey::Int32 { value } => Ok((*value).into()),
            HeaderKey::Int64 { value } => Ok((*value).into()),
            HeaderKey::Int128 { value } => Ok((*value).into()),
            HeaderKey::UnsignedInt8 { value } => Ok((*value).into()),
            HeaderKey::UnsignedInt16 { value } => Ok((*value).into()),
            HeaderKey::UnsignedInt32 { value } => Ok((*value).into()),
            HeaderKey::UnsignedInt64 { value } => Ok((*value).into()),
            HeaderKey::UnsignedInt128 { value } => Ok((*value).into()),
            HeaderKey::Float32 { value } => checked_float32(*value),
            HeaderKey::Float64 { value } => Ok((*value).into()),
        }
    }

    fn from_rust<'a>(py: Python<'a>, value: &RustHeaderKey) -> PyResult<Bound<'a, Self>> {
        let key = match value.kind() {
            HeaderKind::Raw => HeaderKey::Raw {
                value: PyBytes::new(py, value.as_raw().map_err(to_value_error)?).unbind(),
            },
            HeaderKind::String => HeaderKey::String {
                value: value.as_str().map_err(to_value_error)?.to_string(),
            },
            HeaderKind::Bool => HeaderKey::Bool {
                value: value.as_bool().map_err(to_value_error)?,
            },
            HeaderKind::Int8 => HeaderKey::Int8 {
                value: value.as_int8().map_err(to_value_error)?,
            },
            HeaderKind::Int16 => HeaderKey::Int16 {
                value: value.as_int16().map_err(to_value_error)?,
            },
            HeaderKind::Int32 => HeaderKey::Int32 {
                value: value.as_int32().map_err(to_value_error)?,
            },
            HeaderKind::Int64 => HeaderKey::Int64 {
                value: value.as_int64().map_err(to_value_error)?,
            },
            HeaderKind::Int128 => HeaderKey::Int128 {
                value: value.as_int128().map_err(to_value_error)?,
            },
            HeaderKind::Uint8 => HeaderKey::UnsignedInt8 {
                value: value.as_uint8().map_err(to_value_error)?,
            },
            HeaderKind::Uint16 => HeaderKey::UnsignedInt16 {
                value: value.as_uint16().map_err(to_value_error)?,
            },
            HeaderKind::Uint32 => HeaderKey::UnsignedInt32 {
                value: value.as_uint32().map_err(to_value_error)?,
            },
            HeaderKind::Uint64 => HeaderKey::UnsignedInt64 {
                value: value.as_uint64().map_err(to_value_error)?,
            },
            HeaderKind::Uint128 => HeaderKey::UnsignedInt128 {
                value: value.as_uint128().map_err(to_value_error)?,
            },
            HeaderKind::Float32 => HeaderKey::Float32 {
                value: value.as_float32().map_err(to_value_error)?,
            },
            HeaderKind::Float64 => HeaderKey::Float64 {
                value: value.as_float64().map_err(to_value_error)?,
            },
        };
        key.into_pyobject(py)
    }

    fn identity(&self, py: Python<'_>) -> PyResult<HeaderIdentity> {
        match self {
            HeaderKey::Raw { value } => Ok(HeaderIdentity::raw(value.extract::<Vec<u8>>(py)?)),
            HeaderKey::String { value } => Ok(HeaderIdentity::string(value.as_bytes().to_vec())),
            HeaderKey::Bool { value } => Ok(HeaderIdentity::bool(*value)),
            HeaderKey::Int8 { value } => Ok(HeaderIdentity::int8(*value)),
            HeaderKey::Int16 { value } => Ok(HeaderIdentity::int16(*value)),
            HeaderKey::Int32 { value } => Ok(HeaderIdentity::int32(*value)),
            HeaderKey::Int64 { value } => Ok(HeaderIdentity::int64(*value)),
            HeaderKey::Int128 { value } => Ok(HeaderIdentity::int128(*value)),
            HeaderKey::UnsignedInt8 { value } => Ok(HeaderIdentity::uint8(*value)),
            HeaderKey::UnsignedInt16 { value } => Ok(HeaderIdentity::uint16(*value)),
            HeaderKey::UnsignedInt32 { value } => Ok(HeaderIdentity::uint32(*value)),
            HeaderKey::UnsignedInt64 { value } => Ok(HeaderIdentity::uint64(*value)),
            HeaderKey::UnsignedInt128 { value } => Ok(HeaderIdentity::uint128(*value)),
            HeaderKey::Float32 { value } => Ok(HeaderIdentity::float32(*value)),
            HeaderKey::Float64 { value } => Ok(HeaderIdentity::float64(*value)),
        }
    }

    fn repr(&self, py: Python<'_>) -> PyResult<String> {
        match self {
            HeaderKey::Raw { value } => Ok(format!(
                "HeaderKey.Raw({})",
                value.bind(py).repr()?.extract::<String>()?
            )),
            HeaderKey::String { value } => Ok(format!("HeaderKey.String({value:?})")),
            HeaderKey::Bool { value } => Ok(format!("HeaderKey.Bool({value})")),
            HeaderKey::Int8 { value } => Ok(format!("HeaderKey.Int8({value})")),
            HeaderKey::Int16 { value } => Ok(format!("HeaderKey.Int16({value})")),
            HeaderKey::Int32 { value } => Ok(format!("HeaderKey.Int32({value})")),
            HeaderKey::Int64 { value } => Ok(format!("HeaderKey.Int64({value})")),
            HeaderKey::Int128 { value } => Ok(format!("HeaderKey.Int128({value})")),
            HeaderKey::UnsignedInt8 { value } => Ok(format!("HeaderKey.UnsignedInt8({value})")),
            HeaderKey::UnsignedInt16 { value } => Ok(format!("HeaderKey.UnsignedInt16({value})")),
            HeaderKey::UnsignedInt32 { value } => Ok(format!("HeaderKey.UnsignedInt32({value})")),
            HeaderKey::UnsignedInt64 { value } => Ok(format!("HeaderKey.UnsignedInt64({value})")),
            HeaderKey::UnsignedInt128 { value } => Ok(format!("HeaderKey.UnsignedInt128({value})")),
            HeaderKey::Float32 { value } => Ok(format!("HeaderKey.Float32({value:?})")),
            HeaderKey::Float64 { value } => Ok(format!("HeaderKey.Float64({value:?})")),
        }
    }
}

impl HeaderValue {
    fn to_rust(&self, py: Python<'_>) -> PyResult<RustHeaderValue> {
        match self {
            HeaderValue::Raw { value } => {
                RustHeaderValue::try_from(value.extract::<Vec<u8>>(py)?).map_err(to_value_error)
            }
            HeaderValue::String { value } => {
                RustHeaderValue::try_from(value.as_str()).map_err(to_value_error)
            }
            HeaderValue::Bool { value } => Ok((*value).into()),
            HeaderValue::Int8 { value } => Ok((*value).into()),
            HeaderValue::Int16 { value } => Ok((*value).into()),
            HeaderValue::Int32 { value } => Ok((*value).into()),
            HeaderValue::Int64 { value } => Ok((*value).into()),
            HeaderValue::Int128 { value } => Ok((*value).into()),
            HeaderValue::UnsignedInt8 { value } => Ok((*value).into()),
            HeaderValue::UnsignedInt16 { value } => Ok((*value).into()),
            HeaderValue::UnsignedInt32 { value } => Ok((*value).into()),
            HeaderValue::UnsignedInt64 { value } => Ok((*value).into()),
            HeaderValue::UnsignedInt128 { value } => Ok((*value).into()),
            HeaderValue::Float32 { value } => checked_float32(*value),
            HeaderValue::Float64 { value } => Ok((*value).into()),
        }
    }

    fn from_rust<'a>(py: Python<'a>, value: &RustHeaderValue) -> PyResult<Bound<'a, Self>> {
        let value = match value.kind() {
            HeaderKind::Raw => HeaderValue::Raw {
                value: PyBytes::new(py, value.as_raw().map_err(to_value_error)?).unbind(),
            },
            HeaderKind::String => HeaderValue::String {
                value: value.as_str().map_err(to_value_error)?.to_string(),
            },
            HeaderKind::Bool => HeaderValue::Bool {
                value: value.as_bool().map_err(to_value_error)?,
            },
            HeaderKind::Int8 => HeaderValue::Int8 {
                value: value.as_int8().map_err(to_value_error)?,
            },
            HeaderKind::Int16 => HeaderValue::Int16 {
                value: value.as_int16().map_err(to_value_error)?,
            },
            HeaderKind::Int32 => HeaderValue::Int32 {
                value: value.as_int32().map_err(to_value_error)?,
            },
            HeaderKind::Int64 => HeaderValue::Int64 {
                value: value.as_int64().map_err(to_value_error)?,
            },
            HeaderKind::Int128 => HeaderValue::Int128 {
                value: value.as_int128().map_err(to_value_error)?,
            },
            HeaderKind::Uint8 => HeaderValue::UnsignedInt8 {
                value: value.as_uint8().map_err(to_value_error)?,
            },
            HeaderKind::Uint16 => HeaderValue::UnsignedInt16 {
                value: value.as_uint16().map_err(to_value_error)?,
            },
            HeaderKind::Uint32 => HeaderValue::UnsignedInt32 {
                value: value.as_uint32().map_err(to_value_error)?,
            },
            HeaderKind::Uint64 => HeaderValue::UnsignedInt64 {
                value: value.as_uint64().map_err(to_value_error)?,
            },
            HeaderKind::Uint128 => HeaderValue::UnsignedInt128 {
                value: value.as_uint128().map_err(to_value_error)?,
            },
            HeaderKind::Float32 => HeaderValue::Float32 {
                value: value.as_float32().map_err(to_value_error)?,
            },
            HeaderKind::Float64 => HeaderValue::Float64 {
                value: value.as_float64().map_err(to_value_error)?,
            },
        };
        value.into_pyobject(py)
    }

    fn identity(&self, py: Python<'_>) -> PyResult<HeaderIdentity> {
        match self {
            HeaderValue::Raw { value } => Ok(HeaderIdentity::raw(value.extract::<Vec<u8>>(py)?)),
            HeaderValue::String { value } => Ok(HeaderIdentity::string(value.as_bytes().to_vec())),
            HeaderValue::Bool { value } => Ok(HeaderIdentity::bool(*value)),
            HeaderValue::Int8 { value } => Ok(HeaderIdentity::int8(*value)),
            HeaderValue::Int16 { value } => Ok(HeaderIdentity::int16(*value)),
            HeaderValue::Int32 { value } => Ok(HeaderIdentity::int32(*value)),
            HeaderValue::Int64 { value } => Ok(HeaderIdentity::int64(*value)),
            HeaderValue::Int128 { value } => Ok(HeaderIdentity::int128(*value)),
            HeaderValue::UnsignedInt8 { value } => Ok(HeaderIdentity::uint8(*value)),
            HeaderValue::UnsignedInt16 { value } => Ok(HeaderIdentity::uint16(*value)),
            HeaderValue::UnsignedInt32 { value } => Ok(HeaderIdentity::uint32(*value)),
            HeaderValue::UnsignedInt64 { value } => Ok(HeaderIdentity::uint64(*value)),
            HeaderValue::UnsignedInt128 { value } => Ok(HeaderIdentity::uint128(*value)),
            HeaderValue::Float32 { value } => Ok(HeaderIdentity::float32(*value)),
            HeaderValue::Float64 { value } => Ok(HeaderIdentity::float64(*value)),
        }
    }

    fn repr(&self, py: Python<'_>) -> PyResult<String> {
        match self {
            HeaderValue::Raw { value } => Ok(format!(
                "HeaderValue.Raw({})",
                value.bind(py).repr()?.extract::<String>()?
            )),
            HeaderValue::String { value } => Ok(format!("HeaderValue.String({value:?})")),
            HeaderValue::Bool { value } => Ok(format!("HeaderValue.Bool({value})")),
            HeaderValue::Int8 { value } => Ok(format!("HeaderValue.Int8({value})")),
            HeaderValue::Int16 { value } => Ok(format!("HeaderValue.Int16({value})")),
            HeaderValue::Int32 { value } => Ok(format!("HeaderValue.Int32({value})")),
            HeaderValue::Int64 { value } => Ok(format!("HeaderValue.Int64({value})")),
            HeaderValue::Int128 { value } => Ok(format!("HeaderValue.Int128({value})")),
            HeaderValue::UnsignedInt8 { value } => Ok(format!("HeaderValue.UnsignedInt8({value})")),
            HeaderValue::UnsignedInt16 { value } => {
                Ok(format!("HeaderValue.UnsignedInt16({value})"))
            }
            HeaderValue::UnsignedInt32 { value } => {
                Ok(format!("HeaderValue.UnsignedInt32({value})"))
            }
            HeaderValue::UnsignedInt64 { value } => {
                Ok(format!("HeaderValue.UnsignedInt64({value})"))
            }
            HeaderValue::UnsignedInt128 { value } => {
                Ok(format!("HeaderValue.UnsignedInt128({value})"))
            }
            HeaderValue::Float32 { value } => Ok(format!("HeaderValue.Float32({value:?})")),
            HeaderValue::Float64 { value } => Ok(format!("HeaderValue.Float64({value:?})")),
        }
    }
}

#[derive(PartialEq, Eq, Hash)]
struct HeaderIdentity {
    kind: u8,
    value: Vec<u8>,
}

impl HeaderIdentity {
    fn raw(value: Vec<u8>) -> Self {
        Self {
            kind: HeaderKind::Raw.as_code(),
            value,
        }
    }

    fn string(value: Vec<u8>) -> Self {
        Self {
            kind: HeaderKind::String.as_code(),
            value,
        }
    }

    fn bool(value: bool) -> Self {
        Self {
            kind: HeaderKind::Bool.as_code(),
            value: vec![u8::from(value)],
        }
    }

    fn int8(value: i8) -> Self {
        Self {
            kind: HeaderKind::Int8.as_code(),
            value: value.to_le_bytes().to_vec(),
        }
    }

    fn int16(value: i16) -> Self {
        Self {
            kind: HeaderKind::Int16.as_code(),
            value: value.to_le_bytes().to_vec(),
        }
    }

    fn int32(value: i32) -> Self {
        Self {
            kind: HeaderKind::Int32.as_code(),
            value: value.to_le_bytes().to_vec(),
        }
    }

    fn int64(value: i64) -> Self {
        Self {
            kind: HeaderKind::Int64.as_code(),
            value: value.to_le_bytes().to_vec(),
        }
    }

    fn int128(value: i128) -> Self {
        Self {
            kind: HeaderKind::Int128.as_code(),
            value: value.to_le_bytes().to_vec(),
        }
    }

    fn uint8(value: u8) -> Self {
        Self {
            kind: HeaderKind::Uint8.as_code(),
            value: value.to_le_bytes().to_vec(),
        }
    }

    fn uint16(value: u16) -> Self {
        Self {
            kind: HeaderKind::Uint16.as_code(),
            value: value.to_le_bytes().to_vec(),
        }
    }

    fn uint32(value: u32) -> Self {
        Self {
            kind: HeaderKind::Uint32.as_code(),
            value: value.to_le_bytes().to_vec(),
        }
    }

    fn uint64(value: u64) -> Self {
        Self {
            kind: HeaderKind::Uint64.as_code(),
            value: value.to_le_bytes().to_vec(),
        }
    }

    fn uint128(value: u128) -> Self {
        Self {
            kind: HeaderKind::Uint128.as_code(),
            value: value.to_le_bytes().to_vec(),
        }
    }

    fn float32(value: f32) -> Self {
        Self {
            kind: HeaderKind::Float32.as_code(),
            value: value.to_le_bytes().to_vec(),
        }
    }

    fn float64(value: f64) -> Self {
        Self {
            kind: HeaderKind::Float64.as_code(),
            value: value.to_le_bytes().to_vec(),
        }
    }
}

pub(crate) fn py_user_headers_to_rust(
    py: Python<'_>,
    headers: &Bound<'_, PyDict>,
) -> PyResult<RustUserHeaders> {
    // Each key/value pair is converted independently: typed `HeaderKey` /
    // `HeaderValue` are used as-is, while plain Python scalars are converted on
    // a best-effort basis.
    let mut rust_headers = BTreeMap::new();
    for (key, value) in headers.iter() {
        let key = py_header_key_to_rust(py, &key)?;
        let value = py_header_value_to_rust(py, &value)?;
        rust_headers.insert(key, value);
    }
    Ok(rust_headers)
}

pub(crate) fn rust_user_headers_to_py<'a>(
    py: Python<'a>,
    headers: RustUserHeaders,
) -> PyResult<Bound<'a, UserHeaders>> {
    // Always expose the explicitly typed dict[HeaderKey, HeaderValue] so that
    // no wire-type information is silently dropped. Callers who prefer the
    // convenient plain form opt in through `UserHeaders.to_plain`.
    let result = Bound::new(py, UserHeaders)?;
    let mapping = result.as_any();
    for (key, value) in headers {
        let key = HeaderKey::from_rust(py, &key)?;
        let value = HeaderValue::from_rust(py, &value)?;
        mapping.set_item(key, value)?;
    }
    Ok(result)
}

/// User headers dictionary returned by `ReceiveMessage.user_headers`.
///
/// This is a regular `dict[HeaderKey, HeaderValue]` (so all mapping
/// operations work) that additionally exposes `to_plain` for the convenient
/// scalar form.
#[gen_stub_pyclass]
#[pyclass(extends=PyDict)]
pub struct UserHeaders;

#[gen_stub_pymethods]
#[pymethods]
impl UserHeaders {
    /// Wraps a mapping so its entries gain the `to_plain` helper.
    ///
    /// Accepts a dict whose keys and values can each independently be
    /// `HeaderKey`/`HeaderValue` or a plain scalar (`str | bytes | bool |
    /// int | float`). The inherited `dict` initializer copies the provided
    /// mapping.
    #[new]
    #[pyo3(signature = (mapping=None))]
    pub fn new(
        #[gen_stub(override_type(type_repr = "dict[typing.Any, typing.Any] | None"))]
        mapping: Option<&Bound<'_, PyAny>>,
    ) -> Self {
        let _ = mapping;
        UserHeaders
    }

    /// Converts these headers into the convenient plain dictionary form.
    ///
    /// Every header kind maps losslessly onto a Python scalar, so this never
    /// loses information; it only returns an error if a stored field cannot be
    /// decoded.
    #[gen_stub(override_return_type(
        type_repr = "dict[str | bytes | bool | int | float, str | bytes | bool | int | float]"
    ))]
    pub fn to_plain<'a>(slf: &Bound<'a, Self>) -> PyResult<Bound<'a, PyDict>> {
        let py = slf.py();
        let dict = slf.as_any().cast::<PyDict>()?;
        let headers = py_user_headers_to_rust(py, dict)?;
        rust_user_headers_to_plain_py(py, headers)
    }
}

fn py_header_key_to_rust(py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<RustHeaderKey> {
    // A typed `HeaderKey` already carries an explicit wire type.
    if let Ok(header_key) = key.extract::<PyRef<'_, HeaderKey>>() {
        return header_key.to_rust(py);
    }

    // Otherwise best-effort convert the plain Python scalar.
    plain_to_header_field(key)?.ok_or_else(|| {
        PyValueError::new_err("User header keys must be str, bytes, bool, int, float, or HeaderKey")
    })
}

fn py_header_value_to_rust(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<RustHeaderValue> {
    // A typed `HeaderValue` already carries an explicit wire type.
    if let Ok(header_value) = value.extract::<PyRef<'_, HeaderValue>>() {
        return header_value.to_rust(py);
    }

    // Otherwise best-effort convert the plain Python scalar.
    plain_to_header_field(value)?.ok_or_else(|| {
        PyValueError::new_err(
            "User header values must be str, bytes, bool, int, float, or HeaderValue",
        )
    })
}

/// Converts a plain Python scalar into a header field (used for both keys and
/// values). Returns `Ok(None)` when the object is not a supported plain type so
/// the caller can raise a message naming keys vs values; genuine range/length
/// errors are returned as `Err`.
fn plain_to_header_field<T>(obj: &Bound<'_, PyAny>) -> PyResult<Option<HeaderField<T>>> {
    // `bool` must be checked before `int` since Python bools are also ints.
    if obj.is_instance_of::<PyBool>() {
        return Ok(Some(obj.extract::<bool>()?.into()));
    }
    if obj.is_instance_of::<PyInt>() {
        return int_to_header_field(obj).map(Some);
    }
    if obj.is_instance_of::<PyFloat>() {
        return Ok(Some(float_to_header_field(obj.extract::<f64>()?)));
    }
    if obj.is_instance_of::<PyString>() {
        return HeaderField::try_from(obj.extract::<String>()?)
            .map(Some)
            .map_err(to_value_error);
    }
    if obj.is_instance_of::<PyBytes>() {
        return HeaderField::try_from(obj.extract::<Vec<u8>>()?)
            .map(Some)
            .map_err(to_value_error);
    }
    Ok(None)
}

/// Picks the smallest header integer kind that represents the value exactly:
/// non-negative values use the narrowest unsigned kind, negatives the narrowest
/// signed kind. Values beyond the 128-bit range are rejected.
fn int_to_header_field<T>(obj: &Bound<'_, PyAny>) -> PyResult<HeaderField<T>> {
    if let Ok(value) = obj.extract::<u8>() {
        return Ok(value.into());
    }
    if let Ok(value) = obj.extract::<u16>() {
        return Ok(value.into());
    }
    if let Ok(value) = obj.extract::<u32>() {
        return Ok(value.into());
    }
    if let Ok(value) = obj.extract::<u64>() {
        return Ok(value.into());
    }
    if let Ok(value) = obj.extract::<u128>() {
        return Ok(value.into());
    }
    if let Ok(value) = obj.extract::<i8>() {
        return Ok(value.into());
    }
    if let Ok(value) = obj.extract::<i16>() {
        return Ok(value.into());
    }
    if let Ok(value) = obj.extract::<i32>() {
        return Ok(value.into());
    }
    if let Ok(value) = obj.extract::<i64>() {
        return Ok(value.into());
    }
    if let Ok(value) = obj.extract::<i128>() {
        return Ok(value.into());
    }
    Err(PyValueError::new_err(
        "User header int values must fit within the 128-bit range",
    ))
}

/// Uses `Float32` when the value is exactly representable as an f32, otherwise
/// `Float64`, so the plain float is always stored losslessly.
fn float_to_header_field<T>(value: f64) -> HeaderField<T> {
    let narrowed = value as f32;
    if f64::from(narrowed) == value {
        narrowed.into()
    } else {
        value.into()
    }
}

fn checked_float32<T>(value: f32) -> PyResult<HeaderField<T>> {
    if !value.is_finite() {
        return Err(PyValueError::new_err(
            "Float32 header must be a finite value within the 32-bit float range",
        ));
    }
    Ok(value.into())
}

fn rust_user_headers_to_plain_py<'a>(
    py: Python<'a>,
    headers: RustUserHeaders,
) -> PyResult<Bound<'a, PyDict>> {
    // Every header kind maps losslessly onto a Python scalar, so the conversion
    // is direct and only surfaces genuine decode errors.
    let result = PyDict::new(py);
    for (key, value) in headers {
        let key = header_field_to_plain_py(py, &key)?;
        let value = header_field_to_plain_py(py, &value)?;
        result.set_item(key, value)?;
    }
    Ok(result)
}

fn header_field_to_plain_py<'a, T>(
    py: Python<'a>,
    field: &HeaderField<T>,
) -> PyResult<Bound<'a, PyAny>> {
    let plain = match field.kind() {
        HeaderKind::Raw => PyBytes::new(py, field.as_raw().map_err(to_value_error)?).into_any(),
        HeaderKind::String => field
            .as_str()
            .map_err(to_value_error)?
            .into_pyobject(py)?
            .into_any(),
        HeaderKind::Bool => field
            .as_bool()
            .map_err(to_value_error)?
            .into_pyobject(py)?
            .to_owned()
            .into_any(),
        HeaderKind::Int8 => field
            .as_int8()
            .map_err(to_value_error)?
            .into_pyobject(py)?
            .into_any(),
        HeaderKind::Int16 => field
            .as_int16()
            .map_err(to_value_error)?
            .into_pyobject(py)?
            .into_any(),
        HeaderKind::Int32 => field
            .as_int32()
            .map_err(to_value_error)?
            .into_pyobject(py)?
            .into_any(),
        HeaderKind::Int64 => field
            .as_int64()
            .map_err(to_value_error)?
            .into_pyobject(py)?
            .into_any(),
        HeaderKind::Int128 => field
            .as_int128()
            .map_err(to_value_error)?
            .into_pyobject(py)?
            .into_any(),
        HeaderKind::Uint8 => field
            .as_uint8()
            .map_err(to_value_error)?
            .into_pyobject(py)?
            .into_any(),
        HeaderKind::Uint16 => field
            .as_uint16()
            .map_err(to_value_error)?
            .into_pyobject(py)?
            .into_any(),
        HeaderKind::Uint32 => field
            .as_uint32()
            .map_err(to_value_error)?
            .into_pyobject(py)?
            .into_any(),
        HeaderKind::Uint64 => field
            .as_uint64()
            .map_err(to_value_error)?
            .into_pyobject(py)?
            .into_any(),
        HeaderKind::Uint128 => field
            .as_uint128()
            .map_err(to_value_error)?
            .into_pyobject(py)?
            .into_any(),
        HeaderKind::Float32 => f64::from(field.as_float32().map_err(to_value_error)?)
            .into_pyobject(py)?
            .into_any(),
        HeaderKind::Float64 => field
            .as_float64()
            .map_err(to_value_error)?
            .into_pyobject(py)?
            .into_any(),
    };
    Ok(plain)
}

fn header_key_richcmp<'py>(
    py: Python<'py>,
    identity: HeaderIdentity,
    other: &Bound<'py, PyAny>,
    op: CompareOp,
) -> PyResult<Py<PyAny>> {
    let Ok(other) = other.extract::<PyRef<'_, HeaderKey>>() else {
        return match op {
            CompareOp::Eq => Ok(false.into_pyobject(py)?.to_owned().into_any().unbind()),
            CompareOp::Ne => Ok(true.into_pyobject(py)?.to_owned().into_any().unbind()),
            _ => Ok(py.NotImplemented()),
        };
    };

    let result = match op {
        CompareOp::Eq => identity == other.identity(py)?,
        CompareOp::Ne => identity != other.identity(py)?,
        _ => return Ok(py.NotImplemented()),
    };
    Ok(result.into_pyobject(py)?.to_owned().into_any().unbind())
}

fn header_value_richcmp<'py>(
    py: Python<'py>,
    identity: HeaderIdentity,
    other: &Bound<'py, PyAny>,
    op: CompareOp,
) -> PyResult<Py<PyAny>> {
    let Ok(other) = other.extract::<PyRef<'_, HeaderValue>>() else {
        return match op {
            CompareOp::Eq => Ok(false.into_pyobject(py)?.to_owned().into_any().unbind()),
            CompareOp::Ne => Ok(true.into_pyobject(py)?.to_owned().into_any().unbind()),
            _ => Ok(py.NotImplemented()),
        };
    };

    let result = match op {
        CompareOp::Eq => identity == other.identity(py)?,
        CompareOp::Ne => identity != other.identity(py)?,
        _ => return Ok(py.NotImplemented()),
    };
    Ok(result.into_pyobject(py)?.to_owned().into_any().unbind())
}

fn header_identity_hash(identity: HeaderIdentity) -> u64 {
    let mut hasher = DefaultHasher::new();
    identity.hash(&mut hasher);
    hasher.finish()
}

fn py_hash(hash: u64) -> isize {
    let hash = hash as isize;
    // CPython reserves -1 as the hash error sentinel, so valid hashes with
    // that value must be remapped.
    if hash == -1 { -2 } else { hash }
}

fn to_value_error(error: impl ToString) -> PyErr {
    PyValueError::new_err(error.to_string())
}
