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
use pyo3::PyClass;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pyclass::CompareOp;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyString, PyTuple};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_complex_enum, gen_stub_pymethods};

type RustUserHeaders = BTreeMap<RustHeaderKey, RustHeaderValue>;

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

trait PyHeaderFieldToRust: PyClass + Sized {
    type RustType;
    fn from_pyheader(py: Python<'_>, value: &Self) -> PyResult<Self::RustType>;
    fn from_plain(field: &Bound<'_, PyAny>) -> PyResult<Option<Self::RustType>>;
}

fn py_header_field_to_rust<T: PyHeaderFieldToRust>(
    py: Python<'_>,
    field: &Bound<'_, PyAny>,
) -> PyResult<T::RustType> {
    if let Ok(header) = field.extract::<PyRef<'_, T>>() {
        return T::from_pyheader(py, &header);
    }
    T::from_plain(field)?.ok_or_else(|| {
        PyValueError::new_err(
            "User header must be str, bytes, bool, int, float, HeaderKey, or HeaderValue",
        )
    })
}

trait ToPlain {
    fn to_plain<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>>;
}

/// Borrows a typed header ([`HeaderKey`] or [`HeaderValue`]) together with the
/// GIL token for direct conversion to a plain Python scalar.
struct HeaderToPlainRef<'py, 'value, T: ToPlain> {
    py: Python<'py>,
    value: &'value T,
}

impl<'py, T: ToPlain> TryFrom<HeaderToPlainRef<'py, '_, T>> for Bound<'py, PyAny> {
    type Error = PyErr;

    fn try_from(inner: HeaderToPlainRef<'py, '_, T>) -> PyResult<Self> {
        let HeaderToPlainRef { py, value } = inner;
        value.to_plain(py)
    }
}

macro_rules! header_type_impl {
    ($py_ty:ident) => {
        paste::paste! {
            #[gen_stub_pymethods]
            #[pymethods]
            impl $py_ty {
                pub fn __hash__(&self, py: Python<'_>) -> PyResult<isize> {
                    Ok(py_hash(header_identity_hash(self.identity(py)?)))
                }

                pub fn __richcmp__(
                    &self,
                    py: Python<'_>,
                    other: &Bound<'_, PyAny>,
                    op: CompareOp,
                ) -> PyResult<Py<PyAny>> {
                    let identity = self.identity(py)?;
                    let Ok(other_header) = other.extract::<PyRef<'_, $py_ty>>() else {
                        return match op {
                            CompareOp::Eq => {
                                Ok(false.into_pyobject(py)?.to_owned().into_any().unbind())
                            }
                            CompareOp::Ne => {
                                Ok(true.into_pyobject(py)?.to_owned().into_any().unbind())
                            }
                            _ => Ok(py.NotImplemented()),
                        };
                    };
                    let result = match op {
                        CompareOp::Eq => identity == other_header.identity(py)?,
                        CompareOp::Ne => identity != other_header.identity(py)?,
                        _ => return Ok(py.NotImplemented()),
                    };
                    Ok(result.into_pyobject(py)?.to_owned().into_any().unbind())
                }

                pub fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
                    self.repr(py)
                }
            }

            struct [<Py $py_ty Ref>]<'py, 'value> {
                py: Python<'py>,
                value: &'value $py_ty,
            }

            impl TryFrom<[<Py $py_ty Ref>]<'_, '_>> for [<Rust $py_ty>] {
                type Error = PyErr;

                fn try_from(value: [<Py $py_ty Ref>]<'_, '_>) -> PyResult<Self> {
                    let [<Py $py_ty Ref>] { py, value } = value;
                    match value {
                        $py_ty::Raw { value } => {
                            <[<Rust $py_ty>]>::try_from(value.extract::<Vec<u8>>(py)?)
                                .map_err(to_value_error)
                        }
                        $py_ty::String { value } => {
                            <[<Rust $py_ty>]>::try_from(value.as_str()).map_err(to_value_error)
                        }
                        $py_ty::Bool { value } => Ok((*value).into()),
                        $py_ty::Int8 { value } => Ok((*value).into()),
                        $py_ty::Int16 { value } => Ok((*value).into()),
                        $py_ty::Int32 { value } => Ok((*value).into()),
                        $py_ty::Int64 { value } => Ok((*value).into()),
                        $py_ty::Int128 { value } => Ok((*value).into()),
                        $py_ty::UnsignedInt8 { value } => Ok((*value).into()),
                        $py_ty::UnsignedInt16 { value } => Ok((*value).into()),
                        $py_ty::UnsignedInt32 { value } => Ok((*value).into()),
                        $py_ty::UnsignedInt64 { value } => Ok((*value).into()),
                        $py_ty::UnsignedInt128 { value } => Ok((*value).into()),
                        $py_ty::Float32 { value } => checked_float32(*value),
                        $py_ty::Float64 { value } => checked_float64(*value),
                    }
                }
            }

            struct [<Rust $py_ty Ref>]<'py, 'value> {
                py: Python<'py>,
                value: &'value [<Rust $py_ty>],
            }

            impl<'py> TryFrom<[<Rust $py_ty Ref>]<'py, '_>> for Bound<'py, $py_ty> {
                type Error = PyErr;

                fn try_from(value: [<Rust $py_ty Ref>]<'py, '_>) -> PyResult<Self> {
                    let [<Rust $py_ty Ref>] { py, value } = value;
                    let value = match value.kind() {
                        HeaderKind::Raw => $py_ty::Raw {
                            value: PyBytes::new(py, value.as_raw().map_err(to_value_error)?)
                                .unbind(),
                        },
                        HeaderKind::String => $py_ty::String {
                            value: value.as_str().map_err(to_value_error)?.to_string(),
                        },
                        HeaderKind::Bool => $py_ty::Bool {
                            value: value.as_bool().map_err(to_value_error)?,
                        },
                        HeaderKind::Int8 => $py_ty::Int8 {
                            value: value.as_int8().map_err(to_value_error)?,
                        },
                        HeaderKind::Int16 => $py_ty::Int16 {
                            value: value.as_int16().map_err(to_value_error)?,
                        },
                        HeaderKind::Int32 => $py_ty::Int32 {
                            value: value.as_int32().map_err(to_value_error)?,
                        },
                        HeaderKind::Int64 => $py_ty::Int64 {
                            value: value.as_int64().map_err(to_value_error)?,
                        },
                        HeaderKind::Int128 => $py_ty::Int128 {
                            value: value.as_int128().map_err(to_value_error)?,
                        },
                        HeaderKind::Uint8 => $py_ty::UnsignedInt8 {
                            value: value.as_uint8().map_err(to_value_error)?,
                        },
                        HeaderKind::Uint16 => $py_ty::UnsignedInt16 {
                            value: value.as_uint16().map_err(to_value_error)?,
                        },
                        HeaderKind::Uint32 => $py_ty::UnsignedInt32 {
                            value: value.as_uint32().map_err(to_value_error)?,
                        },
                        HeaderKind::Uint64 => $py_ty::UnsignedInt64 {
                            value: value.as_uint64().map_err(to_value_error)?,
                        },
                        HeaderKind::Uint128 => $py_ty::UnsignedInt128 {
                            value: value.as_uint128().map_err(to_value_error)?,
                        },
                        HeaderKind::Float32 => {
                            let v = value.as_float32().map_err(to_value_error)?;
                            if !v.is_finite() {
                                return Err(PyValueError::new_err(
                                    "User header with non-finite Float32 value",
                                ));
                            }
                            $py_ty::Float32 { value: v }
                        },
                        HeaderKind::Float64 => {
                            let v = value.as_float64().map_err(to_value_error)?;
                            if !v.is_finite() {
                                return Err(PyValueError::new_err(
                                    "User header with non-finite Float64 value",
                                ));
                            }
                            $py_ty::Float64 { value: v }
                        },
                    };
                    value.into_pyobject(py)
                }
            }

            impl $py_ty {
                fn identity(&self, py: Python<'_>) -> PyResult<HeaderIdentity> {
                    match self {
                        $py_ty::Raw { value } => {
                            Ok(HeaderIdentity::raw(value.extract::<Vec<u8>>(py)?))
                        }
                        $py_ty::String { value } => {
                            Ok(HeaderIdentity::string(value.as_bytes().to_vec()))
                        }
                        $py_ty::Bool { value } => Ok(HeaderIdentity::bool(*value)),
                        $py_ty::Int8 { value } => Ok(HeaderIdentity::int8(*value)),
                        $py_ty::Int16 { value } => Ok(HeaderIdentity::int16(*value)),
                        $py_ty::Int32 { value } => Ok(HeaderIdentity::int32(*value)),
                        $py_ty::Int64 { value } => Ok(HeaderIdentity::int64(*value)),
                        $py_ty::Int128 { value } => Ok(HeaderIdentity::int128(*value)),
                        $py_ty::UnsignedInt8 { value } => Ok(HeaderIdentity::uint8(*value)),
                        $py_ty::UnsignedInt16 { value } => Ok(HeaderIdentity::uint16(*value)),
                        $py_ty::UnsignedInt32 { value } => Ok(HeaderIdentity::uint32(*value)),
                        $py_ty::UnsignedInt64 { value } => Ok(HeaderIdentity::uint64(*value)),
                        $py_ty::UnsignedInt128 { value } => Ok(HeaderIdentity::uint128(*value)),
                        $py_ty::Float32 { value } => Ok(HeaderIdentity::float32(*value)),
                        $py_ty::Float64 { value } => Ok(HeaderIdentity::float64(*value)),
                    }
                }

                fn repr(&self, py: Python<'_>) -> PyResult<String> {
                    match self {
                        $py_ty::Raw { value } => Ok(format!(
                            concat!(stringify!($py_ty), ".Raw({})"),
                            value.bind(py).repr()?.extract::<String>()?
                        )),
                        $py_ty::String { value } => Ok(format!(
                            concat!(stringify!($py_ty), ".String({:?})"),
                            value
                        )),
                        $py_ty::Bool { value } => Ok(format!(
                            concat!(stringify!($py_ty), ".Bool({})"),
                            value
                        )),
                        $py_ty::Int8 { value } => Ok(format!(
                            concat!(stringify!($py_ty), ".Int8({})"),
                            value
                        )),
                        $py_ty::Int16 { value } => Ok(format!(
                            concat!(stringify!($py_ty), ".Int16({})"),
                            value
                        )),
                        $py_ty::Int32 { value } => Ok(format!(
                            concat!(stringify!($py_ty), ".Int32({})"),
                            value
                        )),
                        $py_ty::Int64 { value } => Ok(format!(
                            concat!(stringify!($py_ty), ".Int64({})"),
                            value
                        )),
                        $py_ty::Int128 { value } => Ok(format!(
                            concat!(stringify!($py_ty), ".Int128({})"),
                            value
                        )),
                        $py_ty::UnsignedInt8 { value } => Ok(format!(
                            concat!(stringify!($py_ty), ".UnsignedInt8({})"),
                            value
                        )),
                        $py_ty::UnsignedInt16 { value } => Ok(format!(
                            concat!(stringify!($py_ty), ".UnsignedInt16({})"),
                            value
                        )),
                        $py_ty::UnsignedInt32 { value } => Ok(format!(
                            concat!(stringify!($py_ty), ".UnsignedInt32({})"),
                            value
                        )),
                        $py_ty::UnsignedInt64 { value } => Ok(format!(
                            concat!(stringify!($py_ty), ".UnsignedInt64({})"),
                            value
                        )),
                        $py_ty::UnsignedInt128 { value } => Ok(format!(
                            concat!(stringify!($py_ty), ".UnsignedInt128({})"),
                            value
                        )),
                        $py_ty::Float32 { value } => Ok(format!(
                            concat!(stringify!($py_ty), ".Float32({:?})"),
                            value
                        )),
                        $py_ty::Float64 { value } => Ok(format!(
                            concat!(stringify!($py_ty), ".Float64({:?})"),
                            value
                        )),
                    }
                }
            }

            impl PyHeaderFieldToRust for $py_ty {
                type RustType = [<Rust $py_ty>];

                fn from_pyheader(py: Python<'_>, value: &Self) -> PyResult<[<Rust $py_ty>]> {
                    <[<Rust $py_ty>]>::try_from([<Py $py_ty Ref>] { py, value })
                }

                fn from_plain(field: &Bound<'_, PyAny>) -> PyResult<Option<[<Rust $py_ty>]>> {
                    plain_to_rust_header_field(field)
                }
            }

            impl ToPlain for $py_ty {
                fn to_plain<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
                    match self {
                        Self::Raw { value } => Ok(value.bind(py).clone().into_any()),
                        Self::String { value } => Ok(value.clone().into_pyobject(py)?.into_any()),
                        Self::Bool { value } => Ok((*value).into_pyobject(py)?.to_owned().into_any()),
                        Self::Int8 { value } => Ok((*value).into_pyobject(py)?.into_any()),
                        Self::Int16 { value } => Ok((*value).into_pyobject(py)?.into_any()),
                        Self::Int32 { value } => Ok((*value).into_pyobject(py)?.into_any()),
                        Self::Int64 { value } => Ok((*value).into_pyobject(py)?.into_any()),
                        Self::Int128 { value } => Ok((*value).into_pyobject(py)?.into_any()),
                        Self::UnsignedInt8 { value } => Ok((*value).into_pyobject(py)?.into_any()),
                        Self::UnsignedInt16 { value } => Ok((*value).into_pyobject(py)?.into_any()),
                        Self::UnsignedInt32 { value } => Ok((*value).into_pyobject(py)?.into_any()),
                        Self::UnsignedInt64 { value } => Ok((*value).into_pyobject(py)?.into_any()),
                        Self::UnsignedInt128 { value } => Ok((*value).into_pyobject(py)?.into_any()),
                        Self::Float32 { value } => {
                            if !value.is_finite() {
                                return Err(PyValueError::new_err(
                                    "User header with non-finite Float32 value",
                                ));
                            }
                            Ok(f64::from(*value).into_pyobject(py)?.into_any())
                        },
                        Self::Float64 { value } => {
                            if !value.is_finite() {
                                return Err(PyValueError::new_err(
                                    "User header with non-finite Float64 value",
                                ));
                            }
                            Ok((*value).into_pyobject(py)?.into_any())
                        },
                    }
                }
            }
        }
    };
}

header_type_impl!(HeaderKey);
header_type_impl!(HeaderValue);

pub(crate) fn py_user_headers_to_rust(
    py: Python<'_>,
    mapping: &Bound<'_, PyAny>,
) -> PyResult<RustUserHeaders> {
    let mut rust_headers = BTreeMap::new();
    for item in mapping.call_method0("items")?.try_iter()? {
        let item = item?;
        let pair: &Bound<'_, PyTuple> = item.cast()?;
        let key = pair.get_item(0)?;
        let value = pair.get_item(1)?;
        let key = py_header_field_to_rust::<HeaderKey>(py, &key)?;
        let value = py_header_field_to_rust::<HeaderValue>(py, &value)?;
        if rust_headers.insert(key, value).is_some() {
            return Err(PyValueError::new_err(
                "Duplicate user header key: each header key must be unique",
            ));
        }
    }
    Ok(rust_headers)
}

pub(crate) fn rust_user_headers_to_py<'a>(
    py: Python<'a>,
    headers: RustUserHeaders,
) -> PyResult<Bound<'a, UserHeaders>> {
    let result = Bound::new(py, UserHeaders)?;
    let mapping = result.as_any();
    for (key, value) in headers {
        let key = Bound::<HeaderKey>::try_from(RustHeaderKeyRef { py, value: &key })?;
        let value = Bound::<HeaderValue>::try_from(RustHeaderValueRef { py, value: &value })?;
        mapping.set_item(key, value)?;
    }
    Ok(result)
}

/// User headers dictionary returned by `ReceiveMessage.user_headers`.
///
/// This is a regular `dict[HeaderKey, HeaderValue]` (so all mapping
/// operations work) that additionally exposes `to_scalar_dict` for the convenient
/// scalar form.
#[gen_stub_pyclass]
#[pyclass(extends=PyDict)]
pub struct UserHeaders;

#[gen_stub_pymethods]
#[pymethods]
impl UserHeaders {
    /// Wraps a mapping so its entries gain the `to_scalar_dict` helper.
    ///
    /// Accepts a dict whose keys and values can each independently be
    /// `HeaderKey`/`HeaderValue` or a plain scalar (`str | bytes | bool |
    /// int | float`). The inherited `dict` initializer copies the provided
    /// mapping.
    #[new]
    #[pyo3(signature = (mapping=None))]
    pub fn new(
        #[gen_stub(override_type(type_repr = "dict | None"))] mapping: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        if let Some(mapping) = mapping {
            let py = mapping.py();
            py_user_headers_to_rust(py, mapping)?;
        }
        Ok(UserHeaders)
    }

    fn __setitem__(
        slf: &Bound<'_, Self>,
        key: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let py = key.py();
        py_header_field_to_rust::<HeaderKey>(py, key)?;
        py_header_field_to_rust::<HeaderValue>(py, value)?;
        slf.as_any().cast::<PyDict>()?.set_item(key, value)?;
        Ok(())
    }

    /// Converts these headers into the convenient plain dictionary form.
    ///
    /// Returns an error if two distinct typed keys map to the same plain
    /// Python scalar (e.g., `UnsignedInt8(1)` and `UnsignedInt16(1)` both
    /// become `int(1)`), or if a stored field cannot be decoded.
    #[gen_stub(override_return_type(
        type_repr = "dict[str | bytes | bool | int | float, str | bytes | bool | int | float]"
    ))]
    pub fn to_scalar_dict<'a>(slf: &Bound<'a, Self>) -> PyResult<Bound<'a, PyDict>> {
        let py = slf.py();
        let dict = slf.as_any().cast::<PyDict>()?;
        let result = PyDict::new(py);
        for (key, value) in dict.iter() {
            let plain_key = py_header_to_plain::<HeaderKey>(py, &key)?;
            let plain_value = py_header_to_plain::<HeaderValue>(py, &value)?;
            if result.contains(&plain_key)? {
                return Err(PyValueError::new_err(
                    "Distinct typed header keys produce the same plain Python scalar; this conversion is lossy and cannot proceed",
                ));
            }
            result.set_item(plain_key, plain_value)?;
        }
        Ok(result)
    }
}

fn py_header_to_plain<'py, T: ToPlain + PyClass>(
    py: Python<'py>,
    any: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    if let Ok(header) = any.extract::<PyRef<'_, T>>() {
        return Bound::<PyAny>::try_from(HeaderToPlainRef {
            py,
            value: &*header,
        });
    }
    if any.is_instance_of::<PyString>()
        || any.is_instance_of::<PyBytes>()
        || any.is_instance_of::<PyInt>()
        || any.is_instance_of::<PyFloat>()
    {
        return Ok(any.clone());
    }
    Err(PyValueError::new_err(
        "User header must be str, bytes, bool, int, float, HeaderKey, or HeaderValue",
    ))
}

/// Converts a plain Python scalar into a header field (used for both keys and
/// values). Returns `Ok(None)` when the object is not a supported plain type so
/// the caller can raise a message naming keys vs values; genuine range/length
/// errors are returned as `Err`.
fn plain_to_rust_header_field<T>(obj: &Bound<'_, PyAny>) -> PyResult<Option<HeaderField<T>>> {
    if obj.is_instance_of::<PyBool>() {
        return Ok(Some(obj.extract::<bool>()?.into()));
    }
    if obj.is_instance_of::<PyInt>() {
        return int_to_rust_header_field(obj).map(Some);
    }
    if obj.is_instance_of::<PyFloat>() {
        return float_to_rust_header_field(obj.extract::<f64>()?).map(Some);
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

fn int_to_rust_header_field<T>(obj: &Bound<'_, PyAny>) -> PyResult<HeaderField<T>> {
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

fn float_to_rust_header_field<T>(value: f64) -> PyResult<HeaderField<T>> {
    if !value.is_finite() {
        return Err(PyValueError::new_err(
            "User header float values must be finite",
        ));
    }
    let narrowed = value as f32;
    if f64::from(narrowed) == value {
        checked_float32(narrowed)
    } else {
        Ok(value.into())
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

fn checked_float64<T>(value: f64) -> PyResult<HeaderField<T>> {
    if !value.is_finite() {
        return Err(PyValueError::new_err("Float64 header value must be finite"));
    }
    Ok(value.into())
}

fn header_identity_hash(identity: HeaderIdentity) -> u64 {
    let mut hasher = DefaultHasher::new();
    identity.hash(&mut hasher);
    hasher.finish()
}

fn py_hash(hash: u64) -> isize {
    let hash = hash as isize;
    if hash == -1 { -2 } else { hash }
}

fn to_value_error(error: impl ToString) -> PyErr {
    PyValueError::new_err(error.to_string())
}
