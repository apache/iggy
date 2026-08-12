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

use iggy::prelude::IggyDuration;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDelta;
use std::time::Duration;

pub fn py_delta_to_iggy_duration(delta: &Py<PyDelta>) -> PyResult<IggyDuration> {
    Python::attach(|py| {
        // The value is already a timedelta, so a negative one is the only failure
        // left to map, and the Python surface must not name Rust types.
        delta
            .bind(py)
            .extract::<Duration>()
            .map(IggyDuration::from)
            .map_err(|_| PyValueError::new_err("duration must not be negative"))
    })
}

pub fn iggy_duration_to_py_delta(
    py: Python<'_>,
    duration: IggyDuration,
) -> PyResult<Bound<'_, PyDelta>> {
    duration.get_duration().into_pyobject(py)
}

/// Renders a duration the way it would be written in Python, so that a `__repr__`
/// built from it can be pasted back into a constructor.
pub fn duration_repr(duration: IggyDuration) -> String {
    // Read the std duration, whose micros are u128: `IggyDuration::as_micros()`
    // truncates to u64, which a timedelta near the Python maximum overflows.
    let micros = duration.get_duration().as_micros();
    if micros.is_multiple_of(1_000_000) {
        format!("datetime.timedelta(seconds={})", micros / 1_000_000)
    } else {
        format!("datetime.timedelta(microseconds={micros})")
    }
}

/// Rejects a zero duration for parameters where zero means an unthrottled loop
/// rather than "disabled".
pub fn reject_zero(duration: IggyDuration, parameter: &str) -> PyResult<IggyDuration> {
    if duration.is_zero() {
        return Err(PyValueError::new_err(format!(
            "'{parameter}' must not be zero"
        )));
    }
    Ok(duration)
}
