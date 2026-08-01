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
use pyo3::prelude::*;
use pyo3::types::{PyDelta, PyDeltaAccess};
use std::time::Duration;

pub fn py_delta_to_iggy_duration(delta: &Py<PyDelta>) -> PyResult<IggyDuration> {
    Python::attach(|py| {
        let delta = delta.bind(py);
        // Python normalizes a negative timedelta to negative days plus
        // non-negative seconds/microseconds, so the sign lives in the sum.
        let seconds = i64::from(delta.get_days()) * 60 * 60 * 24 + i64::from(delta.get_seconds());
        if seconds < 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "duration must not be negative",
            ));
        }
        let nanos = (delta.get_microseconds() * 1_000) as u32;
        Ok(IggyDuration::new(Duration::new(seconds as u64, nanos)))
    })
}

pub fn iggy_duration_to_py_delta(
    py: Python<'_>,
    duration: IggyDuration,
) -> PyResult<Bound<'_, PyDelta>> {
    // IggyDuration::as_micros() truncates to u64; read the std Duration to keep
    // the full u128 so oversized values fail the i32 conversion below instead of
    // wrapping.
    let micros = duration.get_duration().as_micros();
    let total_seconds = micros / 1_000_000;
    let days = i32::try_from(total_seconds / 86_400).map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyOverflowError, _>(
            "duration does not fit into a datetime.timedelta",
        )
    })?;
    let seconds = (total_seconds % 86_400) as i32;
    PyDelta::new(py, days, seconds, (micros % 1_000_000) as i32, true)
}
