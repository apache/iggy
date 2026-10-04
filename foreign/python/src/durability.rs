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

use iggy::prelude::Durability as RustDurability;
use pyo3::{
    exceptions::PyTypeError,
    prelude::*,
    types::{PyDict, PyString},
};

pub struct Durability(pub RustDurability);

impl Durability {
    pub fn register(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
        let kwargs = PyDict::new(py);
        kwargs.set_item("type", py.get_type::<PyString>())?;
        kwargs.set_item("module", "apache_iggy")?;
        let members = [("REPLICATED", "replicated"), ("PERSISTED", "persisted")];
        let enumeration = py
            .import("enum")?
            .getattr("Enum")?
            .call(("Durability", members), Some(&kwargs))?;
        module.add("Durability", enumeration)
    }
}

impl TryFrom<Option<&Bound<'_, PyAny>>> for Durability {
    type Error = PyErr;

    fn try_from(value: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let Some(value) = value else {
            return Ok(Self(RustDurability::Replicated));
        };
        let class = value.py().import("apache_iggy")?.getattr("Durability")?;
        if !value.is_instance(&class)? {
            return Err(PyTypeError::new_err("Expected Durability"));
        }
        let token: String = value.getattr("value")?.extract()?;
        token
            .parse()
            .map(Self)
            .map_err(|_| PyTypeError::new_err("Invalid durability"))
    }
}
