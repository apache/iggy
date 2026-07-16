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

use iggy::prelude::IggyError as RustIggyError;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

#[gen_stub_pyclass]
#[pyclass(skip_from_py_object, extends=PyException)]
#[derive(Clone, Debug)]
pub struct IggyError {
    #[pyo3(get)]
    code: u32,
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    message: String,
}

#[gen_stub_pymethods]
#[pymethods]
impl IggyError {
    #[new]
    fn new(code: u32, name: String, message: String) -> Self {
        Self {
            code,
            name,
            message,
        }
    }

    fn __str__(&self) -> String {
        self.message.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "IggyError(code={}, name='{}', message='{}')",
            self.code, self.name, self.message
        )
    }
}

impl IggyError {
    pub fn new_err_from_rust(e: RustIggyError) -> PyErr {
        PyErr::new::<IggyError, _>((e.as_code(), e.as_string().to_string(), e.to_string()))
    }
}
