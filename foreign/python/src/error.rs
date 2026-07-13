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

use iggy::error::IggyError;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(apache_iggy, PyIggyError, PyException);

impl From<IggyError> for PyErr {
    fn from(err: IggyError) -> Self {
        let code = err.as_code() as u32;
        let name = err.as_string().to_string();
        let message = err.to_string();
        PyIggyError::new_err((code, name, message))
    }
}

pub fn into_pyerr(err: IggyError) -> PyErr {
    err.into()
}
