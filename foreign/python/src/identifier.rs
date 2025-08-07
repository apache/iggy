/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use std::str::FromStr;

use iggy::prelude::{IdKind, Identifier};
use pyo3::prelude::*;
use pyo3_stub_gen::impl_stub_type;

#[derive(FromPyObject, IntoPyObject)]
pub(crate) enum PyIdentifier {
    #[pyo3(transparent, annotation = "str")]
    String(String),
    #[pyo3(transparent, annotation = "int")]
    Int(u32),
}
impl_stub_type!(PyIdentifier = String | isize);

impl From<PyIdentifier> for Identifier {
    fn from(py_identifier: PyIdentifier) -> Self {
        match py_identifier {
            PyIdentifier::String(s) => Identifier::from_str(&s).unwrap(),
            PyIdentifier::Int(i) => Identifier::numeric(i).unwrap(),
        }
    }
}

impl From<&Identifier> for PyIdentifier {
    fn from(val: &Identifier) -> PyIdentifier {
        match val.kind {
            IdKind::String => PyIdentifier::String(val.get_string_value().unwrap()),
            IdKind::Numeric => PyIdentifier::Int(val.get_u32_value().unwrap()),
        }
    }
}
