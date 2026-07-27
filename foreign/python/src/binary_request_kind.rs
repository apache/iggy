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

use iggy::prelude::BinaryRequestKind as RustBinaryRequestKind;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::gen_stub_pyclass_enum;

/// How a raw binary request executes on the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, from_py_object)]
pub enum BinaryRequestKind {
    /// Runs on the receiving node only, outside consensus.
    NonReplicated,
    /// Replicated through consensus before it takes effect. Inert on classic
    /// framing, where both kinds encode identical bytes; under `vsr` an
    /// unknown code declared this way is rejected until the server grows a
    /// replicated extension registry.
    Replicated,
}

impl From<BinaryRequestKind> for RustBinaryRequestKind {
    fn from(kind: BinaryRequestKind) -> Self {
        match kind {
            BinaryRequestKind::NonReplicated => RustBinaryRequestKind::NonReplicated,
            BinaryRequestKind::Replicated => RustBinaryRequestKind::Replicated,
        }
    }
}

impl From<RustBinaryRequestKind> for BinaryRequestKind {
    fn from(kind: RustBinaryRequestKind) -> Self {
        match kind {
            RustBinaryRequestKind::NonReplicated => BinaryRequestKind::NonReplicated,
            RustBinaryRequestKind::Replicated => BinaryRequestKind::Replicated,
        }
    }
}
