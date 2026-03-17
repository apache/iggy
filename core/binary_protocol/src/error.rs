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

/// Protocol-local error type for wire format encode/decode failures.
///
/// Intentionally decoupled from `IggyError` to keep the protocol crate
/// free of domain dependencies. Conversion to `IggyError` happens at
/// the boundary (SDK / server).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WireError {
    #[error("unexpected end of buffer at offset {offset}: need {need} bytes, have {have}")]
    UnexpectedEof {
        offset: usize,
        need: usize,
        have: usize,
    },

    #[error("invalid utf-8 at offset {offset}")]
    InvalidUtf8 { offset: usize },

    #[error("unknown command code: {0}")]
    UnknownCommand(u32),

    #[error("unknown discriminant {value} for {type_name} at offset {offset}")]
    UnknownDiscriminant {
        type_name: &'static str,
        value: u8,
        offset: usize,
    },

    #[error("payload too large: {size} bytes, max {max}")]
    PayloadTooLarge { size: usize, max: usize },

    #[error("validation failed: {0}")]
    Validation(String),
}
