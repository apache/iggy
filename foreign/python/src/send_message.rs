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

use iggy::messages::send_messages::Message as RustSendMessage;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::str::FromStr;

/// A Python class representing a message to be sent.
///
/// This class wraps a Rust message meant for sending, facilitating
/// the creation of such messages from Python and their subsequent use in Rust.
#[pyclass]
#[gen_stub_pyclass]
pub struct SendMessage {
    pub(crate) inner: RustSendMessage,
}

/// Provides the capability to clone a SendMessage.
///
/// This implementation creates a new `RustSendMessage` instance from
/// the string representation of the original `RustSendMessage`.
impl Clone for SendMessage {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl SendMessage {
    /// Constructs a new `SendMessage` instance from a string.
    ///
    /// This method allows for the creation of a `SendMessage` instance
    /// directly from Python using the provided string data.
    #[new]
    pub fn new(data: String) -> Self {
        // TODO: handle errors
        let inner = RustSendMessage::from_str(&data).unwrap();
        Self { inner }
    }
}
