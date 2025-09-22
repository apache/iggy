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

use std::sync::Arc;

use futures::StreamExt;
use iggy::prelude::IggyConsumer as RustIggyConsumer;
use pyo3::exceptions::PyStopIteration;

use crate::receive_message::ReceiveMessage;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::Mutex;

#[pyclass]
pub struct ReceiveMessageIterator {
    pub(crate) inner: Arc<Mutex<RustIggyConsumer>>,
}

#[pymethods]
impl ReceiveMessageIterator {
    pub fn __anext__<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let mut inner = inner.lock().await;
            if let Some(message) = inner.next().await {
                Ok(message
                    .map(|m| ReceiveMessage {
                        inner: m.message,
                        partition_id: m.partition_id,
                    })
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}"))
                    })?)
            } else {
                Err(PyStopIteration::new_err("No more messages"))
            }
        })
    }

    pub fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
}
