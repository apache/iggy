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

use iggy::models::topic::TopicDetails as RustTopicDetails;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

#[gen_stub_pyclass]
#[pyclass]
pub struct TopicDetails {
    pub(crate) inner: RustTopicDetails,
}

impl From<RustTopicDetails> for TopicDetails {
    fn from(topic_details: RustTopicDetails) -> Self {
        Self {
            inner: topic_details,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl TopicDetails {
    #[getter]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    #[getter]
    pub fn name(&self) -> String {
        self.inner.name.to_string()
    }

    #[getter]
    pub fn messages_count(&self) -> u64 {
        self.inner.messages_count
    }

    #[getter]
    pub fn partitions_count(&self) -> u32 {
        self.inner.partitions_count
    }
}
