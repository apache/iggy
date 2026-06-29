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

use iggy_common::{
    ConsumerGroup as RustConsumerGroup, ConsumerGroupDetails as RustConsumerGroupDetails,
    ConsumerGroupMember as RustConsumerGroupMember,
};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

#[gen_stub_pyclass]
#[pyclass]
pub struct ConsumerGroup {
    pub(crate) inner: RustConsumerGroup,
}

impl From<RustConsumerGroup> for ConsumerGroup {
    fn from(group: RustConsumerGroup) -> Self {
        Self { inner: group }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl ConsumerGroup {
    #[getter]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    #[getter]
    pub fn name(&self) -> String {
        self.inner.name.to_string()
    }

    #[getter]
    pub fn partitions_count(&self) -> u32 {
        self.inner.partitions_count
    }

    #[getter]
    pub fn members_count(&self) -> u32 {
        self.inner.members_count
    }
}

#[gen_stub_pyclass]
#[pyclass]
pub struct ConsumerGroupDetails {
    pub(crate) inner: RustConsumerGroupDetails,
}

impl From<RustConsumerGroupDetails> for ConsumerGroupDetails {
    fn from(group: RustConsumerGroupDetails) -> Self {
        Self { inner: group }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl ConsumerGroupDetails {
    #[getter]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    #[getter]
    pub fn name(&self) -> String {
        self.inner.name.to_string()
    }

    #[getter]
    pub fn partitions_count(&self) -> u32 {
        self.inner.partitions_count
    }

    #[getter]
    pub fn members_count(&self) -> u32 {
        self.inner.members_count
    }

    #[getter]
    pub fn members(&self) -> Vec<ConsumerGroupMember> {
        self.inner
            .members
            .iter()
            .map(ConsumerGroupMember::from)
            .collect()
    }
}

#[gen_stub_pyclass]
#[pyclass]
pub struct ConsumerGroupMember {
    pub(crate) inner: RustConsumerGroupMember,
}

impl From<RustConsumerGroupMember> for ConsumerGroupMember {
    fn from(member: RustConsumerGroupMember) -> Self {
        Self { inner: member }
    }
}

impl From<&RustConsumerGroupMember> for ConsumerGroupMember {
    fn from(member: &RustConsumerGroupMember) -> Self {
        Self {
            inner: RustConsumerGroupMember {
                id: member.id,
                partitions_count: member.partitions_count,
                partitions: member.partitions.clone(),
            },
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl ConsumerGroupMember {
    #[getter]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    #[getter]
    pub fn partitions_count(&self) -> u32 {
        self.inner.partitions_count
    }

    #[getter]
    pub fn partitions(&self) -> Vec<u32> {
        self.inner.partitions.clone()
    }
}
