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

use std::time::Duration;

use iggy::prelude::{
    IggyByteSize, IggyExpiry as RustIggyExpiry, MaxTopicSize as RustMaxTopicSize,
    Partition as RustPartition, Topic as RustTopic, TopicDetails as RustTopicDetails,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDelta;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_complex_enum, gen_stub_pymethods};

use crate::consumer::py_delta_to_iggy_duration;

/// The expiry of the messages in a topic.
#[gen_stub_pyclass_complex_enum]
#[pyclass]
pub enum IggyExpiry {
    /// Use the message expiry configured on the server for this topic,
    /// rather than an explicit value set by the client.
    ServerDefault(),
    /// Expire messages this long after they are appended to the topic.
    ///
    /// `duration` must be greater than zero and less than the maximum
    /// microsecond count a `u64` can hold (about 584,542 years): those two
    /// values are reserved on the wire for `ServerDefault` and `NeverExpire`
    /// respectively, so a `duration` at either boundary raises `ValueError`
    /// when passed to `create_topic`/`update_topic`. A negative `timedelta`
    /// also raises `ValueError`.
    ExpireDuration { duration: Py<PyDelta> },
    /// Retain messages indefinitely; they never expire.
    NeverExpire(),
}

impl TryFrom<RustIggyExpiry> for IggyExpiry {
    type Error = PyErr;

    fn try_from(expiry: RustIggyExpiry) -> PyResult<Self> {
        Ok(match expiry {
            RustIggyExpiry::ServerDefault => IggyExpiry::ServerDefault(),
            RustIggyExpiry::ExpireDuration(duration) => IggyExpiry::ExpireDuration {
                duration: iggy_duration_to_py_delta(duration.get_duration())?,
            },
            RustIggyExpiry::NeverExpire => IggyExpiry::NeverExpire(),
        })
    }
}

impl TryFrom<&IggyExpiry> for RustIggyExpiry {
    type Error = PyErr;

    fn try_from(expiry: &IggyExpiry) -> PyResult<Self> {
        Ok(match expiry {
            IggyExpiry::ServerDefault() => RustIggyExpiry::ServerDefault,
            IggyExpiry::ExpireDuration { duration } => {
                let iggy_duration = py_delta_to_iggy_duration(duration)?;
                if iggy_duration.is_zero() {
                    return Err(PyValueError::new_err(
                        "duration must be greater than zero and less than the maximum \
                         representable microsecond count; those values are reserved for \
                         IggyExpiry.ServerDefault() and IggyExpiry.NeverExpire() respectively"
                            .to_string(),
                    ));
                }
                if iggy_duration.get_duration().as_micros() >= u64::MAX as u128 {
                    return Err(PyValueError::new_err(
                        "duration must be greater than zero and less than the maximum \
                         representable microsecond count; those values are reserved for \
                         IggyExpiry.ServerDefault() and IggyExpiry.NeverExpire() respectively"
                            .to_string(),
                    ));
                }
                RustIggyExpiry::ExpireDuration(iggy_duration)
            }
            IggyExpiry::NeverExpire() => RustIggyExpiry::NeverExpire,
        })
    }
}

fn iggy_duration_to_py_delta(duration: Duration) -> PyResult<Py<PyDelta>> {
    let days = duration.as_secs() / 86_400;
    let secs_of_day = duration.as_secs() % 86_400;
    Python::attach(|py| {
        PyDelta::new(
            py,
            days as i32,
            secs_of_day as i32,
            duration.subsec_micros() as i32,
            true,
        )
        .map(|delta| delta.unbind())
        .map_err(|err| {
            PyValueError::new_err(format!(
                "topic message expiry duration does not fit within timedelta bounds: {err}"
            ))
        })
    })
}

/// The maximum size of a topic.
#[gen_stub_pyclass_complex_enum]
#[pyclass]
pub enum MaxTopicSize {
    /// Use the maximum topic size configured on the server, rather than an
    /// explicit value set by the client.
    ServerDefault(),
    /// Cap the topic at this many bytes; as the topic approaches this size,
    /// the server deletes the oldest sealed segments to make room for new
    /// messages.
    ///
    /// `bytes` must be greater than zero and less than the maximum value of
    /// an unsigned 64-bit integer: those two values are reserved on the wire
    /// for `ServerDefault` and `Unlimited` respectively, so a `Custom` size
    /// at either boundary raises `ValueError` when passed to
    /// `create_topic`/`update_topic`.
    Custom { bytes: u64 },
    /// Do not cap the topic size; it may grow without bound.
    Unlimited(),
}

impl From<RustMaxTopicSize> for MaxTopicSize {
    fn from(max_size: RustMaxTopicSize) -> Self {
        match max_size {
            RustMaxTopicSize::ServerDefault => MaxTopicSize::ServerDefault(),
            RustMaxTopicSize::Custom(size) => MaxTopicSize::Custom {
                bytes: size.as_bytes_u64(),
            },
            RustMaxTopicSize::Unlimited => MaxTopicSize::Unlimited(),
        }
    }
}

impl TryFrom<&MaxTopicSize> for RustMaxTopicSize {
    type Error = PyErr;

    fn try_from(max_size: &MaxTopicSize) -> PyResult<Self> {
        Ok(match max_size {
            MaxTopicSize::ServerDefault() => RustMaxTopicSize::ServerDefault,
            MaxTopicSize::Custom { bytes } => {
                if *bytes == 0 || *bytes == u64::MAX {
                    return Err(PyValueError::new_err(
                        "bytes must be greater than zero and less than u64::MAX".to_string(),
                    ));
                }
                RustMaxTopicSize::Custom(IggyByteSize::from(*bytes))
            }
            MaxTopicSize::Unlimited() => RustMaxTopicSize::Unlimited,
        })
    }
}

#[gen_stub_pyclass]
#[pyclass]
pub struct Topic {
    pub(crate) inner: RustTopic,
}

impl From<RustTopic> for Topic {
    fn from(topic: RustTopic) -> Self {
        Self { inner: topic }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl Topic {
    /// The unique identifier (numeric) of the topic.
    #[getter]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    /// The unique name of the topic.
    #[getter]
    pub fn name(&self) -> String {
        self.inner.name.to_string()
    }

    /// The total number of messages in the topic.
    #[getter]
    pub fn messages_count(&self) -> u64 {
        self.inner.messages_count
    }

    /// The total number of partitions in the topic.
    #[getter]
    pub fn partitions_count(&self) -> u32 {
        self.inner.partitions_count
    }

    /// The timestamp when the topic was created, in microseconds.
    #[getter]
    pub fn created_at(&self) -> u64 {
        self.inner.created_at.as_micros()
    }

    /// The total size of the topic in bytes.
    #[getter]
    pub fn size(&self) -> u64 {
        self.inner.size.as_bytes_u64()
    }

    /// The expiry of the messages in the topic.
    #[getter]
    pub fn message_expiry(&self) -> PyResult<IggyExpiry> {
        self.inner.message_expiry.try_into()
    }

    /// Compression algorithm for the topic.
    #[getter]
    pub fn compression_algorithm(&self) -> String {
        self.inner.compression_algorithm.to_string()
    }

    /// The maximum size of the topic.
    #[getter]
    pub fn max_topic_size(&self) -> MaxTopicSize {
        self.inner.max_topic_size.into()
    }

    /// Replication factor for the topic.
    #[getter]
    pub fn replication_factor(&self) -> u8 {
        self.inner.replication_factor
    }
}

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
    /// The unique identifier (numeric) of the topic.
    #[getter]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    /// The unique name of the topic.
    #[getter]
    pub fn name(&self) -> String {
        self.inner.name.to_string()
    }

    /// The total number of messages in the topic.
    #[getter]
    pub fn messages_count(&self) -> u64 {
        self.inner.messages_count
    }

    /// The total number of partitions in the topic.
    #[getter]
    pub fn partitions_count(&self) -> u32 {
        self.inner.partitions_count
    }

    /// The timestamp when the topic was created, in microseconds.
    #[getter]
    pub fn created_at(&self) -> u64 {
        self.inner.created_at.as_micros()
    }

    /// The total size of the topic in bytes.
    #[getter]
    pub fn size(&self) -> u64 {
        self.inner.size.as_bytes_u64()
    }

    /// The expiry of the messages in the topic.
    #[getter]
    pub fn message_expiry(&self) -> PyResult<IggyExpiry> {
        self.inner.message_expiry.try_into()
    }

    /// Compression algorithm for the topic.
    #[getter]
    pub fn compression_algorithm(&self) -> String {
        self.inner.compression_algorithm.to_string()
    }

    /// The maximum size of the topic.
    #[getter]
    pub fn max_topic_size(&self) -> MaxTopicSize {
        self.inner.max_topic_size.into()
    }

    /// Replication factor for the topic.
    #[getter]
    pub fn replication_factor(&self) -> u8 {
        self.inner.replication_factor
    }

    /// The collection of partitions in the topic.
    ///
    /// Rebuilds the list from scratch on every access; cache the result
    /// rather than reading this repeatedly in a loop.
    #[getter]
    pub fn partitions(&self) -> Vec<Partition> {
        self.inner
            .partitions
            .iter()
            .cloned()
            .map(Partition::from)
            .collect()
    }
}

#[gen_stub_pyclass]
#[pyclass]
pub struct Partition {
    pub(crate) inner: RustPartition,
}

impl From<RustPartition> for Partition {
    fn from(partition: RustPartition) -> Self {
        Self { inner: partition }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl Partition {
    /// The unique identifier (numeric) of the partition.
    #[getter]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    /// The timestamp of the partition creation, in microseconds.
    #[getter]
    pub fn created_at(&self) -> u64 {
        self.inner.created_at.as_micros()
    }

    /// The number of segments in the partition.
    #[getter]
    pub fn segments_count(&self) -> u32 {
        self.inner.segments_count
    }

    /// The current offset of the partition.
    #[getter]
    pub fn current_offset(&self) -> u64 {
        self.inner.current_offset
    }

    /// The size of the partition in bytes.
    #[getter]
    pub fn size(&self) -> u64 {
        self.inner.size.as_bytes_u64()
    }

    /// The number of messages in the partition.
    #[getter]
    pub fn messages_count(&self) -> u64 {
        self.inner.messages_count
    }
}
