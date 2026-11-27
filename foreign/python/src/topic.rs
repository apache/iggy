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

use iggy::prelude::{
    IggyByteSize, IggyDuration, IggyExpiry as RustIggyExpiry, MaxTopicSize as RustMaxTopicSize,
    Partition as RustPartition, Topic as RustTopic, TopicDetails as RustTopicDetails,
};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDelta, PyList};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_complex_enum, gen_stub_pymethods};

use crate::duration::{iggy_duration_to_py_delta, py_delta_to_iggy_duration};
use crate::options::options_by_provenance;
use crate::receive_message::PartitionContext;
use crate::resource_options::{GeoMode, TopicType};
use crate::user_headers::UserHeaders;

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
    /// when passed to `TopicOptions`/`update_topic`. A negative `timedelta`
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
                duration: Python::attach(|py| {
                    iggy_duration_to_py_delta(py, duration).map(|delta| delta.unbind())
                })
                .map_err(|err| {
                    PyValueError::new_err(format!(
                        "topic message expiry duration does not fit within timedelta bounds: {err}"
                    ))
                })?,
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
    /// `TopicOptions`/`update_topic`.
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

impl From<&RustTopic> for Topic {
    fn from(topic: &RustTopic) -> Self {
        Self {
            inner: RustTopic {
                id: topic.id,
                created_at: topic.created_at,
                name: topic.name.clone(),
                size: topic.size,
                messages_count: topic.messages_count,
                partitions_count: topic.partitions_count,
                options: topic.options.clone(),
            },
        }
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

    /// The expiry of the messages in the topic, the server default when the
    /// topic does not set one.
    #[getter]
    pub fn message_expiry(&self) -> PyResult<IggyExpiry> {
        self.inner.resolved_options().message_expiry.try_into()
    }

    /// Compression algorithm for the topic, fixed at creation.
    #[getter]
    pub fn compression_algorithm(&self) -> String {
        self.inner
            .resolved_options()
            .compression_algorithm
            .to_string()
    }

    /// The maximum size of the topic, the server default when the topic does
    /// not set one.
    #[getter]
    pub fn max_topic_size(&self) -> MaxTopicSize {
        self.inner.resolved_options().max_topic_size.into()
    }

    /// What the topic is, normal or geo, fixed at creation.
    #[getter]
    pub fn topic_type(&self) -> TopicType {
        self.inner.resolved_options().topic_type.into()
    }

    /// How a geo topic writes its messages, fixed at creation. `None` on a
    /// normal topic.
    #[getter]
    pub fn geo_mode(&self) -> Option<GeoMode> {
        self.inner.resolved_options().geo_mode.map(GeoMode::from)
    }

    /// Longest time a send to the geo topic waits for its upload to start, or
    /// `None` when the topic leaves it to each node's server setting.
    #[gen_stub(override_return_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
    #[getter]
    pub fn fresh_upload_interval<'a>(
        &self,
        py: Python<'a>,
    ) -> PyResult<Option<Bound<'a, PyDelta>>> {
        geo_duration(
            py,
            self.inner
                .resolved_options()
                .geo_durations
                .fresh_upload_interval,
        )
    }

    /// Shortest time between two uploads of the positions the geo topic's
    /// stored offsets wait for, or `None` when the topic leaves it to each
    /// node's server setting.
    #[gen_stub(override_return_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
    #[getter]
    pub fn consumer_offset_upload_interval<'a>(
        &self,
        py: Python<'a>,
    ) -> PyResult<Option<Bound<'a, PyDelta>>> {
        geo_duration(
            py,
            self.inner
                .resolved_options()
                .geo_durations
                .consumer_offset_upload_interval,
        )
    }

    /// How often a node uploads the geo topic's changed positions, or `None`
    /// when the topic leaves it to each node's server setting.
    #[gen_stub(override_return_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
    #[getter]
    pub fn positions_publish_interval<'a>(
        &self,
        py: Python<'a>,
    ) -> PyResult<Option<Bound<'a, PyDelta>>> {
        geo_duration(
            py,
            self.inner
                .resolved_options()
                .geo_durations
                .positions_publish_interval,
        )
    }

    /// How long the tombstone and the objects of the deleted geo topic stay,
    /// or `None` when the topic leaves it to each node's server setting.
    #[gen_stub(override_return_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
    #[getter]
    pub fn tombstone_grace<'a>(&self, py: Python<'a>) -> PyResult<Option<Bound<'a, PyDelta>>> {
        geo_duration(
            py,
            self.inner.resolved_options().geo_durations.tombstone_grace,
        )
    }

    /// How long the geo topic's group polls wait for the peers' positions
    /// after a node starts, or `None` when the topic leaves it to each node's
    /// server setting.
    #[gen_stub(override_return_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
    #[getter]
    pub fn positions_resume_timeout<'a>(
        &self,
        py: Python<'a>,
    ) -> PyResult<Option<Bound<'a, PyDelta>>> {
        geo_duration(
            py,
            self.inner
                .resolved_options()
                .geo_durations
                .positions_resume_timeout,
        )
    }

    /// Longest wait between two long-term rewrites of a partition of the geo
    /// topic, or `None` when the topic leaves it to each node's server
    /// setting.
    #[gen_stub(override_return_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
    #[getter]
    pub fn long_term_rewrite_max_interval<'a>(
        &self,
        py: Python<'a>,
    ) -> PyResult<Option<Bound<'a, PyDelta>>> {
        geo_duration(
            py,
            self.inner
                .resolved_options()
                .geo_durations
                .long_term_rewrite_max_interval,
        )
    }

    /// Memory in bytes each node keeps for what the topic's reads fetch from
    /// the object store, or `None` when the topic leaves it to each node's
    /// server setting. Zero keeps nothing they fetch.
    #[getter]
    pub fn read_cache_size(&self) -> Option<u64> {
        self.inner
            .resolved_options()
            .read_cache
            .size
            .map(|size| size.as_bytes_u64())
    }

    /// Most bytes readahead fetches in front of a sequential reader of the
    /// topic, or `None` when the topic leaves it to each node's server
    /// setting. Zero turns readahead off.
    #[getter]
    pub fn read_ahead(&self) -> Option<u64> {
        self.inner
            .resolved_options()
            .read_cache
            .read_ahead
            .map(|read_ahead| read_ahead.as_bytes_u64())
    }

    /// Options the creating client set explicitly.
    ///
    /// The same `dict[HeaderKey, HeaderValue]` that `ReceiveMessage.user_headers`
    /// returns, since options ride that codec; call `to_scalar_dict()` for the
    /// plain-scalar form.
    #[getter]
    pub fn options<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, UserHeaders>> {
        options_by_provenance(py, &self.inner.options, true)
    }

    /// Options admission resolved for the keys the client did not send.
    ///
    /// Same shape as `options`. These would have resolved differently
    /// under another server configuration.
    #[getter]
    pub fn derived_options<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, UserHeaders>> {
        options_by_provenance(py, &self.inner.options, false)
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

    /// The expiry of the messages in the topic, the server default when the
    /// topic does not set one.
    #[getter]
    pub fn message_expiry(&self) -> PyResult<IggyExpiry> {
        self.inner.resolved_options().message_expiry.try_into()
    }

    /// Compression algorithm for the topic, fixed at creation.
    #[getter]
    pub fn compression_algorithm(&self) -> String {
        self.inner
            .resolved_options()
            .compression_algorithm
            .to_string()
    }

    /// The maximum size of the topic, the server default when the topic does
    /// not set one.
    #[getter]
    pub fn max_topic_size(&self) -> MaxTopicSize {
        self.inner.resolved_options().max_topic_size.into()
    }

    /// What the topic is, normal or geo, fixed at creation.
    #[getter]
    pub fn topic_type(&self) -> TopicType {
        self.inner.resolved_options().topic_type.into()
    }

    /// How a geo topic writes its messages, fixed at creation. `None` on a
    /// normal topic.
    #[getter]
    pub fn geo_mode(&self) -> Option<GeoMode> {
        self.inner.resolved_options().geo_mode.map(GeoMode::from)
    }

    /// Longest time a send to the geo topic waits for its upload to start, or
    /// `None` when the topic leaves it to each node's server setting.
    #[gen_stub(override_return_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
    #[getter]
    pub fn fresh_upload_interval<'a>(
        &self,
        py: Python<'a>,
    ) -> PyResult<Option<Bound<'a, PyDelta>>> {
        geo_duration(
            py,
            self.inner
                .resolved_options()
                .geo_durations
                .fresh_upload_interval,
        )
    }

    /// Shortest time between two uploads of the positions the geo topic's
    /// stored offsets wait for, or `None` when the topic leaves it to each
    /// node's server setting.
    #[gen_stub(override_return_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
    #[getter]
    pub fn consumer_offset_upload_interval<'a>(
        &self,
        py: Python<'a>,
    ) -> PyResult<Option<Bound<'a, PyDelta>>> {
        geo_duration(
            py,
            self.inner
                .resolved_options()
                .geo_durations
                .consumer_offset_upload_interval,
        )
    }

    /// How often a node uploads the geo topic's changed positions, or `None`
    /// when the topic leaves it to each node's server setting.
    #[gen_stub(override_return_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
    #[getter]
    pub fn positions_publish_interval<'a>(
        &self,
        py: Python<'a>,
    ) -> PyResult<Option<Bound<'a, PyDelta>>> {
        geo_duration(
            py,
            self.inner
                .resolved_options()
                .geo_durations
                .positions_publish_interval,
        )
    }

    /// How long the tombstone and the objects of the deleted geo topic stay,
    /// or `None` when the topic leaves it to each node's server setting.
    #[gen_stub(override_return_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
    #[getter]
    pub fn tombstone_grace<'a>(&self, py: Python<'a>) -> PyResult<Option<Bound<'a, PyDelta>>> {
        geo_duration(
            py,
            self.inner.resolved_options().geo_durations.tombstone_grace,
        )
    }

    /// How long the geo topic's group polls wait for the peers' positions
    /// after a node starts, or `None` when the topic leaves it to each node's
    /// server setting.
    #[gen_stub(override_return_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
    #[getter]
    pub fn positions_resume_timeout<'a>(
        &self,
        py: Python<'a>,
    ) -> PyResult<Option<Bound<'a, PyDelta>>> {
        geo_duration(
            py,
            self.inner
                .resolved_options()
                .geo_durations
                .positions_resume_timeout,
        )
    }

    /// Longest wait between two long-term rewrites of a partition of the geo
    /// topic, or `None` when the topic leaves it to each node's server
    /// setting.
    #[gen_stub(override_return_type(type_repr = "datetime.timedelta | None", imports=("datetime")))]
    #[getter]
    pub fn long_term_rewrite_max_interval<'a>(
        &self,
        py: Python<'a>,
    ) -> PyResult<Option<Bound<'a, PyDelta>>> {
        geo_duration(
            py,
            self.inner
                .resolved_options()
                .geo_durations
                .long_term_rewrite_max_interval,
        )
    }

    /// Memory in bytes each node keeps for what the topic's reads fetch from
    /// the object store, or `None` when the topic leaves it to each node's
    /// server setting. Zero keeps nothing they fetch.
    #[getter]
    pub fn read_cache_size(&self) -> Option<u64> {
        self.inner
            .resolved_options()
            .read_cache
            .size
            .map(|size| size.as_bytes_u64())
    }

    /// Most bytes readahead fetches in front of a sequential reader of the
    /// topic, or `None` when the topic leaves it to each node's server
    /// setting. Zero turns readahead off.
    #[getter]
    pub fn read_ahead(&self) -> Option<u64> {
        self.inner
            .resolved_options()
            .read_cache
            .read_ahead
            .map(|read_ahead| read_ahead.as_bytes_u64())
    }

    /// Options the creating client set explicitly.
    ///
    /// The same `dict[HeaderKey, HeaderValue]` that `ReceiveMessage.user_headers`
    /// returns, since options ride that codec; call `to_scalar_dict()` for the
    /// plain-scalar form.
    #[getter]
    pub fn options<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, UserHeaders>> {
        options_by_provenance(py, &self.inner.options, true)
    }

    /// Options admission resolved for the keys the client did not send.
    ///
    /// Same shape as `options`. These would have resolved differently
    /// under another server configuration.
    #[getter]
    pub fn derived_options<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, UserHeaders>> {
        options_by_provenance(py, &self.inner.options, false)
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

    /// The partition incarnation and the metadata operation the details were read at.
    /// The owner generation is `0` because the details belong to no consumer group.
    #[getter]
    pub fn context(&self) -> PartitionContext {
        self.inner.context.into()
    }

    /// Node ids that store the partition, in member order. While one is down, a write
    /// still commits if a quorum of the others is in sync.
    #[gen_stub(override_return_type(type_repr = "builtins.list[builtins.int]"))]
    #[getter]
    pub fn members<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyList>> {
        node_id_list(py, &self.inner.members)
    }

    /// Node ids among `members` that the partition's primary last reported in sync and
    /// that are up. Empty until the primary first reports, and while no member that is
    /// up can lead it.
    #[gen_stub(override_return_type(type_repr = "builtins.list[builtins.int]"))]
    #[getter]
    pub fn in_sync_members<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyList>> {
        node_id_list(py, &self.inner.in_sync_members)
    }
}

/// A duration of a geo topic as a `timedelta`, `None` when the topic sets none.
fn geo_duration(
    py: Python<'_>,
    duration: Option<IggyDuration>,
) -> PyResult<Option<Bound<'_, PyDelta>>> {
    duration
        .map(|duration| iggy_duration_to_py_delta(py, duration))
        .transpose()
}

/// A `Vec<u8>` return would convert to `bytes`, so node ids are built as a list of `int`.
fn node_id_list<'a>(py: Python<'a>, node_ids: &[u8]) -> PyResult<Bound<'a, PyList>> {
    PyList::new(py, node_ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy::prelude::{
        GeoMode as RustGeoMode, GeoTopicDurations, HeaderKey, HeaderValue, IggyTimestamp,
        OptionKey, OptionValue, PartitionContext as RustPartitionContext, ResourceOptions,
        TopicType as RustTopicType,
    };
    use std::time::Duration;

    #[test]
    fn given_geo_topic_options_when_reading_should_expose_topic_type_and_geo_mode() {
        let geo_tiered = topic_options(&[
            (OptionKey::TopicType, RustTopicType::Geo.as_ref()),
            (OptionKey::GeoMode, RustGeoMode::GeoTiered.as_ref()),
        ]);
        let geo_without_mode =
            topic_options(&[(OptionKey::TopicType, RustTopicType::Geo.as_ref())]);

        for (options, topic_type, geo_mode) in [
            (geo_tiered, TopicType::Geo, Some(GeoMode::GeoTiered)),
            (geo_without_mode, TopicType::Geo, Some(GeoMode::Geo)),
            (ResourceOptions::new(), TopicType::Normal, None),
        ] {
            let topic = Topic::from(RustTopic {
                id: 1,
                created_at: IggyTimestamp::from(11),
                name: "orders".to_owned(),
                size: IggyByteSize::from(0),
                messages_count: 0,
                partitions_count: 1,
                options: options.clone(),
            });
            let details = TopicDetails::from(RustTopicDetails {
                id: 1,
                created_at: IggyTimestamp::from(11),
                name: "orders".to_owned(),
                size: IggyByteSize::from(0),
                messages_count: 0,
                partitions_count: 0,
                partitions: Vec::new(),
                options,
            });

            assert_eq!(topic.topic_type(), topic_type);
            assert_eq!(topic.geo_mode(), geo_mode);
            assert_eq!(details.topic_type(), topic_type);
            assert_eq!(details.geo_mode(), geo_mode);
        }
    }

    fn topic_options(entries: &[(OptionKey, &str)]) -> ResourceOptions {
        entries
            .iter()
            .map(|(key, value)| {
                (
                    HeaderKey::from(*key),
                    OptionValue::explicit(HeaderValue::try_from(*value).expect("header value")),
                )
            })
            .collect()
    }

    #[test]
    fn given_geo_topic_durations_when_reading_should_return_set_ones_and_none_for_the_rest() {
        Python::initialize();
        let set: [(OptionKey, u64); 2] = [
            (OptionKey::FreshUploadInterval, 0),
            (OptionKey::TombstoneGrace, 3_600_000_000),
        ];
        let mut geo = topic_options(&[(OptionKey::TopicType, RustTopicType::Geo.as_ref())]);
        for (key, micros) in set {
            geo.insert(
                HeaderKey::from(key),
                OptionValue::explicit(HeaderValue::from(micros)),
            );
        }

        for (options, expected) in [(geo, set.as_slice()), (ResourceOptions::new(), &[])] {
            let topic = Topic::from(RustTopic {
                id: 1,
                created_at: IggyTimestamp::from(11),
                name: "orders".to_owned(),
                size: IggyByteSize::from(0),
                messages_count: 0,
                partitions_count: 1,
                options: options.clone(),
            });
            let details = TopicDetails::from(RustTopicDetails {
                id: 1,
                created_at: IggyTimestamp::from(11),
                name: "orders".to_owned(),
                size: IggyByteSize::from(0),
                messages_count: 0,
                partitions_count: 0,
                partitions: Vec::new(),
                options,
            });

            Python::attach(|py| {
                let readers = [
                    Bound::new(py, topic).expect("topic wraps").into_any(),
                    Bound::new(py, details).expect("details wrap").into_any(),
                ];
                for key in GeoTopicDurations::KEYS {
                    let wanted = expected
                        .iter()
                        .find(|(set_key, _)| *set_key == key)
                        .map(|(_, micros)| Duration::from_micros(*micros));
                    let name: &str = key.into();
                    for reader in &readers {
                        let read = reader
                            .getattr(name)
                            .and_then(|value| value.extract::<Option<Duration>>())
                            .expect("duration reads");
                        assert_eq!(read, wanted, "{name}");
                    }
                }
            });
        }
    }

    #[test]
    fn given_read_cache_options_when_reading_should_keep_zero_and_return_none_when_unset() {
        const MIB: u64 = 1024 * 1024;
        let sized = |entries: &[(OptionKey, u64)]| -> ResourceOptions {
            entries
                .iter()
                .map(|(key, bytes)| {
                    (
                        HeaderKey::from(*key),
                        OptionValue::explicit(HeaderValue::from(*bytes)),
                    )
                })
                .collect()
        };
        let mut geo = topic_options(&[(OptionKey::TopicType, RustTopicType::Geo.as_ref())]);
        geo.extend(sized(&[(OptionKey::ReadAhead, 0)]));

        for (options, read_cache_size, read_ahead) in [
            (sized(&[(OptionKey::ReadCacheSize, 0)]), Some(0), None),
            (
                sized(&[
                    (OptionKey::ReadCacheSize, 16 * MIB),
                    (OptionKey::ReadAhead, 0),
                ]),
                Some(16 * MIB),
                Some(0),
            ),
            (geo, None, Some(0)),
            (ResourceOptions::new(), None, None),
        ] {
            let topic = Topic::from(RustTopic {
                id: 1,
                created_at: IggyTimestamp::from(11),
                name: "orders".to_owned(),
                size: IggyByteSize::from(0),
                messages_count: 0,
                partitions_count: 1,
                options: options.clone(),
            });
            let details = TopicDetails::from(RustTopicDetails {
                id: 1,
                created_at: IggyTimestamp::from(11),
                name: "orders".to_owned(),
                size: IggyByteSize::from(0),
                messages_count: 0,
                partitions_count: 0,
                partitions: Vec::new(),
                options,
            });

            assert_eq!(topic.read_cache_size(), read_cache_size);
            assert_eq!(topic.read_ahead(), read_ahead);
            assert_eq!(details.read_cache_size(), read_cache_size);
            assert_eq!(details.read_ahead(), read_ahead);
        }
    }

    #[test]
    fn given_partition_with_members_when_converting_should_expose_node_ids_as_int_lists() {
        Python::initialize();
        let partition = Partition::from(RustPartition {
            id: 1,
            created_at: IggyTimestamp::from(11),
            segments_count: 1,
            current_offset: 2,
            size: IggyByteSize::from(13),
            messages_count: 3,
            context: RustPartitionContext::default(),
            members: vec![1, 3, 4],
            in_sync_members: vec![1, 4],
        });

        Python::attach(|py| {
            let members = partition.members(py).expect("build members list");
            let in_sync_members = partition
                .in_sync_members(py)
                .expect("build in-sync members list");

            assert_eq!(members.extract::<Vec<u8>>().unwrap(), [1, 3, 4]);
            assert_eq!(in_sync_members.extract::<Vec<u8>>().unwrap(), [1, 4]);
        });
    }
}
