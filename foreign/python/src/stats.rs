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

use crate::duration::iggy_duration_to_py_delta;
use iggy::prelude::{
    CacheMetrics as RustCacheMetrics, CacheMetricsKey as RustCacheMetricsKey, Stats as RustStats,
};
use pyo3::prelude::*;
use pyo3::types::{PyDelta, PyDict};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

/// Key identifying the partition a `CacheMetrics` entry belongs to.
///
/// Hashable and comparable, so it can key the `Stats.cache_metrics` dict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[gen_stub_pyclass]
#[pyclass(eq, frozen, hash, skip_from_py_object)]
pub struct CacheMetricsKey {
    /// The unique identifier (numeric) of the stream.
    #[pyo3(get)]
    pub stream_id: u32,
    /// The unique identifier (numeric) of the topic within the stream.
    #[pyo3(get)]
    pub topic_id: u32,
    /// The unique identifier (numeric) of the partition within the topic.
    #[pyo3(get)]
    pub partition_id: u32,
}

impl From<&RustCacheMetricsKey> for CacheMetricsKey {
    fn from(key: &RustCacheMetricsKey) -> Self {
        Self {
            stream_id: key.stream_id,
            topic_id: key.topic_id,
            partition_id: key.partition_id,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl CacheMetricsKey {
    #[new]
    fn new(stream_id: u32, topic_id: u32, partition_id: u32) -> Self {
        Self {
            stream_id,
            topic_id,
            partition_id,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "CacheMetricsKey(stream_id={}, topic_id={}, partition_id={})",
            self.stream_id, self.topic_id, self.partition_id
        )
    }
}

/// Cache metrics for a specific partition.
#[gen_stub_pyclass]
#[pyclass]
pub struct CacheMetrics {
    /// Number of cache hits.
    #[pyo3(get)]
    pub hits: u64,
    /// Number of cache misses.
    #[pyo3(get)]
    pub misses: u64,
    /// Hit ratio (hits / (hits + misses)).
    #[pyo3(get)]
    pub hit_ratio: f32,
}

impl From<&RustCacheMetrics> for CacheMetrics {
    fn from(metrics: &RustCacheMetrics) -> Self {
        Self {
            hits: metrics.hits,
            misses: metrics.misses,
            hit_ratio: metrics.hit_ratio,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl CacheMetrics {
    fn __repr__(&self) -> String {
        format!(
            "CacheMetrics(hits={}, misses={}, hit_ratio={})",
            self.hits, self.misses, self.hit_ratio
        )
    }
}

/// The statistics and details of the server and its running process.
///
/// The fields are gathered from several sources while the request is served
/// (metadata counters, a process probe, a disk probe), so they are not an
/// atomic snapshot of one instant.
#[gen_stub_pyclass]
#[pyclass]
pub struct Stats {
    pub(crate) inner: RustStats,
    /// Converted once here so that every `cache_metrics` access returns the
    /// same dict instead of re-collecting the whole map.
    cache_metrics: Py<PyDict>,
}

impl From<RustStats> for Stats {
    fn from(stats: RustStats) -> Self {
        let cache_metrics = Python::attach(|py| {
            let dict = PyDict::new(py);
            for (key, metrics) in &stats.cache_metrics {
                dict.set_item(CacheMetricsKey::from(key), CacheMetrics::from(metrics))
                    .expect("insert cache metrics entry");
            }
            dict.unbind()
        });
        Self {
            inner: stats,
            cache_metrics,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl Stats {
    /// The unique identifier of the server process.
    #[getter]
    pub fn process_id(&self) -> u32 {
        self.inner.process_id
    }

    /// The CPU usage of the server process, in percent.
    ///
    /// Measured as a delta since the previous `get_stats` served by the same
    /// server shard, so the first sample a shard serves is 0.
    #[getter]
    pub fn cpu_usage(&self) -> f32 {
        self.inner.cpu_usage
    }

    /// The total CPU usage of the system, in percent, scoped to the cores the
    /// server may run on when confined by an affinity/cpuset mask.
    ///
    /// Same per-shard delta sampling as `cpu_usage`: the first sample a shard
    /// serves is 0.
    #[getter]
    pub fn total_cpu_usage(&self) -> f32 {
        self.inner.total_cpu_usage
    }

    /// The memory usage of the server process, in bytes.
    #[getter]
    pub fn memory_usage(&self) -> u64 {
        self.inner.memory_usage.as_bytes_u64()
    }

    /// The total memory of the system, in bytes, or the effective cgroup memory
    /// limit when the server runs inside a memory-capped cgroup (container,
    /// systemd slice).
    #[getter]
    pub fn total_memory(&self) -> u64 {
        self.inner.total_memory.as_bytes_u64()
    }

    /// The available memory of the system, in bytes, scoped to the cgroup
    /// limit when one applies.
    #[getter]
    pub fn available_memory(&self) -> u64 {
        self.inner.available_memory.as_bytes_u64()
    }

    /// The run time of the server process, with whole-second precision.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "datetime.timedelta", imports=("datetime")))]
    pub fn run_time<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyDelta>> {
        iggy_duration_to_py_delta(py, self.inner.run_time)
    }

    /// The start time of the server process, in microseconds since the Unix
    /// epoch, with whole-second precision.
    #[getter]
    pub fn start_time(&self) -> u64 {
        self.inner.start_time.as_micros()
    }

    /// The total number of bytes read.
    #[getter]
    pub fn read_bytes(&self) -> u64 {
        self.inner.read_bytes.as_bytes_u64()
    }

    /// The total number of bytes written.
    #[getter]
    pub fn written_bytes(&self) -> u64 {
        self.inner.written_bytes.as_bytes_u64()
    }

    /// The total size of the messages, in bytes.
    #[getter]
    pub fn messages_size_bytes(&self) -> u64 {
        self.inner.messages_size_bytes.as_bytes_u64()
    }

    /// The total number of streams.
    #[getter]
    pub fn streams_count(&self) -> u32 {
        self.inner.streams_count
    }

    /// The total number of topics.
    #[getter]
    pub fn topics_count(&self) -> u32 {
        self.inner.topics_count
    }

    /// The total number of partitions.
    #[getter]
    pub fn partitions_count(&self) -> u32 {
        self.inner.partitions_count
    }

    /// The total number of segments.
    #[getter]
    pub fn segments_count(&self) -> u32 {
        self.inner.segments_count
    }

    /// The total number of messages.
    #[getter]
    pub fn messages_count(&self) -> u64 {
        self.inner.messages_count
    }

    /// The total number of connected clients.
    #[getter]
    pub fn clients_count(&self) -> u32 {
        self.inner.clients_count
    }

    /// The total number of consumer groups.
    #[getter]
    pub fn consumer_groups_count(&self) -> u32 {
        self.inner.consumer_groups_count
    }

    /// The name of the host the server runs on.
    #[getter]
    pub fn hostname(&self) -> String {
        self.inner.hostname.clone()
    }

    /// The name of the operating system.
    #[getter]
    pub fn os_name(&self) -> String {
        self.inner.os_name.clone()
    }

    /// The version of the operating system.
    #[getter]
    pub fn os_version(&self) -> String {
        self.inner.os_version.clone()
    }

    /// The version of the kernel.
    #[getter]
    pub fn kernel_version(&self) -> String {
        self.inner.kernel_version.clone()
    }

    /// The version of the Iggy server.
    #[getter]
    pub fn iggy_server_version(&self) -> String {
        self.inner.iggy_server_version.clone()
    }

    /// The numeric semantic version of the Iggy server, or `None` when unknown.
    /// E.g. 1.2.3 -> 1002003 (major * 1000000 + minor * 1000 + patch).
    #[getter]
    #[gen_stub(override_return_type(type_repr = "builtins.int | None"))]
    pub fn iggy_server_semver(&self) -> Option<u32> {
        self.inner.iggy_server_semver
    }

    /// Cache metrics per partition.
    ///
    /// The server does not populate this yet and always replies with an empty
    /// map. Built once when the stats are received; every access returns the
    /// same dict.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "builtins.dict[CacheMetricsKey, CacheMetrics]"))]
    pub fn cache_metrics(&self, py: Python<'_>) -> Py<PyDict> {
        self.cache_metrics.clone_ref(py)
    }

    /// The number of threads in the server process.
    #[getter]
    pub fn threads_count(&self) -> u32 {
        self.inner.threads_count
    }

    /// The available (free) disk space for the data directory, in bytes.
    ///
    /// 0 when the server does not know its data directory or the disk probe
    /// fails.
    #[getter]
    pub fn free_disk_space(&self) -> u64 {
        self.inner.free_disk_space.as_bytes_u64()
    }

    /// The total disk space for the data directory, in bytes.
    ///
    /// 0 when the server does not know its data directory or the disk probe
    /// fails.
    #[getter]
    pub fn total_disk_space(&self) -> u64 {
        self.inner.total_disk_space.as_bytes_u64()
    }

    fn __repr__(&self) -> String {
        format!(
            "Stats(hostname='{}', iggy_server_version='{}', streams_count={}, \
             topics_count={}, partitions_count={}, messages_count={}, clients_count={})",
            self.inner.hostname,
            self.inner.iggy_server_version,
            self.inner.streams_count,
            self.inner.topics_count,
            self.inner.partitions_count,
            self.inner.messages_count,
            self.inner.clients_count
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy::prelude::{IggyByteSize, IggyDuration, IggyTimestamp};
    use std::collections::HashMap;

    fn rust_stats(iggy_server_semver: Option<u32>) -> RustStats {
        RustStats {
            process_id: 1,
            cpu_usage: 0.0,
            total_cpu_usage: 0.0,
            memory_usage: IggyByteSize::default(),
            total_memory: IggyByteSize::default(),
            available_memory: IggyByteSize::default(),
            run_time: IggyDuration::default(),
            start_time: IggyTimestamp::default(),
            read_bytes: IggyByteSize::default(),
            written_bytes: IggyByteSize::default(),
            messages_size_bytes: IggyByteSize::default(),
            streams_count: 0,
            topics_count: 0,
            partitions_count: 0,
            segments_count: 0,
            messages_count: 0,
            clients_count: 0,
            consumer_groups_count: 0,
            hostname: String::new(),
            os_name: String::new(),
            os_version: String::new(),
            kernel_version: String::new(),
            iggy_server_version: String::new(),
            iggy_server_semver,
            cache_metrics: HashMap::new(),
            threads_count: 0,
            free_disk_space: IggyByteSize::default(),
            total_disk_space: IggyByteSize::default(),
        }
    }

    #[test]
    fn iggy_server_semver_none_survives_conversion() {
        Python::initialize();

        assert_eq!(Stats::from(rust_stats(None)).iggy_server_semver(), None);
        assert_eq!(
            Stats::from(rust_stats(Some(1_002_003))).iggy_server_semver(),
            Some(1_002_003)
        );
    }
}
