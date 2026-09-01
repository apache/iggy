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
    CacheMetrics as RustCacheMetrics, CacheMetricsKey as RustCacheMetricsKey, Stats as RustStats,
};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::HashMap;

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
#[gen_stub_pyclass]
#[pyclass]
pub struct Stats {
    pub(crate) inner: RustStats,
}

impl From<RustStats> for Stats {
    fn from(stats: RustStats) -> Self {
        Self { inner: stats }
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
    #[getter]
    pub fn cpu_usage(&self) -> f32 {
        self.inner.cpu_usage
    }

    /// The total CPU usage of the system, in percent.
    #[getter]
    pub fn total_cpu_usage(&self) -> f32 {
        self.inner.total_cpu_usage
    }

    /// The memory usage of the server process, in bytes.
    #[getter]
    pub fn memory_usage(&self) -> u64 {
        self.inner.memory_usage.as_bytes_u64()
    }

    /// The total memory of the system, in bytes.
    #[getter]
    pub fn total_memory(&self) -> u64 {
        self.inner.total_memory.as_bytes_u64()
    }

    /// The available memory of the system, in bytes.
    #[getter]
    pub fn available_memory(&self) -> u64 {
        self.inner.available_memory.as_bytes_u64()
    }

    /// The run time of the server process, in microseconds.
    #[getter]
    pub fn run_time(&self) -> u64 {
        self.inner.run_time.as_micros()
    }

    /// The start time of the server process, in microseconds since the Unix epoch.
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
    /// E.g. 1.2.3 -> 100200300 (major * 1000000 + minor * 1000 + patch).
    #[getter]
    #[gen_stub(override_return_type(type_repr = "int | None"))]
    pub fn iggy_server_semver(&self) -> Option<u32> {
        self.inner.iggy_server_semver
    }

    /// Cache metrics per partition.
    #[getter]
    pub fn cache_metrics(&self) -> HashMap<CacheMetricsKey, CacheMetrics> {
        self.inner
            .cache_metrics
            .iter()
            .map(|(key, metrics)| (CacheMetricsKey::from(key), CacheMetrics::from(metrics)))
            .collect()
    }

    /// The number of threads in the server process.
    #[getter]
    pub fn threads_count(&self) -> u32 {
        self.inner.threads_count
    }

    /// The available (free) disk space for the data directory, in bytes.
    #[getter]
    pub fn free_disk_space(&self) -> u64 {
        self.inner.free_disk_space.as_bytes_u64()
    }

    /// The total disk space for the data directory, in bytes.
    #[getter]
    pub fn total_disk_space(&self) -> u64 {
        self.inner.total_disk_space.as_bytes_u64()
    }
}
