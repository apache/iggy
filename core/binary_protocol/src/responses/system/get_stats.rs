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

use crate::WireError;
use crate::codec::{WireDecode, WireEncode, read_f32_le, read_str, read_u32_le, read_u64_le};
use bytes::{BufMut, BytesMut};

/// Per-partition cache hit/miss statistics.
#[derive(Debug, Clone, PartialEq)]
pub struct CacheMetricEntry {
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub hits: u64,
    pub misses: u64,
    pub hit_ratio: f32,
}

impl CacheMetricEntry {
    const SIZE: usize = 4 + 4 + 4 + 8 + 8 + 4; // 32
}

/// Server statistics response.
///
/// Wire format:
/// ```text
/// [process_id:4][cpu_usage:f32][total_cpu_usage:f32]
/// [memory_usage:8][total_memory:8][available_memory:8]
/// [run_time:8][start_time:8][read_bytes:8][written_bytes:8]
/// [messages_size_bytes:8][streams_count:4][topics_count:4]
/// [partitions_count:4][segments_count:4][messages_count:8]
/// [clients_count:4][consumer_groups_count:4]
/// [hostname_len:4][hostname:N]
/// [os_name_len:4][os_name:N]
/// [os_version_len:4][os_version:N]
/// [kernel_version_len:4][kernel_version:N]
/// [iggy_server_version_len:4][iggy_server_version:N]
/// [iggy_server_semver:4]?
/// [cache_metrics_count:4]
/// For each: [stream_id:4][topic_id:4][partition_id:4][hits:8][misses:8][hit_ratio:f32]
/// [threads_count:4]?
/// [free_disk_space:8]?
/// [total_disk_space:8]?
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct StatsResponse {
    pub process_id: u32,
    pub cpu_usage: f32,
    pub total_cpu_usage: f32,
    pub memory_usage: u64,
    pub total_memory: u64,
    pub available_memory: u64,
    pub run_time: u64,
    pub start_time: u64,
    pub read_bytes: u64,
    pub written_bytes: u64,
    pub messages_size_bytes: u64,
    pub streams_count: u32,
    pub topics_count: u32,
    pub partitions_count: u32,
    pub segments_count: u32,
    pub messages_count: u64,
    pub clients_count: u32,
    pub consumer_groups_count: u32,
    pub hostname: String,
    pub os_name: String,
    pub os_version: String,
    pub kernel_version: String,
    pub iggy_server_version: String,
    pub iggy_server_semver: Option<u32>,
    pub cache_metrics: Vec<CacheMetricEntry>,
    pub threads_count: u32,
    pub free_disk_space: u64,
    pub total_disk_space: u64,
}

// Fixed-size numeric header before the variable-length string section.
const NUMERIC_HEADER_SIZE: usize =
    4 + 4 + 4 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 4 + 4 + 4 + 4 + 8 + 4 + 4; // 104

fn encode_len_prefixed_str(buf: &mut BytesMut, s: &str) {
    #[allow(clippy::cast_possible_truncation)]
    buf.put_u32_le(s.len() as u32);
    buf.put_slice(s.as_bytes());
}

fn decode_len_prefixed_str(buf: &[u8], pos: usize) -> Result<(String, usize), WireError> {
    let len = read_u32_le(buf, pos)? as usize;
    let s = read_str(buf, pos + 4, len)?;
    Ok((s, pos + 4 + len))
}

impl WireEncode for StatsResponse {
    fn encoded_size(&self) -> usize {
        let tail = if self.iggy_server_semver.is_some() {
            4 + 4 + 8 + 8 // semver + threads_count + free_disk_space + total_disk_space
        } else {
            0
        };
        NUMERIC_HEADER_SIZE
            + (4 + self.hostname.len())
            + (4 + self.os_name.len())
            + (4 + self.os_version.len())
            + (4 + self.kernel_version.len())
            + (4 + self.iggy_server_version.len())
            + 4 // cache_metrics_count
            + self.cache_metrics.len() * CacheMetricEntry::SIZE
            + tail
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.process_id);
        buf.put_f32_le(self.cpu_usage);
        buf.put_f32_le(self.total_cpu_usage);
        buf.put_u64_le(self.memory_usage);
        buf.put_u64_le(self.total_memory);
        buf.put_u64_le(self.available_memory);
        buf.put_u64_le(self.run_time);
        buf.put_u64_le(self.start_time);
        buf.put_u64_le(self.read_bytes);
        buf.put_u64_le(self.written_bytes);
        buf.put_u64_le(self.messages_size_bytes);
        buf.put_u32_le(self.streams_count);
        buf.put_u32_le(self.topics_count);
        buf.put_u32_le(self.partitions_count);
        buf.put_u32_le(self.segments_count);
        buf.put_u64_le(self.messages_count);
        buf.put_u32_le(self.clients_count);
        buf.put_u32_le(self.consumer_groups_count);

        encode_len_prefixed_str(buf, &self.hostname);
        encode_len_prefixed_str(buf, &self.os_name);
        encode_len_prefixed_str(buf, &self.os_version);
        encode_len_prefixed_str(buf, &self.kernel_version);
        encode_len_prefixed_str(buf, &self.iggy_server_version);

        if let Some(semver) = self.iggy_server_semver {
            buf.put_u32_le(semver);
        }

        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32_le(self.cache_metrics.len() as u32);
        for entry in &self.cache_metrics {
            buf.put_u32_le(entry.stream_id);
            buf.put_u32_le(entry.topic_id);
            buf.put_u32_le(entry.partition_id);
            buf.put_u64_le(entry.hits);
            buf.put_u64_le(entry.misses);
            buf.put_f32_le(entry.hit_ratio);
        }

        // Tail fields are only present in the new wire format (semver present).
        if self.iggy_server_semver.is_some() {
            buf.put_u32_le(self.threads_count);
            buf.put_u64_le(self.free_disk_space);
            buf.put_u64_le(self.total_disk_space);
        }
    }
}

impl WireDecode for StatsResponse {
    #[allow(clippy::too_many_lines)]
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let process_id = read_u32_le(buf, 0)?;
        let cpu_usage = read_f32_le(buf, 4)?;
        let total_cpu_usage = read_f32_le(buf, 8)?;
        let memory_usage = read_u64_le(buf, 12)?;
        let total_memory = read_u64_le(buf, 20)?;
        let available_memory = read_u64_le(buf, 28)?;
        let run_time = read_u64_le(buf, 36)?;
        let start_time = read_u64_le(buf, 44)?;
        let read_bytes = read_u64_le(buf, 52)?;
        let written_bytes = read_u64_le(buf, 60)?;
        let messages_size_bytes = read_u64_le(buf, 68)?;
        let streams_count = read_u32_le(buf, 76)?;
        let topics_count = read_u32_le(buf, 80)?;
        let partitions_count = read_u32_le(buf, 84)?;
        let segments_count = read_u32_le(buf, 88)?;
        let messages_count = read_u64_le(buf, 92)?;
        let clients_count = read_u32_le(buf, 100)?;
        let consumer_groups_count = read_u32_le(buf, 104)?;

        let mut pos = NUMERIC_HEADER_SIZE;
        let (hostname, next) = decode_len_prefixed_str(buf, pos)?;
        pos = next;
        let (os_name, next) = decode_len_prefixed_str(buf, pos)?;
        pos = next;
        let (os_version, next) = decode_len_prefixed_str(buf, pos)?;
        pos = next;
        let (kernel_version, next) = decode_len_prefixed_str(buf, pos)?;
        pos = next;
        let (iggy_server_version, next) = decode_len_prefixed_str(buf, pos)?;
        pos = next;

        // iggy_server_semver is optional - only present if enough bytes remain
        // before the cache_metrics section. We need at least 8 bytes for
        // semver(4) + cache_count(4), vs 4 bytes for just cache_count(4).
        let remaining = buf.len().saturating_sub(pos);
        let iggy_server_semver = if remaining >= 8 {
            let v = read_u32_le(buf, pos)?;
            pos += 4;
            Some(v)
        } else {
            None
        };

        let cache_count = read_u32_le(buf, pos)? as usize;
        pos += 4;

        let mut cache_metrics = Vec::with_capacity(cache_count);
        for _ in 0..cache_count {
            let stream_id = read_u32_le(buf, pos)?;
            let topic_id = read_u32_le(buf, pos + 4)?;
            let partition_id = read_u32_le(buf, pos + 8)?;
            let hits = read_u64_le(buf, pos + 12)?;
            let misses = read_u64_le(buf, pos + 20)?;
            let hit_ratio = read_f32_le(buf, pos + 28)?;
            pos += CacheMetricEntry::SIZE;
            cache_metrics.push(CacheMetricEntry {
                stream_id,
                topic_id,
                partition_id,
                hits,
                misses,
                hit_ratio,
            });
        }

        // Optional tail fields for backwards compatibility with older servers
        let mut threads_count = 0u32;
        if pos + 4 <= buf.len() {
            threads_count = read_u32_le(buf, pos)?;
            pos += 4;
        }

        let mut free_disk_space = 0u64;
        if pos + 8 <= buf.len() {
            free_disk_space = read_u64_le(buf, pos)?;
            pos += 8;
        }

        let mut total_disk_space = 0u64;
        if pos + 8 <= buf.len() {
            total_disk_space = read_u64_le(buf, pos)?;
            pos += 8;
        }

        Ok((
            Self {
                process_id,
                cpu_usage,
                total_cpu_usage,
                memory_usage,
                total_memory,
                available_memory,
                run_time,
                start_time,
                read_bytes,
                written_bytes,
                messages_size_bytes,
                streams_count,
                topics_count,
                partitions_count,
                segments_count,
                messages_count,
                clients_count,
                consumer_groups_count,
                hostname,
                os_name,
                os_version,
                kernel_version,
                iggy_server_version,
                iggy_server_semver,
                cache_metrics,
                threads_count,
                free_disk_space,
                total_disk_space,
            },
            pos,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_stats() -> StatsResponse {
        StatsResponse {
            process_id: 1234,
            cpu_usage: 25.5,
            total_cpu_usage: 50.0,
            memory_usage: 1_073_741_824,
            total_memory: 8_589_934_592,
            available_memory: 4_294_967_296,
            run_time: 3600,
            start_time: 1_710_000_000_000,
            read_bytes: 1_000_000,
            written_bytes: 500_000,
            messages_size_bytes: 2_000_000,
            streams_count: 3,
            topics_count: 10,
            partitions_count: 30,
            segments_count: 90,
            messages_count: 50_000,
            clients_count: 5,
            consumer_groups_count: 2,
            hostname: "node-1".to_string(),
            os_name: "Linux".to_string(),
            os_version: "6.1".to_string(),
            kernel_version: "6.1.0".to_string(),
            iggy_server_version: "0.6.0".to_string(),
            iggy_server_semver: Some(600),
            cache_metrics: vec![],
            threads_count: 16,
            free_disk_space: 107_374_182_400,
            total_disk_space: 512_110_190_592,
        }
    }

    #[test]
    fn roundtrip_basic() {
        let stats = sample_stats();
        let bytes = stats.to_bytes();
        let (decoded, consumed) = StatsResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, stats);
    }

    #[test]
    fn roundtrip_with_cache_metrics() {
        let mut stats = sample_stats();
        stats.cache_metrics = vec![
            CacheMetricEntry {
                stream_id: 1,
                topic_id: 1,
                partition_id: 0,
                hits: 1000,
                misses: 50,
                hit_ratio: 0.952_380_95,
            },
            CacheMetricEntry {
                stream_id: 2,
                topic_id: 3,
                partition_id: 1,
                hits: 0,
                misses: 100,
                hit_ratio: 0.0,
            },
        ];
        let bytes = stats.to_bytes();
        let (decoded, consumed) = StatsResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, stats);
    }

    #[test]
    fn roundtrip_no_semver() {
        let mut stats = sample_stats();
        stats.iggy_server_semver = None;
        stats.cache_metrics = vec![];
        // Tail fields are not encoded when semver is absent (old wire format).
        stats.threads_count = 0;
        stats.free_disk_space = 0;
        stats.total_disk_space = 0;
        let bytes = stats.to_bytes();
        let (decoded, consumed) = StatsResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, stats);
    }

    #[test]
    fn roundtrip_empty_strings() {
        let stats = StatsResponse {
            hostname: String::new(),
            os_name: String::new(),
            os_version: String::new(),
            kernel_version: String::new(),
            iggy_server_version: String::new(),
            iggy_server_semver: None,
            cache_metrics: vec![],
            threads_count: 0,
            free_disk_space: 0,
            total_disk_space: 0,
            ..sample_stats()
        };
        let bytes = stats.to_bytes();
        let (decoded, consumed) = StatsResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, stats);
    }

    #[test]
    fn numeric_header_offset_check() {
        let stats = sample_stats();
        let bytes = stats.to_bytes();
        assert_eq!(read_u32_le(&bytes, 0).unwrap(), 1234);
        assert_eq!(read_u32_le(&bytes, 76).unwrap(), 3);
        assert_eq!(read_u32_le(&bytes, 104).unwrap(), 2);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn f32_values_preserved() {
        let stats = sample_stats();
        let bytes = stats.to_bytes();
        let (decoded, _) = StatsResponse::decode(&bytes).unwrap();
        // Exact comparison is valid: these f32 values are exactly representable
        // and survive a lossless LE roundtrip without any arithmetic.
        assert_eq!(decoded.cpu_usage, 25.5);
        assert_eq!(decoded.total_cpu_usage, 50.0);
    }
}
