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


using System.Text.Json.Serialization;
using Apache.Iggy.JsonConverters;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Statistics and details of the server and running process.
/// </summary>
public sealed class StatsResponse
{
    /// <summary>
    ///     Process identifier.
    /// </summary>
    public required int ProcessId { get; init; }

    /// <summary>
    ///     CPU usage of the process.
    /// </summary>
    public required float CpuUsage { get; init; }

    /// <summary>
    ///     Total CPU usage of the system.
    /// </summary>
    public required float TotalCpuUsage { get; init; }

    /// <summary>
    ///     Memory usage of the process.
    /// </summary>
    [JsonConverter(typeof(SizeConverter))]
    public required ulong MemoryUsage { get; init; }

    /// <summary>
    ///     Total memory of the system.
    /// </summary>
    [JsonConverter(typeof(SizeConverter))]
    public required ulong TotalMemory { get; init; }

    /// <summary>
    ///     Available memory of the system.
    /// </summary>
    [JsonConverter(typeof(SizeConverter))]
    public required ulong AvailableMemory { get; init; }

    /// <summary>
    ///     Total run time of the process.
    /// </summary>
    public required ulong RunTime { get; init; }

    /// <summary>
    ///     Start time of the process.
    /// </summary>
    [JsonConverter(typeof(DateTimeOffsetConverter))]
    public required DateTimeOffset StartTime { get; init; }

    /// <summary>
    ///     Total bytes read from disk.
    /// </summary>
    [JsonConverter(typeof(SizeConverter))]
    public required ulong ReadBytes { get; init; }

    /// <summary>
    ///     Total bytes written to disk.
    /// </summary>
    [JsonConverter(typeof(SizeConverter))]
    public required ulong WrittenBytes { get; init; }

    /// <summary>
    ///     Total size of the messages in bytes.
    /// </summary>
    [JsonConverter(typeof(SizeConverter))]
    public required ulong MessagesSizeBytes { get; init; }

    /// <summary>
    ///     Total number of streams.
    /// </summary>
    public required int StreamsCount { get; init; }

    /// <summary>
    ///     Total number of topics.
    /// </summary>
    public required int TopicsCount { get; init; }

    /// <summary>
    ///     Total number of partitions.
    /// </summary>
    public required int PartitionsCount { get; init; }

    /// <summary>
    ///     Total number of segments.
    /// </summary>
    public required int SegmentsCount { get; init; }

    /// <summary>
    ///     Total number of messages.
    /// </summary>
    public required ulong MessagesCount { get; init; }

    /// <summary>
    ///     Total number of connected clients.
    /// </summary>
    public required int ClientsCount { get; init; }

    /// <summary>
    ///     Total number of consumer groups.
    /// </summary>
    public required int ConsumerGroupsCount { get; init; }

    /// <summary>
    ///     Hostname of the server.
    /// </summary>
    public required string Hostname { get; init; }

    /// <summary>
    ///     Operating system name.
    /// </summary>
    public required string OsName { get; init; }

    /// <summary>
    ///     Operating system version.
    /// </summary>
    public required string OsVersion { get; init; }

    /// <summary>
    ///     Kernel version.
    /// </summary>
    public required string KernelVersion { get; init; }

    /// <summary>
    ///     Iggy server version.
    /// </summary>
    public required string IggyServerVersion { get; init; }

    /// <summary>
    ///     Aemantic version of the Iggy server in the numeric format e.g. 1.2.3 -> 100200300 (major * 1000000 + minor * 1000 +
    ///     patch).
    /// </summary>
    public uint IggyServerSemver { get; init; }

    /// <summary>
    ///     Cache metrics per partition
    /// </summary>
    public Dictionary<CacheMetricsKey, CacheMetrics> CacheMetrics { get; init; } = [];
}
