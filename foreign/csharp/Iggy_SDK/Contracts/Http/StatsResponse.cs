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


using Apache.Iggy.Extensions;

namespace Apache.Iggy.Contracts.Http;

public sealed class StatsResponse
{
    public required int ProcessId { get; init; }
    public required float CpuUsage { get; init; }
    public required float TotalCpuUsage { get; init; }
    public required ulong MemoryUsage { get; init; }
    public required ulong TotalMemory { get; init; }
    public required ulong AvailableMemory { get; init; }
    public required ulong RunTime { get; init; }
    public required ulong StartTime { get; init; }
    public required ulong ReadBytes { get; init; }
    public required ulong WrittenBytes { get; init; }
    public required ulong MessagesSizeBytes { get; init; }
    public required int StreamsCount { get; init; }
    public required int TopicsCount { get; init; }
    public required int PartitionsCount { get; init; }
    public required int SegmentsCount { get; init; }
    public required ulong MessagesCount { get; init; }
    public required int ClientsCount { get; init; }
    public required int ConsumerGroupsCount { get; init; }
    public required string Hostname { get; init; }
    public required string OsName { get; init; }
    public required string OsVersion { get; init; }
    public required string KernelVersion { get; init; }
    public required string IggyVersion { get; init; }
    public uint IggyServerSemver { get; init; }
    public List<CacheMetrics> CacheMetrics { get; init; } = [];

    internal Stats ToStats()
    {
        return new Stats
        {
            ProcessId = ProcessId,
            CpuUsage = CpuUsage,
            TotalCpuUsage = TotalCpuUsage,
            MemoryUsage = MemoryUsage,
            TotalMemory = TotalMemory,
            AvailableMemory = AvailableMemory,
            RunTime = RunTime,
            StartTime = DateTimeOffsetUtils.FromUnixTimeMicroSeconds(StartTime),
            ReadBytes = ReadBytes,
            WrittenBytes = WrittenBytes,
            MessagesSizeBytes = MessagesSizeBytes,
            StreamsCount = StreamsCount,
            TopicsCount = TopicsCount,
            PartitionsCount = PartitionsCount,
            SegmentsCount = SegmentsCount,
            MessagesCount = MessagesCount,
            ClientsCount = ClientsCount,
            ConsumerGroupsCount = ConsumerGroupsCount,
            Hostname = Hostname,
            OsName = OsName,
            OsVersion = OsVersion,
            KernelVersion = KernelVersion,
            IggyServerVersion = IggyVersion,
            IggyServerSemver = IggyServerSemver,
            CacheMetrics = CacheMetrics
        };
    }

}