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

using Apache.Iggy.Contracts;

namespace Apache.Iggy.Tests.Utils.Stats;

public static class StatsFactory
{
    public static StatsResponse CreateFakeStatsObject()
    {
        return new StatsResponse
        {
            ProcessId = 123,
            CpuUsage = 12.34f,
            MemoryUsage = 567890,
            TotalCpuUsage = 56.78f,
            TotalMemory = 1234567890,
            AvailableMemory = 987654321,
            RunTime = 1234567890,
            StartTime = DateTimeOffset.FromUnixTimeSeconds(1628718600),
            ReadBytes = 1234567890,
            WrittenBytes = 987654321,
            StreamsCount = 10,
            KernelVersion = "4.18.0-305.el8.x86_64",
            MessagesCount = 100000,
            TopicsCount = 5,
            PartitionsCount = 20,
            SegmentsCount = 50,
            OsName = "Linux",
            OsVersion = "4.18.0",
            ConsumerGroupsCount = 8,
            MessagesSizeBytes = 1234567890,
            Hostname = "localhost",
            ClientsCount = 69,
            IggyServerVersion = "1234"
        };
    }
}