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

using System.ComponentModel;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Apache.Iggy.Contracts.Http;
using Apache.Iggy.Extensions;

namespace Apache.Iggy.JsonConfiguration;

public class StatsResponseConverter : JsonConverter<StatsResponse>
{
    public override StatsResponse? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var doc = JsonDocument.ParseValue(ref reader);
        var root = doc.RootElement;

        var processId = root.GetProperty(nameof(Stats.ProcessId).ToSnakeCase()).GetInt32();
        var cpuUsage = root.GetProperty(nameof(Stats.CpuUsage).ToSnakeCase()).GetSingle();
        var totalCpuUsage = root.GetProperty(nameof(Stats.TotalCpuUsage).ToSnakeCase()).GetSingle();

        var memoryUsage = ConvertStringToUlong(root.GetProperty(nameof(Stats.MemoryUsage).ToSnakeCase()).GetString());
        var totalMemoryUsage = ConvertStringToUlong(root.GetProperty(nameof(Stats.TotalMemory).ToSnakeCase()).GetString());
        var availableMemory = ConvertStringToUlong(root.GetProperty(nameof(Stats.AvailableMemory).ToSnakeCase()).GetString());

        var runtime = root.GetProperty(nameof(Stats.RunTime).ToSnakeCase()).GetUInt64();
        var startTime = root.GetProperty(nameof(Stats.StartTime).ToSnakeCase()).GetUInt64();
        var readBytes = ConvertStringToUlong(root.GetProperty(nameof(Stats.ReadBytes).ToSnakeCase()).GetString());
        var writtenBytes = ConvertStringToUlong(root.GetProperty(nameof(Stats.WrittenBytes).ToSnakeCase()).GetString());
        var messageWrittenSizeBytes = ConvertStringToUlong(root.GetProperty(nameof(Stats.MessagesSizeBytes).ToSnakeCase()).GetString());
        var streamsCount = root.GetProperty(nameof(Stats.StreamsCount).ToSnakeCase()).GetInt32();
        var topicsCount = root.GetProperty(nameof(Stats.TopicsCount).ToSnakeCase()).GetInt32();
        var partitionsCount = root.GetProperty(nameof(Stats.PartitionsCount).ToSnakeCase()).GetInt32();
        var segmentsCount = root.GetProperty(nameof(Stats.SegmentsCount).ToSnakeCase()).GetInt32();
        var messagesCount = root.GetProperty(nameof(Stats.MessagesCount).ToSnakeCase()).GetUInt64();
        var clientsCount = root.GetProperty(nameof(Stats.ClientsCount).ToSnakeCase()).GetInt32();
        var consumerGroupsCount = root.GetProperty(nameof(Stats.ConsumerGroupsCount).ToSnakeCase()).GetInt32();
        var hostname = root.GetProperty(nameof(Stats.Hostname).ToSnakeCase()).GetString();
        var osName = root.GetProperty(nameof(Stats.OsName).ToSnakeCase()).GetString();
        var osVersion = root.GetProperty(nameof(Stats.OsVersion).ToSnakeCase()).GetString();
        var kernelVersion = root.GetProperty(nameof(Stats.KernelVersion).ToSnakeCase()).GetString();
        var iggyVersion = root.GetProperty(nameof(Stats.IggyServerVersion).ToSnakeCase()).GetString();
        var iggyServerSemver = root.GetProperty(nameof(Stats.IggyServerSemver).ToSnakeCase()).GetUInt32();


        return new StatsResponse
        {
            AvailableMemory = availableMemory,
            ClientsCount = clientsCount,
            ConsumerGroupsCount = consumerGroupsCount,
            CpuUsage = cpuUsage,
            Hostname = hostname ?? string.Empty,
            KernelVersion = kernelVersion ?? string.Empty,
            MemoryUsage = memoryUsage,
            MessagesCount = messagesCount,
            MessagesSizeBytes = messageWrittenSizeBytes,
            OsName = osName ?? string.Empty,
            OsVersion = osVersion ?? string.Empty,
            PartitionsCount = partitionsCount,
            ProcessId = processId,
            ReadBytes = readBytes,
            RunTime = runtime,
            SegmentsCount = segmentsCount,
            StartTime = startTime,
            StreamsCount = streamsCount,
            TopicsCount = topicsCount,
            TotalCpuUsage = totalCpuUsage,
            TotalMemory = totalMemoryUsage,
            WrittenBytes = writtenBytes,
            IggyVersion = iggyVersion ?? string.Empty,
            IggyServerSemver = iggyServerSemver
        };
    }

    private ulong ConvertStringToUlong(string? usage)
    {
        if (usage is null)
        {
            return 0;
        }

        var usageStringSplit = usage.Split(' ');
        if (usageStringSplit.Length != 2)
        {
            throw new InvalidEnumArgumentException($"Error Wrong format when deserializing MemoryUsage: {usage}");
        }

        var (memoryUsageBytesVal, memoryUnit) = (ParseFloat(usageStringSplit[0]), usageStringSplit[1]);
        return ConvertStringBytesToUlong(memoryUnit, memoryUsageBytesVal);
    }

    private static ulong ConvertStringBytesToUlong(string memoryUnit, float memoryUsageBytesVal)
    {
        var memoryUsage = memoryUnit switch
        {
            "B" => memoryUsageBytesVal,
            "KiB" => memoryUsageBytesVal * 1024,
            "KB" => memoryUsageBytesVal * (ulong)1e03,
            "MiB" => memoryUsageBytesVal * 1024 * 1024,
            "MB" => memoryUsageBytesVal * (ulong)1e06,
            "GiB" => memoryUsageBytesVal * 1024 * 1024 * 1024,
            "GB" => memoryUsageBytesVal * (ulong)1e09,
            "TiB" => memoryUsageBytesVal * 1024 * 1024 * 1024 * 1024,
            "TB" => memoryUsageBytesVal * (ulong)1e12,
            _ => throw new InvalidEnumArgumentException($"Error Wrong Unit when deserializing MemoryUsage: {memoryUnit}")
        };
        return (ulong)memoryUsage;
    }

    private static float ParseFloat(string value)
    {
        return float.Parse(value, NumberStyles.AllowExponent | NumberStyles.Number, CultureInfo.InvariantCulture);
    }

    public override void Write(Utf8JsonWriter writer, StatsResponse value, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }
}