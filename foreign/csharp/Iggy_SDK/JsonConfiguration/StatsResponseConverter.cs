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
        
        int processId = root.GetProperty(nameof(Stats.ProcessId).ToSnakeCase()).GetInt32();
        float cpuUsage = root.GetProperty(nameof(Stats.CpuUsage).ToSnakeCase()).GetSingle();
        float totalCpuUsage = root.GetProperty(nameof(Stats.TotalCpuUsage).ToSnakeCase()).GetSingle();
        
        ulong memoryUsage = ConvertStringToUlong(root.GetProperty(nameof(Stats.MemoryUsage).ToSnakeCase()).GetString());
        ulong totalMemoryUsage = ConvertStringToUlong(root.GetProperty(nameof(Stats.TotalMemory).ToSnakeCase()).GetString());
        ulong availableMemory = ConvertStringToUlong(root.GetProperty(nameof(Stats.AvailableMemory).ToSnakeCase()).GetString());
        
        var runtime = root.GetProperty(nameof(Stats.RunTime).ToSnakeCase()).GetUInt64();
        ulong startTime = root.GetProperty(nameof(Stats.StartTime).ToSnakeCase()).GetUInt64();
        ulong readBytes = ConvertStringToUlong(root.GetProperty(nameof(Stats.ReadBytes).ToSnakeCase()).GetString());
        ulong writtenBytes = ConvertStringToUlong(root.GetProperty(nameof(Stats.WrittenBytes).ToSnakeCase()).GetString());
        ulong messageWrittenSizeBytes = ConvertStringToUlong(root.GetProperty(nameof(Stats.MessagesSizeBytes).ToSnakeCase()).GetString());
        int streamsCount = root.GetProperty(nameof(Stats.StreamsCount).ToSnakeCase()).GetInt32();
        int topicsCount = root.GetProperty(nameof(Stats.TopicsCount).ToSnakeCase()).GetInt32();
        int partitionsCount = root.GetProperty(nameof(Stats.PartitionsCount).ToSnakeCase()).GetInt32();
        int segmentsCount = root.GetProperty(nameof(Stats.SegmentsCount).ToSnakeCase()).GetInt32();
        ulong messagesCount = root.GetProperty(nameof(Stats.MessagesCount).ToSnakeCase()).GetUInt64();
        int clientsCount = root.GetProperty(nameof(Stats.ClientsCount).ToSnakeCase()).GetInt32();
        int consumerGroupsCount = root.GetProperty(nameof(Stats.ConsumerGroupsCount).ToSnakeCase()).GetInt32();
        string? hostname = root.GetProperty(nameof(Stats.Hostname).ToSnakeCase()).GetString();
        string? osName = root.GetProperty(nameof(Stats.OsName).ToSnakeCase()).GetString();
        string? osVersion = root.GetProperty(nameof(Stats.OsVersion).ToSnakeCase()).GetString();
        string? kernelVersion = root.GetProperty(nameof(Stats.KernelVersion).ToSnakeCase()).GetString();
        string? iggyVersion = root.GetProperty(nameof(Stats.IggyServerVersion).ToSnakeCase()).GetString();
        uint iggyServerSemver = root.GetProperty(nameof(Stats.IggyServerSemver).ToSnakeCase()).GetUInt32();
        
        
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
        if(usage is null)
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
        float memoryUsage = memoryUnit switch
        {
            "B" => memoryUsageBytesVal,
            "KiB" => memoryUsageBytesVal * (ulong)1024,
            "KB" => memoryUsageBytesVal * (ulong)1e03,
            "MiB" => memoryUsageBytesVal * (ulong)1024 * 1024,
            "MB" => memoryUsageBytesVal * (ulong)1e06,
            "GiB" => memoryUsageBytesVal * (ulong)1024 * 1024 * 1024,
            "GB" => memoryUsageBytesVal * (ulong)1e09,
            "TiB" => memoryUsageBytesVal * (ulong)1024 * 1024 * 1024 * 1024,
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