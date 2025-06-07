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

using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Extensions;
using System.ComponentModel;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Iggy_SDK.JsonConfiguration;

public class StatsResponseConverter : JsonConverter<StatsResponse>
{
    public override StatsResponse? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var doc = JsonDocument.ParseValue(ref reader);
        var root = doc.RootElement;
        
        int processId = root.GetProperty(nameof(Stats.ProcessId).ToSnakeCase()).GetInt32();
        float cpuUsage = root.GetProperty(nameof(Stats.CpuUsage).ToSnakeCase()).GetSingle();
        float totalCpuUsage = root.GetProperty(nameof(Stats.TotalCpuUsage).ToSnakeCase()).GetSingle();
        
        string? memoryUsageString = root.GetProperty(nameof(Stats.MemoryUsage).ToSnakeCase()).GetString();
        string[] memoryUsageStringSplit = memoryUsageString.Split(' ');
        (float memoryUsageBytesVal, string memoryUnit) = (ParseFloat(memoryUsageStringSplit[0]), memoryUsageStringSplit[1]);
        ulong memoryUsage = ConvertStringBytesToUlong(memoryUnit, memoryUsageBytesVal);
        
        string? totalMemoryString = root.GetProperty(nameof(Stats.TotalMemory).ToSnakeCase()).GetString();
        string[] totalMemoryStringSplit = totalMemoryString.Split(' ');
        (float totalMemoryUsageBytesVal, string totalMemoryUnit) = (ParseFloat(totalMemoryStringSplit[0]), totalMemoryStringSplit[1]);
        ulong totalMemoryUsage = ConvertStringBytesToUlong(totalMemoryUnit, totalMemoryUsageBytesVal);
        
        string? availableMemoryString = root.GetProperty(nameof(Stats.AvailableMemory).ToSnakeCase()).GetString();
        string[] availableMemoryStringSplit = availableMemoryString.Split(' ');
        (float availableMemoryBytesVal, string availableMemoryUnit) = (ParseFloat(availableMemoryStringSplit[0]), availableMemoryStringSplit[1]);
        ulong availableMemory = ConvertStringBytesToUlong(availableMemoryUnit, availableMemoryBytesVal);
        
        var runtime = root.GetProperty(nameof(Stats.RunTime).ToSnakeCase()).GetUInt64();
        ulong startTime = root.GetProperty(nameof(Stats.StartTime).ToSnakeCase()).GetUInt64();
        string? readBytesString = root.GetProperty(nameof(Stats.ReadBytes).ToSnakeCase()).GetString();
        string[] readBytesStringSplit = readBytesString.Split(' ');
        (float readBytesVal, string readBytesUnit) = (ParseFloat(readBytesStringSplit[0]), readBytesStringSplit[1]);
        ulong readBytes = ConvertStringBytesToUlong(readBytesUnit, readBytesVal);
        string? writtenBytesString = root.GetProperty(nameof(Stats.WrittenBytes).ToSnakeCase()).GetString();
        string[] writtenBytesStringSplit = writtenBytesString.Split(' ');
        (float writtenBytesVal, string writtenBytesUnit) = (ParseFloat(writtenBytesStringSplit[0]), writtenBytesStringSplit[1]);
        ulong writtenBytes = ConvertStringBytesToUlong(writtenBytesUnit, writtenBytesVal);
        string? messageWrittenSizeBytesString = root.GetProperty(nameof(Stats.MessagesSizeBytes).ToSnakeCase()).GetString();
        string[] messageWrittenSizeBytesStringSplit = messageWrittenSizeBytesString.Split(' ');
        (float messageWrittenSizeBytesVal, string messageWrittenSizeBytesUnit) = (ParseFloat(messageWrittenSizeBytesStringSplit[0]), messageWrittenSizeBytesStringSplit[1]);
        ulong messageWrittenSizeBytes = ConvertStringBytesToUlong(messageWrittenSizeBytesUnit, messageWrittenSizeBytesVal);
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
        
        return new StatsResponse
        {
            AvailableMemory = availableMemory,
            ClientsCount = clientsCount,
            ConsumerGroupsCount = consumerGroupsCount,
            CpuUsage = cpuUsage,
            Hostname = hostname,
            KernelVersion = kernelVersion,
            MemoryUsage = memoryUsage,
            MessagesCount = messagesCount,
            MessagesSizeBytes = messageWrittenSizeBytes,
            OsName = osName,
            OsVersion = osVersion,
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
            IggyVersion = iggyVersion
        };

    }

    private static ulong ConvertStringBytesToUlong(string memoryUnit, float memoryUsageBytesVal)
    {
        float memoryUsage = memoryUnit switch
        {
            "B" => memoryUsageBytesVal,
            "KiB" => memoryUsageBytesVal * (ulong)1e03,
            "MiB" => memoryUsageBytesVal * (ulong)1e06,
            "GiB" => memoryUsageBytesVal * (ulong)1e09,
            "TiB" => memoryUsageBytesVal * (ulong)1e12,
            _ => throw new InvalidEnumArgumentException("Error Wrong Unit when deserializing MemoryUsage")
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