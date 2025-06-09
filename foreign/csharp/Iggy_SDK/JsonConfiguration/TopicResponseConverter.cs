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
using System.Text.Json;
using System.Text.Json.Serialization;
using Apache.Iggy.Contracts.Http;
using Apache.Iggy.Enums;
using Apache.Iggy.Extensions;

namespace Apache.Iggy.JsonConfiguration;

internal sealed class TopicResponseConverter : JsonConverter<TopicResponse>
{
    private readonly JsonSerializerOptions _options;
    public TopicResponseConverter()
    {
        _options = new JsonSerializerOptions();
        _options.PropertyNamingPolicy = new ToSnakeCaseNamingPolicy();
    }

    public override TopicResponse? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var doc = JsonDocument.ParseValue(ref reader);
        var root = doc.RootElement;
        var id = root.GetProperty(nameof(TopicResponse.Id).ToSnakeCase()).GetInt32();
        var createdAt = root.GetProperty(nameof(TopicResponse.CreatedAt).ToSnakeCase()).GetUInt64();
        var name = root.GetProperty(nameof(TopicResponse.Name).ToSnakeCase()).GetString();
        var compressionAlgorithm = Enum.Parse<CompressionAlgorithm>(root.GetProperty(nameof(TopicResponse.CompressionAlgorithm).ToSnakeCase()).GetString()!, true);
        var sizeBytesString = root.GetProperty(nameof(TopicResponse.Size).ToSnakeCase()).GetString();
        var sizeBytesStringSplit = sizeBytesString.Split(' ');
        var (sizeBytesVal, Unit) = (ulong.Parse(sizeBytesStringSplit[0]), sizeBytesStringSplit[1]);
        var sizeBytes = Unit switch
        {
            "B" => sizeBytesVal,
            "KB" => sizeBytesVal * (ulong)1e03,
            "MB" => sizeBytesVal * (ulong)1e06,
            "GB" => sizeBytesVal * (ulong)1e09,
            "TB" => sizeBytesVal * (ulong)1e12,
            _ => throw new InvalidEnumArgumentException("Error Wrong Unit when deserializing SizeBytes")
        };
        var replicationFactor = root.GetProperty(nameof(TopicResponse.ReplicationFactor).ToSnakeCase()).GetUInt16();
        var maxTopicSize = root.GetProperty(nameof(TopicResponse.MaxTopicSize).ToSnakeCase()).GetUInt64();
        // var maxTopicSizeString = root.GetProperty(nameof(TopicResponse.MaxTopicSize).ToSnakeCase()).GetUInt64();
        // ulong maxTopicSize = 0;
        // if (maxTopicSizeString is not null)
        // {
        //     var maxTopicSizeStringSplit = maxTopicSizeString.Split(' ');
        //     (ulong maxTopicSizeVal, string maxTopicUnit) = (ulong.Parse(maxTopicSizeStringSplit[0]), maxTopicSizeStringSplit[1]);
        //     maxTopicSize = Unit switch
        //     {
        //         "B" => maxTopicSizeVal,
        //         "KB" => maxTopicSizeVal * (ulong)1e03,
        //         "MB" => maxTopicSizeVal * (ulong)1e06,
        //         "GB" => maxTopicSizeVal * (ulong)1e09,
        //         "TB" => maxTopicSizeVal * (ulong)1e12,
        //         _ => throw new InvalidEnumArgumentException("Error Wrong Unit when deserializing SizeBytes")
        //     };
        // }

        var messageExpiryProperty = root.GetProperty(nameof(TopicResponse.MessageExpiry).ToSnakeCase());
        var messageExpiry = messageExpiryProperty.ValueKind switch
        {
            JsonValueKind.Null => (ulong)0,
            JsonValueKind.Number => messageExpiryProperty.GetUInt64(),
            _ => throw new InvalidEnumArgumentException("Error Wrong JsonValueKind when deserializing MessageExpiry")
        };
        var messagesCount = root.GetProperty(nameof(TopicResponse.MessagesCount).ToSnakeCase()).GetUInt64();
        var partitionsCount = root.GetProperty(nameof(TopicResponse.PartitionsCount).ToSnakeCase()).GetInt32();
        root.TryGetProperty(nameof(TopicResponse.Partitions).ToSnakeCase(), out var partitionsProperty);
        var partitions = partitionsProperty.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.Undefined => null,
            JsonValueKind.Array => DeserializePartitions(partitionsProperty),
            _ => throw new InvalidEnumArgumentException("Error Wrong JsonValueKind when deserializing Partitions")
        };
        return new TopicResponse
        {
            Id = id,
            Name = name!,
            Size = sizeBytes,
            MessageExpiry = messageExpiry,
            CompressionAlgorithm = compressionAlgorithm,
            CreatedAt = DateTimeOffsetUtils.FromUnixTimeMicroSeconds(createdAt).LocalDateTime,
            MessagesCount = messagesCount,
            PartitionsCount = partitionsCount,
            ReplicationFactor = (byte)replicationFactor,
            MaxTopicSize = maxTopicSize,
            Partitions = partitions
        };
    }
    private IEnumerable<PartitionContract> DeserializePartitions(JsonElement partitionsElement)
    {
        var partitions = new List<PartitionContract>();
        var partitionObjects = partitionsElement.EnumerateArray();
        foreach (var partition in partitionObjects)
        {
            var id = partition.GetProperty(nameof(PartitionContract.Id).ToSnakeCase()).GetInt32();
            var createdAt = partition.GetProperty(nameof(PartitionContract.CreatedAt).ToSnakeCase())
                .GetUInt64();
            var segmentsCount = partition.GetProperty(nameof(PartitionContract.SegmentsCount).ToSnakeCase())
                .GetInt32();
            var currentOffset = partition.GetProperty(nameof(PartitionContract.CurrentOffset).ToSnakeCase())
                .GetUInt64();
            var sizeBytesString = partition.GetProperty(nameof(PartitionContract.Size).ToSnakeCase()).GetString();
            var sizeBytesStringSplit = sizeBytesString.Split(' ');
            var (sizeBytesVal, Unit) = (ulong.Parse(sizeBytesStringSplit[0]), sizeBytesStringSplit[1]);
            ulong sizeBytes = Unit switch
            {
                "B" => sizeBytesVal,
                "KB" => sizeBytesVal * (ulong)1e03,
                "MB" => sizeBytesVal * (ulong)1e06,
                "GB" => sizeBytesVal * (ulong)1e09,
                "TB" => sizeBytesVal * (ulong)1e12,
                _ => throw new InvalidEnumArgumentException("Error Wrong Unit when deserializing SizeBytes")
            };
            var messagesCount = partition.GetProperty(nameof(PartitionContract.MessagesCount).ToSnakeCase())
                .GetUInt64();
            partitions.Add(new PartitionContract
            {
                Id = id,
                CreatedAt = DateTimeOffsetUtils.FromUnixTimeMicroSeconds(createdAt).LocalDateTime,
                CurrentOffset = currentOffset,
                MessagesCount = messagesCount,
                SegmentsCount = segmentsCount,
                Size = sizeBytes
            });
        }
        return partitions;
    }

    public override void Write(Utf8JsonWriter writer, TopicResponse value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();

        writer.WriteNumber(nameof(TopicResponse.Id).ToSnakeCase(), value.Id);
        writer.WriteString(nameof(TopicResponse.Name).ToSnakeCase(), value.Name);
        writer.WriteNumber(nameof(TopicResponse.Size).ToSnakeCase(), value.Size);
        writer.WriteNumber(nameof(TopicResponse.MessageExpiry).ToSnakeCase(), value.MessageExpiry);
        writer.WriteNumber(nameof(TopicResponse.MessagesCount).ToSnakeCase(), value.MessagesCount);
        writer.WriteNumber(nameof(TopicResponse.PartitionsCount).ToSnakeCase(), value.PartitionsCount);

        if (value.Partitions != null)
        {
            writer.WriteStartArray(nameof(TopicResponse.Partitions).ToSnakeCase());

            foreach (var partition in value.Partitions)
            {
                var partitionJson = JsonSerializer.Serialize(partition, options);
                writer.WriteRawValue(partitionJson);
            }

            writer.WriteEndArray();
        }
        writer.WriteEndObject();
    }
}