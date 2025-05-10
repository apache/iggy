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
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Iggy_SDK.JsonConfiguration;
public sealed class StreamResponseConverter : JsonConverter<StreamResponse>
{
    public override StreamResponse? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var doc = JsonDocument.ParseValue(ref reader);
        var root = doc.RootElement;

        var id = root.GetProperty(nameof(StreamResponse.Id).ToSnakeCase()).GetInt32();
        var createdAt = root.GetProperty(nameof(StreamResponse.CreatedAt).ToSnakeCase()).GetUInt64();
        var name = root.GetProperty(nameof(StreamResponse.Name).ToSnakeCase()).GetString();
        var sizeBytesString = root.GetProperty(nameof(StreamResponse.Size).ToSnakeCase()).GetString();
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
        var messagesCount = root.GetProperty(nameof(StreamResponse.MessagesCount).ToSnakeCase()).GetUInt64();
        var topicsCount = root.GetProperty(nameof(StreamResponse.TopicsCount).ToSnakeCase()).GetInt32();
        root.TryGetProperty(nameof(StreamResponse.Topics).ToSnakeCase(), out var topicsProperty);
        var topics = topicsProperty.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.Undefined => null,
            JsonValueKind.Array => DeserializeTopics(topicsProperty),
            _ => throw new InvalidEnumArgumentException("Error Wrong JsonValueKind when deserializing Topics")
        };

        return new StreamResponse
        {
            Id = id,
            Name = name!,
            Size = sizeBytes,
            CreatedAt = DateTimeOffsetUtils.FromUnixTimeMicroSeconds(createdAt).LocalDateTime,
            MessagesCount = messagesCount,
            TopicsCount = topicsCount,
            Topics = topics
        };
    }

    private IEnumerable<TopicResponse> DeserializeTopics(JsonElement jsonProp)
    {
        var topics = jsonProp.EnumerateArray();
        var result = new List<TopicResponse>();
        foreach (var topic in topics)
        {
            var topicString = topic.GetRawText();
            var topicDeserialized = JsonSerializer.Deserialize<TopicResponse>(topicString,
                JsonConverterFactory.TopicResponseOptions);
            result.Add(topicDeserialized!);
        }

        return result;
    }

    public override void Write(Utf8JsonWriter writer, StreamResponse value, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }
}