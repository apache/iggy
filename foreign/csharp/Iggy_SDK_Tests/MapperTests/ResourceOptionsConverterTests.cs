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

using System.Text.Json;
using System.Text.Json.Serialization;
using Apache.Iggy.Contracts;
using Apache.Iggy.Headers;
using Apache.Iggy.JsonConverters;

namespace Apache.Iggy.Tests.MapperTests;

public sealed class ResourceOptionsConverterTests
{
    private static readonly JsonSerializerOptions HttpOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.SnakeCaseLower) }
    };

    private static readonly JsonSerializerOptions ConverterOptions = new()
    {
        Converters = { new ResourceOptionsConverter() }
    };

    [Fact]
    public void DeserializeTopic_SplitsOptionsByProvenance()
    {
        // Arrange: the object a REST topic response renders both provenances into.
        const string json = """
                            {
                              "id": 1,
                              "created_at": 1750000000000000,
                              "name": "topic",
                              "size": "0 B",
                              "messages_count": 0,
                              "partitions_count": 1,
                              "partitions": [],
                              "options": {
                                "preallocate_segments": { "value": "true", "kind": "bool", "explicit": true },
                                "segment_size": { "value": "134217728", "kind": "uint64", "explicit": true },
                                "messages_required_to_save": { "value": "1024", "kind": "uint32", "explicit": false },
                                "durability": { "value": "replicated", "kind": "string", "explicit": false }
                              }
                            }
                            """;

        // Act
        var topic = JsonSerializer.Deserialize<TopicResponse>(json, HttpOptions);

        // Assert
        Assert.NotNull(topic);
        Assert.NotNull(topic.Options);
        Assert.NotNull(topic.DerivedOptions);

        Assert.Equal(2, topic.Options.Count);
        Assert.True(topic.Options[HeaderKey.FromString("preallocate_segments")].ToBool());
        Assert.Equal(134217728UL, topic.Options[HeaderKey.FromString("segment_size")].ToUInt64());

        Assert.Equal(2, topic.DerivedOptions.Count);
        Assert.Equal(1024U, topic.DerivedOptions[HeaderKey.FromString("messages_required_to_save")].ToUInt32());
        Assert.Equal("replicated", topic.DerivedOptions[HeaderKey.FromString("durability")].ToString());
    }

    [Fact]
    public void given_uint64_http_option_when_reading_should_keep_kind()
    {
        const string json = """
                            {
                              "id": 1,
                              "created_at": 1750000000000000,
                              "name": "topic",
                              "size": "0 B",
                              "messages_count": 0,
                              "partitions_count": 1,
                              "options": { "segment_size": { "value": "134217728", "kind": "uint64", "explicit": true } }
                            }
                            """;

        var topic = JsonSerializer.Deserialize<TopicResponse>(json, HttpOptions);

        var value = Assert.Single(topic!.Options!).Value;
        Assert.Equal(HeaderKind.Uint64, value.Kind);
        Assert.Equal(134217728UL, value.ToUInt64());
    }

    [Fact]
    public void DeserializeTopic_WithoutOptionsLeavesBothDictionariesNull()
    {
        const string json = """
                            {
                              "id": 1,
                              "created_at": 1750000000000000,
                              "name": "topic",
                              "size": "0 B",
                              "max_topic_size": 0,
                              "messages_count": 0,
                              "partitions_count": 1
                            }
                            """;

        var topic = JsonSerializer.Deserialize<TopicResponse>(json, HttpOptions);

        Assert.Null(topic!.Options);
        Assert.Null(topic.DerivedOptions);
    }

    [Fact]
    public void Read_EmptyObjectYieldsTwoEmptyDictionaries()
    {
        var options = JsonSerializer.Deserialize<ResourceOptions>("{}", ConverterOptions);

        Assert.Empty(options.Explicit);
        Assert.Empty(options.Derived);
    }

    [Fact]
    public void Read_RejectsAnEntryWithoutProvenance()
    {
        const string json = """{ "segment_size": { "value": "134217728", "kind": "uint64" } }""";

        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<ResourceOptions>(json, ConverterOptions));
    }

    [Fact]
    public void Read_RejectsAValueThatIsNotAnEntryObject()
    {
        const string json = """{ "segment_size": "134217728" }""";

        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<ResourceOptions>(json, ConverterOptions));
    }

    [Fact]
    public void DeserializeTopics_SplitsOptionsOfEveryTopicInAList()
    {
        const string json = """
                            [
                              {
                                "id": 1,
                                "created_at": 1750000000000000,
                                "name": "first",
                                "size": "0 B",
                                "messages_count": 0,
                                "partitions_count": 1,
                                "options": { "preallocate_segments": { "value": "true", "kind": "bool", "explicit": true } }
                              },
                              {
                                "id": 2,
                                "created_at": 1750000000000000,
                                "name": "second",
                                "size": "0 B",
                                "messages_count": 0,
                                "partitions_count": 1,
                                "options": { "preallocate_segments": { "value": "false", "kind": "bool", "explicit": false } }
                              }
                            ]
                            """;

        var topics = JsonSerializer.Deserialize<IReadOnlyList<TopicResponse>>(json, HttpOptions);

        Assert.NotNull(topics);
        Assert.Single(topics[0].Options!);
        Assert.Empty(topics[0].DerivedOptions!);
        Assert.Empty(topics[1].Options!);
        Assert.Single(topics[1].DerivedOptions!);
    }
}
