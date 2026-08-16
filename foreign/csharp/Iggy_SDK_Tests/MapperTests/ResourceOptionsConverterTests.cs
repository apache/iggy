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
                              "message_expiry": 0,
                              "compression_algorithm": "none",
                              "max_topic_size": 0,
                              "messages_count": 0,
                              "partitions_count": 1,
                              "partitions": [],
                              "options": {
                                "enforce_fsync": { "value": "true", "explicit": true },
                                "segment_size": { "value": "134217728", "explicit": true },
                                "messages_required_to_save": { "value": "1024", "explicit": false },
                                "preallocate_segments": { "value": "false", "explicit": false }
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
        Assert.Equal("true", topic.Options[HeaderKey.FromString("enforce_fsync")].ToString());
        Assert.Equal("134217728", topic.Options[HeaderKey.FromString("segment_size")].ToString());

        Assert.Equal(2, topic.DerivedOptions.Count);
        Assert.Equal("1024", topic.DerivedOptions[HeaderKey.FromString("messages_required_to_save")].ToString());
        Assert.Equal("false", topic.DerivedOptions[HeaderKey.FromString("preallocate_segments")].ToString());
    }

    [Fact]
    public void DeserializeTopic_ReadsEveryValueAsAString()
    {
        // A byte count arrives rendered, so its canonical Uint64 kind is not recoverable over REST.
        const string json = """
                            {
                              "id": 1,
                              "created_at": 1750000000000000,
                              "name": "topic",
                              "size": "0 B",
                              "max_topic_size": 0,
                              "messages_count": 0,
                              "partitions_count": 1,
                              "options": { "segment_size": { "value": "134217728", "explicit": true } }
                            }
                            """;

        var topic = JsonSerializer.Deserialize<TopicResponse>(json, HttpOptions);

        var value = Assert.Single(topic!.Options!).Value;
        Assert.Equal(HeaderKind.String, value.Kind);
        Assert.Throws<InvalidOperationException>(() => value.ToUInt64());
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
        const string json = """{ "segment_size": { "value": "134217728" } }""";

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
                                "max_topic_size": 0,
                                "messages_count": 0,
                                "partitions_count": 1,
                                "options": { "enforce_fsync": { "value": "true", "explicit": true } }
                              },
                              {
                                "id": 2,
                                "created_at": 1750000000000000,
                                "name": "second",
                                "size": "0 B",
                                "max_topic_size": 0,
                                "messages_count": 0,
                                "partitions_count": 1,
                                "options": { "enforce_fsync": { "value": "false", "explicit": false } }
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
