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

using System.Net;
using System.Text;
using Apache.Iggy.Contracts;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient.Implementations;

namespace Apache.Iggy.Tests.ClientTests;

public sealed class HttpTopicOptionsTests
{
    private const string TopicResponseJson = """
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
                                                 "segment_size": { "value": "134217728", "explicit": false }
                                               }
                                             }
                                             """;

    /// <summary>
    ///     Recorded verbatim from GET /options/topic. The server renders its catalog straight off its
    ///     own types, so default_value arrives as an array of byte values and never as Base64.
    /// </summary>
    private const string OptionsCatalogJson = """
                                              [
                                                {
                                                  "key": "compression_algorithm",
                                                  "kind": "string",
                                                  "default_value": [110, 111, 110, 101],
                                                  "description": "Compression algorithm (none, gzip)"
                                                },
                                                {
                                                  "key": "segment_size",
                                                  "kind": "uint64",
                                                  "default_value": [0, 0, 0, 64, 0, 0, 0, 0],
                                                  "description": "Segment size in bytes"
                                                },
                                                {
                                                  "key": "enforce_fsync",
                                                  "kind": "bool",
                                                  "default_value": [0],
                                                  "description": "Whether writes to this topic's partitions fsync"
                                                }
                                              ]
                                              """;

    private static readonly Identifier StreamId = Identifier.Numeric(1);

    [Fact]
    public async Task CreateTopic_SplitsTheResponseOptionsByProvenance()
    {
        var handler = new StubHandler(TopicResponseJson);
        var client = new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });

        var topic = await client.CreateTopicAsync(StreamId, "topic", 1,
            token: TestContext.Current.CancellationToken);

        Assert.NotNull(topic);
        var explicitOption = Assert.Single(topic.Options!);
        Assert.Equal("enforce_fsync", explicitOption.Key.AsString());
        Assert.Equal("true", explicitOption.Value.ToString());

        var derivedOption = Assert.Single(topic.DerivedOptions!);
        Assert.Equal("segment_size", derivedOption.Key.AsString());
        Assert.Equal("134217728", derivedOption.Value.ToString());
    }

    [Fact]
    public async Task CreateTopic_SendsABoolOptionAsTheWordTheServerParses()
    {
        var handler = new StubHandler(TopicResponseJson);
        var client = new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });

        await client.CreateTopicAsync(StreamId, "topic", 1,
            options: new TopicOptions { EnforceFsync = true, SegmentSize = 134217728 }.ToDictionary(),
            token: TestContext.Current.CancellationToken);

        Assert.Contains("\"enforce_fsync\":\"true\"", handler.RequestBody);
        Assert.Contains("\"segment_size\":\"134217728\"", handler.RequestBody);
    }

    [Fact]
    public async Task GetTopicById_SplitsTheResponseOptionsByProvenance()
    {
        var handler = new StubHandler(TopicResponseJson);
        var client = new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });

        var topic = await client.GetTopicByIdAsync(StreamId, Identifier.Numeric(1),
            TestContext.Current.CancellationToken);

        Assert.Equal(HeaderKind.String, Assert.Single(topic!.Options!).Value.Kind);
        Assert.Equal(HeaderKind.String, Assert.Single(topic.DerivedOptions!).Value.Kind);
    }

    [Fact]
    public async Task DescribeOptions_ReadsTheCatalogTheServerRenders()
    {
        var handler = new StubHandler(OptionsCatalogJson);
        var client = new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });

        var specs = await client.DescribeOptionsAsync(OptionsScope.Topic, TestContext.Current.CancellationToken);

        Assert.Equal(3, specs.Count);

        Assert.Equal("compression_algorithm", specs[0].Key);
        Assert.Equal(HeaderKind.String, specs[0].Kind);
        Assert.Equal("none"u8.ToArray(), specs[0].DefaultValue);
        Assert.Equal("Compression algorithm (none, gzip)", specs[0].Description);

        Assert.Equal("segment_size", specs[1].Key);
        Assert.Equal(HeaderKind.Uint64, specs[1].Kind);
        Assert.Equal(1073741824UL, BitConverter.ToUInt64(specs[1].DefaultValue));

        Assert.Equal("enforce_fsync", specs[2].Key);
        Assert.Equal(HeaderKind.Bool, specs[2].Kind);
        Assert.Equal([0], specs[2].DefaultValue);
    }

    [Fact]
    public async Task DescribeOptions_ReadsAnEmptyCatalog()
    {
        var handler = new StubHandler("[]");
        var client = new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });

        var specs = await client.DescribeOptionsAsync(OptionsScope.Stream, TestContext.Current.CancellationToken);

        Assert.Empty(specs);
        Assert.Equal("/options/stream", handler.RequestPath);
    }

    private sealed class StubHandler(string json) : HttpMessageHandler
    {
        internal string RequestBody { get; private set; } = string.Empty;

        internal string RequestPath { get; private set; } = string.Empty;

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
            CancellationToken ct)
        {
            RequestPath = request.RequestUri?.AbsolutePath ?? string.Empty;
            if (request.Content is not null)
            {
                RequestBody = await request.Content.ReadAsStringAsync(ct);
            }

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(json, Encoding.UTF8, "application/json")
            };
        }
    }
}
