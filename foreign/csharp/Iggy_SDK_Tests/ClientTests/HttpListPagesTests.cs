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
using System.Text.Json;
using Apache.Iggy.Contracts;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Vsr;
using static Apache.Iggy.Tests.ClientTests.HttpTopicOptionsTests;
using static Apache.Iggy.Tests.ContractsTests.ListPagesFixtureTests;

namespace Apache.Iggy.Tests.ClientTests;

public sealed class HttpListPagesTests
{
    private const string LastPageJson = """{"items":[]}""";

    private const string StreamsPageJson = """
                                           {"items":[
                                             {"id":1,"created_at":1750000000000000,"name":"orders","size":"0 B",
                                              "messages_count":0,"topics_count":0},
                                             {"id":4,"created_at":1750000000000001,"name":"payments-eu","size":"0 B",
                                              "messages_count":0,"topics_count":0}],
                                            "next":"pg1_AQQAAAAAAAAA"}
                                           """;

    private static readonly Identifier StreamId = Identifier.Numeric(7);
    private static readonly Identifier TopicId = Identifier.Numeric(3);

    public static TheoryData<string> Requests =>
        [.. Fixture.GetProperty("requests").EnumerateArray().Select(vector => vector.GetProperty("case").GetString()!)];

    [Theory]
    [MemberData(nameof(Requests))]
    public async Task given_a_request_vector_when_reading_a_page_over_http_should_send_its_route_and_query(string name)
    {
        var vector = Case(Fixture, "requests", name);
        var input = vector.GetProperty("input");
        var command = vector.GetProperty("command").GetString()!;
        var parents = Parents(input);
        var handler = new StubHandler(LastPageJson);
        using var client = Client(handler);

        var next = await ReadPage(client, command, parents, input);

        Assert.Null(next);
        Assert.Equal(Route(command, parents), handler.RequestPath);
        Assert.Equal(
            vector.GetProperty("http_query").EnumerateObject().ToDictionary(key => key.Name,
                key => key.Value.GetString()!),
            FormDecode(handler.RequestQuery.TrimStart('?')));
    }

    /// <summary>The server decodes the query as a form, so a raw plus would arrive as a space.</summary>
    [Fact]
    public async Task given_a_name_with_reserved_characters_when_reading_a_page_over_http_should_send_it_escaped()
    {
        const string name = "a+b&c=d %e#f/?";
        var handler = new StubHandler(LastPageJson);
        using var client = Client(handler);

        await client.GetStreamsPageAsync(new NamedPageQuery { Name = name }, TestContext.Current.CancellationToken);

        Assert.Equal("/streams", handler.RequestPath);
        Assert.Equal(name, FormDecode(handler.RequestQuery.TrimStart('?'))["name"]);
    }

    [Fact]
    public async Task given_a_page_with_and_without_next_when_reading_over_http_should_keep_next_only_when_sent()
    {
        var token = TestContext.Current.CancellationToken;
        using var withNext = Client(new StubHandler(StreamsPageJson));
        using var withoutNext = Client(new StubHandler(LastPageJson));
        using var nullNext = Client(new StubHandler("""{"items":[],"next":null}"""));

        var page = await withNext.GetStreamsPageAsync(new NamedPageQuery(), token);

        Assert.Equal([(1u, "orders"), (4u, "payments-eu")], page.Items.Select(stream => (stream.Id, stream.Name)));
        Assert.Equal("pg1_AQQAAAAAAAAA", page.Next?.ToString());
        Assert.Null((await withoutNext.GetStreamsPageAsync(new NamedPageQuery(), token)).Next);
        Assert.Null((await nullNext.GetStreamsPageAsync(new NamedPageQuery(), token)).Next);
    }

    [Theory]
    [InlineData("""{"items":[],"next":"pg1_AQcAAAAAAAA*"}""")]
    [InlineData("""{"items":[],"next":7}""")]
    public async Task given_a_next_that_is_not_a_cursor_when_reading_over_http_should_raise_a_json_error(string json)
    {
        using var client = Client(new StubHandler(json));

        await Assert.ThrowsAsync<JsonException>(() =>
            client.GetStreamsPageAsync(new NamedPageQuery(), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task given_a_broken_query_when_reading_a_page_over_http_should_refuse_it_before_sending()
    {
        var token = TestContext.Current.CancellationToken;
        var broken = new PageQuery { Limit = 0 };
        var brokenNamed = new NamedPageQuery { Page = broken };
        var handler = new StubHandler(LastPageJson);
        using var client = Client(handler);

        Task[] refused =
        [
            client.GetStreamsPageAsync(brokenNamed, token),
            client.GetStreamsPageAsync(new NamedPageQuery { Name = string.Empty }, token),
            client.GetTopicsPageAsync(StreamId, brokenNamed, token),
            client.GetPartitionsPageAsync(StreamId, TopicId, broken, token),
            client.GetConsumerGroupsPageAsync(StreamId, TopicId, brokenNamed, token),
            client.GetUsersPageAsync(brokenNamed, token)
        ];
        foreach (var page in refused)
        {
            var refusal = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => page);
            Assert.Equal(VsrError.INVALID_COMMAND, refusal.StatusCode);
            Assert.False(refusal.FromServer);
        }

        Assert.Empty(handler.RequestPath);
    }

    /// <summary>A 404 for a topic that does not exist fails the read, as over TCP. It is not an empty page.</summary>
    [Fact]
    public async Task given_a_missing_topic_when_reading_a_page_over_http_should_raise_the_server_error()
    {
        var handler = new StubHandler(
            $$"""{"id":{{VsrError.TOPIC_ID_NOT_FOUND}},"code":"topic_id_not_found","reason":"Topic not found."}""")
        {
            StatusCode = HttpStatusCode.NotFound
        };
        using var client = Client(handler);

        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            client.GetPartitionsPageAsync(StreamId, TopicId, new PageQuery(), TestContext.Current.CancellationToken));

        Assert.Equal(VsrError.TOPIC_ID_NOT_FOUND, error.StatusCode);
        Assert.True(error.FromServer);
    }

    private static async Task<PageCursor?> ReadPage(HttpMessageStream client, string command, Identifier[] parents,
        JsonElement input)
    {
        var token = TestContext.Current.CancellationToken;
        return command switch
        {
            "get_streams" => (await client.GetStreamsPageAsync(NamedQuery(input), token)).Next,
            "get_topics" => (await client.GetTopicsPageAsync(parents[0], NamedQuery(input), token)).Next,
            "get_partitions" => (await client.GetPartitionsPageAsync(parents[0], parents[1], Query(input), token)).Next,
            "get_consumer_groups" =>
                (await client.GetConsumerGroupsPageAsync(parents[0], parents[1], NamedQuery(input), token)).Next,
            "get_users" => (await client.GetUsersPageAsync(NamedQuery(input), token)).Next,
            _ => throw new ArgumentOutOfRangeException(nameof(command), command, "The fixture names an unknown list.")
        };
    }

    private static string Route(string command, Identifier[] parents)
    {
        return command switch
        {
            "get_streams" => "/streams",
            "get_topics" => $"/streams/{parents[0]}/topics",
            "get_partitions" => $"/streams/{parents[0]}/topics/{parents[1]}/partitions",
            "get_consumer_groups" => $"/streams/{parents[0]}/topics/{parents[1]}/consumer-groups",
            "get_users" => "/users",
            _ => throw new ArgumentOutOfRangeException(nameof(command), command, "The fixture names an unknown list.")
        };
    }

    private static HttpMessageStream Client(StubHandler handler)
    {
        return new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
    }
}
