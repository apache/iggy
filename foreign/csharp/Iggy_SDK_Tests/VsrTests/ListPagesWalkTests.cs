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

using System.Collections.Concurrent;
using System.Text.Json;
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging.Abstractions;
using static Apache.Iggy.Tests.ContractsTests.ListPagesFixtureTests;
using static Apache.Iggy.Tests.VsrTests.MockFrames;

namespace Apache.Iggy.Tests.VsrTests;

/// <summary>The page walk of the whole-list methods and the refusal of a broken query, against one mock node.</summary>
public sealed class ListPagesWalkTests
{
    /// <summary>The first page and the second page of the walk both answer the same next cursor.</summary>
    private const int PagesWithTheSameNext = 2;

    private static readonly Identifier Stream = Identifier.Numeric(7);
    private static readonly Identifier Topic = Identifier.Numeric(3);
    private static readonly NamedPageQuery WholeList = new() { Page = new PageQuery { Limit = PageQuery.MAX_LIMIT } };

    /// <summary>
    ///     The first page of each list, from the shared vectors. Each has two items and a next cursor. The second page
    ///     holds the same two items without the cursor, so a walk that drops a page fails.
    /// </summary>
    private static readonly Dictionary<int, string> FirstPages = new()
    {
        [CommandCodes.GET_STREAMS_CODE] = "streams_with_next",
        [CommandCodes.GET_TOPICS_CODE] = "topics_created_descending_with_next",
        [CommandCodes.GET_CONSUMER_GROUPS_CODE] = "consumer_groups_with_next",
        [CommandCodes.GET_USERS_CODE] = "users_with_next"
    };

    [Fact]
    public async Task given_two_pages_when_reading_whole_lists_should_send_the_next_cursor_and_join_the_pages()
    {
        var token = TestContext.Current.CancellationToken;
        Assert.Equal(Vector("requests", "streams_whole_list_walk").GetProperty("request_hex").GetString(),
            Hex(TcpContracts.GetPage(WholeList)));
        var lists = new ConcurrentQueue<MockRequest>();
        using var node = new MockNode();
        node.Serve(request =>
        {
            if (!FirstPages.TryGetValue(request.Code, out var firstPage))
            {
                return Other(node, request);
            }

            var first = lists.All(seen => seen.Code != request.Code);
            lists.Enqueue(request);
            return Reply(OPERATION_NON_REPLICATED, first ? Response(firstPage) : LastPage(firstPage));
        });
        using var client = await ConnectAsync(node);

        Assert.Equal([1u, 4u, 1u, 4u], (await client.GetStreamsAsync(token)).Select(stream => stream.Id));
        Assert.Equal([3u, 1u, 3u, 1u], (await client.GetTopicsAsync(Stream, token)).Select(topic => topic.Id));
        Assert.Equal([1u, 3u, 1u, 3u],
            (await client.GetConsumerGroupsAsync(Stream, Topic, token)).Select(group => group.Id));
        Assert.Equal([1u, 2u, 1u, 2u], (await client.GetUsersAsync(token)).Select(user => user.Id));

        Assert.All(lists, request => Assert.Equal(OPERATION_NON_REPLICATED, request.Operation));
        Assert.Equal(
        [
            Hex(TcpContracts.GetPage(WholeList)),
            SecondPage(CommandCodes.GET_STREAMS_CODE),
            Hex(TcpContracts.GetPage(WholeList, Stream)),
            SecondPage(CommandCodes.GET_TOPICS_CODE, Stream),
            Hex(TcpContracts.GetPage(WholeList, Stream, Topic)),
            SecondPage(CommandCodes.GET_CONSUMER_GROUPS_CODE, Stream, Topic),
            Hex(TcpContracts.GetPage(WholeList)),
            SecondPage(CommandCodes.GET_USERS_CODE)
        ], lists.Select(request => Hex(request.Body)));
    }

    /// <summary>
    ///     A walk compares the next cursor of a page with the cursor that it sent. A one-page list sends no cursor and
    ///     gets no next, so it still reads.
    /// </summary>
    [Fact]
    public async Task given_a_page_that_repeats_the_sent_cursor_when_reading_a_whole_list_should_fail_as_malformed()
    {
        var token = TestContext.Current.CancellationToken;
        using var node = new MockNode();
        node.Serve(request => request.Code switch
        {
            // The page after the repeat is a last page, so a walk that misses the repeat ends and fails the test.
            CommandCodes.GET_STREAMS_CODE => Reply(OPERATION_NON_REPLICATED,
                node.Requests(CommandCodes.GET_STREAMS_CODE) <= PagesWithTheSameNext
                    ? Response("streams_with_next")
                    : LastPage("streams_with_next")),
            CommandCodes.GET_USERS_CODE => Reply(OPERATION_NON_REPLICATED, LastPage("users_with_next")),
            _ => Other(node, request)
        });
        using var client = await ConnectAsync(node);

        Assert.Equal([1u, 2u], (await client.GetUsersAsync(token)).Select(user => user.Id));
        await Assert.ThrowsAsync<MalformedResponseException>(() => client.GetStreamsAsync(token));
        Assert.Equal(PagesWithTheSameNext, node.Requests(CommandCodes.GET_STREAMS_CODE));
    }

    [Fact]
    public async Task given_a_caller_sign_in_during_a_walk_when_reading_the_next_page_should_fail_as_unauthenticated()
    {
        var token = TestContext.Current.CancellationToken;
        using var node = new MockNode();
        node.Serve(request => request.Code == CommandCodes.GET_STREAMS_CODE
            ? Reply(OPERATION_NON_REPLICATED, node.Requests(CommandCodes.GET_STREAMS_CODE) == 1
                ? Response("streams_with_next")
                : LastPage("streams_with_next"))
            : Other(node, request));
        using var client = await ConnectAsync(node);
        var continuations = new NativeRecoveryTests.HeldContinuations();

        var walk = continuations.Run(() => client.GetStreamsAsync(token));
        await continuations.Posted.WaitAsync(token);
        Assert.Equal(1, node.Requests(CommandCodes.GET_STREAMS_CODE));
        await client.LoginUserAsync("other", "secret", token);

        var failure = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            continuations.FinishAsync(walk, token).WaitAsync(TimeSpan.FromSeconds(10), token));
        Assert.Equal(VsrError.UNAUTHENTICATED, failure.StatusCode);
        Assert.False(failure.FromServer);
        Assert.Equal(1, node.Requests(CommandCodes.GET_STREAMS_CODE));
    }

    [Fact]
    public async Task given_a_broken_query_when_reading_a_page_should_refuse_it_before_sending()
    {
        var token = TestContext.Current.CancellationToken;
        var broken = new PageQuery { Limit = 0 };
        var brokenNamed = new NamedPageQuery { Page = broken };
        var partitionsRequest = Vector("requests", "partitions_numeric_ids_first_page");
        var lists = new ConcurrentQueue<MockRequest>();
        using var node = new MockNode();
        node.Serve(request =>
        {
            if (request.Code != CommandCodes.GET_PARTITIONS_CODE && !FirstPages.ContainsKey(request.Code))
            {
                return Other(node, request);
            }

            lists.Enqueue(request);
            return Reply(OPERATION_NON_REPLICATED, Response("partitions_with_next"));
        });
        using var client = await ConnectAsync(node);

        Task[] refused =
        [
            client.GetStreamsPageAsync(brokenNamed, token),
            client.GetStreamsPageAsync(new NamedPageQuery { Name = string.Empty }, token),
            client.GetTopicsPageAsync(Stream, brokenNamed, token),
            client.GetPartitionsPageAsync(Stream, Topic, broken, token),
            client.GetConsumerGroupsPageAsync(Stream, Topic, brokenNamed, token),
            client.GetUsersPageAsync(brokenNamed, token)
        ];
        foreach (var page in refused)
        {
            var refusal = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => page);
            Assert.Equal(VsrError.INVALID_COMMAND, refusal.StatusCode);
            Assert.False(refusal.FromServer);
        }

        var partitions = await client.GetPartitionsPageAsync(Stream, Topic, Query(partitionsRequest.GetProperty("input")),
            token);

        Assert.Equal([0u, 1u], partitions.Items.Select(partition => partition.Id));
        Assert.Equal("pg1_AQEAAAAAAAAA", partitions.Next?.ToString());
        var sent = Assert.Single(lists);
        Assert.Equal(CommandCodes.GET_PARTITIONS_CODE, sent.Code);
        Assert.Equal(OPERATION_NON_REPLICATED, sent.Operation);
        Assert.Equal(partitionsRequest.GetProperty("request_hex").GetString(), Hex(sent.Body));
    }

    /// <summary>A server of this version always sends the next cursor length and the count, even for no items.</summary>
    [Fact]
    public async Task given_an_empty_reply_when_reading_a_page_should_refuse_it_as_malformed()
    {
        using var node = new MockNode();
        var emptyBody = Case(Fixture.GetProperty("refused"), "responses", "empty_body");
        node.Serve(request => request.Code == CommandCodes.GET_STREAMS_CODE
            ? Reply(OPERATION_NON_REPLICATED,
                Convert.FromHexString(emptyBody.GetProperty("response_hex").GetString()!))
            : Other(node, request));
        using var client = await ConnectAsync(node);

        await Assert.ThrowsAsync<MalformedResponseException>(() =>
            client.GetStreamsAsync(TestContext.Current.CancellationToken));
    }

    private static byte[] Other(MockNode node, MockRequest request)
    {
        return request.Code == GET_CLUSTER_METADATA_CODE ? StandaloneRoster(node.Port) : Answer(request);
    }

    private static JsonElement Vector(string group, string name)
    {
        return Case(Fixture, group, name);
    }

    private static byte[] Response(string name)
    {
        return Convert.FromHexString(Vector("responses", name).GetProperty("response_hex").GetString()!);
    }

    /// <summary>The reply of a vector with its next cursor cut out: the same items on the last page.</summary>
    private static byte[] LastPage(string name)
    {
        var page = Response(name);
        return [0, .. page.AsSpan(1 + page[0])];
    }

    /// <summary>The second request of a walk carries the next cursor of the first page of the list.</summary>
    private static string SecondPage(int code, params Identifier[] parents)
    {
        var next = PageCursor.Parse(Vector("responses", FirstPages[code]).GetProperty("next").GetString()!);
        return Hex(TcpContracts.GetPage(WholeList with { Page = WholeList.Page with { Cursor = next } }, parents));
    }

    private static string Hex(byte[] bytes)
    {
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }

    private static async Task<TcpMessageStream> ConnectAsync(MockNode node)
    {
        var client = new TcpMessageStream(new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{node.Port}",
            Protocol = Protocol.Tcp
        }, NullLoggerFactory.Instance);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.LoginUserAsync("user", "secret", TestContext.Current.CancellationToken);

        return client;
    }
}
