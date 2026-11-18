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

using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

/// <summary>
///     Page reads against a shared server. Other tests create entities at the same time, so each test filters by a name
///     of its own and never expects a page count: a page that holds the last match can still have a next.
/// </summary>
public class ListPagesTests
{
    private const int InvalidCommand = 3;
    private const uint SmallPage = 2;
    private const int StreamsCount = 3;
    private const uint PartitionsCount = 3;

    /// <summary>A cursor of an unknown kind. The client reads only its text, so the server is the one to refuse it.</summary>
    private const string UnknownKindCursor = "pg1_AwcAAAAAAAAA";

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task given_three_streams_when_walking_pages_of_two_should_read_them_in_each_order(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);
        var name = $"pages-{Guid.NewGuid():N}";
        var created = new List<StreamResponse>();
        try
        {
            for (var index = 0; index < StreamsCount; index++)
            {
                created.Add((await client.CreateStreamAsync($"{name}-{index}")).ShouldNotBeNull());
            }

            List<uint> byId = [.. created.Select(stream => stream.Id).Order()];
            List<uint> byCreated =
                [.. created.OrderBy(stream => stream.CreatedAt).ThenBy(stream => stream.Id).Select(stream => stream.Id)];

            var query = new NamedPageQuery { Name = name, Page = new PageQuery { Limit = SmallPage } };

            (await WalkStreams(client, query)).ShouldBe(byId);
            (await WalkStreams(client, query with { Page = query.Page with { Descending = true } }))
                .ShouldBe(byId.AsEnumerable().Reverse());
            (await WalkStreams(client, query with { Page = query.Page with { Order = PageOrder.Created } }))
                .ShouldBe(byCreated);
        }
        finally
        {
            foreach (var stream in created)
            {
                await client.DeleteStreamAsync(Identifier.Numeric(stream.Id));
            }
        }
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task given_topic_with_three_partitions_when_reading_partition_pages_should_return_them_by_id(
        Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);
        var stream = (await client.CreateStreamAsync($"pages-partitions-{Guid.NewGuid():N}")).ShouldNotBeNull();
        var streamId = Identifier.Numeric(stream.Id);
        try
        {
            var topic = (await client.CreateTopicAsync(streamId, "events", PartitionsCount)).ShouldNotBeNull();
            var topicId = Identifier.Numeric(topic.Id);

            var first = await client.GetPartitionsPageAsync(streamId, topicId, new PageQuery { Limit = SmallPage });
            var last = await client.GetPartitionsPageAsync(streamId, topicId,
                new PageQuery { Limit = SmallPage, Cursor = first.Next.ShouldNotBeNull() });
            var descending = await client.GetPartitionsPageAsync(streamId, topicId,
                new PageQuery { Descending = true });

            first.Items.Select(partition => partition.Id).ShouldBe([0u, 1u]);
            last.Items.Select(partition => partition.Id).ShouldBe([2u]);
            last.Next.ShouldBeNull();
            descending.Items.Select(partition => partition.Id).ShouldBe([2u, 1u, 0u]);
            descending.Next.ShouldBeNull();
        }
        finally
        {
            await client.DeleteStreamAsync(streamId);
        }
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task given_a_name_filter_when_reading_topics_groups_and_users_should_keep_only_matching_names(
        Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);
        var name = $"pages-{Guid.NewGuid():N}";
        var stream = (await client.CreateStreamAsync($"{name}-stream")).ShouldNotBeNull();
        var streamId = Identifier.Numeric(stream.Id);
        UserResponse? user = null;
        try
        {
            var topic = (await client.CreateTopicAsync(streamId, $"{name}-topic", 1)).ShouldNotBeNull();
            await client.CreateTopicAsync(streamId, "other-topic", 1);
            var topicId = Identifier.Numeric(topic.Id);
            await client.CreateConsumerGroupAsync(streamId, topicId, $"{name}-group");
            await client.CreateConsumerGroupAsync(streamId, topicId, "other-group");
            user = (await client.CreateUserAsync(name, "test_password_1", UserStatus.Active)).ShouldNotBeNull();

            var topics = await client.GetTopicsPageAsync(streamId, new NamedPageQuery { Name = name });
            var groups = await client.GetConsumerGroupsPageAsync(streamId, topicId, new NamedPageQuery { Name = name });
            var users = await client.GetUsersPageAsync(new NamedPageQuery { Name = name });

            topics.Items.Select(item => item.Name).ShouldBe([$"{name}-topic"]);
            groups.Items.Select(item => item.Name).ShouldBe([$"{name}-group"]);
            users.Items.Select(item => item.Username).ShouldBe([name]);
        }
        finally
        {
            if (user is not null)
            {
                await client.DeleteUserAsync(Identifier.Numeric(user.Id));
            }

            await client.DeleteStreamAsync(streamId);
        }
    }

    /// <summary>The HTTP server decodes the query as a form, so each reserved character must arrive escaped.</summary>
    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task given_a_name_with_reserved_characters_when_filtering_streams_should_match_it_exactly(
        Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);
        var name = $"pages-{Guid.NewGuid():N} a+b&c=d %e#f/?";
        var stream = (await client.CreateStreamAsync(name)).ShouldNotBeNull();
        try
        {
            var page = await client.GetStreamsPageAsync(new NamedPageQuery { Name = name });

            page.Items.Select(item => item.Id).ShouldBe([stream.Id]);
        }
        finally
        {
            await client.DeleteStreamAsync(Identifier.Numeric(stream.Id));
        }
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task given_a_cursor_the_server_cannot_use_when_reading_should_refuse_it_and_keep_working(
        Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);
        var stream = (await client.CreateStreamAsync($"pages-refused-{Guid.NewGuid():N}")).ShouldNotBeNull();
        try
        {
            var refusal = await Should.ThrowAsync<IggyInvalidStatusCodeException>(client.GetStreamsPageAsync(
                new NamedPageQuery { Page = new PageQuery { Cursor = PageCursor.Parse(UnknownKindCursor) } }));

            refusal.StatusCode.ShouldBe(InvalidCommand);
            refusal.FromServer.ShouldBeTrue();
            (await client.GetStreamsPageAsync(new NamedPageQuery { Name = stream.Name })).Items
                .Select(item => item.Id).ShouldBe([stream.Id]);
        }
        finally
        {
            await client.DeleteStreamAsync(Identifier.Numeric(stream.Id));
        }
    }

    /// <summary>Reads every page of a query and checks that the read took more than one page.</summary>
    private static async Task<List<uint>> WalkStreams(IIggyClient client, NamedPageQuery query)
    {
        var ids = new List<uint>();
        var pages = 0;
        Page<StreamResponse> page;
        do
        {
            page = await client.GetStreamsPageAsync(query);
            page.Items.Count.ShouldBeLessThanOrEqualTo((int)query.Page.Limit);
            ids.AddRange(page.Items.Select(stream => stream.Id));
            query = query with { Page = query.Page with { Cursor = page.Next } };
            pages++;
        } while (page.Next is not null);

        pages.ShouldBeGreaterThan(1);
        return ids;
    }
}
