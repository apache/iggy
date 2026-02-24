// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.

using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

public class ConsumerGroupTests
{
    private const uint PartitionsCount = 10;
    private const string TopicName = "cg-topic";
    private const string GroupName = "test_consumer_group";

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    private async Task<(IIggyClient client, string streamName)> CreateStreamAndTopic(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"cg-stream-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), TopicName, PartitionsCount);
        return (client, streamName);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreateConsumerGroup_HappyPath_Should_CreateConsumerGroup_Successfully(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamAndTopic(protocol);

        var consumerGroup = await client.CreateConsumerGroupAsync(
            Identifier.String(streamName), Identifier.String(TopicName), GroupName);

        consumerGroup.ShouldNotBeNull();
        consumerGroup.Id.ShouldBeGreaterThanOrEqualTo(0u);
        consumerGroup.PartitionsCount.ShouldBe(PartitionsCount);
        consumerGroup.MembersCount.ShouldBe(0u);
        consumerGroup.Name.ShouldBe(GroupName);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreateConsumerGroup_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamAndTopic(protocol);
        await client.CreateConsumerGroupAsync(Identifier.String(streamName),
            Identifier.String(TopicName), GroupName);

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            client.CreateConsumerGroupAsync(Identifier.String(streamName),
                Identifier.String(TopicName), GroupName));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetConsumerGroupById_Should_Return_ValidResponse(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamAndTopic(protocol);
        var cg = await client.CreateConsumerGroupAsync(Identifier.String(streamName),
            Identifier.String(TopicName), GroupName);

        var response = await client.GetConsumerGroupByIdAsync(
            Identifier.String(streamName), Identifier.String(TopicName),
            Identifier.Numeric(cg!.Id));

        response.ShouldNotBeNull();
        response.Id.ShouldBe(cg.Id);
        response.Name.ShouldBe(GroupName);
        response.PartitionsCount.ShouldBe(PartitionsCount);
        response.MembersCount.ShouldBe(0u);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetConsumerGroups_Should_Return_ValidResponse(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamAndTopic(protocol);
        await client.CreateConsumerGroupAsync(Identifier.String(streamName),
            Identifier.String(TopicName), GroupName);

        IReadOnlyList<ConsumerGroupResponse> response = await client.GetConsumerGroupsAsync(
            Identifier.String(streamName), Identifier.String(TopicName));

        response.ShouldNotBeNull();
        response.Count.ShouldBe(1);

        var group = response.FirstOrDefault();
        group.ShouldNotBeNull();
        group.Name.ShouldBe(GroupName);
        group.PartitionsCount.ShouldBe(PartitionsCount);
        group.MembersCount.ShouldBe(0u);
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task JoinConsumerGroup_Tcp_Should_JoinConsumerGroup_Successfully(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamAndTopic(protocol);
        var cg = await client.CreateConsumerGroupAsync(Identifier.String(streamName),
            Identifier.String(TopicName), GroupName);

        await Should.NotThrowAsync(() =>
            client.JoinConsumerGroupAsync(Identifier.String(streamName),
                Identifier.String(TopicName), Identifier.Numeric(cg!.Id)));

        // Verify via GetMe that the client is now a member of the consumer group
        var me = await client.GetMeAsync();
        me.ShouldNotBeNull();
        me.ConsumerGroupsCount.ShouldBe(1);
        me.ConsumerGroups.ShouldContain(x => x.GroupId == (int)cg!.Id);
    }

    [Test]
    [SkipTcp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task JoinConsumerGroup_Http_Should_Throw_FeatureUnavailable(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamAndTopic(protocol);
        var cg = await client.CreateConsumerGroupAsync(Identifier.String(streamName),
            Identifier.String(TopicName), GroupName);

        await Should.ThrowAsync<FeatureUnavailableException>(() =>
            client.JoinConsumerGroupAsync(Identifier.String(streamName),
                Identifier.String(TopicName), Identifier.Numeric(cg!.Id)));
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task LeaveConsumerGroup_Tcp_Should_LeaveConsumerGroup_Successfully(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamAndTopic(protocol);
        var cg = await client.CreateConsumerGroupAsync(Identifier.String(streamName),
            Identifier.String(TopicName), GroupName);
        await client.JoinConsumerGroupAsync(Identifier.String(streamName),
            Identifier.String(TopicName), Identifier.Numeric(cg!.Id));

        await Should.NotThrowAsync(() =>
            client.LeaveConsumerGroupAsync(Identifier.String(streamName),
                Identifier.String(TopicName), Identifier.Numeric(cg.Id)));

        // Verify via GetMe that the client is no longer a member of the consumer group
        var me = await client.GetMeAsync();
        me.ShouldNotBeNull();
        me.ConsumerGroupsCount.ShouldBe(0);
        me.ConsumerGroups.ShouldNotContain(x => x.GroupId == (int)cg.Id);
    }

    [Test]
    [SkipTcp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task LeaveConsumerGroup_Http_Should_Throw_FeatureUnavailable(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamAndTopic(protocol);
        var cg = await client.CreateConsumerGroupAsync(Identifier.String(streamName),
            Identifier.String(TopicName), GroupName);

        await Should.ThrowAsync<FeatureUnavailableException>(() =>
            client.LeaveConsumerGroupAsync(Identifier.String(streamName),
                Identifier.String(TopicName), Identifier.Numeric(cg!.Id)));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetConsumerGroupById_WithMembers_Should_Return_ValidResponse(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamAndTopic(protocol);
        var cg = await client.CreateConsumerGroupAsync(Identifier.String(streamName),
            Identifier.String(TopicName), GroupName);

        var clients = new List<IIggyClient>();
        for (var i = 0; i < 2; i++)
        {
            var memberClient = await Fixture.CreateClient(Protocol.Tcp);
            clients.Add(memberClient);
            await memberClient.LoginUser("iggy", "iggy");
            await memberClient.JoinConsumerGroupAsync(Identifier.String(streamName),
                Identifier.String(TopicName), Identifier.Numeric(cg!.Id));
        }

        var response = await client.GetConsumerGroupByIdAsync(
            Identifier.String(streamName), Identifier.String(TopicName),
            Identifier.Numeric(cg!.Id));

        response.ShouldNotBeNull();
        response.Id.ShouldBe(cg.Id);
        response.PartitionsCount.ShouldBe(PartitionsCount);
        response.MembersCount.ShouldBe(2u);
        response.Members.ShouldNotBeNull();
        response.Members.Count.ShouldBe(2);
        response.Members.ShouldAllBe(x => x.PartitionsCount >= 1 && x.PartitionsCount < (int)PartitionsCount);
        response.Members.ShouldAllBe(x => x.Partitions.Count >= 1 && x.Partitions.Count < (int)PartitionsCount);
        response.Members.Sum(x => x.PartitionsCount).ShouldBe((int)PartitionsCount);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeleteConsumerGroup_Should_DeleteConsumerGroup_Successfully(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamAndTopic(protocol);
        var cg = await client.CreateConsumerGroupAsync(Identifier.String(streamName),
            Identifier.String(TopicName), GroupName);

        await Should.NotThrowAsync(() =>
            client.DeleteConsumerGroupAsync(Identifier.String(streamName),
                Identifier.String(TopicName), Identifier.Numeric(cg!.Id)));
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task JoinConsumerGroup_Tcp_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamAndTopic(protocol);

        // Try to join a consumer group that doesn't exist
        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            client.JoinConsumerGroupAsync(Identifier.String(streamName),
                Identifier.String(TopicName), Identifier.Numeric(99999)));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeleteConsumerGroup_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamAndTopic(protocol);

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            client.DeleteConsumerGroupAsync(Identifier.String(streamName),
                Identifier.String(TopicName), Identifier.Numeric(99999)));
    }
}
