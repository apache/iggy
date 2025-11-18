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
using Apache.Iggy.Tests.Integrations.Helpers;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

public class ConsumerGroupTests
{
    private static readonly uint GroupId = 0;
    private static readonly string GroupName = "test_consumer_group";
    private Identifier TopicId => Identifier.String(Fixture.TopicId);

    [ClassDataSource<ConsumerGroupFixture>(Shared = SharedType.PerClass)]
    public required ConsumerGroupFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreateConsumerGroup_HappyPath_Should_CreateConsumerGroup_Successfully(Protocol protocol)
    {
        var consumerGroup
            = await Fixture.Clients[protocol]
                .CreateConsumerGroupAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), TopicId,
                    GroupName);

        consumerGroup.ShouldNotBeNull();
        consumerGroup.Id.ShouldBe(GroupId);
        consumerGroup.PartitionsCount.ShouldBe(Fixture.PartitionsCount);
        consumerGroup.MembersCount.ShouldBe(0u);
        consumerGroup.Name.ShouldBe(GroupName);
    }

    [Test]
    [DependsOn(nameof(CreateConsumerGroup_HappyPath_Should_CreateConsumerGroup_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreateConsumerGroup_Should_Throw_InvalidResponse(Protocol protocol)
    {
        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            Fixture.Clients[protocol]
                .CreateConsumerGroupAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), TopicId,
                    GroupName));
    }

    [Test]
    [DependsOn(nameof(CreateConsumerGroup_Should_Throw_InvalidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetConsumerGroupById_Should_Return_ValidResponse(Protocol protocol)
    {
        var response = await Fixture.Clients[protocol]
            .GetConsumerGroupByIdAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), TopicId,
                Identifier.Numeric(GroupId));

        response.ShouldNotBeNull();
        response.Id.ShouldBe(GroupId);
        response.Name.ShouldBe(GroupName);
        response.PartitionsCount.ShouldBe(Fixture.PartitionsCount);
        response.MembersCount.ShouldBe(0u);
    }

    [Test]
    [DependsOn(nameof(GetConsumerGroupById_Should_Return_ValidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetConsumerGroups_Should_Return_ValidResponse(Protocol protocol)
    {
        IReadOnlyList<ConsumerGroupResponse> response
            = await Fixture.Clients[protocol]
                .GetConsumerGroupsAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), TopicId);

        response.ShouldNotBeNull();
        response.Count.ShouldBe(1);

        var group = response.FirstOrDefault();
        group.ShouldNotBeNull();
        group.Id.ShouldBe(GroupId);
        group.Name.ShouldBe(GroupName);
        group.PartitionsCount.ShouldBe(Fixture.PartitionsCount);
        group.MembersCount.ShouldBe(0u);
    }

    [Test]
    [SkipHttp]
    [DependsOn(nameof(GetConsumerGroups_Should_Return_ValidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task JoinConsumerGroup_Tcp_Should_JoinConsumerGroup_Successfully(Protocol protocol)
    {
        await Should.NotThrowAsync(() =>
            Fixture.Clients[protocol]
                .JoinConsumerGroupAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), TopicId,
                    Identifier.Numeric(GroupId)));
    }

    [Test]
    [SkipTcp]
    [DependsOn(nameof(GetConsumerGroups_Should_Return_ValidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task JoinConsumerGroup_Http_Should_Throw_FeatureUnavailable(Protocol protocol)
    {
        await Should.ThrowAsync<FeatureUnavailableException>(() =>
            Fixture.Clients[protocol]
                .JoinConsumerGroupAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), TopicId,
                    Identifier.Numeric(GroupId)));
    }

    [Test]
    [SkipHttp]
    [DependsOn(nameof(JoinConsumerGroup_Tcp_Should_JoinConsumerGroup_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task LeaveConsumerGroup_Tcp_Should_LeaveConsumerGroup_Successfully(Protocol protocol)
    {
        await Should.NotThrowAsync(() =>
            Fixture.Clients[protocol]
                .LeaveConsumerGroupAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), TopicId,
                    Identifier.Numeric(GroupId)));
    }

    [Test]
    [SkipTcp]
    [DependsOn(nameof(JoinConsumerGroup_Http_Should_Throw_FeatureUnavailable))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task LeaveConsumerGroup_Http_Should_Throw_FeatureUnavailable(Protocol protocol)
    {
        await Should.ThrowAsync<FeatureUnavailableException>(() =>
            Fixture.Clients[protocol]
                .LeaveConsumerGroupAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), TopicId,
                    Identifier.Numeric(GroupId)));
    }

    [Test]
    [DependsOn(nameof(LeaveConsumerGroup_Http_Should_Throw_FeatureUnavailable))]
    [DependsOn(nameof(LeaveConsumerGroup_Tcp_Should_LeaveConsumerGroup_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetConsumerGroupById_WithMembers_Should_Return_ValidResponse(Protocol protocol)
    {
        var clientIds = new List<uint>();
        var clients = new List<IIggyClient>();
        for (var i = 0; i < 2; i++)
        {
            var client = await Fixture.IggyServerFixture.CreateClient(Protocol.Tcp, protocol);
            clients.Add(client);
            await client.LoginUser("iggy", "iggy");
            var me = await client.GetMeAsync();
            clientIds.Add(me!.ClientId);
            await client.JoinConsumerGroupAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), TopicId,
                Identifier.Numeric(GroupId));
        }

        var response = await Fixture.Clients[protocol]
            .GetConsumerGroupByIdAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), TopicId,
                Identifier.Numeric(GroupId));

        response.ShouldNotBeNull();
        response.Id.ShouldBe(GroupId);
        response.PartitionsCount.ShouldBe(Fixture.PartitionsCount);
        response.MembersCount.ShouldBe(2u);
        response.Members.ShouldNotBeNull();
        response.Members.Count.ShouldBe(2);
        response.Members.ShouldAllBe(x => x.PartitionsCount == 5);
        response.Members.ShouldAllBe(x => x.Partitions.Count == 5);
    }

    [Test]
    [DependsOn(nameof(GetConsumerGroupById_WithMembers_Should_Return_ValidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeleteConsumerGroup_Should_DeleteConsumerGroup_Successfully(Protocol protocol)
    {
        await Should.NotThrowAsync(() =>
            Fixture.Clients[protocol].DeleteConsumerGroupAsync(
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), TopicId, Identifier.Numeric(GroupId)));
    }

    [Test]
    [SkipHttp]
    [DependsOn(nameof(DeleteConsumerGroup_Should_DeleteConsumerGroup_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task JoinConsumerGroup_Tcp_Should_Throw_InvalidResponse(Protocol protocol)
    {
        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            Fixture.Clients[protocol]
                .JoinConsumerGroupAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), TopicId,
                    Identifier.Numeric(GroupId)));
    }


    [Test]
    [DependsOn(nameof(JoinConsumerGroup_Tcp_Should_Throw_InvalidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeleteConsumerGroup_Should_Throw_InvalidResponse(Protocol protocol)
    {
        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            Fixture.Clients[protocol].DeleteConsumerGroupAsync(
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), TopicId, Identifier.Numeric(GroupId)));
    }
}
