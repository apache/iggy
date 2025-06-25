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

using Apache.Iggy.Contracts.Http;
using Apache.Iggy.Contracts.Http.Auth;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

[MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
public class ConsumerGroupTests(Protocol protocol)
{
    private static readonly int GroupId = 1;

    private readonly CreateConsumerGroupRequest _createConsumerGroupRequest = new()
    {
        StreamId = Identifier.Numeric(1),
        TopicId = Identifier.Numeric(1),
        ConsumerGroupId = GroupId,
        Name = "test_consumer_group"
    };

    [ClassDataSource<ConsumerGroupFixture>(Shared = SharedType.PerClass)]
    public required ConsumerGroupFixture Fixture { get; init; }

    [Test]
    public async Task CreateConsumerGroup_HappyPath_Should_CreateConsumerGroup_Successfully()
    {
        var consumerGroup = await Fixture.Clients[protocol].CreateConsumerGroupAsync(_createConsumerGroupRequest);

        consumerGroup.ShouldNotBeNull();
        consumerGroup.Id.ShouldBe(_createConsumerGroupRequest.ConsumerGroupId);
        consumerGroup.PartitionsCount.ShouldBe(Fixture.TopicRequest.PartitionsCount);
        consumerGroup.MembersCount.ShouldBe(0);
        consumerGroup.Name.ShouldBe(_createConsumerGroupRequest.Name);
    }

    [Test]
    [DependsOn(nameof(CreateConsumerGroup_HappyPath_Should_CreateConsumerGroup_Successfully))]
    public async Task CreateConsumerGroup_Should_Throw_InvalidResponse()
    {
        await Should.ThrowAsync<InvalidResponseException>(() => Fixture.Clients[protocol].CreateConsumerGroupAsync(_createConsumerGroupRequest));
    }

    [Test]
    [DependsOn(nameof(CreateConsumerGroup_Should_Throw_InvalidResponse))]
    public async Task GetConsumerGroupById_Should_Return_ValidResponse()
    {
        var response = await Fixture.Clients[protocol].GetConsumerGroupByIdAsync(_createConsumerGroupRequest.StreamId,
            _createConsumerGroupRequest.TopicId, Identifier.Numeric(1));

        response.ShouldNotBeNull();
        response.Id.ShouldBe(GroupId);
        response.Name.ShouldBe(_createConsumerGroupRequest.Name);
        response.PartitionsCount.ShouldBe(Fixture.TopicRequest.PartitionsCount);
        response.MembersCount.ShouldBe(0);
    }

    [Test]
    [DependsOn(nameof(GetConsumerGroupById_Should_Return_ValidResponse))]
    public async Task GetConsumerGroups_Should_Return_ValidResponse()
    {
        IReadOnlyList<ConsumerGroupResponse> response = await Fixture.Clients[protocol].GetConsumerGroupsAsync(
            _createConsumerGroupRequest.StreamId, _createConsumerGroupRequest.TopicId);

        response.ShouldNotBeNull();
        response.Count.ShouldBe(1);

        var group = response.FirstOrDefault();
        group.ShouldNotBeNull();
        group.Id.ShouldBe(GroupId);
        group.Name.ShouldBe(_createConsumerGroupRequest.Name);
        group.PartitionsCount.ShouldBe(Fixture.TopicRequest.PartitionsCount);
        group.MembersCount.ShouldBe(0);
    }

    [Test]
    [SkipHttp]
    [DependsOn(nameof(GetConsumerGroups_Should_Return_ValidResponse))]
    public async Task JoinConsumerGroup_Tcp_Should_JoinConsumerGroup_Successfully()
    {
        await Should.NotThrowAsync(() => Fixture.Clients[protocol].JoinConsumerGroupAsync(new JoinConsumerGroupRequest
        {
            TopicId = _createConsumerGroupRequest.TopicId,
            StreamId = _createConsumerGroupRequest.StreamId,
            ConsumerGroupId = Identifier.Numeric(GroupId)
        }));
    }

    [Test]
    [SkipTcp]
    [DependsOn(nameof(GetConsumerGroups_Should_Return_ValidResponse))]
    public async Task JoinConsumerGroup_Http_Should_Throw_FeatureUnavailable()
    {
        await Should.ThrowAsync<FeatureUnavailableException>(() => Fixture.Clients[protocol].JoinConsumerGroupAsync(new JoinConsumerGroupRequest
        {
            TopicId = _createConsumerGroupRequest.TopicId,
            StreamId = _createConsumerGroupRequest.StreamId,
            ConsumerGroupId = Identifier.Numeric(GroupId)
        }));
    }

    [Test]
    [SkipHttp]
    [DependsOn(nameof(JoinConsumerGroup_Tcp_Should_JoinConsumerGroup_Successfully))]
    public async Task LeaveConsumerGroup_Tcp_Should_LeaveConsumerGroup_Successfully()
    {
        await Should.NotThrowAsync(() => Fixture.Clients[protocol].LeaveConsumerGroupAsync(new LeaveConsumerGroupRequest
        {
            TopicId = _createConsumerGroupRequest.TopicId,
            StreamId = _createConsumerGroupRequest.StreamId,
            ConsumerGroupId = Identifier.Numeric(GroupId)
        }));
    }

    [Test]
    [SkipTcp]
    [DependsOn(nameof(JoinConsumerGroup_Http_Should_Throw_FeatureUnavailable))]
    public async Task LeaveConsumerGroup_Http_Should_Throw_FeatureUnavailable()
    {
        await Should.ThrowAsync<FeatureUnavailableException>(() => Fixture.Clients[protocol].LeaveConsumerGroupAsync(new LeaveConsumerGroupRequest
        {
            TopicId = _createConsumerGroupRequest.TopicId,
            StreamId = _createConsumerGroupRequest.StreamId,
            ConsumerGroupId = Identifier.Numeric(GroupId)
        }));
    }

    [Test]
    [DependsOn(nameof(LeaveConsumerGroup_Http_Should_Throw_FeatureUnavailable))]
    [DependsOn(nameof(LeaveConsumerGroup_Tcp_Should_LeaveConsumerGroup_Successfully))]
    public async Task GetConsumerGroupById_WithMembers_Should_Return_ValidResponse()
    {
        var clientIds = new List<uint>();
        var clients = new List<IIggyClient>();
        for (var i = 0; i < 2; i++)
        {
            var client = Fixture.CreateClient(Protocol.Tcp, protocol);
            clients.Add(client);
            await client.LoginUser(new LoginUserRequest
            {
                Username = "iggy",
                Password = "iggy"
            });
            var me = await client.GetMeAsync();
            clientIds.Add(me!.ClientId);
            await client.JoinConsumerGroupAsync(new JoinConsumerGroupRequest
            {
                ConsumerGroupId = Identifier.Numeric(_createConsumerGroupRequest.ConsumerGroupId),
                StreamId = _createConsumerGroupRequest.StreamId,
                TopicId = _createConsumerGroupRequest.TopicId
            });
        }

        var response = await Fixture.Clients[protocol].GetConsumerGroupByIdAsync(_createConsumerGroupRequest.StreamId,
            _createConsumerGroupRequest.TopicId, Identifier.Numeric(1));

        response.ShouldNotBeNull();
        response.Id.ShouldBe(GroupId);
        response.PartitionsCount.ShouldBe(Fixture.TopicRequest.PartitionsCount);
        response.MembersCount.ShouldBe(2);
        response.Members.ShouldNotBeNull();
        response.Members.Count.ShouldBe(2);
        response.Members.ShouldAllBe(m => clientIds.Contains(m.Id));
        response.Members.ShouldAllBe(x => x.PartitionsCount == 5);
        response.Members.ShouldAllBe(x => x.Partitions.Count == 5);
    }

    [Test]
    [DependsOn(nameof(GetConsumerGroupById_WithMembers_Should_Return_ValidResponse))]
    public async Task DeleteConsumerGroup_Should_DeleteConsumerGroup_Successfully()
    {
        await Should.NotThrowAsync(() => Fixture.Clients[protocol].DeleteConsumerGroupAsync(new DeleteConsumerGroupRequest
        {
            TopicId = _createConsumerGroupRequest.TopicId,
            StreamId = _createConsumerGroupRequest.StreamId,
            ConsumerGroupId = Identifier.Numeric(GroupId)
        }));
    }

    [Test]
    [SkipHttp]
    [DependsOn(nameof(DeleteConsumerGroup_Should_DeleteConsumerGroup_Successfully))]
    public async Task JoinConsumerGroup_Tcp_Should_Throw_InvalidResponse()
    {
        await Should.ThrowAsync<InvalidResponseException>(() => Fixture.Clients[protocol].JoinConsumerGroupAsync(new JoinConsumerGroupRequest
        {
            TopicId = _createConsumerGroupRequest.TopicId,
            StreamId = _createConsumerGroupRequest.StreamId,
            ConsumerGroupId = Identifier.Numeric(GroupId)
        }));
    }


    [Test]
    [DependsOn(nameof(JoinConsumerGroup_Tcp_Should_Throw_InvalidResponse))]
    public async Task DeleteConsumerGroup_Should_Throw_InvalidResponse()
    {
        await Should.ThrowAsync<InvalidResponseException>(() => Fixture.Clients[protocol].DeleteConsumerGroupAsync(new DeleteConsumerGroupRequest
        {
            TopicId = _createConsumerGroupRequest.TopicId,
            StreamId = _createConsumerGroupRequest.StreamId,
            ConsumerGroupId = Identifier.Numeric(GroupId)
        }));
    }
}