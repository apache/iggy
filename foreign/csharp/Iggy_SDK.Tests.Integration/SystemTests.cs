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
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

[MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
public class SystemTests(Protocol protocol)
{
    [ClassDataSource<SystemFixture>(Shared = SharedType.PerClass)]
    public required SystemFixture Fixture { get; init; }

    [Test]
    public async Task GetClients_Should_Return_CorrectClientsCount()
    {
        IReadOnlyList<ClientResponse> clients = await Fixture.Clients[protocol].GetClientsAsync();

        clients.Count.ShouldBe(Fixture.TotalClientsCount);
        foreach (var client in clients)
        {
            client.ClientId.ShouldNotBe(0u);
            client.UserId.ShouldBe(1u);
            client.Address.ShouldNotBeNullOrEmpty();
            client.Transport.ShouldBe("TCP");
        }
    }

    [Test]
    [DependsOn(nameof(GetClients_Should_Return_CorrectClientsCount))]
    public async Task GetClient_Should_Return_CorrectClient()
    {
        IReadOnlyList<ClientResponse> clients = await Fixture.Clients[protocol].GetClientsAsync();

        clients.Count.ShouldBe(Fixture.TotalClientsCount);
        var id = clients[0].ClientId;
        var response = await Fixture.Clients[protocol].GetClientByIdAsync(id);
        response!.ClientId.ShouldBe(id);
        response.UserId.ShouldBe(1u);
        response.Address.ShouldNotBeNullOrEmpty();
        response.Transport.ShouldBe("TCP");
        response.ConsumerGroupsCount.ShouldBe(0);
        response.ConsumerGroups.ShouldBeEmpty();
    }

    [Test]
    [SkipHttp]
    [DependsOn(nameof(GetClient_Should_Return_CorrectClient))]
    public async Task GetMe_Tcp_Should_Return_MyClient()
    {
        var me = await Fixture.Clients[protocol].GetMeAsync();
        me.ShouldNotBeNull();
        me.ClientId.ShouldNotBe(0u);
        me.UserId.ShouldBe(1u);
        me.Address.ShouldNotBeNullOrEmpty();
        me.Transport.ShouldBe("TCP");
    }

    [Test]
    [SkipTcp]
    [DependsOn(nameof(GetClient_Should_Return_CorrectClient))]
    public async Task GetMe_HTTP_Should_Throw_FeatureUnavailableException()
    {
        await Should.ThrowAsync<FeatureUnavailableException>(() => Fixture.Clients[protocol].GetMeAsync());
    }

    [Test]
    [DependsOn(nameof(GetMe_HTTP_Should_Throw_FeatureUnavailableException))]
    [DependsOn(nameof(GetMe_Tcp_Should_Return_MyClient))]
    public async Task GetClient_WithConsumerGroup_Should_Return_CorrectClient()
    {
        var client = Fixture.CreateClient(Protocol.Tcp, protocol);
        await client.LoginUser(new LoginUserRequest
        {
            Password = "iggy",
            Username = "iggy"
        });
        await client.CreateStreamAsync(new StreamRequest
        {
            Name = "test_stream",
            StreamId = 1
        });
        await client.CreateTopicAsync(Identifier.Numeric(1), new TopicRequest
        {
            Name = "test_topic",
            PartitionsCount = 2
        });
        var consumerGroup = await client.CreateConsumerGroupAsync(new CreateConsumerGroupRequest
        {
            Name = "test_consumer_group",
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(1),
            ConsumerGroupId = 1
        });
        await client.JoinConsumerGroupAsync(new JoinConsumerGroupRequest
        {
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(1),
            ConsumerGroupId = Identifier.Numeric(consumerGroup!.Id)
        });
        var me = await client.GetMeAsync();


        var response = await Fixture.Clients[protocol].GetClientByIdAsync(me!.ClientId);
        response.ShouldNotBeNull();
        response.UserId.ShouldBe(1u);
        response.Address.ShouldNotBeNullOrEmpty();
        response.Transport.ShouldBe("TCP");
        response.ConsumerGroupsCount.ShouldBe(1);
        response.ConsumerGroups.ShouldNotBeEmpty();
    }


    [Test]
    [DependsOn(nameof(GetClient_WithConsumerGroup_Should_Return_CorrectClient))]
    public async Task GetStats_Should_ReturnValidResponse()
    {
        var response = await Fixture.Clients[protocol].GetStatsAsync();
        response.ShouldNotBeNull();
        response.MessagesCount.ShouldBe(0u);
        response.PartitionsCount.ShouldBe(2);
        response.StreamsCount.ShouldBe(1);
        response.TopicsCount.ShouldBe(1);
    }

    [Test]
    [DependsOn(nameof(GetStats_Should_ReturnValidResponse))]
    public async Task Ping_Should_Pong()
    {
        await Should.NotThrowAsync(Fixture.Clients[protocol].PingAsync());
    }
}