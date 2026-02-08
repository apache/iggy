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
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Apache.Iggy.Tests.Integrations.Helpers;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class SystemTests
{
    [ClassDataSource<SystemFixture>(Shared = SharedType.PerClass)]
    public required SystemFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetClients_Should_Return_CorrectClientsCount(Protocol protocol)
    {
        IReadOnlyList<ClientResponse> clients = await Fixture.Clients[protocol].GetClientsAsync();

        clients.Count.ShouldBeGreaterThanOrEqualTo(Fixture.TotalClientsCount);
        foreach (var client in clients)
        {
            client.ClientId.ShouldNotBe(0u);
            client.Address.ShouldNotBeNullOrEmpty();
            client.Transport.ShouldBe(Protocol.Tcp);
        }
    }

    [Test]
    [DependsOn(nameof(GetClients_Should_Return_CorrectClientsCount))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetClient_Should_Return_CorrectClient(Protocol protocol)
    {
        IReadOnlyList<ClientResponse> clients = await Fixture.Clients[protocol].GetClientsAsync();

        var client = await Fixture.IggyServerFixture.CreateClient(Protocol.Tcp, protocol);
        await client.LoginUser("iggy", "iggy");
        var clientInfo = await client.GetMeAsync();
        clientInfo.ShouldNotBeNull();

        clients.Count.ShouldBeGreaterThanOrEqualTo(Fixture.TotalClientsCount);
        var response = await Fixture.Clients[protocol].GetClientByIdAsync(clientInfo.ClientId);
        response.ShouldNotBeNull();
        response.ClientId.ShouldBe(clientInfo.ClientId);
        response.UserId.ShouldNotBeNull();
        response.UserId.Value.ShouldBeGreaterThanOrEqualTo(0u);
        response.Address.ShouldNotBeNullOrEmpty();
        response.Transport.ShouldBe(Protocol.Tcp);
        response.ConsumerGroupsCount.ShouldBe(0);
        response.ConsumerGroups.ShouldBeEmpty();
    }

    [Test]
    [SkipHttp]
    [DependsOn(nameof(GetClient_Should_Return_CorrectClient))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetMe_Tcp_Should_Return_MyClient(Protocol protocol)
    {
        var me = await Fixture.Clients[protocol].GetMeAsync();
        me.ShouldNotBeNull();
        me.ClientId.ShouldNotBe(0u);
        me.UserId.ShouldBe(0u);
        me.Address.ShouldNotBeNullOrEmpty();
        me.Transport.ShouldBe(Protocol.Tcp);
    }

    [Test]
    [SkipTcp]
    [DependsOn(nameof(GetClient_Should_Return_CorrectClient))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetMe_HTTP_Should_Throw_FeatureUnavailableException(Protocol protocol)
    {
        await Should.ThrowAsync<FeatureUnavailableException>(() => Fixture.Clients[protocol].GetMeAsync());
    }

    [Test]
    [DependsOn(nameof(GetMe_HTTP_Should_Throw_FeatureUnavailableException))]
    [DependsOn(nameof(GetMe_Tcp_Should_Return_MyClient))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetClient_WithConsumerGroup_Should_Return_CorrectClient(Protocol protocol)
    {
        var client = await Fixture.IggyServerFixture.CreateClient(Protocol.Tcp, protocol);
        await client.LoginUser("iggy", "iggy");
        var stream = await client.CreateStreamAsync(Fixture.StreamId.GetWithProtocol(protocol));
        await client.CreateTopicAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), "test_topic", 2);

        var consumerGroup
            = await client.CreateConsumerGroupAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String("test_topic"), "test_consumer_group");
        await client.JoinConsumerGroupAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            Identifier.String("test_topic"), Identifier.String("test_consumer_group"));
        var me = await client.GetMeAsync();

        var response = await Fixture.Clients[protocol].GetClientByIdAsync(me!.ClientId);
        response.ShouldNotBeNull();
        response.UserId.ShouldBe(0u);
        response.Address.ShouldNotBeNullOrEmpty();
        response.Transport.ShouldBe(Protocol.Tcp);
        response.ConsumerGroupsCount.ShouldBe(1);
        response.ConsumerGroups.ShouldNotBeEmpty();
        response.ConsumerGroups.ShouldContain(x => x.GroupId == consumerGroup!.Id);
        response.ConsumerGroups.ShouldContain(x => x.TopicId == 0);
        response.ConsumerGroups.ShouldContain(x => x.StreamId == stream!.Id);
    }


    [Test]
    [DependsOn(nameof(GetClient_WithConsumerGroup_Should_Return_CorrectClient))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetStats_Should_ReturnValidResponse(Protocol protocol)
    {
        await Fixture.Clients[protocol].SendMessagesAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            Identifier.Numeric(0), Partitioning.None(), [new Message(Guid.NewGuid(), "Test message"u8.ToArray())]);

        await Fixture.Clients[protocol].PollMessagesAsync(new MessageFetchRequest
        {
            StreamId = Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            TopicId = Identifier.Numeric(0),
            AutoCommit = true,
            Consumer = Consumer.New(1),
            Count = 1,
            PartitionId = 1,
            PollingStrategy = PollingStrategy.First()
        });

        var response = await Fixture.Clients[protocol].GetStatsAsync();
        response.ShouldNotBeNull();
        response.ProcessId.ShouldBeGreaterThanOrEqualTo(0);
        response.CpuUsage.ShouldBeGreaterThanOrEqualTo(0);
        response.TotalCpuUsage.ShouldBeGreaterThanOrEqualTo(0);
        response.MemoryUsage.ShouldBeGreaterThanOrEqualTo(0u);
        response.TotalMemory.ShouldBeGreaterThanOrEqualTo(0u);
        response.AvailableMemory.ShouldNotBe(0u);
        response.RunTime.ShouldBeGreaterThanOrEqualTo(0u);
        response.StartTime.ShouldBe(DateTimeOffset.UtcNow, TimeSpan.FromMinutes(5));
        response.ReadBytes.ShouldBeGreaterThanOrEqualTo(0u);
        response.WrittenBytes.ShouldBeGreaterThanOrEqualTo(0u);
        response.MessagesSizeBytes.ShouldBeGreaterThanOrEqualTo(0u);
        response.StreamsCount.ShouldNotBe(0);
        response.TopicsCount.ShouldNotBe(0);
        response.PartitionsCount.ShouldNotBe(0);
        response.SegmentsCount.ShouldNotBe(0);
        response.MessagesCount.ShouldNotBe(0u);
        response.ClientsCount.ShouldNotBe(0);
        response.ConsumerGroupsCount.ShouldNotBe(0);
        response.Hostname.ShouldNotBeNullOrEmpty();
        response.OsName.ShouldNotBeNullOrEmpty();
        response.OsVersion.ShouldNotBeNullOrEmpty();
        response.KernelVersion.ShouldNotBeNullOrEmpty();
        response.IggyServerVersion.ShouldNotBeNullOrEmpty();
        response.IggyServerSemver.ShouldNotBe(0u);
    }

    [Test]
    [DependsOn(nameof(GetStats_Should_ReturnValidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Ping_Should_Pong(Protocol protocol)
    {
        await Should.NotThrowAsync(Fixture.Clients[protocol].PingAsync());
    }

    [Test]
    [DependsOn(nameof(Ping_Should_Pong))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetSnapshot_Should_Return_ValidZipData(Protocol protocol)
    {
        var snapshot = await Fixture.Clients[protocol].GetSnapshotAsync(
            SnapshotCompression.Deflated,
            [SystemSnapshotType.Test]);

        snapshot.ShouldNotBeNull();
        snapshot.Length.ShouldBeGreaterThan(0);

        // Verify it's a valid ZIP archive by checking the ZIP magic bytes (PK\x03\x04)
        snapshot[0].ShouldBe((byte)'P');
        snapshot[1].ShouldBe((byte)'K');
    }

    // [Test]
    // [DependsOn(nameof(Ping_Should_Pong))]
    // [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    // public async Task GetClusterMetadata_Should_Return_ClusterMetadata(Protocol protocol)
    // {
    //     var clusterMetadata = await Fixture.Clients[protocol].GetClusterMetadataAsync();
    //
    //     clusterMetadata.ShouldNotBeNull();
    //     clusterMetadata.Id.ShouldBe(1u);
    //     clusterMetadata.Name.ShouldBe("iggy-cluster");
    //     clusterMetadata.Transport.ShouldBe(Protocol.Tcp);
    //     clusterMetadata.Nodes.ShouldNotBeEmpty();
    //     clusterMetadata.Nodes.Length.ShouldBe(2);
    //     clusterMetadata.Nodes[0].Id.ShouldBe(1u);
    //     clusterMetadata.Nodes[0].Name.ShouldBe("iggy-node-1");
    //     clusterMetadata.Nodes[0].Address.ShouldBe("127.0.0.1:8090");
    //     clusterMetadata.Nodes[0].Role.ShouldBe(ClusterNodeRole.Leader);
    //     clusterMetadata.Nodes[0].Status.ShouldBe(ClusterNodeStatus.Healthy);
    //     clusterMetadata.Nodes[1].Id.ShouldBe(2u);
    //     clusterMetadata.Nodes[1].Name.ShouldBe("iggy-node-2");
    //     clusterMetadata.Nodes[1].Address.ShouldBe("127.0.0.1:8092");
    //     clusterMetadata.Nodes[1].Role.ShouldBe(ClusterNodeRole.Follower);
    //     clusterMetadata.Nodes[1].Status.ShouldBe(ClusterNodeStatus.Healthy);
    // }
}
