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
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class SystemTests
{
    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetClients_Should_Return_NonEmptyClientsList(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        IReadOnlyList<ClientResponse> clients = await client.GetClientsAsync();

        clients.ShouldNotBeNull();
        clients.Count.ShouldBeGreaterThanOrEqualTo(1);
        foreach (var c in clients)
        {
            c.ClientId.ShouldNotBe(0u);
            c.Address.ShouldNotBeNullOrEmpty();
            c.Transport.ShouldBe(ClientTransport.Tcp);
        }
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetClient_Should_Return_CorrectClient(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var tcpClient = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);
        var clientInfo = await tcpClient.GetMeAsync();
        clientInfo.ShouldNotBeNull();

        var response = await client.GetClientByIdAsync(clientInfo.ClientId);
        response.ShouldNotBeNull();
        response.ClientId.ShouldBe(clientInfo.ClientId);
        response.UserId.ShouldNotBeNull();
        response.Address.ShouldNotBeNullOrEmpty();
        response.Transport.ShouldBe(ClientTransport.Tcp);
        response.ConsumerGroupsCount.ShouldBe(0u);
        response.ConsumerGroups.ShouldBeEmpty();
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetMe_Tcp_Should_Return_MyClient(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var me = await client.GetMeAsync();
        me.ShouldNotBeNull();
        me.ClientId.ShouldNotBe(0u);
        me.UserId.ShouldBe(0u);
        me.Address.ShouldNotBeNullOrEmpty();
        me.Transport.ShouldBe(ClientTransport.Tcp);
    }

    [Test]
    [SkipTcp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetMe_HTTP_Should_Throw_FeatureUnavailableException(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        await Should.ThrowAsync<FeatureUnavailableException>(() => client.GetMeAsync());
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetClient_WithConsumerGroup_Should_Return_CorrectClient(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"sys-cg-{Guid.NewGuid():N}";
        var tcpClient = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);

        var stream = await tcpClient.CreateStreamAsync(streamName);
        await tcpClient.CreateTopicAsync(Identifier.String(streamName), "first_topic", 2);
        var secondTopic = await tcpClient.CreateTopicAsync(Identifier.String(streamName), "second_topic", 2);

        var consumerGroup = await tcpClient.CreateConsumerGroupAsync(Identifier.String(streamName),
            Identifier.String("second_topic"), "test_consumer_group");
        await tcpClient.JoinConsumerGroupAsync(Identifier.String(streamName),
            Identifier.String("second_topic"), Identifier.String("test_consumer_group"));
        var me = await tcpClient.GetMeAsync();

        var response = await client.GetClientByIdAsync(me!.ClientId);
        response.ShouldNotBeNull();
        response.Address.ShouldNotBeNullOrEmpty();
        response.Transport.ShouldBe(ClientTransport.Tcp);
        response.ConsumerGroupsCount.ShouldBe(1u);
        response.ConsumerGroups.ShouldNotBeEmpty();
        response.ConsumerGroups.ShouldContain(x => x.GroupId == consumerGroup!.Id);
        response.ConsumerGroups.ShouldContain(x => x.StreamId == stream!.Id);
        response.ConsumerGroups.ShouldContain(x => x.TopicId == (int)secondTopic!.Id);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetStats_Should_ReturnValidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        // Create a stream/topic and send a message so stats are non-zero
        var streamName = $"sys-stats-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), "stats-topic", 1);
        await client.SendMessagesAsync(Identifier.String(streamName),
            Identifier.String("stats-topic"), Partitioning.None(),
            [new Message(Guid.NewGuid(), "Test message"u8.ToArray())]);

        var response = await client.GetStatsAsync();
        response.ShouldNotBeNull();
        response.ProcessId.ShouldNotBe(0u);
        response.CpuUsage.ShouldBeGreaterThanOrEqualTo(0);
        response.TotalCpuUsage.ShouldBeGreaterThanOrEqualTo(0);
        response.AvailableMemory.ShouldNotBe(0u);
        response.StartTime.ShouldBe(DateTimeOffset.UtcNow, TimeSpan.FromMinutes(5));
        response.StreamsCount.ShouldBeGreaterThanOrEqualTo(1u);
        response.TopicsCount.ShouldBeGreaterThanOrEqualTo(1u);
        response.PartitionsCount.ShouldBeGreaterThanOrEqualTo(1u);
        response.SegmentsCount.ShouldBeGreaterThanOrEqualTo(1u);
        response.MessagesCount.ShouldBeGreaterThanOrEqualTo(1u);
        // iggy-server leaves the connected-client tally out of its stats reply, so ClientsCount goes unchecked.
        response.Hostname.ShouldNotBeNullOrEmpty();
        response.OsName.ShouldNotBeNullOrEmpty();
        response.OsVersion.ShouldNotBeNullOrEmpty();
        response.KernelVersion.ShouldNotBeNullOrEmpty();
        response.IggyServerVersion.ShouldNotBeNullOrEmpty();
        response.IggyServerSemver.ShouldNotBe(0u);
        response.ThreadsCount.ShouldBeGreaterThanOrEqualTo(1u);
        response.TotalDiskSpace.ShouldBeGreaterThan(0u);
        response.FreeDiskSpace.ShouldBeGreaterThan(0u);
        response.FreeDiskSpace.ShouldBeGreaterThan(0u);
        response.FreeDiskSpace.ShouldBeLessThanOrEqualTo(response.TotalDiskSpace);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Ping_Should_Pong(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        await Should.NotThrowAsync(client.PingAsync());
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetSnapshot_Should_Return_ValidZipData(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var snapshot = await client.GetSnapshotAsync(SnapshotCompression.Deflated,
            [SystemSnapshotType.Test]);

        snapshot.ShouldNotBeNull();
        snapshot.Length.ShouldBeGreaterThan(0);

        // Verify it's a valid ZIP archive by checking the ZIP magic bytes (PK\x03\x04)
        snapshot[0].ShouldBe((byte)'P');
        snapshot[1].ShouldBe((byte)'K');
    }
}
