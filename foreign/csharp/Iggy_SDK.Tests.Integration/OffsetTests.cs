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

using Apache.Iggy.Enums;
using Apache.Iggy.Kinds;
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Apache.Iggy.Tests.Integrations.Helpers;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

public class OffsetTests
{
    private const ulong SetOffset = 2;

    [ClassDataSource<OffsetFixtures>(Shared = SharedType.PerClass)]
    public required OffsetFixtures Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task StoreOffset_IndividualConsumer_Should_StoreOffset_Successfully(Protocol protocol)
    {
        await Fixture.Clients[protocol]
            .StoreOffsetAsync(Consumer.New("test-consumer"), Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicRequest.Name), SetOffset, 0);
    }

    [Test]
    [DependsOn(nameof(StoreOffset_IndividualConsumer_Should_StoreOffset_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetOffset_IndividualConsumer_Should_GetOffset_Successfully(Protocol protocol)
    {
        var offset = await Fixture.Clients[protocol]
            .GetOffsetAsync(Consumer.New("test-consumer"), Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicRequest.Name), 0);

        offset.ShouldNotBeNull();
        offset.StoredOffset.ShouldBe(SetOffset);
        offset.PartitionId.ShouldBe(0);
        offset.CurrentOffset.ShouldBe(3u);
    }

    [Test]
    [DependsOn(nameof(GetOffset_IndividualConsumer_Should_GetOffset_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task StoreOffset_ConsumerGroup_Should_StoreOffset_Successfully(Protocol protocol)
    {
        await Fixture.Clients[protocol]
            .CreateConsumerGroupAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicRequest.Name), "test_consumer_group");

        await Fixture.Clients[Protocol.Tcp].JoinConsumerGroupAsync(
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            Identifier.String(Fixture.TopicRequest.Name), Identifier.String("test_consumer_group"));

        await Fixture.Clients[protocol]
            .StoreOffsetAsync(Consumer.Group("test_consumer_group"), Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicRequest.Name), SetOffset, 0);
    }

    [Test]
    [DependsOn(nameof(StoreOffset_ConsumerGroup_Should_StoreOffset_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetOffset_ConsumerGroup_Should_GetOffset_Successfully(Protocol protocol)
    {
        var offset = await Fixture.Clients[protocol]
            .GetOffsetAsync(Consumer.Group("test_consumer_group"), Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicRequest.Name), 0);

        offset.ShouldNotBeNull();
        offset.StoredOffset.ShouldBe(SetOffset);
        offset.PartitionId.ShouldBe(0);
        offset.CurrentOffset.ShouldBe(3u);
    }

    [Test]
    [DependsOn(nameof(StoreOffset_ConsumerGroup_Should_StoreOffset_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetOffset_ConsumerGroup_ByName_Should_GetOffset_Successfully(Protocol protocol)
    {
        var offset = await Fixture.Clients[protocol].GetOffsetAsync(Consumer.Group("test_consumer_group"),
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), Identifier.String(Fixture.TopicRequest.Name),
            0);

        offset.ShouldNotBeNull();
        offset.StoredOffset.ShouldBe(SetOffset);
        offset.PartitionId.ShouldBe(0);
        offset.CurrentOffset.ShouldBe(3u);
    }

    [Test]
    [SkipHttp]
    [DependsOn(nameof(GetOffset_ConsumerGroup_ByName_Should_GetOffset_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeleteOffset_ConsumerGroup_Should_DeleteOffset_Successfully(Protocol protocol)
    {
        await Fixture.Clients[protocol].DeleteOffsetAsync(Consumer.Group("test_consumer_group"),
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), Identifier.String(Fixture.TopicRequest.Name),
            0);

        var offset = await Fixture.Clients[protocol].GetOffsetAsync(Consumer.Group("test_consumer_group"),
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), Identifier.String(Fixture.TopicRequest.Name),
            0);

        offset.ShouldBeNull();
    }
}
