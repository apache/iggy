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
using Apache.Iggy.Enums;
using Apache.Iggy.Kinds;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

[MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
public class OffsetTests(Protocol protocol)
{
    private const ulong SetOffset = 2;

    [ClassDataSource<OffsetFixtures>(Shared = SharedType.PerClass)]
    public required OffsetFixtures Fixture { get; init; }

    [Test]
    public async Task StoreOffset_IndividualConsumer_Should_StoreOffset_Successfully()
    {
        await Fixture.Clients[protocol].StoreOffsetAsync(new StoreOffsetRequest
        {
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(1),
            PartitionId = 1,
            Offset = SetOffset,
            Consumer = Consumer.New(1)
        });
    }

    [Test]
    [DependsOn(nameof(StoreOffset_IndividualConsumer_Should_StoreOffset_Successfully))]
    public async Task GetOffset_IndividualConsumer_Should_GetOffset_Successfully()
    {
        var offset = await Fixture.Clients[protocol].GetOffsetAsync(new OffsetRequest
        {
            Consumer = Consumer.New(1),
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(1),
            PartitionId = 1
        });

        offset.ShouldNotBeNull();
        offset.StoredOffset.ShouldBe(SetOffset);
        offset.PartitionId.ShouldBe(1);
        offset.CurrentOffset.ShouldBe(3u);
    }

    [Test]
    [DependsOn(nameof(GetOffset_IndividualConsumer_Should_GetOffset_Successfully))]
    public async Task StoreOffset_ConsumerGroup_Should_StoreOffset_Successfully()
    {
        await Fixture.Clients[protocol].CreateConsumerGroupAsync(new CreateConsumerGroupRequest
        {
            Name = "test_consumer_group",
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(1),
            ConsumerGroupId = 1
        });

        await Fixture.Clients[protocol].StoreOffsetAsync(new StoreOffsetRequest
        {
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(1),
            PartitionId = 1,
            Offset = SetOffset,
            Consumer = Consumer.Group(1)
        });
    }

    [Test]
    [DependsOn(nameof(StoreOffset_ConsumerGroup_Should_StoreOffset_Successfully))]
    public async Task GetOffset_ConsumerGroup_Should_GetOffset_Successfully()
    {
        var offset = await Fixture.Clients[protocol].GetOffsetAsync(new OffsetRequest
        {
            Consumer = Consumer.Group(1),
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(1),
            PartitionId = 1
        });

        offset.ShouldNotBeNull();
        offset.StoredOffset.ShouldBe(SetOffset);
        offset.PartitionId.ShouldBe(1);
        offset.CurrentOffset.ShouldBe(3u);
    }
}