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

using Apache.Iggy.Contracts.Http;
using Apache.Iggy.Tests.E2ETests.Fixtures;
using Apache.Iggy.Tests.E2ETests.Fixtures.Bootstraps;
using Apache.Iggy.Tests.Utils;
using Apache.Iggy.Tests.Utils.Offsets;
using FluentAssertions;

namespace Apache.Iggy.Tests.E2ETests;

[TestCaseOrderer("Apache.Iggy.Tests.Utils.PriorityOrderer", "Apache.Iggy.Tests")]
public sealed class OffsetE2E : IClassFixture<IggyOffsetFixture>
{
    private const string SkipMessage = "TCP implementation needs to be aligned with Iggyrs core changes";
    private readonly IggyOffsetFixture _fixture;
    private readonly StoreOffsetRequest _storeOffsetIndividualConsumer;
    private readonly OffsetRequest _offsetIndividualConsumer;

    private const int GET_INDIVIDUAL_CONSUMER_ID = 1;
    private const int GET_PARTITION_ID = 1;
    private const ulong GET_OFFSET = 0;

    public OffsetE2E(IggyOffsetFixture fixture)
    {
        _fixture = fixture;
        _storeOffsetIndividualConsumer = OffsetFactory.CreateOffsetContract(
            (int)OffsetFixtureBootstrap.StreamRequest.StreamId!, (int)OffsetFixtureBootstrap.TopicRequest.TopicId!, GET_INDIVIDUAL_CONSUMER_ID, GET_OFFSET,
            GET_PARTITION_ID);
        _offsetIndividualConsumer = OffsetFactory.CreateOffsetRequest((int)OffsetFixtureBootstrap.StreamRequest.StreamId,
            (int)OffsetFixtureBootstrap.TopicRequest.TopicId, GET_PARTITION_ID, GET_INDIVIDUAL_CONSUMER_ID);
    }

    [Fact, TestPriority(1)]
    public async Task StoreOffset_IndividualConsumer_Should_StoreOffset_Successfully()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select( sut => Task.Run(async () =>
        {
            await sut.Invoking(x => x.Client.StoreOffsetAsync(_storeOffsetIndividualConsumer))
                .Should()
                .NotThrowAsync();
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(2)]
    public async Task GetOffset_IndividualConsumer_Should_GetOffset_Successfully()
    {
        // act
        var tasks = _fixture.SubjectsUnderTest.Select( sut => Task.Run(async () =>
        {
            var offset = await sut.Client.GetOffsetAsync(_offsetIndividualConsumer);
            offset.Should().NotBeNull();
            offset!.StoredOffset.Should().Be(_storeOffsetIndividualConsumer.Offset);
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
}