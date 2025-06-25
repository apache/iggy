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
using Apache.Iggy.Exceptions;
using Apache.Iggy.Tests.E2ETests.Fixtures;
using Apache.Iggy.Tests.E2ETests.Fixtures.Bootstraps;
using Apache.Iggy.Tests.Utils;
using Apache.Iggy.Tests.Utils.Groups;
using FluentAssertions;

namespace Apache.Iggy.Tests.E2ETests;

[TestCaseOrderer("Apache.Iggy.Tests.Utils.PriorityOrderer", "Apache.Iggy.Tests")]
public sealed class ConsumerGroupE2E : IClassFixture<IggyConsumerGroupFixture>
{
    private const string SkipMessage = "TCP implementation needs to be aligned with Iggy core changes";

    private readonly IggyConsumerGroupFixture _fixture;

    private const int GROUP_ID = 1;

    private static readonly CreateConsumerGroupRequest _createConsumerGroupRequest =
        ConsumerGroupFactory.CreateRequest(
            (int)ConsumerGroupFixtureBootstrap.StreamRequest.StreamId!,
            (int)ConsumerGroupFixtureBootstrap.TopicRequest.TopicId!, GROUP_ID);

    private static readonly JoinConsumerGroupRequest _joinConsumerGroupRequest =
        ConsumerGroupFactory.CreateJoinGroupRequest(
            (int)ConsumerGroupFixtureBootstrap.StreamRequest.StreamId,
            (int)ConsumerGroupFixtureBootstrap.TopicRequest.TopicId, GROUP_ID);

    private static readonly LeaveConsumerGroupRequest _leaveConsumerGroupRequest =
        ConsumerGroupFactory.CreateLeaveGroupRequest(
            (int)ConsumerGroupFixtureBootstrap.StreamRequest.StreamId,
            (int)ConsumerGroupFixtureBootstrap.TopicRequest.TopicId, GROUP_ID);

    private static readonly DeleteConsumerGroupRequest _deleteConsumerGroupRequest =
        ConsumerGroupFactory.CreateDeleteGroupRequest(
            (int)ConsumerGroupFixtureBootstrap.StreamRequest.StreamId,
            (int)ConsumerGroupFixtureBootstrap.TopicRequest.TopicId, GROUP_ID);

    private Identifier ConsumerGroupId = Identifier.Numeric(GROUP_ID);

    public ConsumerGroupE2E(IggyConsumerGroupFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact, TestPriority(1)]
    public async Task CreateConsumerGroup_HappyPath_Should_CreateConsumerGroup_Successfully()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            var consumerGroup = await sut.Client.CreateConsumerGroupAsync(_createConsumerGroupRequest);
            consumerGroup.Should().NotBeNull();
            consumerGroup!.Id.Should().Be(GROUP_ID);
            consumerGroup.PartitionsCount.Should().Be(ConsumerGroupFixtureBootstrap.TopicRequest.PartitionsCount);
            consumerGroup.MembersCount.Should().Be(0);
            consumerGroup.Name.Should().Be(_createConsumerGroupRequest.Name);
        })).ToArray();

        await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(2)]
    public async Task CreateConsumerGroup_Should_Throw_InvalidResponse()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(x => x.Client.CreateConsumerGroupAsync(_createConsumerGroupRequest))
                .Should()
                .ThrowExactlyAsync<InvalidResponseException>();
        })).ToArray();

        await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(3)]
    public async Task GetConsumerGroupById_Should_Return_ValidResponse()
    {
        // act
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            var response = await sut.Client.GetConsumerGroupByIdAsync(
                Identifier.Numeric((int)ConsumerGroupFixtureBootstrap.StreamRequest.StreamId!), Identifier.Numeric((int)ConsumerGroupFixtureBootstrap.TopicRequest.TopicId!),
                ConsumerGroupId);

            response.Should().NotBeNull();
            response!.Id.Should().Be(GROUP_ID);
            response.PartitionsCount.Should().Be(ConsumerGroupFixtureBootstrap.TopicRequest.PartitionsCount);
            response.MembersCount.Should().Be(0);
        })).ToArray();

        await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(4)]
    public async Task GetConsumerGroups_Should_Return_ValidResponse()
    {
        // act
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            var response = await sut.Client.GetConsumerGroupsAsync(
                Identifier.Numeric((int)ConsumerGroupFixtureBootstrap.StreamRequest.StreamId!), Identifier.Numeric((int)ConsumerGroupFixtureBootstrap.TopicRequest.TopicId!));

            response.Should().NotBeNull();
            response.Count.Should().Be(1);
            var group = response.FirstOrDefault();
            group!.Id.Should().Be(GROUP_ID);
            group.PartitionsCount.Should().Be(ConsumerGroupFixtureBootstrap.TopicRequest.PartitionsCount);
            group.MembersCount.Should().Be(0);
        })).ToArray();

        await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(5)]
    public async Task JoinConsumerGroup_Should_JoinConsumerGroup_Successfully()
    {
        // act & assert
        await _fixture.HttpClient.Client.Invoking(y =>
                y.JoinConsumerGroupAsync(_joinConsumerGroupRequest)
            ).Should()
            .ThrowExactlyAsync<FeatureUnavailableException>();

        await _fixture.TcpClient.Client.Invoking(x => x.JoinConsumerGroupAsync(_joinConsumerGroupRequest))
            .Should()
            .NotThrowAsync();
    }

    [Fact, TestPriority(6)]
    public async Task LeaveConsumerGroup_Should_LeaveConsumerGroup_Successfully()
    {
        // act & assert
        await _fixture.HttpClient.Client.Invoking(x => x.LeaveConsumerGroupAsync(_leaveConsumerGroupRequest))
            .Should()
            .ThrowAsync<FeatureUnavailableException>();

        await _fixture.TcpClient.Client.Invoking(x => x.LeaveConsumerGroupAsync(_leaveConsumerGroupRequest))
            .Should()
            .NotThrowAsync();
    }

    [Fact, TestPriority(7)]
    public async Task DeleteConsumerGroup_Should_DeleteConsumerGroup_Successfully()
    {
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(x => x.Client.DeleteConsumerGroupAsync(_deleteConsumerGroupRequest))
                .Should()
                .NotThrowAsync();
        })).ToArray();
        await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(8)]
    public async Task JoinConsumerGroup_Should_Throw_InvalidResponse()
    {
        // act & assert
        await _fixture.HttpClient.Client.Invoking(x => x.LeaveConsumerGroupAsync(_leaveConsumerGroupRequest))
            .Should()
            .ThrowAsync<FeatureUnavailableException>();

        await _fixture.TcpClient.Client.Invoking(x => x.JoinConsumerGroupAsync(_joinConsumerGroupRequest))
            .Should()
            .ThrowExactlyAsync<InvalidResponseException>();
    }

    [Fact, TestPriority(9)]
    public async Task DeleteConsumerGroup_Should_Throw_InvalidResponse()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(x => x.Client.DeleteConsumerGroupAsync(_deleteConsumerGroupRequest))
                .Should()
                .ThrowExactlyAsync<InvalidResponseException>();
        })).ToArray();

        await Task.WhenAll(tasks);
    }
}
