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

using FluentAssertions;
using Iggy_SDK;
using Iggy_SDK_Tests.E2ETests.Fixtures;
using Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;
using Iggy_SDK_Tests.Utils;
using Iggy_SDK.Exceptions;

namespace Iggy_SDK_Tests.E2ETests;

[TestCaseOrderer("Iggy_SDK_Tests.Utils.PriorityOrderer", "Iggy_SDK_Tests")]
public sealed class TopicsE2E : IClassFixture<IggyTopicFixture>
{
    private readonly IggyTopicFixture _fixture;
    
    public TopicsE2E(IggyTopicFixture fixture)
    {
        _fixture = fixture;
    }
    
    [Fact, TestPriority(1)]
    public async Task Create_NewTopic_Should_Return_Successfully()
    {
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            var response = await sut.Client.CreateTopicAsync(Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!), TopicsFixtureBootstrap.TopicRequest);
            
            response.Should().NotBeNull();
            response!.Id.Should().Be(TopicsFixtureBootstrap.TopicRequest.TopicId);
            response.Name.Should().Be(TopicsFixtureBootstrap.TopicRequest.Name);
            response.CompressionAlgorithm.Should().Be(TopicsFixtureBootstrap.TopicRequest.CompressionAlgorithm);
            response.Partitions.Should().HaveCount(TopicsFixtureBootstrap.TopicRequest.PartitionsCount);
            response.MessageExpiry.Should().Be(TopicsFixtureBootstrap.TopicRequest.MessageExpiry);
            response.Size.Should().Be(0);
            response.CreatedAt.UtcDateTime.Should().BeAfter(DateTimeOffset.UtcNow.UtcDateTime.AddSeconds(-20));
        })).ToArray();
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(2)]
    public async Task Create_DuplicateTopic_Should_Throw_InvalidResponse()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(async x =>
                    await x.Client.CreateTopicAsync(Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!), TopicsFixtureBootstrap.TopicRequest))
                .Should()
                .ThrowExactlyAsync<InvalidResponseException>();
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(3)]
    public async Task Get_ExistingTopic_Should_ReturnValidResponse()
    {
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            var response = await sut.Client.GetTopicByIdAsync(Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!),
                Identifier.Numeric((int)TopicsFixtureBootstrap.TopicRequest.TopicId!));
            response.Should().NotBeNull();
            response!.Id.Should().Be(TopicsFixtureBootstrap.TopicRequest.TopicId);
            response.Name.Should().Be(TopicsFixtureBootstrap.TopicRequest.Name);
            response.CompressionAlgorithm.Should().Be(TopicsFixtureBootstrap.TopicRequest.CompressionAlgorithm);
            response.Partitions.Should().HaveCount(TopicsFixtureBootstrap.TopicRequest.PartitionsCount);
            response.MessageExpiry.Should().Be(TopicsFixtureBootstrap.TopicRequest.MessageExpiry);
            response.Size.Should().Be(0);
            response.MessagesCount.Should().Be(0);
            response.MaxTopicSize.Should().Be(TopicsFixtureBootstrap.TopicRequest.MaxTopicSize);
            response.MessageExpiry.Should().Be(TopicsFixtureBootstrap.TopicRequest.MessageExpiry);
            response.CreatedAt.UtcDateTime.Year.Should().Be(DateTimeOffset.UtcNow.Year);
            response.CreatedAt.UtcDateTime.Month.Should().Be(DateTimeOffset.UtcNow.Month);
            response.CreatedAt.UtcDateTime.Day.Should().Be(DateTimeOffset.UtcNow.Day);
            response.CreatedAt.UtcDateTime.Minute.Should().Be(DateTimeOffset.UtcNow.Minute);
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(4)]
    public async Task Get_ExistingTopics_Should_ReturnValidResponse()
    {
        var createTasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(async x =>
                    await x.Client.CreateTopicAsync(Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!), TopicsFixtureBootstrap.TopicRequestSecond))
                .Should()
                .NotThrowAsync();
        })).ToArray();
        await Task.WhenAll(createTasks);
        
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            var response = await sut.Client.GetTopicsAsync(Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!));
            response.Should().NotBeNull();
            response.Count.Should().Be(2);
            response.Select(x=>x.Id).Should().ContainSingle(x=> x== TopicsFixtureBootstrap.TopicRequest.TopicId);
            response.Select(x=>x.Id).Should().ContainSingle(x=> x== TopicsFixtureBootstrap.TopicRequestSecond.TopicId);
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(5)]
    public async Task Update_ExistingTopic_Should_UpdateTopic_Successfully()
    {
        // act
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(async x =>
                    await x.Client.UpdateTopicAsync(Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!),
                        Identifier.Numeric((int)TopicsFixtureBootstrap.TopicRequest.TopicId!), TopicsFixtureBootstrap.UpdateTopicRequest))
                .Should()
                .NotThrowAsync();
        
            var result = await sut.Client.GetTopicByIdAsync(Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!),
                Identifier.Numeric((int)TopicsFixtureBootstrap.TopicRequest.TopicId!));
            result.Should().NotBeNull();
            result!.Name.Should().Be(TopicsFixtureBootstrap.UpdateTopicRequest.Name);
            result.MessageExpiry.Should().Be(TopicsFixtureBootstrap.UpdateTopicRequest.MessageExpiry);
            result.CompressionAlgorithm.Should().Be(TopicsFixtureBootstrap.UpdateTopicRequest.CompressionAlgorithm);
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(6)]
    public async Task Purge_ExistingTopic_Should_PurgeTopic_Successfully()
    {
        // act
        // TODO: Check if the topic is empty after purge
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(async x =>
                    await x.Client.PurgeTopicAsync(Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!),
                        Identifier.Numeric((int)TopicsFixtureBootstrap.TopicRequest.TopicId!)))
                .Should()
                .NotThrowAsync();
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(7)]
    public async Task Delete_ExistingTopic_Should_DeleteTopic_Successfully()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(async x =>
                    await x.Client.DeleteTopicAsync(Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!),
                        Identifier.Numeric((int)TopicsFixtureBootstrap.TopicRequest.TopicId!)))
                .Should()
                .NotThrowAsync();
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(8)]
    public async Task Delete_NonExistingTopic_Should_Throw_InvalidResponse()
    {
        // act & assert
       
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(async x =>
                    await x.Client.DeleteTopicAsync(Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!),
                        Identifier.Numeric((int)TopicsFixtureBootstrap.TopicRequest.TopicId!)))
                .Should()
                .ThrowExactlyAsync<InvalidResponseException>();
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(9)]
    public async Task Get_NonExistingTopic_Should_Throw_InvalidResponse()
    {
        // act & assert
        
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(async x =>
                    await x.Client.GetTopicByIdAsync(Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!),
                        Identifier.Numeric((int)TopicsFixtureBootstrap.TopicRequest.TopicId!)))
                .Should()
                .ThrowExactlyAsync<InvalidResponseException>();
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
}