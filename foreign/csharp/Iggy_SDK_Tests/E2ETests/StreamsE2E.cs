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
using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Exceptions;

namespace Iggy_SDK_Tests.E2ETests;

[TestCaseOrderer("Iggy_SDK_Tests.Utils.PriorityOrderer", "Iggy_SDK_Tests")]
public sealed class StreamsE2E : IClassFixture<IggyStreamFixture>
{
    private readonly IggyStreamFixture _fixture;

    public StreamsE2E(IggyStreamFixture fixture)
    {
        _fixture = fixture;
    }
    
    [Fact, TestPriority(1)]
    public async Task CreateStream_HappyPath_Should_CreateStream_Successfully()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            var response = await sut.Client.CreateStreamAsync(StreamsFixtureBootstrap.StreamRequest);
            
            response.Should().NotBeNull();
            response!.Id.Should().Be(StreamsFixtureBootstrap.StreamRequest.StreamId);
            response.Name.Should().Be(StreamsFixtureBootstrap.StreamRequest.Name);
            response.Size.Should().Be(0);
            response.CreatedAt.UtcDateTime.Should().BeAfter(DateTimeOffset.UtcNow.UtcDateTime.AddSeconds(-20));
            response.MessagesCount.Should().Be(0);
            response.TopicsCount.Should().Be(0);
            response.Topics.Should().BeEmpty();
        })).ToArray();
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(2)]
    public async Task CreateStream_Duplicate_Should_Throw_InvalidResponse()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(async x => await x.Client.CreateStreamAsync(StreamsFixtureBootstrap.StreamRequest))
                .Should()
                .ThrowExactlyAsync<InvalidResponseException>();
        })).ToArray();
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(3)]
    public async Task GetStreamById_Should_ReturnValidResponse()
    {
        // act
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            var response = await sut.Client.GetStreamByIdAsync(Identifier.Numeric((int)StreamsFixtureBootstrap.StreamRequest.StreamId!));
            response.Should().NotBeNull();
            response!.Id.Should().Be(StreamsFixtureBootstrap.StreamRequest.StreamId);
            response.Name.Should().Be(StreamsFixtureBootstrap.StreamRequest.Name);
        })).ToArray();
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(4)]
    public async Task UpdateStream_Should_UpdateStream_Successfully()
    {
        // act
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(async x => await x.Client.UpdateStreamAsync(Identifier.Numeric((int)StreamsFixtureBootstrap.StreamRequest.StreamId!), StreamsFixtureBootstrap.UpdateStreamRequest))
                .Should()
                .NotThrowAsync();
            
            var result = await sut.Client.GetStreamByIdAsync(Identifier.Numeric((int)StreamsFixtureBootstrap.StreamRequest.StreamId!));
            result.Should().NotBeNull();
            result!.Name.Should().Be(StreamsFixtureBootstrap.UpdateStreamRequest.Name);
        })).ToArray();
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(5)]
    public async Task PurgeStream_Should_PurgeStream_Successfully()
    {
        // act
        // TODO: Check if the stream is empty after purging
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(async x => await x.Client.PurgeStreamAsync(Identifier.Numeric((int)StreamsFixtureBootstrap.StreamRequest.StreamId!)))
                .Should()
                .NotThrowAsync();
        })).ToArray();
        await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(6)]
    public async Task DeleteStream_Should_DeleteStream_Successfully()
    {
        // act
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(async x => await x.Client.DeleteStreamAsync(Identifier.Numeric((int)StreamsFixtureBootstrap.StreamRequest.StreamId!)))
                .Should()
                .NotThrowAsync();
        })).ToArray();
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(7)]
    public async Task DeleteStream_Should_Throw_InvalidResponse()
    {
        // act
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(async x => await x.Client.DeleteStreamAsync(Identifier.Numeric((int)StreamsFixtureBootstrap.StreamRequest.StreamId!)))
                .Should()
                .ThrowExactlyAsync<InvalidResponseException>();
        })).ToArray();
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(8)]
    public async Task GetStreamById_Should_Throw_InvalidResponse()
    {
        // act
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(async x =>
                await x.Client.GetStreamByIdAsync(Identifier.Numeric((int)StreamsFixtureBootstrap.StreamRequest.StreamId!)))
            .Should()
            .ThrowExactlyAsync<InvalidResponseException>();
        })).ToArray();
        await Task.WhenAll(tasks);
    }
}