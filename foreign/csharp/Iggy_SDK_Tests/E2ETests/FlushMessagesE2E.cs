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
using Iggy_SDK_Tests.E2ETests.Fixtures;
using Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;
using Iggy_SDK_Tests.Utils;
using Iggy_SDK;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Exceptions;

namespace Iggy_SDK_Tests.E2ETests;

[TestCaseOrderer("Iggy_SDK_Tests.Utils.PriorityOrderer", "Iggy_SDK_Tests")]
public sealed class FlushMessagesE2E : IClassFixture<IggyFlushMessagesFixture>
{
    private const string SkipMessage = "TCP implementation needs to be aligned with Iggyrs core changes";
    private readonly IggyFlushMessagesFixture _fixture;

    private static readonly FlushUnsavedBufferRequest _flushRequestFsync = new()
    {
        StreamId = Identifier.Numeric(FlushMessagesFixtureBootstrap.StreamId),
        TopicId = Identifier.Numeric(FlushMessagesFixtureBootstrap.TopicId),
        PartitionId = FlushMessagesFixtureBootstrap.PartitionId,
        Fsync = true
    };
      
    private static readonly FlushUnsavedBufferRequest _flushRequest = new()
    {
        StreamId = Identifier.Numeric(FlushMessagesFixtureBootstrap.StreamId),
        TopicId = Identifier.Numeric(FlushMessagesFixtureBootstrap.TopicId),
        PartitionId = FlushMessagesFixtureBootstrap.PartitionId,
        Fsync = false
    };
    
    private static readonly FlushUnsavedBufferRequest _flushInvalidStreamRequest = new()
    {
        StreamId = Identifier.Numeric(FlushMessagesFixtureBootstrap.InvalidStreamId),
        TopicId = Identifier.Numeric(FlushMessagesFixtureBootstrap.TopicId),
        PartitionId = FlushMessagesFixtureBootstrap.PartitionId,
        Fsync = false
    };
    

    public FlushMessagesE2E(IggyFlushMessagesFixture fixture)
    {
        _fixture = fixture;
    }
    
    [Fact, TestPriority(1)]
    public async Task FlushUnsavedBuffer_WithFsync_Should_Flush_Successfully()
    {
        // act
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(x => x.Client.FlushUnsavedBufferAsync(_flushRequestFsync))
                .Should()
                .NotThrowAsync();
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(2)]
    public async Task FlushUnsavedBuffer_WithOutFsync_Should_Flush_Successfully()
    {
        // act
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(x => x.Client.FlushUnsavedBufferAsync(_flushRequest))
                .Should()
                .NotThrowAsync();
            
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(3)]
    public async Task FlushUnsavedBuffer_Should_Throw_WhenStream_DoesNotExist()
    {
        // act
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(x => x.Client.FlushUnsavedBufferAsync(_flushInvalidStreamRequest))
                .Should()
                .ThrowAsync<InvalidResponseException>();
            
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
}