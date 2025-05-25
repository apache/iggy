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
using Iggy_SDK_Tests.Utils.Partitions;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Exceptions;

namespace Iggy_SDK_Tests.E2ETests;

[TestCaseOrderer("Iggy_SDK_Tests.Utils.PriorityOrderer", "Iggy_SDK_Tests")]
public sealed class PartitionsE2E : IClassFixture<IggyPartitionFixture>
{
    private readonly IggyPartitionFixture _fixture;
    private readonly CreatePartitionsRequest _partitionsRequest;
    private readonly DeletePartitionsRequest _deletePartitionsRequest;
    public PartitionsE2E(IggyPartitionFixture fixture)
    {
        _fixture = fixture;
        _partitionsRequest =
            PartitionFactory.CreatePartitionsRequest((int)PartitionsFixtureBootstrap.StreamRequest.StreamId!, (int)PartitionsFixtureBootstrap.TopicRequest.TopicId!);
        _deletePartitionsRequest = PartitionFactory.CreateDeletePartitionsRequest((int)PartitionsFixtureBootstrap.StreamRequest.StreamId,
            (int)PartitionsFixtureBootstrap.TopicRequest.TopicId, _partitionsRequest.PartitionsCount);
    }

    [Fact, TestPriority(1)]
    public async Task CreatePartition_HappyPath_Should_CreatePartition_Successfully()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select( sut => Task.Run(async () =>
        {
            await sut.Invoking(x => x.Client.CreatePartitionsAsync(_partitionsRequest))
                .Should()
                .NotThrowAsync();
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(2)]
    public async Task DeletePartition_Should_DeletePartition_Successfully()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select( sut => Task.Run(async () =>
        {
            await sut.Invoking(x => x.Client.DeletePartitionsAsync(_deletePartitionsRequest))
                .Should()
                .NotThrowAsync();
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(3)]
    public async Task DeletePartition_Should_Throw_WhenTopic_DoesNotExist()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select( sut => Task.Run(async () =>
        {
            await sut.Client.CreatePartitionsAsync(_partitionsRequest);
            await sut.Client.DeleteTopicAsync(Identifier.Numeric((int)PartitionsFixtureBootstrap.StreamRequest.StreamId!),
                Identifier.Numeric((int)PartitionsFixtureBootstrap.TopicRequest.TopicId!));
            await sut.Invoking(x => x.Client.DeletePartitionsAsync(_deletePartitionsRequest))
                .Should()
                .ThrowExactlyAsync<InvalidResponseException>();
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(4)]
    public async Task DeletePartition_Should_Throw_WhenStream_DoesNotExist()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select( sut => Task.Run(async () =>
        {
            await sut.Client.CreateTopicAsync(Identifier.Numeric((int)PartitionsFixtureBootstrap.StreamRequest.StreamId!), PartitionsFixtureBootstrap.TopicRequest);
            await sut.Client.DeleteStreamAsync(Identifier.Numeric((int)PartitionsFixtureBootstrap.StreamRequest.StreamId));
            await sut.Client.Invoking(x => x.DeletePartitionsAsync(_deletePartitionsRequest))
                .Should()
                .ThrowExactlyAsync<InvalidResponseException>();
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
}