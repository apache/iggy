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
using Iggy_SDK_Tests.Utils;
using Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;

namespace Iggy_SDK_Tests.E2ETests;

[TestCaseOrderer("Iggy_SDK_Tests.Utils.PriorityOrderer", "Iggy_SDK_Tests")]
public sealed class SystemE2E : IClassFixture<IggySystemFixture>
{
    private readonly IggySystemFixture _fixture;

    public SystemE2E(IggySystemFixture fixture)
    {
        _fixture = fixture;
    }
    
    [Fact, TestPriority(1)]
    public async Task GetClients_Should_Return_CorrectClientsCount()
    {
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            var clients = await sut.Client.GetClientsAsync();
            clients.Count.Should().Be(SystemFixtureBootstrap.TotalClientsCount);
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
        
    [Fact, TestPriority(2)]
    public async Task GetClient_Should_Return_CorrectClient()
    {
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            var clients = await sut.Client.GetClientsAsync();
            clients.Count.Should().Be(SystemFixtureBootstrap.TotalClientsCount);
            uint id = clients[0].ClientId;
            var response = await sut.Client.GetClientByIdAsync(id);
            response!.ClientId.Should().Be(id);
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    // [Fact, TestPriority(3)]
    // public async Task HTTPGetClient_Should_Return_CorrectClient()
    // {
    //     var sut = _fixture.SubjectsUnderTest[sutByHttp];
    //     var clients = await sut.GetClientsAsync();
    //     clients.Count.Should().Be(ClientsFixtureBootstrap.TotalClientsCount);
    //     uint id = clients[0].ClientId;
    //     var response = await sut.GetClientByIdAsync(id);
    //     response!.ClientId.Should().Be(id);
    // }

    [Fact, TestPriority(3)]
    public async Task GetStats_Should_ReturnValidResponse()
    {
        // act
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            var response = await sut.Client.GetStatsAsync();
            response.Should().NotBeNull();
            response!.MessagesCount.Should().Be(0);
            response.PartitionsCount.Should().Be(0);
            response.StreamsCount.Should().Be(0);
            response.TopicsCount.Should().Be(0);
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
}