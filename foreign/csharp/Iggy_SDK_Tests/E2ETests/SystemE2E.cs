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
            foreach (var client in clients)
            {
                client.ClientId.Should().NotBe(0);
                client.UserId.Should().Be(1);
                client.Address.Should().NotBeNullOrEmpty();
                client.Transport.Should().Be("TCP");
            }
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
            response.UserId.Should().Be(1);
            response.Address.Should().NotBeNullOrEmpty();
            response.Transport.Should().Be("TCP");
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(3)]
    public async Task GetMe_Should_Return_MyClient()
    {
        await _fixture.Invoking(x=>x.HttpClient.Client.GetMeAsync())
            .Should()
            .ThrowAsync<NotImplementedException>();


        var me = await _fixture.TcpClient.Client.GetMeAsync();
        me.Should().NotBeNull();
        me.ClientId.Should().NotBe(0);
        me.UserId.Should().Be(1);
        me.Address.Should().NotBeNullOrEmpty();
        me.Transport.Should().Be("TCP");
    }

    [Fact, TestPriority(4)]
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
    
    [Fact, TestPriority(5)]
    public async Task Ping_Should_Pong()
    {
        // act
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(async x => await x.Client.PingAsync())
                .Should()
                .NotThrowAsync();
            
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
}