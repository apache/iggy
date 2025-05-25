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
using Iggy_SDK;
using Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;
using Iggy_SDK_Tests.Utils;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Contracts.Http.Auth;
using Iggy_SDK.Enums;
using Iggy_SDK.Exceptions;

namespace Iggy_SDK_Tests.E2ETests;

[TestCaseOrderer("Iggy_SDK_Tests.Utils.PriorityOrderer", "Iggy_SDK_Tests")]
public sealed class UsersE2E : IClassFixture<IggyTcpUsersFixture>
{
    private readonly IggyTcpUsersFixture _fixture;
    public UsersE2E(IggyTcpUsersFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact, TestPriority(1)]
    public async Task CreateUser_Should_CreateUser_Successfully()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Client.Invoking(async x =>
                await x.CreateUser(UsersFixtureBootstrap.UserRequest)
            )
            .Should()
            .NotThrowAsync();
        }));
        
        await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(2)]
    public async Task CreateUser_Duplicate_Should_Throw_InvalidResponse()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Client.Invoking(async x =>
                await x.CreateUser(UsersFixtureBootstrap.UserRequest)
            )
            .Should()
            .ThrowExactlyAsync<InvalidResponseException>();
        }));
        
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(3)]
    public async Task GetUser_Should_ReturnValidResponse()
    {
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            // act
            var response = await sut.Client.GetUser(Identifier.Numeric(2));
            
            // assert
            response.Should().NotBeNull();
            response!.Id.Should().Be(2);
            response.Username.Should().Be(UsersFixtureBootstrap.UserRequest.Username);
            response.Status.Should().Be(UsersFixtureBootstrap.UserRequest.Status);
            response.Permissions!.Global.Should().NotBeNull();
            response.Permissions.Global.Should().BeEquivalentTo(UsersFixtureBootstrap.UserRequest.Permissions!.Global);
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(4)]
    public async Task GetUsers_Should_ReturnValidResponse()
    {
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            // act
            var response = await sut.Client.GetUsers();
            
            // assert
            response.Should().NotBeEmpty();
            response.Should().HaveCount(2);
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(5)]
    public async Task UpdateUser_Should_UpdateUser_Successfully()
    {
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            //act
            await sut.Client.Invoking(async x =>
                await x.UpdateUser(new UpdateUserRequest
                {
                    UserId = Identifier.Numeric(2), Username = UsersFixtureBootstrap.NewUsername, UserStatus = UserStatus.Active
                })).Should().NotThrowAsync();
            
            var user = await sut.Client.GetUser(Identifier.Numeric(2));
            
            // assert
            user!.Username.Should().Be(UsersFixtureBootstrap.NewUsername);
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(6)]
    public async Task UpdatePermissions_Should_UpdatePermissions_Successfully()
    {
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            // act
            await sut.Client.Invoking(async x =>
                await x.UpdatePermissions(new UpdateUserPermissionsRequest
                {
                    UserId = Identifier.Numeric(2), Permissions = UsersFixtureBootstrap.UpdatePermissionsRequest
                })).Should().NotThrowAsync();
            
            var user = await sut.Client.GetUser(Identifier.Numeric(2));
            
            // assert
            user!.Permissions!.Global.Should().NotBeNull();
            user.Permissions.Should().BeEquivalentTo(UsersFixtureBootstrap.UpdatePermissionsRequest);
            user.Permissions!.Streams.Should().NotBeNull();
            user.Permissions.Streams.Should().HaveCount(1);
            
            user.Permissions.Global.ManageServers.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Global!.ManageServers);
            user.Permissions.Global.ManageUsers.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Global!.ManageUsers);
            user.Permissions.Global.ManageStreams.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Global!.ManageStreams);
            user.Permissions.Global.ManageTopics.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Global!.ManageTopics);
            user.Permissions.Global.PollMessages.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Global!.PollMessages);
            user.Permissions.Global.ReadServers.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Global!.ReadServers);
            user.Permissions.Global.ReadStreams.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Global!.ReadStreams);
            user.Permissions.Global.ReadTopics.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Global!.ReadTopics);
            user.Permissions.Global.ReadUsers.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Global!.ReadUsers);
            
            var stream = user.Permissions.Streams!.FirstOrDefault();
            stream.Should().NotBeNull();
            stream!.Key.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Streams!.First().Key);
            stream.Value.Should().NotBeNull();
            stream.Value.ManageStream.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Streams!.First().Value.ManageStream);
            stream.Value.ManageTopics.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Streams!.First().Value.ManageTopics);
            stream.Value.ReadStream.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Streams!.First().Value.ReadStream);
            stream.Value.SendMessages.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Streams!.First().Value.SendMessages);
            stream.Value.ReadTopics.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Streams!.First().Value.ReadTopics);
            stream.Value.PollMessages.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Streams!.First().Value.PollMessages);
            stream.Value.Topics.Should().NotBeNull();
            
            stream.Value.Topics!.Should().HaveCount(1);
            stream.Value.Topics!.Should().ContainKey(UsersFixtureBootstrap.UpdatePermissionsRequest.Streams!.First().Value.Topics!.First().Key);
            stream.Value.Topics!.First().Value.Should().NotBeNull();
            stream.Value.Topics!.First().Key.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Streams!.First().Value.Topics!.First().Key);
            stream.Value.Topics!.First().Value.ManageTopic.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Streams!.First().Value.Topics!.First().Value.ManageTopic);
            stream.Value.Topics!.First().Value.PollMessages.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Streams!.First().Value.Topics!.First().Value.PollMessages);
            stream.Value.Topics!.First().Value.ReadTopic.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Streams!.First().Value.Topics!.First().Value.ReadTopic);
            stream.Value.Topics!.First().Value.SendMessages.Should().Be(UsersFixtureBootstrap.UpdatePermissionsRequest.Streams!.First().Value.Topics!.First().Value.SendMessages);
            
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(7)]
    public async Task ChangePassword_Should_ChangePassword_Successfully()
    {
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            // act
            await sut.Client.Invoking(async x =>
                await x.ChangePassword(new ChangePasswordRequest()
                {
                    UserId = Identifier.Numeric(2), 
                    CurrentPassword = "user1",
                    NewPassword = "user2"
                })).Should().NotThrowAsync();
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(8)]
    public async Task DeleteUser_Should_DeleteUser_Successfully()
    {
        var task = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            // act & assert
            await sut.Client.Invoking(async x =>
                await x.DeleteUser(Identifier.Numeric(2))).Should().NotThrowAsync();
        })).ToArray();
        
        await Task.WhenAll(task);
    }
    
    [Fact, TestPriority(8)]
    public async Task LogoutUser_Should_LogoutUser_Successfully()
    {
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            // act & assert
            await sut.Client.Invoking(async x =>
                await x.LogoutUser()).Should().NotThrowAsync();
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
}

