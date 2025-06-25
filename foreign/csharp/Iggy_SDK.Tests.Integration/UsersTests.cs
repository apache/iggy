﻿// // Licensed to the Apache Software Foundation (ASF) under one
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
using Apache.Iggy.Contracts.Http.Auth;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

[MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
public class UsersTests(Protocol protocol)
{
    private const string Username = "test_user_1";
    private const string NewUsername = "new_user_name";

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerClass)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    public async Task CreateUser_Should_CreateUser_Successfully()
    {
        var request = new CreateUserRequest
        {
            Username = Username,
            Password = "test_password_1",
            Status = UserStatus.Active
        };

        var result = await Fixture.Clients[protocol].CreateUser(request);
        result.ShouldNotBeNull();
        result.Username.ShouldBe(request.Username);
        result.Status.ShouldBe(request.Status);
        result.Id.ShouldBeGreaterThan(0u);
        result.CreatedAt.ShouldBeGreaterThan(0u);
    }

    [Test]
    [DependsOn(nameof(CreateUser_Should_CreateUser_Successfully))]
    public async Task CreateUser_Duplicate_Should_Throw_InvalidResponse()
    {
        var request = new CreateUserRequest
        {
            Username = Username,
            Password = "test1",
            Status = UserStatus.Active
        };

        await Should.ThrowAsync<InvalidResponseException>(Fixture.Clients[protocol].CreateUser(request));
    }

    [Test]
    [DependsOn(nameof(CreateUser_Duplicate_Should_Throw_InvalidResponse))]
    public async Task GetUser_WithoutPermissions_Should_ReturnValidResponse()
    {
        var response = await Fixture.Clients[protocol].GetUser(Identifier.Numeric(2));

        response.ShouldNotBeNull();
        response.Id.ShouldBe((uint)2);
        response.Username.ShouldBe(Username);
        response.Status.ShouldBe(UserStatus.Active);
        response.CreatedAt.ShouldBeGreaterThan(0u);
        response.Permissions.ShouldBeNull();
    }

    [Test]
    [DependsOn(nameof(GetUser_WithoutPermissions_Should_ReturnValidResponse))]
    public async Task GetUsers_Should_ReturnValidResponse()
    {
        IReadOnlyList<UserResponse> response = await Fixture.Clients[protocol].GetUsers();

        response.ShouldNotBeNull();
        response.ShouldNotBeEmpty();
        response.ShouldContain(user => user.Id == 2 && user.Username == Username && user.Status == UserStatus.Active);
        response.ShouldAllBe(user => user.CreatedAt > 0u);
        response.ShouldAllBe(user => user.Permissions == null);
    }

    [Test]
    [DependsOn(nameof(GetUsers_Should_ReturnValidResponse))]
    public async Task UpdateUser_Should_UpdateUser_Successfully()
    {
        await Should.NotThrowAsync(Fixture.Clients[protocol].UpdateUser(new UpdateUserRequest
        {
            UserId = Identifier.Numeric(2), Username = "new_user_name", UserStatus = UserStatus.Active
        }));

        var user = await Fixture.Clients[protocol].GetUser(Identifier.Numeric(2));

        user.ShouldNotBeNull();
        user.Id.ShouldBe(2u);
        user.Username.ShouldBe("new_user_name");
        user.Status.ShouldBe(UserStatus.Active);
        user.CreatedAt.ShouldBeGreaterThan(0u);
        user.Permissions.ShouldBeNull();
    }

    [Test]
    [DependsOn(nameof(UpdateUser_Should_UpdateUser_Successfully))]
    public async Task UpdatePermissions_Should_UpdatePermissions_Successfully()
    {
        var permissions = CreatePermissions();
        await Should.NotThrowAsync(Fixture.Clients[protocol].UpdatePermissions(new UpdateUserPermissionsRequest
        {
            UserId = Identifier.Numeric(2),
            Permissions = permissions
        }));

        var user = await Fixture.Clients[protocol].GetUser(Identifier.Numeric(2));

        user.ShouldNotBeNull();
        user.Id.ShouldBe(2u);
        user.Permissions.ShouldNotBeNull();
        user.Permissions!.Global.ShouldNotBeNull();
        user.Permissions.Global.ManageServers.ShouldBeTrue();
        user.Permissions.Global.ManageUsers.ShouldBeTrue();
        user.Permissions.Global.ManageStreams.ShouldBeTrue();
        user.Permissions.Global.ManageTopics.ShouldBeTrue();
        user.Permissions.Global.PollMessages.ShouldBeTrue();
        user.Permissions.Global.ReadServers.ShouldBeTrue();
        user.Permissions.Global.ReadStreams.ShouldBeTrue();
        user.Permissions.Global.ReadTopics.ShouldBeTrue();
        user.Permissions.Global.ReadUsers.ShouldBeTrue();
        user.Permissions.Global.SendMessages.ShouldBeTrue();
        user.Permissions.Streams.ShouldNotBeNull();
        user.Permissions.Streams.ShouldContainKey(1);
        user.Permissions.Streams[1].ManageStream.ShouldBeTrue();
        user.Permissions.Streams[1].ManageTopics.ShouldBeTrue();
        user.Permissions.Streams[1].ReadStream.ShouldBeTrue();
        user.Permissions.Streams[1].SendMessages.ShouldBeTrue();
        user.Permissions.Streams[1].ReadTopics.ShouldBeTrue();
        user.Permissions.Streams[1].PollMessages.ShouldBeTrue();
        user.Permissions.Streams[1].Topics.ShouldNotBeNull();
        user.Permissions.Streams[1].Topics!.ShouldContainKey(1);
        user.Permissions.Streams[1].Topics![1].ManageTopic.ShouldBeTrue();
        user.Permissions.Streams[1].Topics![1].PollMessages.ShouldBeTrue();
        user.Permissions.Streams[1].Topics![1].ReadTopic.ShouldBeTrue();
        user.Permissions.Streams[1].Topics![1].SendMessages.ShouldBeTrue();
    }

    [Test]
    [DependsOn(nameof(UpdatePermissions_Should_UpdatePermissions_Successfully))]
    public async Task ChangePassword_Should_ChangePassword_Successfully()
    {
        await Should.NotThrowAsync(Fixture.Clients[protocol].ChangePassword(new ChangePasswordRequest
        {
            UserId = Identifier.Numeric(2),
            CurrentPassword = "test_password_1",
            NewPassword = "user2"
        }));
    }

    [Test]
    [DependsOn(nameof(ChangePassword_Should_ChangePassword_Successfully))]
    public async Task ChangePassword_WrongCurrentPassword_Should_Throw_InvalidResponse()
    {
        await Should.ThrowAsync<InvalidResponseException>(Fixture.Clients[protocol].ChangePassword(new ChangePasswordRequest
        {
            UserId = Identifier.Numeric(2),
            CurrentPassword = "test_password_1",
            NewPassword = "user2"
        }));
    }

    [Test]
    [DependsOn(nameof(ChangePassword_WrongCurrentPassword_Should_Throw_InvalidResponse))]
    public async Task LoginUser_Should_LoginUser_Successfully()
    {
        var client = Fixture.CreateClient(protocol);

        var response = await client.LoginUser(new LoginUserRequest
        {
            Username = NewUsername,
            Password = "user2"
        });

        response.ShouldNotBeNull();
        response.UserId.ShouldBe(2);
        switch (protocol)
        {
            case Protocol.Tcp:
                response.AccessToken.ShouldBeNull();
                break;
            case Protocol.Http:
                response.AccessToken.ShouldNotBeNull();
                break;
        }
    }

    [Test]
    [DependsOn(nameof(LoginUser_Should_LoginUser_Successfully))]
    public async Task DeleteUser_Should_DeleteUser_Successfully()
    {
        await Should.NotThrowAsync(Fixture.Clients[protocol].DeleteUser(Identifier.Numeric(2)));
    }

    [Test]
    [DependsOn(nameof(DeleteUser_Should_DeleteUser_Successfully))]
    public async Task LogoutUser_Should_LogoutUser_Successfully()
    {
        // act & assert
        await Should.NotThrowAsync(Fixture.Clients[protocol].LogoutUser());
    }

    private static Permissions CreatePermissions()
    {
        return new Permissions
        {
            Global = new GlobalPermissions
            {
                ManageServers = true,
                ManageUsers = true,
                ManageStreams = true,
                ManageTopics = true,
                PollMessages = true,
                ReadServers = true,
                ReadStreams = true,
                ReadTopics = true,
                ReadUsers = true,
                SendMessages = true
            },
            Streams = new Dictionary<int, StreamPermissions>
            {
                {
                    1, new StreamPermissions
                    {
                        ManageStream = true,
                        ManageTopics = true,
                        ReadStream = true,
                        SendMessages = true,
                        ReadTopics = true,
                        PollMessages = true,
                        Topics = new Dictionary<int, TopicPermissions>
                        {
                            {
                                1, new TopicPermissions
                                {
                                    ManageTopic = true,
                                    PollMessages = true,
                                    ReadTopic = true,
                                    SendMessages = true
                                }
                            }
                        }
                    }
                }
            }
        };
    }
}