// // Licensed to the Apache Software Foundation (ASF) under one
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

using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Contracts.Http.Auth;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Apache.Iggy.Tests.Integrations.Helpers;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

public class UsersTests
{
    private const string Username = "fixture_users_user_1";
    private const string NewUsername = "fixture_users_user_1_new";

    [ClassDataSource<UsersFixture>(Shared = SharedType.PerClass)]
    public required UsersFixture Fixture { get; init; }


    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreateUser_Should_CreateUser_Successfully(Protocol protocol)
    {
        var request = new CreateUserRequest(Username.GetWithProtocol(protocol), "test_password_1", UserStatus.Active,
            null);

        var result = await Fixture.Clients[protocol].CreateUser(request.Username, request.Password, request.Status);
        result.ShouldNotBeNull();
        result.Username.ShouldBe(request.Username);
        result.Status.ShouldBe(request.Status);
        result.Id.ShouldBeGreaterThan(0u);
        result.CreatedAt.ShouldBeGreaterThan(0u);
    }

    [Test]
    [DependsOn(nameof(CreateUser_Should_CreateUser_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreateUser_Duplicate_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var request = new CreateUserRequest(Username.GetWithProtocol(protocol), "test1", UserStatus.Active, null);

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(Fixture.Clients[protocol]
            .CreateUser(request.Username, request.Password, request.Status));
    }

    [Test]
    [DependsOn(nameof(CreateUser_Duplicate_Should_Throw_InvalidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetUser_WithoutPermissions_Should_ReturnValidResponse(Protocol protocol)
    {
        var response = await Fixture.Clients[protocol].GetUser(Identifier.String(Username.GetWithProtocol(protocol)));

        response.ShouldNotBeNull();
        response.Id.ShouldBeGreaterThanOrEqualTo(0u);
        response.Username.ShouldBe(Username.GetWithProtocol(protocol));
        response.Status.ShouldBe(UserStatus.Active);
        response.CreatedAt.ShouldBeGreaterThan(0u);
        response.Permissions.ShouldBeNull();
    }

    [Test]
    [DependsOn(nameof(GetUser_WithoutPermissions_Should_ReturnValidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetUsers_Should_ReturnValidResponse(Protocol protocol)
    {
        IReadOnlyList<UserResponse> response = await Fixture.Clients[protocol].GetUsers();

        response.ShouldNotBeNull();
        response.ShouldNotBeEmpty();
        response.ShouldContain(user =>
            user.Username == Username.GetWithProtocol(protocol) && user.Status == UserStatus.Active);
        response.ShouldAllBe(user => user.CreatedAt > 0u);
        response.ShouldAllBe(user => user.Permissions == null);
    }

    [Test]
    [DependsOn(nameof(GetUsers_Should_ReturnValidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task UpdateUser_Should_UpdateUser_Successfully(Protocol protocol)
    {
        var newUsername = NewUsername.GetWithProtocol(protocol);
        await Should.NotThrowAsync(Fixture.Clients[protocol]
            .UpdateUser(Identifier.String(Username.GetWithProtocol(protocol)), newUsername, UserStatus.Active));

        var user = await Fixture.Clients[protocol].GetUser(Identifier.String(newUsername));

        user.ShouldNotBeNull();
        user.Id.ShouldBeGreaterThanOrEqualTo(0u);
        user.Username.ShouldBe(newUsername);
        user.Status.ShouldBe(UserStatus.Active);
        user.CreatedAt.ShouldBeGreaterThan(0u);
        user.Permissions.ShouldBeNull();

        await Should.NotThrowAsync(Fixture.Clients[protocol]
            .UpdateUser(Identifier.String(newUsername), Username.GetWithProtocol(protocol), UserStatus.Active));
    }

    [Test]
    [DependsOn(nameof(UpdateUser_Should_UpdateUser_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task UpdatePermissions_Should_UpdatePermissions_Successfully(Protocol protocol)
    {
        var permissions = CreatePermissions();
        await Should.NotThrowAsync(Fixture.Clients[protocol]
            .UpdatePermissions(Identifier.String(Username.GetWithProtocol(protocol)), permissions));

        var user = await Fixture.Clients[protocol].GetUser(Identifier.String(Username.GetWithProtocol(protocol)));

        user.ShouldNotBeNull();
        user.Id.ShouldBeGreaterThanOrEqualTo(0u);
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
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ChangePassword_Should_ChangePassword_Successfully(Protocol protocol)
    {
        await Should.NotThrowAsync(Fixture.Clients[protocol]
            .ChangePassword(Identifier.String(Username.GetWithProtocol(protocol)), "test_password_1", "user2"));
    }

    [Test]
    [DependsOn(nameof(ChangePassword_Should_ChangePassword_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ChangePassword_WrongCurrentPassword_Should_Throw_InvalidResponse(Protocol protocol)
    {
        await Should.ThrowAsync<IggyInvalidStatusCodeException>(Fixture.Clients[protocol]
            .ChangePassword(Identifier.String(Username.GetWithProtocol(protocol)), "test_password_1", "user2"));
    }

    [Test]
    [DependsOn(nameof(ChangePassword_WrongCurrentPassword_Should_Throw_InvalidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task LoginUser_Should_LoginUser_Successfully(Protocol protocol)
    {
        var client = await Fixture.IggyServerFixture.CreateClient(protocol);

        var response = await client.LoginUser(Username.GetWithProtocol(protocol), "user2");

        response.ShouldNotBeNull();
        response.UserId.ShouldBeGreaterThan(0);
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
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeleteUser_Should_DeleteUser_Successfully(Protocol protocol)
    {
        var userToRemove = await Fixture.Clients[protocol]
            .CreateUser("user-to-remove".GetWithProtocol(protocol), "test123", UserStatus.Active);
        userToRemove.ShouldNotBeNull();

        await Should.NotThrowAsync(Fixture.Clients[protocol]
            .DeleteUser(Identifier.String(userToRemove.Username)));

    }

    [Test]
    [DependsOn(nameof(DeleteUser_Should_DeleteUser_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task LogoutUser_Should_LogoutUser_Successfully(Protocol protocol)
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
