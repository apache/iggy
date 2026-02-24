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
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

public class UsersTests
{
    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreateUser_Should_CreateUser_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var username = $"user-{Guid.NewGuid():N}"[..20];
        var result = await client.CreateUser(username, "test_password_1", UserStatus.Active);
        result.ShouldNotBeNull();
        result.Username.ShouldBe(username);
        result.Status.ShouldBe(UserStatus.Active);
        result.Id.ShouldBeGreaterThan(0u);
        result.CreatedAt.ShouldBeGreaterThan(0u);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreateUser_Duplicate_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var username = $"dup-{Guid.NewGuid():N}"[..20];
        await client.CreateUser(username, "test1", UserStatus.Active);

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(
            client.CreateUser(username, "test1", UserStatus.Active));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetUser_WithoutPermissions_Should_ReturnValidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var username = $"get-{Guid.NewGuid():N}"[..20];
        await client.CreateUser(username, "test1", UserStatus.Active);

        var response = await client.GetUser(Identifier.String(username));

        response.ShouldNotBeNull();
        response.Id.ShouldBeGreaterThanOrEqualTo(0u);
        response.Username.ShouldBe(username);
        response.Status.ShouldBe(UserStatus.Active);
        response.CreatedAt.ShouldBeGreaterThan(0u);
        response.Permissions.ShouldBeNull();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetUsers_Should_ReturnValidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var username = $"lst-{Guid.NewGuid():N}"[..20];
        await client.CreateUser(username, "test1", UserStatus.Active);

        IReadOnlyList<UserResponse> response = await client.GetUsers();

        response.ShouldNotBeNull();
        response.ShouldNotBeEmpty();
        response.ShouldContain(user => user.Username == username && user.Status == UserStatus.Active);
        response.ShouldAllBe(user => user.CreatedAt > 0u);
        response.ShouldAllBe(user => user.Permissions == null);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task UpdateUser_Should_UpdateUser_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var username = $"upd-{Guid.NewGuid():N}"[..20];
        var newUsername = $"new-{Guid.NewGuid():N}"[..20];
        await client.CreateUser(username, "test1", UserStatus.Active);

        await Should.NotThrowAsync(client.UpdateUser(Identifier.String(username), newUsername, UserStatus.Active));

        var user = await client.GetUser(Identifier.String(newUsername));

        user.ShouldNotBeNull();
        user.Id.ShouldBeGreaterThanOrEqualTo(0u);
        user.Username.ShouldBe(newUsername);
        user.Status.ShouldBe(UserStatus.Active);
        user.CreatedAt.ShouldBeGreaterThan(0u);
        user.Permissions.ShouldBeNull();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task UpdatePermissions_Should_UpdatePermissions_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var username = $"perm-{Guid.NewGuid():N}"[..20];
        await client.CreateUser(username, "test1", UserStatus.Active);

        var permissions = CreatePermissions();
        await Should.NotThrowAsync(client.UpdatePermissions(Identifier.String(username), permissions));

        var user = await client.GetUser(Identifier.String(username));

        user.ShouldNotBeNull();
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
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ChangePassword_Should_ChangePassword_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var username = $"chpw-{Guid.NewGuid():N}"[..20];
        await client.CreateUser(username, "old_password", UserStatus.Active);

        await Should.NotThrowAsync(client.ChangePassword(Identifier.String(username), "old_password", "new_password"));

        // Verify password was actually changed by logging in with the new credentials
        var loginClient = await Fixture.CreateClient(protocol);
        var loginResponse = await loginClient.LoginUser(username, "new_password");
        loginResponse.ShouldNotBeNull();
        loginResponse.UserId.ShouldBeGreaterThan(0);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ChangePassword_WrongCurrentPassword_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var username = $"chpwf-{Guid.NewGuid():N}"[..20];
        await client.CreateUser(username, "correct_password", UserStatus.Active);

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(
            client.ChangePassword(Identifier.String(username), "wrong_password", "new_password"));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task LoginUser_Should_LoginUser_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var username = $"login-{Guid.NewGuid():N}"[..20];
        await client.CreateUser(username, "login_password", UserStatus.Active);

        var loginClient = await Fixture.CreateClient(protocol);
        var response = await loginClient.LoginUser(username, "login_password");

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
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeleteUser_Should_DeleteUser_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var username = $"del-{Guid.NewGuid():N}"[..20];
        var userToRemove = await client.CreateUser(username, "test123", UserStatus.Active);
        userToRemove.ShouldNotBeNull();

        await Should.NotThrowAsync(client.DeleteUser(Identifier.String(userToRemove.Username)));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task LogoutUser_Should_LogoutUser_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        await Should.NotThrowAsync(client.LogoutUser());
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
