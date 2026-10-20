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

using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
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
        var result = await client.CreateUserAsync(username, "test_password_1", UserStatus.Active);
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
        await client.CreateUserAsync(username, "test1", UserStatus.Active);

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(client.CreateUserAsync(username, "test1",
            UserStatus.Active));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetUser_WithoutPermissions_Should_ReturnValidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var username = $"get-{Guid.NewGuid():N}"[..20];
        await client.CreateUserAsync(username, "test1", UserStatus.Active);

        var response = await client.GetUserAsync(Identifier.String(username));

        response.ShouldNotBeNull();
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
        await client.CreateUserAsync(username, "test1", UserStatus.Active);

        IReadOnlyList<UserResponse> response = await client.GetUsersAsync();

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
        await client.CreateUserAsync(username, "test1", UserStatus.Active);

        await Should.NotThrowAsync(client.UpdateUserAsync(Identifier.String(username), newUsername, UserStatus.Active));

        var user = await client.GetUserAsync(Identifier.String(newUsername));

        user.ShouldNotBeNull();
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
        await client.CreateUserAsync(username, "test1", UserStatus.Active);

        // The server refuses a grant on a stream or topic that does not exist.
        var stream = await client.CreateStreamAsync($"perm-{Guid.NewGuid():N}");
        stream.ShouldNotBeNull();
        var topic = await client.CreateTopicAsync(Identifier.Numeric(stream.Id), "perm-topic", 1);
        topic.ShouldNotBeNull();

        var permissions = CreatePermissions(stream.Id, topic.Id);
        await Should.NotThrowAsync(client.UpdatePermissionsAsync(Identifier.String(username), permissions));

        var user = await client.GetUserAsync(Identifier.String(username));

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
        user.Permissions.Streams.ShouldContainKey(stream.Id);
        user.Permissions.Streams[stream.Id].ManageStream.ShouldBeTrue();
        user.Permissions.Streams[stream.Id].ManageTopics.ShouldBeTrue();
        user.Permissions.Streams[stream.Id].ReadStream.ShouldBeTrue();
        user.Permissions.Streams[stream.Id].SendMessages.ShouldBeTrue();
        user.Permissions.Streams[stream.Id].ReadTopics.ShouldBeTrue();
        user.Permissions.Streams[stream.Id].PollMessages.ShouldBeTrue();
        user.Permissions.Streams[stream.Id].Topics.ShouldNotBeNull();
        user.Permissions.Streams[stream.Id].Topics!.ShouldContainKey(topic.Id);
        user.Permissions.Streams[stream.Id].Topics![topic.Id].ManageTopic.ShouldBeTrue();
        user.Permissions.Streams[stream.Id].Topics![topic.Id].PollMessages.ShouldBeTrue();
        user.Permissions.Streams[stream.Id].Topics![topic.Id].ReadTopic.ShouldBeTrue();
        user.Permissions.Streams[stream.Id].Topics![topic.Id].SendMessages.ShouldBeTrue();

        await client.DeleteStreamAsync(Identifier.Numeric(stream.Id));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ChangePassword_Should_ChangePassword_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var username = $"chpw-{Guid.NewGuid():N}"[..20];
        await client.CreateUserAsync(username, "old_password", UserStatus.Active);

        await Should.NotThrowAsync(client.ChangePasswordAsync(Identifier.String(username), "old_password",
            "new_password"));

        // Verify password was actually changed by logging in with the new credentials
        var loginClient = await Fixture.CreateClient(protocol, true);
        var loginResponse = await loginClient.LoginUserAsync(username, "new_password");
        loginResponse.ShouldNotBeNull();
        loginResponse.UserId.ShouldBeGreaterThan(0u);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ChangePassword_WrongCurrentPassword_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var username = $"chpwf-{Guid.NewGuid():N}"[..20];
        await client.CreateUserAsync(username, "correct_password", UserStatus.Active);

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(client.ChangePasswordAsync(Identifier.String(username),
            "wrong_password", "new_password"));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task LoginUser_Should_LoginUser_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var username = $"login-{Guid.NewGuid():N}"[..20];
        await client.CreateUserAsync(username, "login_password", UserStatus.Active);

        var loginClient = await Fixture.CreateClient(protocol, true);
        var response = await loginClient.LoginUserAsync(username, "login_password");

        response.ShouldNotBeNull();
        response.UserId.ShouldBeGreaterThan(0u);
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
        var userToRemove = await client.CreateUserAsync(username, "test123", UserStatus.Active);
        userToRemove.ShouldNotBeNull();

        await Should.NotThrowAsync(client.DeleteUserAsync(Identifier.String(userToRemove.Username)));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task LogoutUser_Should_LogoutUser_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        await Should.NotThrowAsync(client.LogoutUserAsync());
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Permissions_Should_RejectDanglingGrants_And_PruneDeletedResources(Protocol protocol)
    {
        const uint missingResourceId = uint.MaxValue;
        const int streamNotFound = 1009;
        const int topicNotFound = 2010;
        var client = await Fixture.CreateAuthenticatedClient(protocol);
        var username = $"grants-{Guid.NewGuid():N}"[..20];
        var stream = await client.CreateStreamAsync($"grants-{Guid.NewGuid():N}");
        stream.ShouldNotBeNull();
        var streamId = Identifier.Numeric(stream.Id);
        var topic = await client.CreateTopicAsync(streamId, "grants-topic", 1);
        topic.ShouldNotBeNull();
        var permissions = CreatePermissions(stream.Id, topic.Id);
        var user = await client.CreateUserAsync(username, "test-password", UserStatus.Active, permissions);
        user.ShouldNotBeNull();
        var userId = Identifier.Numeric(user.Id);

        foreach (var (grantStream, grantTopic, code) in new[]
                 {
                     (missingResourceId, topic.Id, streamNotFound),
                     (stream.Id, missingResourceId, topicNotFound)
                 })
        {
            var dangling = CreatePermissions(grantStream, grantTopic);
            var createError = await Should.ThrowAsync<IggyInvalidStatusCodeException>(
                client.CreateUserAsync($"missing-{Guid.NewGuid():N}"[..20], "test-password", UserStatus.Active, dangling));
            createError.StatusCode.ShouldBe(code);
            var updateError = await Should.ThrowAsync<IggyInvalidStatusCodeException>(
                client.UpdatePermissionsAsync(userId, dangling));
            updateError.StatusCode.ShouldBe(code);
            var unchanged = await client.GetUserAsync(userId);
            unchanged.ShouldNotBeNull();
            unchanged.Permissions.ShouldNotBeNull();
            unchanged.Permissions.Streams![stream.Id].Topics!.ShouldContainKey(topic.Id);
        }

        await client.DeleteTopicAsync(streamId, Identifier.Numeric(topic.Id));
        var afterTopic = await client.GetUserAsync(userId);
        afterTopic.ShouldNotBeNull();
        afterTopic.Permissions.ShouldNotBeNull();
        (afterTopic.Permissions.Streams![stream.Id].Topics?.Count ?? 0).ShouldBe(0);
        afterTopic.Permissions.Streams[stream.Id].PollMessages.ShouldBeTrue();
        await client.DeleteStreamAsync(streamId);
        var afterStream = await client.GetUserAsync(userId);
        afterStream.ShouldNotBeNull();
        afterStream.Permissions.ShouldNotBeNull();
        (afterStream.Permissions.Streams?.Count ?? 0).ShouldBe(0);
        afterStream.Permissions.Global.PollMessages.ShouldBeTrue();
        await client.DeleteUserAsync(userId);
    }

    private static Permissions CreatePermissions(uint streamId, uint topicId)
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
            Streams = new Dictionary<uint, StreamPermissions>
            {
                {
                    streamId, new StreamPermissions
                    {
                        ManageStream = true,
                        ManageTopics = true,
                        ReadStream = true,
                        SendMessages = true,
                        ReadTopics = true,
                        PollMessages = true,
                        Topics = new Dictionary<uint, TopicPermissions>
                        {
                            {
                                topicId, new TopicPermissions
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
