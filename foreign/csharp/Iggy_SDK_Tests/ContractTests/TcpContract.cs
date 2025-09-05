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

using System.Buffers.Binary;
using System.Text;
using Apache.Iggy.Contracts.Http.Auth;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Enums;
using Apache.Iggy.Extensions;
using Apache.Iggy.Kinds;
using Apache.Iggy.Tests.Utils;
using Apache.Iggy.Tests.Utils.Messages;
using Apache.Iggy.Tests.Utils.Topics;
using Apache.Iggy.Tests.Utils.Users;
using FluentAssertions;
using Partitioning = Apache.Iggy.Enums.Partitioning;

namespace Apache.Iggy.Tests.ContractTests;

public sealed class TcpContract
{
    [Fact]
    public void TcpContracts_DeletePersonalRequestToken_HasValidBytes()
    {
        // Arrange
        var name = "TestUser";

        // Act
        var result = TcpContracts.DeletePersonalRequestToken(name);

        // Assert
        Assert.Equal(5 + name.Length, result.Length);
        Assert.Equal((byte)name.Length, result[0]);
        Assert.Equal(Encoding.UTF8.GetBytes(name), result[1..(1 + name.Length)]);
    }

    [Fact]
    public void TcpContracts_CreatePersonalAccessToken_HasValidBytes_ValidExpiry()
    {
        // Arrange
        var name = "TestUser";
        var expiry = 3600u;

        // Act
        var result = TcpContracts.CreatePersonalAccessToken(name, expiry);

        // Assert
        Assert.Equal(9 + name.Length, result.Length); // The expected length
        Assert.Equal(Encoding.UTF8.GetBytes(name), result[1..(1 + name.Length)]); // The expected length of the name
        Assert.Equal((uint)3600, BinaryPrimitives.ReadUInt32LittleEndian(result[(1 + name.Length)..]));
    }

    [Fact]
    public void TcpContracts_CreatePersonalAccessToken_HasValidBytes_NullExpiry()
    {
        // Arrange
        var name = "TestUser";
        ulong? expiry = null;

        // Act
        var result = TcpContracts.CreatePersonalAccessToken(name, expiry);

        // Assert
        Assert.Equal(9 + name.Length, result.Length); // The expected length
        Assert.Equal(Encoding.UTF8.GetBytes(name), result[1..(1 + name.Length)]); // The expected length of the name
        Assert.Equal((uint)0, BinaryPrimitives.ReadUInt64LittleEndian(result[(1 + name.Length)..]));
    }

    [Fact]
    public void TcpContracts_LoginUser_HasCorrectBytes()
    {
        // Arrange
        var request = new LoginUserRequest("testuser", "testpassword", null, null);

        // Act
        var result = TcpContracts.LoginUser(request.Username, request.Password, request.Version, request.Context);

        // Assert
        var expectedLength = request.Username.Length + request.Password.Length + 2 + 4 + 4;
        Assert.Equal(expectedLength, result.Length);

        var position = 0;
        var usernameLength = result[position];
        position += 1;
        var usernameBytes = result[position..(position + usernameLength)];
        position += usernameLength;
        var passwordLength = result[position];
        position += 1;
        var passwordBytes = result[position..(position + passwordLength)];

        var decodedUsername = Encoding.UTF8.GetString(usernameBytes);
        var decodedPassword = Encoding.UTF8.GetString(passwordBytes);

        Assert.Equal(request.Username, decodedUsername);
        Assert.Equal(request.Password, decodedPassword);
    }

    [Fact]
    public void TcpContracts_LoginUserWithOptional_HasCorrectBytes()
    {
        // Arrange
        var request = new LoginUserRequest("testuser", "testpassword", "1.0.0", "optional context");

        // Act
        var result = TcpContracts.LoginUser(request.Username, request.Password, request.Version, request.Context);

        // Assert
        var expectedLength = 51;
        Assert.Equal(expectedLength, result.Length);

        var position = 0;
        var usernameLength = result[position];
        position += 1;
        var usernameBytes = result[position..(position + usernameLength)];
        position += usernameLength;
        var passwordLength = result[position];
        position += 1;
        var passwordBytes = result[position..(position + passwordLength)];
        position += passwordLength;
        var versionLength = BinaryPrimitives.ReadInt32LittleEndian(result[position..(position + 4)]);
        position += 4;
        var versionBytes = result[position..(position + versionLength)];
        position += versionLength;
        var contextLength = BinaryPrimitives.ReadInt32LittleEndian(result[position..(position + 4)]);
        position += 4;
        var contextBytes = result[position..(position + contextLength)];

        var decodedUsername = Encoding.UTF8.GetString(usernameBytes);
        var decodedPassword = Encoding.UTF8.GetString(passwordBytes);
        var decodedVersion = Encoding.UTF8.GetString(versionBytes);
        var decodedContext = Encoding.UTF8.GetString(contextBytes);

        Assert.Equal(request.Username, decodedUsername);
        Assert.Equal(request.Password, decodedPassword);
        Assert.Equal(request.Version, decodedVersion);
        Assert.Equal(request.Context, decodedContext);
    }

    [Fact]
    public void TcpContracts_UpdateUser_HasCorrectBytes()
    {
        // Arrange
        var userId = Identifier.Numeric(1);
        var request = new UpdateUserRequest("newusername", UserStatus.Inactive);

        // Act
        var result = TcpContracts.UpdateUser(userId, request.Username, request.UserStatus);

        // Assert
        var expectedLength = userId.Length + 2 +
                             (request.Username?.Length ?? 0) + 2 + 1 + 1;
        Assert.Equal(expectedLength, result.Length);

        var position = 2;
        var userIdBytes = result[position..(position + userId.Length)];
        position += userId.Length;
        var usernameFlag = result[position];
        position += 1;
        if (usernameFlag == 1)
        {
            var usernameLength = result[position];
            position += 1;
            var usernameBytes = result[position..(position + usernameLength)];
            position += usernameLength;
            var decodedUsername = Encoding.UTF8.GetString(usernameBytes);
            Assert.Equal(request.Username, decodedUsername);
        }
        else
        {
            Assert.Null(request.Username);
        }

        var statusFlag = result[position];
        position += 1;
        if (statusFlag == 1)
        {
            var userStatus = result[position] switch
            {
                1 => UserStatus.Active,
                2 => UserStatus.Inactive,
                _ => throw new ArgumentOutOfRangeException()
            };
            Assert.Equal(request.UserStatus, userStatus);
        }
        else
        {
            Assert.Null(request.UserStatus);
        }

        Assert.Equal(userId.Value, userIdBytes);
    }

    [Fact]
    public void TcpContracts_CreateUser_NoPermission_HasCorrectBytes()
    {
        // Arrange
        var request = new CreateUserRequest("testuser", "testpassword", UserStatus.Active, null);
        // Act
        var result = TcpContracts.CreateUser(request.Username, request.Password, request.Status, request.Permissions);

        // Assert
        var position = 0;

        Assert.Equal((byte)request.Username.Length, result[position]);
        position += 1;

        var usernameBytes = result[position..(position + request.Username.Length)];
        position += request.Username.Length;
        var decodedUsername = Encoding.UTF8.GetString(usernameBytes);
        Assert.Equal(request.Username, decodedUsername);

        Assert.Equal((byte)request.Password.Length, result[position]);
        position += 1;

        var passwordBytes = result[position..(position + request.Password.Length)];
        position += request.Password.Length;
        var decodedPassword = Encoding.UTF8.GetString(passwordBytes);
        Assert.Equal(request.Password, decodedPassword);

        var expectedStatusByte = request.Status switch
        {
            UserStatus.Active => (byte)1,
            UserStatus.Inactive => (byte)2,
            _ => throw new ArgumentOutOfRangeException()
        };
        Assert.Equal(expectedStatusByte, result[position]);
        position += 1;

        var permissionsFlag = result[position];
        position += 1;
        if (permissionsFlag == 1)
        {
            var permissionsSize = BinaryPrimitives.ReadInt32LittleEndian(result[position..(position + 4)]);
            position += 4;

            var permissionsBytes = result[position..(position + permissionsSize)];
        }
        else
        {
            Assert.Null(request.Permissions);
        }
    }

    [Fact]
    public void TcpContracts_ChangePassword_HasCorrectBytes()
    {
        // Arrange
        var userId = Identifier.Numeric(1);
        var currentPassword = "oldpassword";
        var newPassword = "newpassword";

        // Act
        var result = TcpContracts.ChangePassword(userId, currentPassword, newPassword);

        // Assert
        var position = 2;

        var userIdBytes = result[position..(position + userId.Length)];
        position += userId.Length;
        Assert.Equal(userId.Value, userIdBytes);

        Assert.Equal((byte)currentPassword.Length, result[position]);
        position += 1;

        var currentPasswordBytes = result[position..(position + currentPassword.Length)];
        position += currentPassword.Length;
        var decodedCurrentPassword = Encoding.UTF8.GetString(currentPasswordBytes);
        Assert.Equal(currentPassword, decodedCurrentPassword);

        Assert.Equal((byte)newPassword.Length, result[position]);
        position += 1;

        var newPasswordBytes = result[position..(position + newPassword.Length)];
        position += newPassword.Length;
        var decodedNewPassword = Encoding.UTF8.GetString(newPasswordBytes);
        Assert.Equal(newPassword, decodedNewPassword);
    }

    [Fact]
    public void TcpContracts_UpdatePermissions_HasCorrectBytes()
    {
        // Arrange
        var userId = Identifier.Numeric(1);
        var permissions = PermissionsFactory.CreatePermissions();

        // Act
        var result = TcpContracts.UpdatePermissions(userId, permissions);

        // Assert
        var position = 2;

        var userIdBytes = result[position..(position + userId.Length)];
        position += userId.Length;
        Assert.Equal(userId.Value, userIdBytes);

        var permissionsFlag = result[position];
        position += 1;
        if (permissionsFlag == 1)
        {
            var permissionsSize = BinaryPrimitives.ReadInt32LittleEndian(result[position..(position + 4)]);
            position += 4;

            var permissionsBytes = result[position..(position + permissionsSize)];

            var mappedPermissions = PermissionsFactory.PermissionsFromBytes(permissionsBytes);

            permissions.Global.Should().BeEquivalentTo(mappedPermissions.Global);

            if (permissions.Streams != null)
            {
                Assert.NotNull(mappedPermissions.Streams);

                foreach (var (streamId, stream) in permissions.Streams)
                {
                    Assert.True(mappedPermissions.Streams.ContainsKey(streamId));
                    var mappedStream = mappedPermissions.Streams[streamId];

                    Assert.Equal(stream.ManageStream, mappedStream.ManageStream);
                    Assert.Equal(stream.ReadStream, mappedStream.ReadStream);
                    Assert.Equal(stream.ManageTopics, mappedStream.ManageTopics);
                    Assert.Equal(stream.ReadTopics, mappedStream.ReadTopics);
                    Assert.Equal(stream.PollMessages, mappedStream.PollMessages);
                    Assert.Equal(stream.SendMessages, mappedStream.SendMessages);

                    if (stream.Topics != null)
                    {
                        Assert.NotNull(mappedStream.Topics);

                        foreach (var (topicId, topic) in stream.Topics)
                        {
                            Assert.True(mappedStream.Topics.ContainsKey(topicId));
                            var mappedTopic = mappedStream.Topics[topicId];

                            Assert.Equal(topic.ManageTopic, mappedTopic.ManageTopic);
                            Assert.Equal(topic.ReadTopic, mappedTopic.ReadTopic);
                            Assert.Equal(topic.PollMessages, mappedTopic.PollMessages);
                            Assert.Equal(topic.SendMessages, mappedTopic.SendMessages);
                        }
                    }
                    else
                    {
                        Assert.Null(mappedStream.Topics);
                    }
                }
            }
            else
            {
                Assert.Null(mappedPermissions.Streams);
            }
        }
        else
        {
            Assert.Null(permissions);
        }
    }

    [Fact]
    public void TcpContracts_CreateUser_WithPermission_HasCorrectBytes()
    {
        // Arrange
        var request = new CreateUserRequest("testuser", "testpassword", UserStatus.Active,
            PermissionsFactory.CreatePermissions());

        // Act
        var result = TcpContracts.CreateUser(request.Username, request.Password, request.Status, request.Permissions);

        // Assert
        var position = 0;

        Assert.Equal((byte)request.Username.Length, result[position]);
        position += 1;

        var usernameBytes = result[position..(position + request.Username.Length)];
        position += request.Username.Length;
        var decodedUsername = Encoding.UTF8.GetString(usernameBytes);
        Assert.Equal(request.Username, decodedUsername);

        Assert.Equal((byte)request.Password.Length, result[position]);
        position += 1;

        var passwordBytes = result[position..(position + request.Password.Length)];
        position += request.Password.Length;
        var decodedPassword = Encoding.UTF8.GetString(passwordBytes);
        Assert.Equal(request.Password, decodedPassword);

        var expectedStatusByte = request.Status switch
        {
            UserStatus.Active => (byte)1,
            UserStatus.Inactive => (byte)2,
            _ => throw new ArgumentOutOfRangeException()
        };
        Assert.Equal(expectedStatusByte, result[position]);
        position += 1;

        var permissionsFlag = result[position];
        position += 1;
        if (permissionsFlag == 1)
        {
            var permissionsSize = BinaryPrimitives.ReadInt32LittleEndian(result[position..(position + 4)]);
            position += 4;

            var permissionsBytes = result[position..(position + permissionsSize)];
            var mappedPermissions = PermissionsFactory.PermissionsFromBytes(permissionsBytes);
            request.Permissions!.Global.Should().BeEquivalentTo(mappedPermissions.Global);

            if (request.Permissions.Streams != null)
            {
                Assert.NotNull(mappedPermissions.Streams);

                foreach (var (streamId, stream) in request.Permissions.Streams)
                {
                    Assert.True(mappedPermissions.Streams.ContainsKey(streamId));
                    var mappedStream = mappedPermissions.Streams[streamId];

                    Assert.Equal(stream.ManageStream, mappedStream.ManageStream);
                    Assert.Equal(stream.ReadStream, mappedStream.ReadStream);
                    Assert.Equal(stream.ManageTopics, mappedStream.ManageTopics);
                    Assert.Equal(stream.ReadTopics, mappedStream.ReadTopics);
                    Assert.Equal(stream.PollMessages, mappedStream.PollMessages);
                    Assert.Equal(stream.SendMessages, mappedStream.SendMessages);

                    if (stream.Topics != null)
                    {
                        Assert.NotNull(mappedStream.Topics);

                        foreach (var (topicId, topic) in stream.Topics)
                        {
                            Assert.True(mappedStream.Topics.ContainsKey(topicId));
                            var mappedTopic = mappedStream.Topics[topicId];

                            Assert.Equal(topic.ManageTopic, mappedTopic.ManageTopic);
                            Assert.Equal(topic.ReadTopic, mappedTopic.ReadTopic);
                            Assert.Equal(topic.PollMessages, mappedTopic.PollMessages);
                            Assert.Equal(topic.SendMessages, mappedTopic.SendMessages);
                        }
                    }
                    else
                    {
                        Assert.Null(mappedStream.Topics);
                    }
                }
            }
            else
            {
                Assert.Null(request.Permissions);
            }
        }
    }

    [Fact]
    public void TcpContracts_ChangePasswordRequest_HasCorrectBytes()
    {
        // Arrange
        var userId = Identifier.Numeric(1);
        var currentPassword = "oldpassword";
        var newPassword = "newpassword";

        // Act
        var result = TcpContracts.ChangePassword(userId, currentPassword, newPassword);

        // Assert
        var expectedLength = userId.Length + 2 +
                             currentPassword.Length + newPassword.Length + 2;
        Assert.Equal(expectedLength, result.Length);

        // Validate bytes can be translated back to properties
        var position = 2;
        var userIdBytes = result[position..(position + userId.Length)];
        position += userId.Length;
        var currentPasswordLength = result[position];
        position += 1;
        var currentPasswordBytes = result[position..(position + currentPasswordLength)];
        position += currentPasswordLength;
        var newPasswordLength = result[position];
        position += 1;
        var newPasswordBytes = result[position..(position + newPasswordLength)];

        var decodedUserId = BinaryPrimitives.ReadInt32LittleEndian(userIdBytes);
        var decodedCurrentPassword = Encoding.UTF8.GetString(currentPasswordBytes);
        var decodedNewPassword = Encoding.UTF8.GetString(newPasswordBytes);

        Assert.Equal(userId.Value, userIdBytes);
        Assert.Equal(currentPassword, decodedCurrentPassword);
        Assert.Equal(newPassword, decodedNewPassword);
    }

    [Fact]
    public void TcpContracts_MessageFetchRequest_HasCorrectBytes()
    {
        // Arrange
        var request = MessageFactory.CreateMessageFetchRequestConsumer();
        var messageBufferSize = 23 + 2 + 4 + 2 + 2 + request.Consumer.Id.Length;
        var result = new byte[messageBufferSize];

        // Act
        TcpContracts.GetMessages(result, request.Consumer, request.StreamId, request.TopicId,
            request.PollingStrategy, request.Count, request.AutoCommit, request.PartitionId);

        // Assert
        Assert.Equal(result[0] switch
        {
            1 => ConsumerType.Consumer,
            2 => ConsumerType.ConsumerGroup,
            _ => throw new ArgumentOutOfRangeException()
        }, request.Consumer.Type);
        Assert.Equal(request.Consumer.Id.Kind.GetByte(), result[1]);
        Assert.Equal(request.StreamId.Value, BytesToIdentifierNumeric(result, 7).Value);
        Assert.Equal(request.TopicId.Value, BytesToIdentifierNumeric(result, 13).Value);
        Assert.Equal(request.StreamId.Kind, BytesToIdentifierNumeric(result, 7).Kind);
        Assert.Equal(request.TopicId.Kind, BytesToIdentifierNumeric(result, 13).Kind);
        Assert.Equal(request.StreamId.Length, BytesToIdentifierNumeric(result, 7).Length);
        Assert.Equal(request.TopicId.Length, BytesToIdentifierNumeric(result, 13).Length);
        Assert.Equal(request.PartitionId, BitConverter.ToUInt32(result[19..23]));
        Assert.Equal(result[23] switch
        {
            1 => MessagePolling.Offset,
            2 => MessagePolling.Timestamp,
            3 => MessagePolling.First,
            4 => MessagePolling.Last,
            5 => MessagePolling.Next,
            _ => throw new ArgumentOutOfRangeException()
        }, request.PollingStrategy.Kind);
        Assert.Equal(request.PollingStrategy.Value, BitConverter.ToUInt64(result[24..32]));
        Assert.Equal(request.Count, BitConverter.ToInt32(result[32..36]));
        Assert.Equal(request.AutoCommit, result[36] switch
        {
            0 => false,
            1 => true,
            _ => throw new ArgumentOutOfRangeException()
        });
    }


    [Fact]
    public void TcpContracts_MessageSendRequest_WithNoHeaders_HasCorrectBytes()
    {
        // Arrange
        var streamId = Identifier.Numeric(1);
        var topicId = Identifier.Numeric(1);
        var request = MessageFactory.CreateMessageSendRequest();
        var messageBufferSize = request.Messages.Sum(message => 56 + 16 + message.Payload.Length)
                                + request.Partitioning.Length + 22;
        var result = new byte[messageBufferSize];


        // Act
        TcpContracts.CreateMessage(result, streamId, topicId, request.Partitioning, request.Messages);

        //Assert
        Assert.Equal(22, BitConverter.ToInt32(result[..4])); // metadata
        Assert.Equal(streamId.Value, BytesToIdentifierNumeric(result, 4).Value);
        Assert.Equal(topicId.Value, BytesToIdentifierNumeric(result, 10).Value);
        Assert.Equal(streamId.Length, BytesToIdentifierNumeric(result, 4).Length);
        Assert.Equal(topicId.Length, BytesToIdentifierNumeric(result, 10).Length);
        Assert.Equal(streamId.Kind, BytesToIdentifierNumeric(result, 4).Kind);
        Assert.Equal(topicId.Kind, BytesToIdentifierNumeric(result, 10).Kind);
        Assert.Equal(request.Partitioning.Kind, result[16] switch
        {
            1 => Partitioning.Balanced,
            2 => Partitioning.PartitionId,
            3 => Partitioning.MessageKey,
            _ => throw new ArgumentOutOfRangeException()
        });
        Assert.Equal(request.Partitioning.Length, result[17]);
        Assert.Equal(request.Partitioning.Value.Length, result[18..(18 + request.Partitioning.Length)].Length);

        var currentIndex = 26 + 16 * 2;
        foreach (var message in request.Messages)
        {
            Assert.Equal(message.Header.Checksum, BitConverter.ToUInt64(result[currentIndex..(currentIndex + 8)]));
            Assert.Equal(message.Header.Id,
                BinaryPrimitives.ReadUInt128LittleEndian(result[(currentIndex + 8)..(currentIndex + 24)]));
            Assert.Equal(message.Header.Offset,
                BitConverter.ToUInt64(result[(currentIndex + 24)..(currentIndex + 32)]));
            Assert.Equal(message.Header.Timestamp,
                DateTimeOffsetUtils.FromUnixTimeMicroSeconds(
                    BitConverter.ToUInt64(result[(currentIndex + 32)..(currentIndex + 40)])));
            Assert.Equal(message.Header.OriginTimestamp,
                BitConverter.ToUInt64(result[(currentIndex + 40)..(currentIndex + 48)]));
            var userHeadersLength = BitConverter.ToInt32(result[(currentIndex + 48)..(currentIndex + 52)]);
            Assert.Equal(message.Header.UserHeadersLength, userHeadersLength);
            var payloadLength = BitConverter.ToInt32(result[(currentIndex + 52)..(currentIndex + 56)]);
            Assert.Equal(message.Header.PayloadLength, payloadLength);

            currentIndex += 56;
            var payload = result[currentIndex..(currentIndex + payloadLength)].ToArray();
            currentIndex += payloadLength;

            Assert.Equal(message.Payload.Length, payload.Length);
            Assert.Equal(message.Payload, payload);
        }
    }


    [Fact]
    public void TcpContracts_CreateStream_HasCorrectBytes()
    {
        // Arrange
        var name = "test-stream" + Random.Shared.Next(1, 69) + Utility.RandomString(12).ToLower();
        var streamId = (uint)Random.Shared.Next(1, 2137);

        // Act
        Span<byte> result = TcpContracts.CreateStream(name, streamId).AsSpan();

        // Assert
        var expectedBytesLength = sizeof(int) + name.Length + 1;

        Assert.Equal(expectedBytesLength, result.Length);
        Assert.Equal(streamId, BitConverter.ToUInt32(result[..5]));
        Assert.Equal(name, Encoding.UTF8.GetString(result[5..]));
    }

    [Fact]
    public void TcpContracts_CreateGroup_HasCorrectBytes()
    {
        // Arrange
        var streamId = Identifier.String("my-stream");
        var topicId = Identifier.String("my-topic");
        var name = Utility.RandomString(69);
        var consumerGroupId = Random.Shared.Next(1, 69);

        // Act
        Span<byte> result = TcpContracts.CreateGroup(streamId, topicId, name, (uint)consumerGroupId).AsSpan();

        // Assert
        var expectedBytesLength = 2 + streamId.Length + 2 + topicId.Length + 4 + 1 + name.Length;

        Assert.Equal(expectedBytesLength, result.Length);
        Assert.Equal(streamId.Value, BytesToIdentifierString(result, 0).Value);
        Assert.Equal(topicId.Value, BytesToIdentifierString(result, 2 + streamId.Length).Value);
        Assert.Equal(streamId.Kind, BytesToIdentifierString(result, 0).Kind);
        Assert.Equal(topicId.Kind, BytesToIdentifierString(result, 2 + streamId.Length).Kind);
        Assert.Equal(streamId.Length, BytesToIdentifierString(result, 0).Length);
        Assert.Equal(topicId.Length, BytesToIdentifierString(result, 2 + streamId.Length).Length);
        var position = 2 + streamId.Length + 2 + topicId.Length + 4;
        Assert.Equal(name.Length, result[position]);
    }

    [Fact]
    public void TcpContracts_DeleteGroup_HasCorrectBytes()
    {
        // Arrange
        var streamId = Identifier.Numeric(1);
        var topicId = Identifier.Numeric(1);
        var groupId = Identifier.Numeric(1);

        // Act
        Span<byte> result = TcpContracts.DeleteGroup(streamId, topicId, groupId).AsSpan();

        // Assert
        var expectedBytesLength = 2 + streamId.Length + 2 + topicId.Length + groupId.Length + 2;

        Assert.Equal(expectedBytesLength, result.Length);
        Assert.Equal(streamId.Value, BytesToIdentifierNumeric(result, 0).Value);
        Assert.Equal(topicId.Value, BytesToIdentifierNumeric(result, 6).Value);
        Assert.Equal(groupId.Value, BytesToIdentifierNumeric(result, 12).Value);
    }

    [Fact]
    public void TcpContracts_GetGroups_HasCorrectBytes()
    {
        // Arrange
        var streamId = Identifier.String("my-stream");
        var topicId = Identifier.Numeric(1);

        // Act
        Span<byte> result = TcpContracts.GetGroups(streamId, topicId).AsSpan();

        // Assert
        var expectedBytesLength = 2 + streamId.Length + 2 + topicId.Length;

        Assert.Equal(expectedBytesLength, result.Length);
        Assert.Equal(streamId.Value, BytesToIdentifierString(result, 0).Value);
        Assert.Equal(topicId.Value, BytesToIdentifierNumeric(result, 2 + streamId.Length).Value);
    }

    [Fact]
    public void TcpContracts_JoinGroup_HasCorrectBytes()
    {
        // Arrange
        var streamId = Identifier.Numeric(Random.Shared.Next(1, 10));
        var topicId = Identifier.Numeric(Random.Shared.Next(1, 10));
        var consumerGroupId = Identifier.Numeric(Random.Shared.Next(1, 10));

        // Act
        Span<byte> result = TcpContracts.JoinGroup(streamId, topicId, consumerGroupId).AsSpan();

        // Assert
        var expectedBytesLength = 2 + streamId.Length + 2 + topicId.Length + consumerGroupId.Length + 2;

        Assert.Equal(expectedBytesLength, result.Length);
        Assert.Equal(streamId.Value, BytesToIdentifierNumeric(result, 0).Value);
        Assert.Equal(topicId.Value, BytesToIdentifierNumeric(result, 6).Value);
        Assert.Equal(consumerGroupId.Value, BytesToIdentifierNumeric(result, 12).Value);
    }


    [Fact]
    public void TcpContracts_LeaveGroup_HasCorrectBytes()
    {
        // Arrange
        var streamId = Identifier.Numeric(Random.Shared.Next(1, 10));
        var topicId = Identifier.Numeric(Random.Shared.Next(1, 10));
        var consumerGroupId = Identifier.Numeric(Random.Shared.Next(1, 10));

        // Act
        Span<byte> result = TcpContracts.LeaveGroup(streamId, topicId, consumerGroupId).AsSpan();

        // Assert
        var expectedBytesLength = 2 + streamId.Length + 2 + topicId.Length + consumerGroupId.Length + 2;

        Assert.Equal(expectedBytesLength, result.Length);
        Assert.Equal(streamId.Value, BytesToIdentifierNumeric(result, 0).Value);
        Assert.Equal(topicId.Value, BytesToIdentifierNumeric(result, 6).Value);
        Assert.Equal(consumerGroupId.Value, BytesToIdentifierNumeric(result, 12).Value);
    }


    [Fact]
    public void TcpContracts_GetGroup_HasCorrectBytes()
    {
        // Arrange
        var streamId = Identifier.String("my-stream");
        var topicId = Identifier.Numeric(1);
        var groupId = 1;
        var groupIdentifier = Identifier.Numeric(groupId);

        // Act
        Span<byte> result = TcpContracts.GetGroup(streamId, topicId, groupIdentifier).AsSpan();

        // Assert
        var expectedBytesLength = 2 + streamId.Length + 2 + topicId.Length + groupIdentifier.Length + 2;

        Assert.Equal(expectedBytesLength, result.Length);
        Assert.Equal(streamId.Value, BytesToIdentifierString(result, 0).Value);
        Assert.Equal(topicId.Value, BytesToIdentifierNumeric(result, 2 + streamId.Length).Value);
        Assert.Equal(streamId.Kind, BytesToIdentifierString(result, 0).Kind);
        Assert.Equal(topicId.Kind, BytesToIdentifierNumeric(result, 2 + streamId.Length).Kind);
        Assert.Equal(streamId.Length, BytesToIdentifierString(result, 0).Length);
        Assert.Equal(topicId.Length, BytesToIdentifierNumeric(result, 2 + streamId.Length).Length);
        var position = 2 + streamId.Length + 2 + topicId.Length;
        Assert.Equal(groupIdentifier.Kind.GetByte(), result[position]);
    }


    [Fact]
    public void TcpContracts_CreateTopic_HasCorrectBytes()
    {
        // Arrange
        var streamId = Identifier.Numeric(1);
        var request = TopicFactory.CreateTopicRequest();

        // Act
        Span<byte> result = TcpContracts.CreateTopic(streamId, request.Name, request.PartitionsCount,
            request.CompressionAlgorithm, request.TopicId, request.ReplicationFactor, request.MessageExpiry,
            request.MaxTopicSize).AsSpan();

        // Assert
        var expectedBytesLength = 2 + streamId.Length + 27 + request.Name.Length;

        Assert.Equal(expectedBytesLength, result.Length);
        Assert.Equal(streamId.Value, BytesToIdentifierNumeric(result, 0).Value);
        Assert.Equal(streamId.Length, BytesToIdentifierNumeric(result, 0).Length);
        Assert.Equal(streamId.Kind, BytesToIdentifierNumeric(result, 0).Kind);
        Assert.Equal(request.TopicId, BitConverter.ToUInt32(result[6..10]));
        Assert.Equal(request.PartitionsCount, BitConverter.ToUInt32(result[10..14]));
        Assert.Equal((int)request.CompressionAlgorithm, result[14]);
        Assert.Equal(request.MessageExpiry, BitConverter.ToUInt64(result[15..23]));
        Assert.Equal(request.MaxTopicSize, BitConverter.ToUInt64(result[23..31]));
        Assert.Equal(request.ReplicationFactor, result[31]);
        Assert.Equal(request.Name.Length, result[32]);
        Assert.Equal(request.Name, Encoding.UTF8.GetString(result[33..]));
    }


    [Fact]
    public void TcpContracts_GetTopicById_HasCorrectBytes()
    {
        // Arrange
        var streamId = Identifier.Numeric(1);
        var topicId = Identifier.Numeric(1);

        // Act
        Span<byte> result = TcpContracts.GetTopicById(streamId, topicId).AsSpan();

        // Assert
        var expectedBytesLength = 2 + streamId.Length + 2 + topicId.Length;

        Assert.Equal(expectedBytesLength, result.Length);
        Assert.Equal(streamId.Value, BytesToIdentifierNumeric(result, 0).Value);
        Assert.Equal(streamId.Length, BytesToIdentifierNumeric(result, 0).Length);
        Assert.Equal(streamId.Kind, BytesToIdentifierNumeric(result, 0).Kind);
        Assert.Equal(topicId.Value, BytesToIdentifierNumeric(result, 6).Value);
        Assert.Equal(topicId.Length, BytesToIdentifierNumeric(result, 6).Length);
        Assert.Equal(topicId.Kind, BytesToIdentifierNumeric(result, 6).Kind);
    }


    [Fact]
    public void TcpContracts_DeleteTopic_HasCorrectBytes()
    {
        // Arrange
        var streamId = Identifier.Numeric(1);
        var topicId = Identifier.Numeric(1);

        // Act
        Span<byte> result = TcpContracts.DeleteTopic(streamId, topicId).AsSpan();

        // Assert
        var expectedBytesLength = 2 + streamId.Length + 2 + topicId.Length;

        Assert.Equal(expectedBytesLength, result.Length);
        Assert.Equal(streamId.Value, BytesToIdentifierNumeric(result, 0).Value);
        Assert.Equal(streamId.Length, BytesToIdentifierNumeric(result, 0).Length);
        Assert.Equal(streamId.Kind, BytesToIdentifierNumeric(result, 0).Kind);
        Assert.Equal(topicId.Value, BytesToIdentifierNumeric(result, 6).Value);
        Assert.Equal(topicId.Length, BytesToIdentifierNumeric(result, 6).Length);
        Assert.Equal(topicId.Kind, BytesToIdentifierNumeric(result, 6).Kind);
    }

    [Fact]
    public void TcpContracts_UpdateOffset_HasCorrectBytes()
    {
        // Arrange
        var streamId = Identifier.Numeric(1);
        var topicId = Identifier.Numeric(1);
        var consumer = Consumer.New(1);
        var offset = (ulong)Random.Shared.Next(1, 10);
        var partitionId = (uint)Random.Shared.Next(1, 10);

        // Act
        Span<byte> result = TcpContracts.UpdateOffset(streamId, topicId, consumer, offset, partitionId).AsSpan();

        // Assert
        var expectedBytesLength = 2 + streamId.Length + 2 + topicId.Length + 5 + 2 + consumer.Id.Length + 8;

        Assert.Equal(expectedBytesLength, result.Length);
        Assert.Equal(1, result[0]);
        Assert.Equal(consumer.Id.Kind.GetByte(), result[1]);
        Assert.Equal(streamId.Value, BytesToIdentifierNumeric(result, 7).Value);
        Assert.Equal(streamId.Length, BytesToIdentifierNumeric(result, 7).Length);
        Assert.Equal(streamId.Kind, BytesToIdentifierNumeric(result, 7).Kind);
        Assert.Equal(topicId.Value, BytesToIdentifierNumeric(result, 13).Value);
        Assert.Equal(topicId.Length, BytesToIdentifierNumeric(result, 13).Length);
        Assert.Equal(topicId.Kind, BytesToIdentifierNumeric(result, 13).Kind);
        Assert.Equal(partitionId, BitConverter.ToUInt32(result[19..23]));
        Assert.Equal(offset, BitConverter.ToUInt64(result[23..31]));
    }


    [Fact]
    public void TcpContracts_GetOffset_HasCorrectBytes()
    {
        // Arrange
        var streamId = Identifier.Numeric(Random.Shared.Next(1, 10));
        var topicId = Identifier.Numeric(Random.Shared.Next(1, 10));
        var consumer = Consumer.New(1);
        var partitionId = (uint)Random.Shared.Next(1, 10);

        // Act
        Span<byte> result = TcpContracts.GetOffset(streamId, topicId, consumer, partitionId).AsSpan();

        // Assert
        var expectedBytesLength = 2 + streamId.Length + 2 + topicId.Length + 5 + 2 + consumer.Id.Length;

        Assert.Equal(expectedBytesLength, result.Length);
        Assert.Equal(1, result[0]);
        Assert.Equal(consumer.Id.Kind.GetByte(), result[1]);
        Assert.Equal(streamId.Value, BytesToIdentifierNumeric(result, 7).Value);
        Assert.Equal(streamId.Length, BytesToIdentifierNumeric(result, 7).Length);
        Assert.Equal(streamId.Kind, BytesToIdentifierNumeric(result, 7).Kind);
        Assert.Equal(topicId.Value, BytesToIdentifierNumeric(result, 13).Value);
        Assert.Equal(topicId.Length, BytesToIdentifierNumeric(result, 13).Length);
        Assert.Equal(topicId.Kind, BytesToIdentifierNumeric(result, 13).Kind);
        Assert.Equal(partitionId, BitConverter.ToUInt32(result[19..23]));
    }

    [Fact]
    public void TcpContracts_CreatePartitions_HasCorrectBytes()
    {
        // Arrange
        var streamId = Identifier.Numeric(1);
        var topicId = Identifier.Numeric(1);
        var partitionsCount = (uint)Random.Shared.Next(1, 69);

        // Act
        Span<byte> result = TcpContracts.CreatePartitions(streamId, topicId, partitionsCount).AsSpan();

        // Assert
        Assert.Equal(streamId.Value, BytesToIdentifierNumeric(result, 0).Value);
        Assert.Equal(streamId.Length, BytesToIdentifierNumeric(result, 0).Length);
        Assert.Equal(streamId.Kind, BytesToIdentifierNumeric(result, 0).Kind);
        Assert.Equal(topicId.Value, BytesToIdentifierNumeric(result, 6).Value);
        Assert.Equal(topicId.Length, BytesToIdentifierNumeric(result, 6).Length);
        Assert.Equal(topicId.Kind, BytesToIdentifierNumeric(result, 6).Kind);
        Assert.Equal(partitionsCount, BitConverter.ToUInt32(result[12..16]));
    }


    [Fact]
    public void TcpContracts_DeletePartitions_HasCorrectBytes()
    {
        // Arrange
        var streamId = Identifier.Numeric(1);
        var topicId = Identifier.Numeric(1);
        var partitionsCount = (uint)Random.Shared.Next(1, 69);

        // Act
        Span<byte> result = TcpContracts.DeletePartitions(streamId, topicId, partitionsCount).AsSpan();

        // Assert
        Assert.Equal(streamId.Value, BytesToIdentifierNumeric(result, 0).Value);
        Assert.Equal(streamId.Length, BytesToIdentifierNumeric(result, 0).Length);
        Assert.Equal(streamId.Kind, BytesToIdentifierNumeric(result, 0).Kind);
        Assert.Equal(topicId.Value, BytesToIdentifierNumeric(result, 6).Value);
        Assert.Equal(topicId.Length, BytesToIdentifierNumeric(result, 6).Length);
        Assert.Equal(topicId.Kind, BytesToIdentifierNumeric(result, 6).Kind);
        Assert.Equal(partitionsCount, BitConverter.ToUInt32(result[12..16]));
    }

    private static Identifier BytesToIdentifierNumeric(Span<byte> bytes, int startPos)
    {
        var idKind = bytes[startPos] switch
        {
            1 => IdKind.Numeric,
            2 => IdKind.String,
            _ => throw new ArgumentOutOfRangeException()
        };
        var identifierLength = (int)bytes[startPos + 1];
        var valueBytes = new byte[identifierLength];
        for (var i = 0; i < identifierLength; i++)
        {
            valueBytes[i] = bytes[i + startPos + 2];
        }

        return new Identifier
        {
            Kind = IdKind.Numeric,
            Length = identifierLength,
            Value = valueBytes
        };
    }

    private static Identifier BytesToIdentifierString(Span<byte> bytes, int startPos)
    {
        var idKind = bytes[startPos] switch
        {
            1 => IdKind.Numeric,
            2 => IdKind.String,
            _ => throw new ArgumentOutOfRangeException()
        };
        var identifierLength = (int)bytes[startPos + 1];
        var valueBytes = new byte[identifierLength];
        for (var i = 0; i < identifierLength; i++)
        {
            valueBytes[i] = bytes[i + startPos + 2];
        }

        return new Identifier
        {
            Kind = IdKind.String,
            Length = identifierLength,
            Value = valueBytes
        };
    }

    private static void WriteBytesFromStreamAndTopicIdToSpan(Identifier streamId, Identifier topicId, Span<byte> bytes,
        int startPos = 0)
    {
        bytes[startPos] = streamId.Kind switch
        {
            IdKind.Numeric => 1,
            IdKind.String => 2,
            _ => throw new ArgumentOutOfRangeException()
        };
        bytes[startPos + 1] = (byte)streamId.Length;
        for (var i = 0; i < streamId.Length; i++)
        {
            bytes[i + startPos + 2] = streamId.Value[i];
        }

        var position = startPos + 2 + streamId.Length;
        bytes[position] = topicId.Kind switch
        {
            IdKind.Numeric => 1,
            IdKind.String => 2,
            _ => throw new ArgumentOutOfRangeException()
        };
        bytes[position + 1] = (byte)topicId.Length;
        for (var i = 0; i < topicId.Length; i++)
        {
            bytes[i + position + 2] = topicId.Value[i];
        }
    }
}