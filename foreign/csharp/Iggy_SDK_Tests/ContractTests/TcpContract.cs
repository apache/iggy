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
using Apache.Iggy.Contracts.Http;
using Apache.Iggy.Contracts.Http.Auth;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Enums;
using Apache.Iggy.Extensions;
using Apache.Iggy.Tests.Utils;
using Apache.Iggy.Tests.Utils.Groups;
using Apache.Iggy.Tests.Utils.Messages;
using Apache.Iggy.Tests.Utils.Offsets;
using Apache.Iggy.Tests.Utils.Partitions;
using Apache.Iggy.Tests.Utils.Streams;
using Apache.Iggy.Tests.Utils.Topics;
using Apache.Iggy.Tests.Utils.Users;
using FluentAssertions;

namespace Apache.Iggy.Tests.ContractTests;

public sealed class TcpContract
{
    
    [Fact]
    public void TcpContracts_DeletePersonalRequestToken_HasValidBytes()
    {
        // Arrange
        var request = new DeletePersonalAccessTokenRequest
        {
            Name = "TestUser"
        };

        // Act
        byte[] result = TcpContracts.DeletePersonalRequestToken(request);

        // Assert
        Assert.Equal(5 + request.Name.Length, result.Length);
        Assert.Equal((byte)request.Name.Length, result[0]);
        Assert.Equal(Encoding.UTF8.GetBytes(request.Name), result[1..(1 + request.Name.Length)]);
    }
    [Fact]
    public void TcpContracts_CreatePersonalAccessToken_HasValidBytes_ValidExpiry()
    {
        // Arrange
        var request = new CreatePersonalAccessTokenRequest
        {
            Name = "TestUser",
            Expiry = 3600 // Valid Expiry Value
        };

        // Act
        byte[] result = TcpContracts.CreatePersonalAccessToken(request);

        // Assert
        Assert.Equal(9 + request.Name.Length, result.Length); // The expected length
        Assert.Equal(Encoding.UTF8.GetBytes(request.Name), result[1..(1 + request.Name.Length)]); // The expected length of the name
        Assert.Equal((uint)3600, BinaryPrimitives.ReadUInt32LittleEndian(result[(1 + request.Name.Length)..]));
    }

    [Fact]
    public void TcpContracts_CreatePersonalAccessToken_HasValidBytes_NullExpiry()
    {
        // Arrange
        var request = new CreatePersonalAccessTokenRequest
        {
            Name = "TestUser",
            Expiry = null // Null Expiry
        };

        // Act
        byte[] result = TcpContracts.CreatePersonalAccessToken(request);

        // Assert
        Assert.Equal(9 + request.Name.Length, result.Length); // The expected length
        Assert.Equal(Encoding.UTF8.GetBytes(request.Name), result[1..(1 + request.Name.Length)]); // The expected length of the name
        Assert.Equal((uint)0, BinaryPrimitives.ReadUInt64LittleEndian(result[(1 + request.Name.Length)..]));
    }
    
    [Fact]
    public void TcpContracts_LoginUser_HasCorrectBytes()
    {
        // Arrange
        var request = new LoginUserRequest
        {
            Username = "testuser",
            Password = "testpassword"
        };

        // Act
        var result = TcpContracts.LoginUser(request);

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
        var request = new LoginUserRequest
        {
            Username = "testuser",
            Password = "testpassword",
            Context = "optional context",
            Version = "1.0.0"
        };

        // Act
        var result = TcpContracts.LoginUser(request);

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
        var request = new UpdateUserRequest
        {
            UserId = Identifier.Numeric(1),
            Username = "newusername",
            UserStatus = UserStatus.Active
        };

        // Act
        var result = TcpContracts.UpdateUser(request);

        // Assert
        var expectedLength = request.UserId.Length + 2 +
                             (request.Username?.Length ?? 0) + 2 + 1 + 1;
        Assert.Equal(expectedLength, result.Length);

        var position = 2;
        var userIdBytes = result[position..(position + request.UserId.Length)];
        position += request.UserId.Length;
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
            var userStatus = statusFlag switch
            {
                1 => UserStatus.Active,
                2 => UserStatus.Inactive
            };
            Assert.Equal(request.UserStatus, userStatus);
        }
        else
        {
            Assert.Null(request.UserStatus);
        }
        Assert.Equal(request.UserId.Value, userIdBytes);
    }
    [Fact]
    public void TcpContracts_CreateUser_NoPermission_HasCorrectBytes()
    {
        // Arrange
        var request = new CreateUserRequest
        {
            Username = "testuser",
            Password = "testpassword",
            Status = UserStatus.Active,
            Permissions = null,
        };
        // Act
        var result = TcpContracts.CreateUser(request);

        // Assert
        int position = 0;

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
        var request = new ChangePasswordRequest
        {
            UserId = Identifier.Numeric(1),
            CurrentPassword = "oldpassword",
            NewPassword = "newpassword"
        };

        // Act
        var result = TcpContracts.ChangePassword(request);

        // Assert
        int position = 2;

        var userIdBytes = result[position..(position + request.UserId.Length)];
        position += request.UserId.Length;
        Assert.Equal(request.UserId.Value, userIdBytes);

        Assert.Equal((byte)request.CurrentPassword.Length, result[position]);
        position += 1;

        var currentPasswordBytes = result[position..(position + request.CurrentPassword.Length)];
        position += request.CurrentPassword.Length;
        var decodedCurrentPassword = Encoding.UTF8.GetString(currentPasswordBytes);
        Assert.Equal(request.CurrentPassword, decodedCurrentPassword);

        Assert.Equal((byte)request.NewPassword.Length, result[position]);
        position += 1;

        var newPasswordBytes = result[position..(position + request.NewPassword.Length)];
        position += request.NewPassword.Length;
        var decodedNewPassword = Encoding.UTF8.GetString(newPasswordBytes);
        Assert.Equal(request.NewPassword, decodedNewPassword);
    }

    [Fact]
    public void TcpContracts_UpdatePermissions_HasCorrectBytes()
    {
        // Arrange
        var request = new UpdateUserPermissionsRequest
        {
            UserId = Identifier.Numeric(1),
            Permissions = PermissionsFactory.CreatePermissions()
        };

        // Act
        var result = TcpContracts.UpdatePermissions(request);

        // Assert
        int position = 2;

        var userIdBytes = result[position..(position + request.UserId.Length)];
        position += request.UserId.Length;
        Assert.Equal(request.UserId.Value, userIdBytes);

        var permissionsFlag = result[position];
        position += 1;
        if (permissionsFlag == 1)
        {
            var permissionsSize = BinaryPrimitives.ReadInt32LittleEndian(result[position..(position + 4)]);
            position += 4;

            var permissionsBytes = result[position..(position + permissionsSize)];

            var mappedPermissions = PermissionsFactory.PermissionsFromBytes(permissionsBytes);

            request.Permissions.Global.Should().BeEquivalentTo(mappedPermissions.Global);

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
                Assert.Null(mappedPermissions.Streams);
            }
        }
        else
        {
            Assert.Null(request.Permissions);
        }
    }

    [Fact]
    public void TcpContracts_CreateUser_WithPermission_HasCorrectBytes()
    {
        // Arrange
        var request = new CreateUserRequest
        {
            Username = "testuser",
            Password = "testpassword",
            Status = UserStatus.Active,
            Permissions = PermissionsFactory.CreatePermissions(),
        };
        // Act
        var result = TcpContracts.CreateUser(request);

        // Assert
        int position = 0;

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
            request.Permissions.Global.Should().BeEquivalentTo(mappedPermissions.Global);

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
        var request = new ChangePasswordRequest
        {
            UserId = Identifier.Numeric(1),
            CurrentPassword = "oldpassword",
            NewPassword = "newpassword"
        };

        // Act
        var result = TcpContracts.ChangePassword(request);

        // Assert
        var expectedLength = request.UserId.Length + 2 +
                             request.CurrentPassword.Length + request.NewPassword.Length + 2;
        Assert.Equal(expectedLength, result.Length);

        // Validate bytes can be translated back to properties
        var position = 2;
        var userIdBytes = result[position..(position + request.UserId.Length)];
        position += request.UserId.Length;
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

        Assert.Equal(request.UserId.Value, userIdBytes);
        Assert.Equal(request.CurrentPassword, decodedCurrentPassword);
        Assert.Equal(request.NewPassword, decodedNewPassword);
    }

    [Fact]
    public void TcpContracts_MessageFetchRequest_HasCorrectBytes()
    {
        // Arrange
        var request = MessageFactory.CreateMessageFetchRequestConsumer();
        int messageBufferSize = 23 + 2 + 4 + 2 + 2 + request.Consumer.Id.Length;
        var result = new byte[messageBufferSize];

        // Act
        TcpContracts.GetMessages(result, request);

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
        Assert.Equal(request.PartitionId, BitConverter.ToInt32(result[19..23]));
        Assert.Equal(
            result[23] switch
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

        int currentIndex = 26 + (16 * 2);
        foreach (var message in request.Messages)
        {
            Assert.Equal(message.Header.Checksum, BitConverter.ToUInt64(result[currentIndex..(currentIndex + 8)]));
            Assert.Equal(message.Header.Id, BinaryPrimitives.ReadUInt128LittleEndian(result[(currentIndex + 8)..(currentIndex + 24)]));
            Assert.Equal(message.Header.Offset, BitConverter.ToUInt64(result[(currentIndex + 24)..(currentIndex + 32)]));
            Assert.Equal(message.Header.Timestamp, DateTimeOffsetUtils.FromUnixTimeMicroSeconds(BitConverter.ToUInt64(result[(currentIndex + 32)..(currentIndex + 40)])));
            Assert.Equal(message.Header.OriginTimestamp, BitConverter.ToUInt64(result[(currentIndex + 40)..(currentIndex + 48)]));
            var userHeadersLength = BitConverter.ToInt32(result[(currentIndex + 48)..(currentIndex + 52)]);
            Assert.Equal(message.Header.UserHeadersLength, userHeadersLength);
            var payloadLength = BitConverter.ToInt32(result[(currentIndex + 52)..(currentIndex + 56)]);
            Assert.Equal(message.Header.PayloadLength, payloadLength);

            currentIndex += 56;
            byte[] payload = result[currentIndex..(currentIndex + payloadLength)].ToArray();
            currentIndex += payloadLength;

            Assert.Equal(message.Payload.Length, payload.Length);
            Assert.Equal(message.Payload, payload);
        }
    }


    [Fact]
    public void TcpContracts_CreateStream_HasCorrectBytes()
    {
        // Arrange
        var request = StreamFactory.CreateStreamRequest();

        // Act
        var result = TcpContracts.CreateStream(request).AsSpan();

        // Assert
        int expectedBytesLength = sizeof(int) + request.Name.Length + 1;

        Assert.Equal(expectedBytesLength, result.Length);
        Assert.Equal(request.StreamId, BitConverter.ToInt32(result[..5]));
        Assert.Equal(request.Name, Encoding.UTF8.GetString(result[5..]));
    }

    [Fact]
    public void TcpContracts_CreateGroup_HasCorrectBytes()
    {
        // Arrange
        var streamId = Identifier.String("my-stream");
        var topicId = Identifier.String("my-topic");
        var request = new CreateConsumerGroupRequest
        {
            Name = Utility.RandomString(69),
            StreamId = streamId,
            TopicId = topicId,
            ConsumerGroupId = Random.Shared.Next(1, 69)
        };
        // Act
        var result = TcpContracts.CreateGroup(request).AsSpan();

        // Assert
        int expectedBytesLength = 2 + streamId.Length + 2 + topicId.Length + 4 + 1 + request.Name.Length;

        Assert.Equal(expectedBytesLength, result.Length);
        Assert.Equal(streamId.Value, BytesToIdentifierString(result, 0).Value);
        Assert.Equal(topicId.Value, BytesToIdentifierString(result, 2 + streamId.Length).Value);
        Assert.Equal(streamId.Kind, BytesToIdentifierString(result, 0).Kind);
        Assert.Equal(topicId.Kind, BytesToIdentifierString(result, 2 + streamId.Length).Kind);
        Assert.Equal(streamId.Length, BytesToIdentifierString(result, 0).Length);
        Assert.Equal(topicId.Length, BytesToIdentifierString(result, 2 + streamId.Length).Length);
        var position = 2 + streamId.Length + 2 + topicId.Length + 4;
        Assert.Equal(request.Name.Length, result[position]);

    }

    [Fact]
    public void TcpContracts_DeleteGroup_HasCorrectBytes()
    {
        // Arrange
        var streamId = Identifier.Numeric(1);
        var topicId = Identifier.Numeric(1);
        var groupId = Identifier.Numeric(1);

        // Act
        var result = TcpContracts.DeleteGroup(streamId, topicId, groupId).AsSpan();

        // Assert
        int expectedBytesLength = 2 + streamId.Length + 2 + topicId.Length + groupId.Length + 2;

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
        var result = TcpContracts.GetGroups(streamId, topicId).AsSpan();

        // Assert
        int expectedBytesLength = 2 + streamId.Length + 2 + topicId.Length;

        Assert.Equal(expectedBytesLength, result.Length);
        Assert.Equal(streamId.Value, BytesToIdentifierString(result, 0).Value);
        Assert.Equal(topicId.Value, BytesToIdentifierNumeric(result, 2 + streamId.Length).Value);
    }

    [Fact]
    public void TcpContracts_JoinGroup_HasCorrectBytes()
    {
        // Arrange
        var request = ConsumerGroupFactory.CreateJoinGroupRequest();

        // Act
        var result = TcpContracts.JoinGroup(request).AsSpan();

        // Assert
        int expectedBytesLength = 2 + request.StreamId.Length + 2 + request.TopicId.Length + request.ConsumerGroupId.Length + 2;

        Assert.Equal(expectedBytesLength, result.Length);
        Assert.Equal(request.StreamId.Value, BytesToIdentifierNumeric(result, 0).Value);
        Assert.Equal(request.TopicId.Value, BytesToIdentifierNumeric(result, 6).Value);
        Assert.Equal(request.ConsumerGroupId.Value, BytesToIdentifierNumeric(result, 12).Value);
    }


    [Fact]
    public void TcpContracts_LeaveGroup_HasCorrectBytes()
    {
        // Arrange
        var request = ConsumerGroupFactory.CreateLeaveGroupRequest();

        // Act
        var result = TcpContracts.LeaveGroup(request).AsSpan();

        // Assert
        int expectedBytesLength = 2 + request.StreamId.Length + 2 + request.TopicId.Length + request.ConsumerGroupId.Length + 2;

        Assert.Equal(expectedBytesLength, result.Length);
        Assert.Equal(request.StreamId.Value, BytesToIdentifierNumeric(result, 0).Value);
        Assert.Equal(request.TopicId.Value, BytesToIdentifierNumeric(result, 6).Value);
        Assert.Equal(request.ConsumerGroupId.Value, BytesToIdentifierNumeric(result, 12).Value);
    }


    [Fact]
    public void TcpContracts_GetGroup_HasCorrectBytes()
    {
        // Arrange
        var streamId = Identifier.String("my-stream");
        var topicId = Identifier.Numeric(1);
        int groupId = 1;
        var groupIdentifier = Identifier.Numeric(groupId);

        // Act
        var result = TcpContracts.GetGroup(streamId, topicId, groupIdentifier).AsSpan();

        // Assert
        int expectedBytesLength = 2 + streamId.Length + 2 + topicId.Length + groupIdentifier.Length + 2;

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
        var result = TcpContracts.CreateTopic(streamId, request).AsSpan();

        // Assert
        int expectedBytesLength = 2 + streamId.Length + 27 + request.Name.Length; 
        
        Assert.Equal(expectedBytesLength, result.Length);
        Assert.Equal(streamId.Value, BytesToIdentifierNumeric(result, 0).Value);
        Assert.Equal(streamId.Length, BytesToIdentifierNumeric(result, 0).Length);
        Assert.Equal(streamId.Kind, BytesToIdentifierNumeric(result, 0).Kind);
        Assert.Equal(request.TopicId, BitConverter.ToInt32(result[6..10]));
        Assert.Equal(request.PartitionsCount, BitConverter.ToInt32(result[10..14]));
        Assert.Equal((int)request.CompressionAlgorithm, result[14]);
        Assert.Equal(request.MessageExpiry, BitConverter.ToUInt64(result[15..23]));
        Assert.Equal(request.MaxTopicSize, BitConverter.ToUInt64(result[23..31]));
        Assert.Equal(request.ReplicationFactor, (int)result[31]);
        Assert.Equal(request.Name.Length, (int)result[32]);
        Assert.Equal(request.Name, Encoding.UTF8.GetString(result[33..]));
    }


    [Fact]
    public void TcpContracts_GetTopicById_HasCorrectBytes()
    {
        // Arrange
        var streamId = Identifier.Numeric(1);
        var topicId = Identifier.Numeric(1);

        // Act
        var result = TcpContracts.GetTopicById(streamId, topicId).AsSpan();

        // Assert
        int expectedBytesLength = 2 + streamId.Length + 2 + topicId.Length;

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
        var result = TcpContracts.DeleteTopic(streamId, topicId).AsSpan();

        // Assert
        int expectedBytesLength = 2 + streamId.Length + 2 + topicId.Length;

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
        var contract = OffsetFactory.CreateOffsetContract();

        // Act
        var result = TcpContracts.UpdateOffset(contract).AsSpan();

        // Assert
        int expectedBytesLength = 2 + contract.StreamId.Length + 2 + contract.TopicId.Length + 5 + 2 + contract.Consumer.Id.Length + 8;

        Assert.Equal(expectedBytesLength, result.Length);
        Assert.Equal(1, result[0]);
        Assert.Equal(contract.Consumer.Id.Kind.GetByte(), result[1]);
        Assert.Equal(contract.StreamId.Value, BytesToIdentifierNumeric(result, 7).Value);
        Assert.Equal(contract.StreamId.Length, BytesToIdentifierNumeric(result, 7).Length);
        Assert.Equal(contract.StreamId.Kind, BytesToIdentifierNumeric(result, 7).Kind);
        Assert.Equal(contract.TopicId.Value, BytesToIdentifierNumeric(result, 13).Value);
        Assert.Equal(contract.TopicId.Length, BytesToIdentifierNumeric(result, 13).Length);
        Assert.Equal(contract.TopicId.Kind, BytesToIdentifierNumeric(result, 13).Kind);
        Assert.Equal(contract.PartitionId, BitConverter.ToInt32(result[19..23]));
        Assert.Equal(contract.Offset, BitConverter.ToUInt64(result[23..31]));
    }



    [Fact]
    public void TcpContracts_GetOffset_HasCorrectBytes()
    {
        // Arrange
        var request = OffsetFactory.CreateOffsetRequest();

        // Act
        var result = TcpContracts.GetOffset(request).AsSpan();

        // Assert
        int expectedBytesLength = 2 + request.StreamId.Length + 2 + request.TopicId.Length + 5 + 2 + request.Consumer.Id.Length;

        Assert.Equal(expectedBytesLength, result.Length);
        Assert.Equal(1, result[0]);
        Assert.Equal(request.Consumer.Id.Kind.GetByte(), result[1]);
        Assert.Equal(request.StreamId.Value, BytesToIdentifierNumeric(result, 7).Value);
        Assert.Equal(request.StreamId.Length, BytesToIdentifierNumeric(result, 7).Length);
        Assert.Equal(request.StreamId.Kind, BytesToIdentifierNumeric(result, 7).Kind);
        Assert.Equal(request.TopicId.Value, BytesToIdentifierNumeric(result, 13).Value);
        Assert.Equal(request.TopicId.Length, BytesToIdentifierNumeric(result, 13).Length);
        Assert.Equal(request.TopicId.Kind, BytesToIdentifierNumeric(result, 13).Kind);
        Assert.Equal(request.PartitionId, BitConverter.ToInt32(result[19..23]));
    }

    [Fact]
    public void TcpContracts_CreatePartitions_HasCorrectBytes()
    {
        // Arrange
        var request = PartitionFactory.CreatePartitionsRequest();

        // Act
        var result = TcpContracts.CreatePartitions(request).AsSpan();

        // Assert
        Assert.Equal(request.StreamId.Value, BytesToIdentifierNumeric(result, 0).Value);
        Assert.Equal(request.StreamId.Length, BytesToIdentifierNumeric(result, 0).Length);
        Assert.Equal(request.StreamId.Kind, BytesToIdentifierNumeric(result, 0).Kind);
        Assert.Equal(request.TopicId.Value, BytesToIdentifierNumeric(result, 6).Value);
        Assert.Equal(request.TopicId.Length, BytesToIdentifierNumeric(result, 6).Length);
        Assert.Equal(request.TopicId.Kind, BytesToIdentifierNumeric(result, 6).Kind);
        Assert.Equal(request.PartitionsCount, BitConverter.ToInt32(result[12..16]));

    }


    [Fact]
    public void TcpContracts_DeletePartitions_HasCorrectBytes()
    {
        // Arrange
        var request = PartitionFactory.CreateDeletePartitionsRequest();

        // Act
        var result = TcpContracts.DeletePartitions(request).AsSpan();

        // Assert
        Assert.Equal(request.StreamId.Value, BytesToIdentifierNumeric(result, 0).Value);
        Assert.Equal(request.StreamId.Length, BytesToIdentifierNumeric(result, 0).Length);
        Assert.Equal(request.StreamId.Kind, BytesToIdentifierNumeric(result, 0).Kind);
        Assert.Equal(request.TopicId.Value, BytesToIdentifierNumeric(result, 6).Value);
        Assert.Equal(request.TopicId.Length, BytesToIdentifierNumeric(result, 6).Length);
        Assert.Equal(request.TopicId.Kind, BytesToIdentifierNumeric(result, 6).Kind);
        Assert.Equal(request.PartitionsCount, BitConverter.ToInt32(result[12..16]));

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
        for (int i = 0; i < identifierLength; i++)
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
        for (int i = 0; i < identifierLength; i++)
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
    private static void WriteBytesFromStreamAndTopicIdToSpan(Identifier streamId, Identifier topicId, Span<byte> bytes, int startPos = 0)
    {
        bytes[startPos] = streamId.Kind switch
        {
            IdKind.Numeric => 1,
            IdKind.String => 2,
            _ => throw new ArgumentOutOfRangeException()
        };
        bytes[startPos + 1] = (byte)streamId.Length;
        for (int i = 0; i < streamId.Length; i++)
        {
            bytes[i + startPos + 2] = streamId.Value[i];
        }

        int position = startPos + 2 + streamId.Length;
        bytes[position] = topicId.Kind switch
        {
            IdKind.Numeric => 1,
            IdKind.String => 2,
            _ => throw new ArgumentOutOfRangeException()
        };
        bytes[position + 1] = (byte)topicId.Length;
        for (int i = 0; i < topicId.Length; i++)
        {
            bytes[i + position + 2] = topicId.Value[i];
        }
    }
}
