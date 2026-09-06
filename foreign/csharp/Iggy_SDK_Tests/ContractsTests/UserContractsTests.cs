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
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Contracts.Tcp;

namespace Apache.Iggy.Tests.ContractsTests;

public sealed class UserContractsTests
{
    // [user id: kind 1 + length 1 + value 4][has permissions: 1][permissions length: 4][permissions]
    private const int PermissionsOffset = 11;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void UpdatePermissions_WithGlobalOnlyPermissions_KeepsManageServersAndWritesNoStreams(
        bool emptyStreamsDictionary)
    {
        var permissions = new Permissions
        {
            Global = new GlobalPermissions
            {
                ManageServers = true,
                ReadServers = true,
                ManageUsers = false,
                ReadUsers = false,
                ManageStreams = false,
                ReadStreams = false,
                ManageTopics = false,
                ReadTopics = false,
                PollMessages = false,
                SendMessages = false
            },
            Streams = emptyStreamsDictionary ? new Dictionary<uint, StreamPermissions>() : null
        };

        var bytes = TcpContracts.UpdatePermissions(Identifier.Numeric(1u), permissions);

        var permissionsLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(PermissionsOffset - 4, 4));
        Assert.Equal(11, permissionsLength);
        Assert.Equal(PermissionsOffset + 11, bytes.Length);

        var wire = bytes.AsSpan(PermissionsOffset, 11);
        Assert.Equal(1, wire[0]);
        Assert.Equal(1, wire[1]);
        Assert.Equal(0, wire[10]);
    }

    [Fact]
    public void UpdatePermissions_WithNullPermissions_WritesHasPermissionsZero()
    {
        var bytes = TcpContracts.UpdatePermissions(Identifier.Numeric(1u), null);

        Assert.Equal(PermissionsOffset - 4, bytes.Length);
        Assert.Equal(0, bytes[^1]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void UpdatePermissions_WithStreamWithoutTopics_WritesHasTopicsZero(bool emptyTopicsDictionary)
    {
        var permissions = new Permissions
        {
            Global = new GlobalPermissions
            {
                ManageServers = false,
                ReadServers = false,
                ManageUsers = false,
                ReadUsers = false,
                ManageStreams = false,
                ReadStreams = false,
                ManageTopics = false,
                ReadTopics = false,
                PollMessages = false,
                SendMessages = false
            },
            Streams = new Dictionary<uint, StreamPermissions>
            {
                [7] = new StreamPermissions
                {
                    ManageStream = false,
                    ReadStream = true,
                    ManageTopics = false,
                    ReadTopics = false,
                    PollMessages = false,
                    SendMessages = false,
                    Topics = emptyTopicsDictionary ? new Dictionary<uint, TopicPermissions>() : null
                }
            }
        };

        var bytes = TcpContracts.UpdatePermissions(Identifier.Numeric(1u), permissions);

        // [global: 10][has streams: 1][stream id: 4][stream flags: 6][has topics: 1][has next stream: 1]
        const int permissionsLength = 10 + 1 + 4 + 6 + 1 + 1;
        Assert.Equal(permissionsLength, BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(PermissionsOffset - 4, 4)));
        Assert.Equal(PermissionsOffset + permissionsLength, bytes.Length);

        var wire = bytes.AsSpan(PermissionsOffset, permissionsLength);
        Assert.Equal(1, wire[10]);
        Assert.Equal(7u, BinaryPrimitives.ReadUInt32LittleEndian(wire.Slice(11, 4)));
        Assert.Equal(1, wire[16]);
        Assert.Equal(0, wire[21]);
        Assert.Equal(0, wire[22]);
    }
}
