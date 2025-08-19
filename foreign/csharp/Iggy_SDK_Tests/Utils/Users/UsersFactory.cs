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
using Apache.Iggy.Contracts.Http.Auth;
using Apache.Iggy.Enums;

namespace Apache.Iggy.Tests.Utils.Users;

public static class UsersFactory
{
    internal static CreateUserRequest CreateUserRequest(string? username = null, string? password = null,
        Permissions? permissions = null)
    {
        return new CreateUserRequest(username ?? Utility.RandomString(Random.Shared.Next(5, 25)),
            password ?? Utility.RandomString(Random.Shared.Next(5, 25)),
            UserStatus.Active,
            permissions ?? CreatePermissions());
    }

    internal static Dictionary<int, StreamPermissions> CreateStreamPermissions(int streamId = 1, int topicId = 1)
    {
        var streamsPermission = new Dictionary<int, StreamPermissions>();
        var topicPermissions = new Dictionary<int, TopicPermissions>();
        topicPermissions.Add(topicId,
            new TopicPermissions
            {
                ManageTopic = true,
                PollMessages = true,
                ReadTopic = Random.Shared.Next(1) == 1,
                SendMessages = Random.Shared.Next(1) == 1
            });
        streamsPermission.Add(streamId,
            new StreamPermissions
            {
                ManageStream = true,
                ManageTopics = true,
                ReadStream = true,
                ReadTopics = Random.Shared.Next(1) == 1,
                PollMessages = Random.Shared.Next(1) == 1,
                SendMessages = Random.Shared.Next(1) == 1,
                Topics = topicPermissions
            });
        return streamsPermission;
    }

    internal static Permissions CreatePermissions()
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
                ReadServers = Random.Shared.Next(1) == 1,
                ReadStreams = Random.Shared.Next(1) == 1,
                ReadTopics = Random.Shared.Next(1) == 1,
                ReadUsers = Random.Shared.Next(1) == 1,
                SendMessages = Random.Shared.Next(1) == 1
            },
            Streams = CreateStreamPermissions()
        };
    }
}