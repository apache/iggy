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

namespace Apache.Iggy.Tests.Utils.Users;

internal static class PermissionsFactory
{
    internal static Permissions CreatePermissions()
    {
        return new Permissions
        {
            Global
                = new GlobalPermissions
                {
                    ManageServers = Random.Shared.Next() % 2 == 0,
                    ReadServers = Random.Shared.Next() % 2 == 0,
                    ManageUsers = Random.Shared.Next() % 2 == 0,
                    ReadUsers = Random.Shared.Next() % 2 == 0,
                    ManageStreams = Random.Shared.Next() % 2 == 0,
                    ReadStreams = Random.Shared.Next() % 2 == 0,
                    ManageTopics = Random.Shared.Next() % 2 == 0,
                    ReadTopics = Random.Shared.Next() % 2 == 0,
                    PollMessages = Random.Shared.Next() % 2 == 0,
                    SendMessages = Random.Shared.Next() % 2 == 0
                },
            Streams = new Dictionary<int, StreamPermissions>
            {
                {
                    Random.Shared.Next(1, 30), new StreamPermissions
                    {
                        ManageStream = Random.Shared.Next() % 2 == 0,
                        ReadStream = Random.Shared.Next() % 2 == 0,
                        ManageTopics = Random.Shared.Next() % 2 == 0,
                        ReadTopics = Random.Shared.Next() % 2 == 0,
                        PollMessages = Random.Shared.Next() % 2 == 0,
                        SendMessages = Random.Shared.Next() % 2 == 0,
                        Topics = new Dictionary<int, TopicPermissions>
                        {
                            {
                                Random.Shared.Next(1, 30),
                                new TopicPermissions
                                {
                                    ManageTopic = Random.Shared.Next() % 2 == 0,
                                    ReadTopic = Random.Shared.Next() % 2 == 0,
                                    PollMessages = Random.Shared.Next() % 2 == 0,
                                    SendMessages = Random.Shared.Next() % 2 == 0
                                }
                            },
                            {
                                Random.Shared.Next(31, 69),
                                new TopicPermissions
                                {
                                    ManageTopic = Random.Shared.Next() % 2 == 0,
                                    ReadTopic = Random.Shared.Next() % 2 == 0,
                                    PollMessages = Random.Shared.Next() % 2 == 0,
                                    SendMessages = Random.Shared.Next() % 2 == 0
                                }
                            }
                        }
                    }
                },
                {
                    Random.Shared.Next(31, 69), new StreamPermissions
                    {
                        ManageStream = Random.Shared.Next() % 2 == 0,
                        ReadStream = Random.Shared.Next() % 2 == 0,
                        ManageTopics = Random.Shared.Next() % 2 == 0,
                        ReadTopics = Random.Shared.Next() % 2 == 0,
                        PollMessages = Random.Shared.Next() % 2 == 0,
                        SendMessages = Random.Shared.Next() % 2 == 0,
                        Topics = null
                    }
                }
            }
        };
    }

    internal static Permissions PermissionsFromBytes(byte[] bytes)
    {
        var streamMap = new Dictionary<int, StreamPermissions>();
        var index = 0;

        var globalPermissions = new GlobalPermissions
        {
            ManageServers = bytes[index++] == 1,
            ReadServers = bytes[index++] == 1,
            ManageUsers = bytes[index++] == 1,
            ReadUsers = bytes[index++] == 1,
            ManageStreams = bytes[index++] == 1,
            ReadStreams = bytes[index++] == 1,
            ManageTopics = bytes[index++] == 1,
            ReadTopics = bytes[index++] == 1,
            PollMessages = bytes[index++] == 1,
            SendMessages = bytes[index++] == 1
        };

        if (bytes[index++] == 1)
        {
            while (true)
            {
                var streamId = BinaryPrimitives.ReadInt32LittleEndian(bytes[index..(index + 4)]);
                index += sizeof(int);

                var manageStream = bytes[index++] == 1;
                var readStream = bytes[index++] == 1;
                var manageTopics = bytes[index++] == 1;
                var readTopics = bytes[index++] == 1;
                var pollMessagesStream = bytes[index++] == 1;
                var sendMessagesStream = bytes[index++] == 1;
                var topicsMap = new Dictionary<int, TopicPermissions>();

                if (bytes[index++] == 1)
                {
                    while (true)
                    {
                        var topicId = BinaryPrimitives.ReadInt32LittleEndian(bytes[index..(index + 4)]);
                        index += sizeof(int);

                        var manageTopic = bytes[index++] == 1;
                        var readTopic = bytes[index++] == 1;
                        var pollMessagesTopic = bytes[index++] == 1;
                        var sendMessagesTopic = bytes[index++] == 1;

                        topicsMap.Add(topicId,
                            new TopicPermissions
                            {
                                ManageTopic = manageTopic,
                                ReadTopic = readTopic,
                                PollMessages = pollMessagesTopic,
                                SendMessages = sendMessagesTopic
                            });

                        if (bytes[index++] == 0)
                        {
                            break;
                        }
                    }
                }

                streamMap.Add(streamId,
                    new StreamPermissions
                    {
                        ManageStream = manageStream,
                        ReadStream = readStream,
                        ManageTopics = manageTopics,
                        ReadTopics = readTopics,
                        PollMessages = pollMessagesStream,
                        SendMessages = sendMessagesStream,
                        Topics = topicsMap.Count > 0 ? topicsMap : null
                    });

                if (bytes[index++] == 0)
                {
                    break;
                }
            }
        }

        return new Permissions
        {
            Global = globalPermissions,
            Streams = streamMap.Count > 0 ? streamMap : null
        };
    }
}