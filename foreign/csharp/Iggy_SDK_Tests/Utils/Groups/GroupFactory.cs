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

using Apache.Iggy.Contracts.Http;

namespace Apache.Iggy.Tests.Utils.Groups;

internal static class ConsumerGroupFactory
{

    internal static (int id, int membersCount, int partitionsCount, string name) CreateConsumerGroupResponseFields()
    {
        int id1 = Random.Shared.Next(1, 10);
        int membersCount1 = Random.Shared.Next(1, 10);
        int partitionsCount1 = Random.Shared.Next(1, 10);
        string name = Utility.RandomString(69);
        return (id1, membersCount1, partitionsCount1, name);
    }
    internal static ConsumerGroupResponse CreateGroupResponse()
    {
        return new ConsumerGroupResponse
        {
            Id = Random.Shared.Next(1, 10),
            Name = Utility.RandomString(69),
            MembersCount = Random.Shared.Next(1, 10),
            PartitionsCount = Random.Shared.Next(1, 10)
        };
    }

    internal static IEnumerable<ConsumerGroupResponse> CreateGroupsResponse(int count)
    {
        foreach (int x in Enumerable.Range(1, count))
            yield return new ConsumerGroupResponse
            {
                Id = Random.Shared.Next(1, 10),
                Name = Utility.RandomString(69),
                MembersCount = Random.Shared.Next(1, 10),
                PartitionsCount = Random.Shared.Next(1, 10)
            };
    }

    internal static IEnumerable<ConsumerGroupResponse> Empty()
    {
        return Enumerable.Empty<ConsumerGroupResponse>();
    }

    internal static CreateConsumerGroupRequest CreateRequest(int streamId, int topicId, int groupId)
    {
        return new CreateConsumerGroupRequest
        {
            Name = Utility.RandomString(69),
            StreamId = Identifier.Numeric(streamId),
            TopicId = Identifier.Numeric(topicId),
            ConsumerGroupId = groupId,
        };
    }
    internal static JoinConsumerGroupRequest CreateJoinGroupRequest(int streamId, int topicId, int groupId)
    {
        return new JoinConsumerGroupRequest
        {
            StreamId = Identifier.Numeric(streamId),
            ConsumerGroupId = Identifier.Numeric(groupId),
            TopicId = Identifier.Numeric(topicId)
        };
    }
    internal static JoinConsumerGroupRequest CreateJoinGroupRequest()
    {
        return new JoinConsumerGroupRequest
        {
            StreamId = Identifier.Numeric(Random.Shared.Next(1, 10)),
            ConsumerGroupId = Identifier.Numeric(Random.Shared.Next(1, 10)),
            TopicId = Identifier.Numeric(Random.Shared.Next(1, 10))
        };
    }

    internal static LeaveConsumerGroupRequest CreateLeaveGroupRequest(int streamId, int topicId, int groupId)
    {
        return new LeaveConsumerGroupRequest
        {
            StreamId = Identifier.Numeric(streamId),
            ConsumerGroupId = Identifier.Numeric(groupId),
            TopicId = Identifier.Numeric(topicId)
        };
    }
    internal static DeleteConsumerGroupRequest CreateDeleteGroupRequest(int streamId, int topicId, int groupId)
    {
        return new DeleteConsumerGroupRequest
        {
            StreamId = Identifier.Numeric(streamId),
            ConsumerGroupId = Identifier.Numeric(groupId),
            TopicId = Identifier.Numeric(topicId)
        };
    }
    internal static LeaveConsumerGroupRequest CreateLeaveGroupRequest()
    {
        return new LeaveConsumerGroupRequest
        {
            StreamId = Identifier.Numeric(Random.Shared.Next(1, 10)),
            ConsumerGroupId = Identifier.Numeric(Random.Shared.Next(1, 10)),
            TopicId = Identifier.Numeric(Random.Shared.Next(1, 10))
        };
    }
}