using Iggy_SDK;
using Iggy_SDK.Contracts.Http;

namespace Iggy_SDK_Tests.Utils.Groups;

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