using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Enums;
namespace Iggy_SDK_Tests.Utils.Users;

public static class UsersFactory
{
    public static CreateUserRequest CreateUserRequest(string? username = null, string?  password = null, Permissions? permissions = null)
    {
        return new CreateUserRequest
        {
            Password = username ?? Utility.RandomString(Random.Shared.Next(5,25)),
            Username = password ?? Utility.RandomString(Random.Shared.Next(5,25)), 
            Status = UserStatus.Active,
            Permissions = permissions ?? CreatePermissions()
        };
    }
    public static Dictionary<int, StreamPermissions> CreateStreamPermissions(int streamId = 1, int topicId = 1)
    {
        var streamsPermission = new Dictionary<int, StreamPermissions>();
        var topicPermissions = new Dictionary<int, TopicPermissions>();
        topicPermissions.Add(topicId, new TopicPermissions
        {
            ManageTopic = Random.Shared.Next(1) == 1,
            PollMessages = Random.Shared.Next(1) == 1,
            ReadTopic = Random.Shared.Next(1) == 1,
            SendMessages = Random.Shared.Next(1) == 1
        });
        streamsPermission.Add(streamId, new StreamPermissions
        {
            ManageStream = Random.Shared.Next(1) == 1,
            ManageTopics =  Random.Shared.Next(1) == 1,
            ReadStream =    Random.Shared.Next(1) == 1,
            ReadTopics =    Random.Shared.Next(1) == 1,
            PollMessages =  Random.Shared.Next(1) == 1,
            SendMessages =  Random.Shared.Next(1) == 1,
            Topics = topicPermissions
        });
        return streamsPermission;
    }
    public static Permissions CreatePermissions()
    {
        return new Permissions
        {
            Global = new GlobalPermissions
            {
                ManageServers = Random.Shared.Next(1) == 1,
                ManageUsers = Random.Shared.Next(1) == 1,
                ManageStreams = Random.Shared.Next(1) == 1,
                ManageTopics = Random.Shared.Next(1) == 1,
                PollMessages = Random.Shared.Next(1) == 1,
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