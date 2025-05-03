namespace Iggy_SDK.Contracts.Http;

public sealed class LeaveConsumerGroupRequest
{
    public required Identifier StreamId { get; init; }
    public required Identifier TopicId { get; init; }
    public required Identifier ConsumerGroupId { get; init; }
}