namespace Iggy_SDK.Contracts.Http;

public sealed class ConsumerGroupInfo
{
    public required int StreamId { get; init; }
    public required int TopicId { get; init; }
    public required int ConsumerGroupId { get; init; }
}