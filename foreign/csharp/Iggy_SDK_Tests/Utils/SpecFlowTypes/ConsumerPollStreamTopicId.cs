namespace Iggy_SDK_Tests.Utils.SpecFlowTypes;

public sealed class ConsumerPollStreamTopicId
{
    public required int StreamId { get; init; }
    public required int ConsumerTopicId { get; init; }
    public required int ConsumerGroupTopicId { get; init; }
}