using Iggy_SDK.Enums;
using Iggy_SDK.Kinds;
namespace Iggy_SDK.Contracts.Http;

public sealed class PollMessagesRequest
{
    public required Consumer Consumer { get; init; }
    public required Identifier StreamId { get; init; }
    public required Identifier TopicId { get; init; }
    public required int PartitionId { get; init; }
    public required PollingStrategy PollingStrategy { get; set; }
    public required int Count { get; init; }
}