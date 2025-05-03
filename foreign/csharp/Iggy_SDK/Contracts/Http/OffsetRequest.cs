using Iggy_SDK.Kinds;

namespace Iggy_SDK.Contracts.Http;

public sealed class OffsetRequest
{
    public required Consumer Consumer { get; init; }
    public required Identifier StreamId { get; init; }
    public required Identifier TopicId { get; init; }
    public required int PartitionId { get; init; }
}