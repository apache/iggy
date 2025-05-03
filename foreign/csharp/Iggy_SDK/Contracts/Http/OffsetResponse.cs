namespace Iggy_SDK.Contracts.Http;

public sealed class OffsetResponse
{
    public required int PartitionId { get; init; }
    public required ulong CurrentOffset { get; init; }
    public required ulong StoredOffset { get; init; }
}