namespace Iggy_SDK.Contracts.Http;

public sealed class PartitionContract
{
    public required int Id { get; init; }
    public required ulong MessagesCount { get; init; }
    public required DateTimeOffset CreatedAt { get; init; }
    public required int SegmentsCount { get; init; }
    public required ulong CurrentOffset { get; init; }
    public required ulong Size { get; init; }
}