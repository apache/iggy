namespace Iggy_SDK.Contracts.Http;

public sealed class ConsumerGroupMember
{
    public required int Id { get; init; }
    public required int PartitionsCount { get; init; }
    public required List<int> Partitions { get; init; }
}