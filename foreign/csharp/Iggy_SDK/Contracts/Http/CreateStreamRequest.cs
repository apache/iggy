namespace Iggy_SDK.Contracts.Http;

public sealed class StreamRequest
{
    public int? StreamId { get; init; }
    public required string Name { get; init; }
}