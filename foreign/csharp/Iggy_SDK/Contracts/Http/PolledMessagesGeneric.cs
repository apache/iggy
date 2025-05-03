namespace Iggy_SDK.Contracts.Http;

public sealed class PolledMessages<T>
{
    public required int PartitionId { get; init; }
    public required ulong CurrentOffset { get; init; }
    public required IReadOnlyList<MessageResponse<T>> Messages { get; init; }

    public static PolledMessages<T> Empty =>
        new()
        {
            Messages = Array.Empty<MessageResponse<T>>().AsReadOnly(),
            CurrentOffset = 0,
            PartitionId = 0,
        };
}