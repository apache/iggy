namespace Iggy_SDK.Contracts.Http;

public sealed class PolledMessages
{
    public required int PartitionId { get; init; }
    public required ulong CurrentOffset { get; init; }
    public required IReadOnlyList<MessageResponse> Messages { get; init; }

    public static PolledMessages Empty =>
        new()
        {
            Messages = new List<MessageResponse>().AsReadOnly(),
            CurrentOffset = 0,
            PartitionId = 0,
        };
}