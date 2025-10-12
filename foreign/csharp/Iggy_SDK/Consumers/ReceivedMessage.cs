using Apache.Iggy.Contracts;

namespace Apache.Iggy.Consumers;

public class ReceivedMessage
{
    public required MessageResponse Message { get; init; }
    public required ulong CurrentOffset { get; init; }
    public uint PartitionId { get; init; }
    public MessageStatus Status { get; init; } = MessageStatus.Success;
    public Exception? Error { get; init; }
}
