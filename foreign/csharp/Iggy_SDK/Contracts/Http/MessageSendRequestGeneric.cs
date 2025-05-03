using Iggy_SDK.Kinds;
namespace Iggy_SDK.Contracts.Http;

public sealed class MessageSendRequest<TMessage>
{
    public required Identifier StreamId { get; init; }
    public required Identifier TopicId { get; init; }
    public required Partitioning Partitioning { get; init; }
    public required IList<TMessage> Messages { get; init; }
}