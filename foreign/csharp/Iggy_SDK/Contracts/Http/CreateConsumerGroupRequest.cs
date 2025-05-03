using System.Text.Json.Serialization;

namespace Iggy_SDK.Contracts.Http;

public sealed class CreateConsumerGroupRequest
{
    [JsonIgnore]
    public required Identifier StreamId { get; init; }
    [JsonIgnore]
    public required Identifier TopicId { get; init; }
    public required int ConsumerGroupId { get; init; }
    public required string Name { get; init; }
}