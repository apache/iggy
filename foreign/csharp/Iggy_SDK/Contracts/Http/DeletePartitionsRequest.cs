using System.Text.Json.Serialization;

namespace Iggy_SDK.Contracts.Http;

public sealed class DeletePartitionsRequest
{
    [JsonIgnore]
    public required Identifier StreamId { get; init; }
    [JsonIgnore]
    public required Identifier TopicId { get; init; }
    public required int PartitionsCount { get; init; }
}