using Iggy_SDK.Kinds;
using System.Text.Json.Serialization;

namespace Iggy_SDK.Contracts.Http;

public sealed class StoreOffsetRequest
{
    [JsonIgnore]
    public required Identifier StreamId { get; init; }
    [JsonIgnore]
    public required Identifier TopicId { get; init; }
    public required Consumer Consumer { get; init; }
    public required int PartitionId { get; init; }
    public required ulong Offset { get; init; }
}