namespace Iggy_SDK.Contracts.Http;

public sealed class ClientResponse
{
    public required uint ClientId { get; init; }
    public required string Adress { get; init; }
    public required uint UserId { get; init; }
    public required string Transport { get; init; }
    public required int ConsumerGroupsCount { get; init; }
    public IEnumerable<ConsumerGroupInfo>? ConsumerGroups { get; init; }
}