namespace Iggy_SDK.Contracts.Http;

public sealed class PersonalAccessTokenResponse
{
   public required string Name { get; init; } 
   public DateTimeOffset? ExpiryAt { get; init; }
}