namespace Iggy_SDK.Contracts.Http;

public sealed class CreatePersonalAccessTokenRequest
{
    public required string Name { get; init; }  
    public uint? Expiry { get; init; } 
}