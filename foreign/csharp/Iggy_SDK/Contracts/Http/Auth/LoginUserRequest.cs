namespace Iggy_SDK.Contracts.Http.Auth;

public sealed class LoginUserRequest
{
    public required string Username { get; init; }
    public required string Password { get; init; }
    
    public string? Version { get; init; }
    
    public string? Context { get; init; }
}