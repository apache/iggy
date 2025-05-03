using Iggy_SDK.Enums;
namespace Iggy_SDK.Contracts.Http;

public sealed class CreateUserRequest
{
    public required string Username { get; init; }
    public required string Password { get; init; }
    public required UserStatus Status { get; init; }
    public Permissions? Permissions { get; init; }
}