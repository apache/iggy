using Iggy_SDK.Enums;
namespace Iggy_SDK.Contracts.Http;

public sealed class UserResponse
{
    public required uint Id { get; init; }
    public required ulong CreatedAt { get; init; }
    public required UserStatus Status { get; init; }
    public required string Username { get; init; }
    public Permissions? Permissions { get; init; }
}