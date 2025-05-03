using Iggy_SDK.Enums;
using System.Text.Json.Serialization;
namespace Iggy_SDK.Contracts.Http;

public sealed class UpdateUserRequest
{
    [JsonIgnore]
    public required Identifier UserId { get; init; }
    public string? Username { get; init; }
    public UserStatus? UserStatus { get; init; }
}