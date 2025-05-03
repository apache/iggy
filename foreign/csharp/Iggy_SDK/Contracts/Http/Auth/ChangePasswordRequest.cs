using System.Text.Json.Serialization;
namespace Iggy_SDK.Contracts.Http;

public sealed class ChangePasswordRequest
{
    [JsonIgnore]
    public required Identifier UserId { get; init; }
    public required string CurrentPassword { get; init; }
    public required string NewPassword { get; init; }
}