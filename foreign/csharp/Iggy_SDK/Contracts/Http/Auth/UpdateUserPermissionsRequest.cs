using System.Text.Json.Serialization;
namespace Iggy_SDK.Contracts.Http;

public sealed class UpdateUserPermissionsRequest
{
    [JsonIgnore]
    public required Identifier UserId { get; init; }
    public Permissions? Permissions { get; init; }
}