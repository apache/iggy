using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Extensions;
using System.Text.Json;
using System.Text.Json.Serialization;
namespace Iggy_SDK.JsonConfiguration;

public sealed class PersonalAccessTokenResponseConverter : JsonConverter<PersonalAccessTokenResponse>
{
    public override PersonalAccessTokenResponse? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var doc = JsonDocument.ParseValue(ref reader);
        var root = doc.RootElement;

        var name = root.GetProperty(nameof(PersonalAccessTokenResponse.Name).ToSnakeCase()).GetString();
        root.TryGetProperty(nameof(PersonalAccessTokenResponse.ExpiryAt).ToSnakeCase(), out var expiryElement);
        DateTimeOffset? expiry = expiryElement.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.Undefined => null,
            JsonValueKind.Number => DateTimeOffsetUtils.FromUnixTimeMicroSeconds(expiryElement.GetUInt64()).LocalDateTime,
            _ => throw new ArgumentOutOfRangeException()
        };
        return new PersonalAccessTokenResponse
        {
            Name = name!,
            ExpiryAt = expiry
        };
    }
    public override void Write(Utf8JsonWriter writer, PersonalAccessTokenResponse value, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }
}