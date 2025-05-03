using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Extensions;
using System.Text.Json;
using System.Text.Json.Serialization;
namespace Iggy_SDK.JsonConfiguration;

public sealed class AuthResponseConverter : JsonConverter<AuthResponse>
{

    public override AuthResponse? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var doc = JsonDocument.ParseValue(ref reader);
        var root = doc.RootElement;
        
        var userId = root.GetProperty(nameof(AuthResponse.UserId).ToSnakeCase()).GetInt32();
        var accessToken = root.GetProperty(nameof(AuthResponse.AccessToken).ToSnakeCase());
        var token = accessToken.GetProperty(nameof(TokenInfo.Token).ToSnakeCase()).GetString();
        var accessTokenExpiry = accessToken.GetProperty(nameof(TokenInfo.Expiry).ToSnakeCase()).GetInt64();

        return new AuthResponse(
            userId,
            new TokenInfo(token, DateTimeOffset.FromUnixTimeSeconds(accessTokenExpiry).LocalDateTime)
        );
    }
    public override void Write(Utf8JsonWriter writer, AuthResponse value, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }
}