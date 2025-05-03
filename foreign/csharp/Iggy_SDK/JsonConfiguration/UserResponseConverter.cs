using Iggy_SDK.Contracts.Http;
using System.Text.Json;
using System.Text.Json.Serialization;
namespace Iggy_SDK.JsonConfiguration;

public sealed class UserResponseConverter : JsonConverter<UserResponse>
{

    public override UserResponse? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }
    public override void Write(Utf8JsonWriter writer, UserResponse value, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }
}