using Iggy_SDK.Extensions;
using Iggy_SDK.Headers;
using Iggy_SDK.Messages;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Iggy_SDK.JsonConfiguration;

internal sealed class MessageConverter : JsonConverter<HttpMessage>
{
    public override HttpMessage Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }

    public override void Write(Utf8JsonWriter writer, HttpMessage value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        var jsonOptions = new JsonSerializerOptions();
        jsonOptions.Converters.Add(new UInt128Converter());

        writer.WritePropertyName(nameof(value.Id).ToSnakeCase());
        var idJson = JsonSerializer.Serialize(value.Id, jsonOptions);
        using (JsonDocument doc = JsonDocument.Parse(idJson))
        {
            doc.RootElement.WriteTo(writer);
        }

        writer.WriteString(nameof(value.Payload).ToSnakeCase(), value.Payload);
        if (value.Headers is not null)
        {
            writer.WriteStartObject("headers");
            foreach (var (headerKey, headerValue) in value.Headers)
            {
                writer.WriteStartObject(headerKey.Value.ToSnakeCase());
                var headerKind = headerValue.Kind switch
                {
                    HeaderKind.Bool => "bool",
                    HeaderKind.Int32 => "int32",
                    HeaderKind.Int64 => "int64",
                    HeaderKind.Int128 => "int128",
                    HeaderKind.Uint32 => "uint32",
                    HeaderKind.Uint64 => "uint64",
                    HeaderKind.Uint128 => "uint128",
                    HeaderKind.Float => "float32",
                    HeaderKind.Double => "float64",
                    HeaderKind.String => "string",
                    HeaderKind.Raw => "raw",
                    _ => throw new ArgumentOutOfRangeException()
                };
                writer.WriteString(nameof(headerValue.Kind).ToSnakeCase(), headerKind);
                writer.WriteBase64String(nameof(headerValue.Value).ToSnakeCase(), headerValue.Value);
                writer.WriteEndObject();
            }
            writer.WriteEndObject();

        }
        writer.WriteEndObject();
    }
}