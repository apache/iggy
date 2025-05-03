using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Enums;
using Iggy_SDK.Extensions;
using Iggy_SDK.Headers;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Iggy_SDK.JsonConfiguration;

internal sealed class MessageResponseConverter : JsonConverter<PolledMessages>
{
    private readonly Func<byte[], byte[]>? _decryptor;
    public MessageResponseConverter(Func<byte[], byte[]>? decryptor)
    {
        _decryptor = decryptor;
    }

    public override PolledMessages Read(ref Utf8JsonReader reader, Type typeToConvert,
        JsonSerializerOptions options)
    {
        using var doc = JsonDocument.ParseValue(ref reader);

        var root = doc.RootElement;

        var partitionId = root.GetProperty(nameof(PolledMessages.PartitionId).ToSnakeCase()).GetInt32();
        var currentOffset = root.GetProperty(nameof(PolledMessages.CurrentOffset).ToSnakeCase()).GetUInt64();
        var messages = root.GetProperty(nameof(PolledMessages.Messages).ToSnakeCase());
        //var messagesCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[12..16]);

        var messageResponses = new List<MessageResponse>();
        foreach (var element in messages.EnumerateArray())
        {
            var offset = element.GetProperty(nameof(MessageResponse.Offset).ToSnakeCase()).GetUInt64();
            var timestamp = element.GetProperty(nameof(MessageResponse.Timestamp).ToSnakeCase()).GetUInt64();
            var id = element.GetProperty(nameof(MessageResponse.Id).ToSnakeCase()).GetUInt128();
            var payload = element.GetProperty(nameof(MessageResponse.Payload).ToSnakeCase()).GetBytesFromBase64();
            var checksum = element.GetProperty(nameof(MessageResponse.Checksum).ToSnakeCase()).GetUInt32();
            var state = element.GetProperty(nameof(MessageResponse.State).ToSnakeCase()).GetString() switch
            {
                "available" => MessageState.Available,
                "unavailable" => MessageState.Unavailable,
                "poisoned" => MessageState.Poisoned,
                "marked_for_deletion" => MessageState.MarkedForDeletion,
                _ => throw new ArgumentOutOfRangeException()
            };
            var headersElement = element.GetProperty(nameof(MessageResponse.Headers).ToSnakeCase());

            var headers = new Dictionary<HeaderKey, HeaderValue>();
            if (headersElement.ValueKind != JsonValueKind.Null)
            {
                var headersJsonArray = headersElement.EnumerateObject();
                foreach (var header in headersJsonArray)
                {
                    var headerKey = header.Name;
                    var headerKind = header.Value.GetProperty(nameof(HeaderValue.Kind).ToSnakeCase()).GetString();
                    var headerValue = header.Value.GetProperty(nameof(HeaderValue.Value).ToSnakeCase()).GetBytesFromBase64();
                    headers.Add(HeaderKey.New(headerKey), new HeaderValue
                    {
                        Kind = headerKind switch
                        {
                            "bool" => HeaderKind.Bool,
                            "int32" => HeaderKind.Int32,
                            "int64" => HeaderKind.Int64,
                            "int128" => HeaderKind.Int128,
                            "uint32" => HeaderKind.Uint32,
                            "uint64" => HeaderKind.Uint64,
                            "uint128" => HeaderKind.Uint128,
                            "float32" => HeaderKind.Float,
                            "float64" => HeaderKind.Double,
                            "string" => HeaderKind.String,
                            "raw" => HeaderKind.Raw,
                            /*
                            "raw" => Ok(HeaderKind::Raw),
                            "string" => Ok(HeaderKind::String),
                            "bool" => Ok(HeaderKind::Bool),
                            "int8" => Ok(HeaderKind::Int8),
                            "int16" => Ok(HeaderKind::Int16),
                            "int32" => Ok(HeaderKind::Int32),
                            "int64" => Ok(HeaderKind::Int64),
                            "int128" => Ok(HeaderKind::Int128),
                            "uint8" => Ok(HeaderKind::Uint8),
                            "uint16" => Ok(HeaderKind::Uint16),
                            "uint32" => Ok(HeaderKind::Uint32),
                            "uint64" => Ok(HeaderKind::Uint64),
                            "uint128" => Ok(HeaderKind::Uint128),
                            "float32" => Ok(HeaderKind::Float32),
                            "float64" => Ok(HeaderKind::Float64),
                            */
                            _ => throw new ArgumentOutOfRangeException()
                        },
                        Value = headerValue
                    });
                }
            }
            messageResponses.Add(new MessageResponse
            {
                Id = new Guid(id.GetBytesFromUInt128()),
                Offset = offset,
                Timestamp = timestamp,
                Checksum = checksum,
                Headers = headers.Count > 0 ? headers : null,
                State = state,
                Payload = _decryptor is not null ? _decryptor(payload) : payload
            });
        }

        return new PolledMessages
        {
            Messages = messageResponses.AsReadOnly(),
            CurrentOffset = currentOffset,
            PartitionId = partitionId
        };
    }


    public override void Write(Utf8JsonWriter writer, PolledMessages value, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }
}