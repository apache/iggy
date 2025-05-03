using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Enums;
using Iggy_SDK.Extensions;
using Iggy_SDK.Messages;
using System.ComponentModel;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Iggy_SDK.JsonConfiguration;

internal sealed class MessagesConverter : JsonConverter<MessageSendRequest>
{
    public override MessageSendRequest? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }

    public override void Write(Utf8JsonWriter writer, MessageSendRequest value, JsonSerializerOptions options)
    {
        if (value.Messages.Any())
        {
            var msgList = new List<HttpMessage>();
            foreach (var message in value.Messages)
            {
                var base64 = Convert.ToBase64String(message.Payload);
                msgList.Add(new HttpMessage
                {
                    Id = message.Id.ToUInt128(),
                    Payload = base64,
                    Headers = message.Headers,
                });
            }

            writer.WriteStartObject();
            writer.WriteStartObject("partitioning");

            writer.WriteString(nameof(MessageSendRequest.Partitioning.Kind).ToSnakeCase(), value: value.Partitioning.Kind switch
            {
                Partitioning.Balanced => "none",
                Partitioning.MessageKey => "entity_id",
                Partitioning.PartitionId => "partition_id",
                _ => throw new InvalidEnumArgumentException()
            });
            writer.WriteBase64String(nameof(MessageSendRequest.Partitioning.Value).ToSnakeCase(), value.Partitioning.Value);
            writer.WriteEndObject();

            writer.WriteStartArray("messages");
            foreach (var msg in msgList)
            {
                JsonSerializer.Serialize(writer, msg, JsonConverterFactory.HttpMessageOptions);
            }
            writer.WriteEndArray();

            writer.WriteEndObject();
            return;
        }
        writer.WriteStringValue("");
    }
}