using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
namespace Iggy_SDK.JsonConfiguration;

public static class JsonConverterFactory
{
    public static JsonSerializerOptions SnakeCaseOptions
        => new() 
        {
            PropertyNamingPolicy = new ToSnakeCaseNamingPolicy(),
            WriteIndented = true,
            //This code makes the source generated JsonSerializer work with JsonIgnore
            //attribute for required properties
            TypeInfoResolver = new DefaultJsonTypeInfoResolver
            {
                Modifiers =
                {
                    ti =>
                    {
                        if (ti.Kind == JsonTypeInfoKind.Object)
                        {
                            JsonPropertyInfo[] props = ti.Properties
                                .Where(prop => prop.AttributeProvider == null || prop.AttributeProvider
                                    .GetCustomAttributes(typeof(JsonIgnoreAttribute), false).Length == 0)
                                .ToArray();

                            if (props.Length != ti.Properties.Count)
                            {
                                ti.Properties.Clear();
                                foreach (var prop in props)
                                {
                                    ti.Properties.Add(prop);
                                }
                            }
                        }
                    }
                }
            },
            Converters = 
            {
                new UInt128Converter(), 
                new JsonStringEnumConverter(new ToSnakeCaseNamingPolicy())
            }
        };
    public static JsonSerializerOptions StreamResponseOptions
        => new() 
        {
            Converters =
            {
                new StreamResponseConverter()
            }
        };
    
    public static JsonSerializerOptions AuthResponseOptions
        => new() 
        {
            Converters =
            {
                new AuthResponseConverter()
            }
        };
    
    public static JsonSerializerOptions TopicResponseOptions
        => new()
        {
            Converters =
            {
                new TopicResponseConverter()
            }
        };
    
    public static JsonSerializerOptions StatsResponseOptions
        => new()
        {
            Converters =
            {
                new StatsResponseConverter()
            }
        };
    
    public static JsonSerializerOptions CreateTopicOptions
        => new()
        {
            Converters =
            {
                new CreateTopicConverter()
            }
        };
    
    public static JsonSerializerOptions MessageResponseOptions(Func<byte[], byte[]>? decryptor)
        => new()
        {
            Converters =
            {
                new MessageResponseConverter(decryptor)
            }
        };
    
    public static JsonSerializerOptions HttpMessageOptions
        => new()
        {
            Converters =
            {
                new MessageConverter()
            }
        };
    
    public static JsonSerializerOptions MessagesOptions
        => new()
        {
            Converters =
            {
                new MessagesConverter()
            }
        };
    public static JsonSerializerOptions PersonalAccessTokenOptions
        => new()
        {
            Converters =
            {
                new PersonalAccessTokenResponseConverter()
            }
        };
    
    public static JsonSerializerOptions MessageResponseGenericOptions<TMessage>(Func<byte[], TMessage> serializer, Func<byte[], byte[]>? decryptor)
        => new()
        {
            Converters =
            {
                new MessageResponseGenericConverter<TMessage>(serializer, decryptor)
            }
        };
}