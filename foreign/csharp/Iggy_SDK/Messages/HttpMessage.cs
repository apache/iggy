using Iggy_SDK.Headers;

namespace Iggy_SDK.Messages;

//[JsonConverter(typeof(MessageConverter))]
public struct HttpMessage
{
    public required UInt128 Id { get; init; }
    public required string Payload { get; init; }
    public Dictionary<HeaderKey, HeaderValue>? Headers { get; init; }
}