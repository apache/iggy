using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Headers;
namespace Iggy_SDK.IggyClient;

public interface IIggyPublisher
{
    Task SendMessagesAsync(MessageSendRequest request, Func<byte[], byte[]>?
        encryptor = null, CancellationToken token = default);
    Task SendMessagesAsync<TMessage>(MessageSendRequest<TMessage> request,
         Func<TMessage, byte[]> serializer,
        Func<byte[], byte[]>? encryptor = null, Dictionary<HeaderKey, HeaderValue>? headers = null,
        CancellationToken token = default);
}