using Iggy_SDK.Contracts.Http;
namespace Iggy_SDK.IggyClient;

public interface IIggyConsumer
{
    Task<PolledMessages> FetchMessagesAsync(MessageFetchRequest request, Func<byte[], byte[]>? decryptor = null,
        CancellationToken token = default);
    Task<PolledMessages<TMessage>> FetchMessagesAsync<TMessage>(MessageFetchRequest request,
        Func<byte[], TMessage> deserializer, Func<byte[], byte[]>? decryptor = null, CancellationToken token = default);
    IAsyncEnumerable<MessageResponse<TMessage>> PollMessagesAsync<TMessage>(PollMessagesRequest request,
        Func<byte[], TMessage> deserializer, Func<byte[], byte[]>? decryptor = null, 
        CancellationToken token = default);

}