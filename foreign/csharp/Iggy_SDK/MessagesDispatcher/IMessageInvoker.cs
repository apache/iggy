using Iggy_SDK.Contracts.Http;
namespace Iggy_SDK.MessagesDispatcher;
internal interface IMessageInvoker
{
    internal Task SendMessagesAsync(MessageSendRequest request,
        CancellationToken token = default);
}