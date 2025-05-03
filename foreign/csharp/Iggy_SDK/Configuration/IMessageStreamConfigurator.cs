using Iggy_SDK.Enums;

namespace Iggy_SDK.Configuration;

public interface IMessageStreamConfigurator
{
    public string BaseAdress { get; set; }
    public Protocol Protocol { get; set; }
    public Action<MessageBatchingSettings> MessageBatchingSettings { get; set; }
    public Action<MessagePollingSettings> MessagePollingSettings { get; set; }
    public Action<TlsSettings> TlsSettings { get; set; }
    public int ReceiveBufferSize { get; set; }
    public int SendBufferSize { get; set; }
}