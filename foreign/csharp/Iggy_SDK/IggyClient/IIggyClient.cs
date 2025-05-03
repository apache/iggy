namespace Iggy_SDK.IggyClient;

public interface IIggyClient : IIggyPublisher, IIggyStream, IIggyTopic, IIggyConsumer, IIggyOffset, IIggyConsumerGroup,
    IIggyUtils, IIggyPartition, IIggyUsers, IIggyPersonalAccessToken
{
}