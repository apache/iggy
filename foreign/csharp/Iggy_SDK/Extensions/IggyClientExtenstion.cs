using Apache.Iggy.Consumers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Publishers;

namespace Apache.Iggy.Extensions;

public static class IggyClientExtenstion
{
    public static IggyConsumerBuilder CreateConsumerBuilder(this IIggyClient client, Identifier streamId, Identifier topicId)
    {
        return IggyConsumerBuilder.Create(client, streamId, topicId);
    }
    
    public static IggyPublisherBuilder CreatePublisherBuilder(this IIggyClient client, Identifier streamId, Identifier topicId)
    {
        return IggyPublisherBuilder.Create(client, streamId, topicId);
    }
}