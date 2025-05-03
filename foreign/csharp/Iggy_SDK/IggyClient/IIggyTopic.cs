using Iggy_SDK.Contracts.Http;
namespace Iggy_SDK.IggyClient;

public interface IIggyTopic
{
    Task<IReadOnlyList<TopicResponse>> GetTopicsAsync(Identifier streamId, CancellationToken token = default);
    Task<TopicResponse?> GetTopicByIdAsync(Identifier streamId, Identifier topicId, CancellationToken token = default);
    Task CreateTopicAsync(Identifier streamId, TopicRequest topic, CancellationToken token = default);
    Task UpdateTopicAsync(Identifier streamId, Identifier topicId, UpdateTopicRequest request, CancellationToken token = default);
    Task DeleteTopicAsync(Identifier streamId, Identifier topicId, CancellationToken token = default);
}