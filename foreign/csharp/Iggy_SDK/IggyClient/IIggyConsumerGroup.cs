using Iggy_SDK.Contracts.Http;
namespace Iggy_SDK.IggyClient;

public interface IIggyConsumerGroup
{
    Task<IReadOnlyList<ConsumerGroupResponse>> GetConsumerGroupsAsync(Identifier streamId, Identifier topicId,
        CancellationToken token = default);
    Task<ConsumerGroupResponse?> GetConsumerGroupByIdAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default);
    Task CreateConsumerGroupAsync(CreateConsumerGroupRequest request, CancellationToken token = default);
    Task DeleteConsumerGroupAsync(DeleteConsumerGroupRequest request, CancellationToken token = default);
    Task JoinConsumerGroupAsync(JoinConsumerGroupRequest request, CancellationToken token = default);
    Task LeaveConsumerGroupAsync(LeaveConsumerGroupRequest request, CancellationToken token = default);

}