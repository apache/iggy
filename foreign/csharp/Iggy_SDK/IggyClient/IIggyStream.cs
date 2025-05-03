using Iggy_SDK.Contracts.Http;
namespace Iggy_SDK.IggyClient;

public interface IIggyStream
{
    Task CreateStreamAsync(StreamRequest request, CancellationToken token = default);
    Task<StreamResponse?> GetStreamByIdAsync(Identifier streamId, CancellationToken token = default);
    Task UpdateStreamAsync(Identifier streamId, UpdateStreamRequest request, CancellationToken token = default);
    Task<IReadOnlyList<StreamResponse>> GetStreamsAsync(CancellationToken token = default);
    Task DeleteStreamAsync(Identifier streamId, CancellationToken token = default);

}