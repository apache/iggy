using Iggy_SDK.Contracts.Http;
namespace Iggy_SDK.IggyClient;

public interface IIggyUtils
{
    public Task<Stats?> GetStatsAsync(CancellationToken token = default);
    public Task<IReadOnlyList<ClientResponse>> GetClientsAsync(CancellationToken token = default);
    public Task<ClientResponse?> GetClientByIdAsync(uint clientId, CancellationToken token = default);
}