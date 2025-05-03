using Iggy_SDK.Contracts.Http;
namespace Iggy_SDK.IggyClient;

public interface IIggyPersonalAccessToken
{
    public Task<IReadOnlyList<PersonalAccessTokenResponse>> GetPersonalAccessTokensAsync(CancellationToken token = default);
    public Task<RawPersonalAccessToken?> CreatePersonalAccessTokenAsync(CreatePersonalAccessTokenRequest request, CancellationToken token = default);
    public Task DeletePersonalAccessTokenAsync(DeletePersonalAccessTokenRequest request, CancellationToken token = default);
    public Task<AuthResponse?> LoginWithPersonalAccessToken(LoginWithPersonalAccessToken request, CancellationToken token = default);
}