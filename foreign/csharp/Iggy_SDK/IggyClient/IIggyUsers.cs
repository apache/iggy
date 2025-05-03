using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Contracts.Http.Auth;

namespace Iggy_SDK.IggyClient;

public interface IIggyUsers
{
    public Task<UserResponse?> GetUser(Identifier userId, CancellationToken token = default);
    public Task<IReadOnlyList<UserResponse>> GetUsers(CancellationToken token = default);
    public Task CreateUser(CreateUserRequest request, CancellationToken token = default);
    public Task DeleteUser(Identifier userId, CancellationToken token = default);
    public Task UpdateUser(UpdateUserRequest request, CancellationToken token = default);
    public Task UpdatePermissions(UpdateUserPermissionsRequest request, CancellationToken token = default);
    public Task ChangePassword(ChangePasswordRequest request, CancellationToken token = default);
    public Task<AuthResponse?> LoginUser(LoginUserRequest request, CancellationToken token = default);
    public Task LogoutUser(CancellationToken token = default);
}