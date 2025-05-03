using Iggy_SDK_Tests.Utils.Users;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Contracts.Http.Auth;
using Iggy_SDK.IggyClient;

namespace Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;

public class UsersFixtureBootstrap : IIggyBootstrap
{
    public static CreateUserRequest UserRequest = UsersFactory.CreateUserRequest("user1", "user1");
    public static Permissions UpdatePermissionsRequest = UsersFactory.CreatePermissions();
    public const string NewUsername = "new_username";
    public static LoginUserRequest LoginRequest = new LoginUserRequest{ Username = "user1", Password = "user1" };
    public Task BootstrapResourcesAsync(int tcpPort, int httpPort, IIggyClient httpClient, IIggyClient tcpClient)
    {
        return Task.CompletedTask;
    }
}