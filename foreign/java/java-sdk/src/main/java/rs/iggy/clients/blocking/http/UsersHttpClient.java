package rs.iggy.clients.blocking.http;

import com.fasterxml.jackson.core.type.TypeReference;
import rs.iggy.clients.blocking.UsersClient;
import rs.iggy.identifier.UserId;
import rs.iggy.user.*;
import java.util.List;
import java.util.Optional;

class UsersHttpClient implements UsersClient {

    private static final String USERS = "/users";
    private final InternalHttpClient httpClient;

    public UsersHttpClient(InternalHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public Optional<UserInfoDetails> getUser(UserId userId) {
        var request = httpClient.prepareGetRequest(USERS + "/" + userId);
        return httpClient.executeWithOptionalResponse(request, UserInfoDetails.class);
    }

    @Override
    public List<UserInfo> getUsers() {
        var request = httpClient.prepareGetRequest(USERS);
        return httpClient.execute(request, new TypeReference<>() {
        });
    }

    @Override
    public UserInfoDetails createUser(String username, String password, UserStatus status, Optional<Permissions> permissions) {
        var request = httpClient.preparePostRequest(USERS, new CreateUser(username, password, status, permissions));
        return httpClient.execute(request, new TypeReference<>() {
        });
    }

    @Override
    public void deleteUser(UserId userId) {
        var request = httpClient.prepareDeleteRequest(USERS + "/" + userId);
        httpClient.execute(request);
    }

    @Override
    public void updateUser(UserId userId, Optional<String> username, Optional<UserStatus> status) {
        var request = httpClient.preparePutRequest(USERS + "/" + userId, new UpdateUser(username, status));
        httpClient.execute(request);
    }

    @Override
    public void updatePermissions(UserId userId, Optional<Permissions> permissions) {
        var request = httpClient.preparePutRequest(USERS + "/" + userId + "/permissions",
                new UpdatePermissions(permissions));
        httpClient.execute(request);
    }

    @Override
    public void changePassword(UserId userId, String currentPassword, String newPassword) {
        var request = httpClient.preparePutRequest(USERS + "/" + userId + "/password",
                new ChangePassword(currentPassword, newPassword));
        httpClient.execute(request);
    }

    @Override
    public IdentityInfo login(String username, String password) {
        String version = "0.6.30";
        String context = "java-sdk";
        var request = httpClient.preparePostRequest(USERS + "/login", new Login(username, password, version, context));
        var response = httpClient.execute(request, IdentityInfo.class);
        httpClient.setToken(response.accessToken().map(TokenInfo::token));
        return response;
    }

    @Override
    public void logout() {
        var request = httpClient.prepareDeleteRequest(USERS + "/logout");
        httpClient.execute(request);
        httpClient.setToken(Optional.empty());
    }

    record Login(String username, String password, String version, String context) {
    }

    private record CreateUser(String username, String password, UserStatus status, Optional<Permissions> permissions) {
    }

    private record UpdateUser(Optional<String> username, Optional<UserStatus> status) {
    }

    private record UpdatePermissions(Optional<Permissions> permissions) {
    }

    private record ChangePassword(String currentPassword, String newPassword) {
    }
}
