package rs.iggy.clients.blocking;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rs.iggy.user.*;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class UsersClientBaseTest extends IntegrationTest {

    protected UsersClient usersClient;

    @BeforeEach
    void beforeEachBase() {
        usersClient = client.users();
    }

    @Test
    void shouldLogin() {
        // when
        var identityInfo = usersClient.login("iggy", "iggy");

        // then
        assertThat(identityInfo).isNotNull();
        assertThat(identityInfo.userId()).isEqualTo(1L);
    }

    @Test
    void shouldGetUser() {
        // given
        login();

        // when
        var user = usersClient.getUser(1L);

        // then
        assertThat(user).isPresent();
    }

    @Test
    void shouldCreateAndDeleteUser() {
        // given
        login();

        // when
        var createdUser = usersClient.createUser("test",
                "test",
                UserStatus.Active,
                Optional.of(new Permissions(createGlobalPermissions(true), Collections.emptyMap())));

        // then
        assertThat(createdUser).isNotNull();
        assertThat(createdUser.permissions().map(Permissions::global).map(GlobalPermissions::manageServers).orElse(false)).isTrue();

        // when
        List<UserInfo> users = usersClient.getUsers();

        // then
        assertThat(users).hasSize(2);
        assertThat(users).map(UserInfo::username).containsExactlyInAnyOrder("iggy", "test");

        // when
        usersClient.deleteUser(createdUser.id());

        // then
        users = usersClient.getUsers();
        assertThat(users).hasSize(1);
    }

    @Test
    void shouldUpdateUserStatus() {
        // given
        login();
        UserInfoDetails user = usersClient.createUser("test", "test", UserStatus.Active, Optional.empty());

        // when
        usersClient.updateUser(user.id(), Optional.empty(), Optional.of(UserStatus.Inactive));

        // then
        List<UserInfo> users = usersClient.getUsers();
        assertThat(users).map(UserInfo::status).contains(UserStatus.Inactive);
    }

    @Test
    void shouldUpdateUserPermissions() {
        // given
        var permissions = new Permissions(createGlobalPermissions(true), Collections.emptyMap());
        login();
        UserInfoDetails user = usersClient.createUser("test", "test", UserStatus.Active, Optional.of(permissions));

        // when
        usersClient.updatePermissions(user.id(), Optional.of(new Permissions(createGlobalPermissions(false), Collections.emptyMap())));

        // then
        var updatedUser = usersClient.getUser(user.id());
        assertThat(updatedUser).isPresent();
        assertThat(updatedUser.get()
                .permissions()
                .map(Permissions::global)
                .map(GlobalPermissions::manageServers)
                .orElse(true)).isFalse();
    }

    @Test
    void shouldChangePassword() {
        // given
        IdentityInfo identity = usersClient.login("iggy", "iggy");

        // when
        usersClient.changePassword(identity.userId(), "iggy", "new-pass");
        usersClient.logout();
        IdentityInfo newLogin = usersClient.login("iggy", "new-pass");

        // then
        assertThat(newLogin).isNotNull();
    }

    @Test
    void shouldReturnEmptyForNonExistingUser() {
        // given
        login();

        // when
        var user = usersClient.getUser(404L);

        // then
        assertThat(user).isEmpty();
    }

    private static @NotNull GlobalPermissions createGlobalPermissions(boolean manageServers) {
        return new GlobalPermissions(manageServers, false, false, false, false, false, false, false, false, false);
    }

}
