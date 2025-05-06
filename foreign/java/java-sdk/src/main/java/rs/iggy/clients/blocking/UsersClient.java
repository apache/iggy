package rs.iggy.clients.blocking;

import rs.iggy.identifier.UserId;
import rs.iggy.user.*;
import java.util.List;
import java.util.Optional;

public interface UsersClient {

    default Optional<UserInfoDetails> getUser(Long userId) {
        return getUser(UserId.of(userId));
    }

    Optional<UserInfoDetails> getUser(UserId userId);

    List<UserInfo> getUsers();

    UserInfoDetails createUser(String username, String password, UserStatus status, Optional<Permissions> permissions);

    default void deleteUser(Long userId) {
        deleteUser(UserId.of(userId));
    }

    void deleteUser(UserId userId);

    default void updateUser(Long userId, Optional<String> username, Optional<UserStatus> status) {
        updateUser(UserId.of(userId), username, status);
    }

    void updateUser(UserId userId, Optional<String> username, Optional<UserStatus> status);

    default void updatePermissions(Long userId, Optional<Permissions> permissions) {
        updatePermissions(UserId.of(userId), permissions);
    }

    void updatePermissions(UserId userId, Optional<Permissions> permissions);

    default void changePassword(Long userId, String currentPassword, String newPassword) {
        changePassword(UserId.of(userId), currentPassword, newPassword);
    }

    void changePassword(UserId userId, String currentPassword, String newPassword);

    IdentityInfo login(String username, String password);

    void logout();

}
