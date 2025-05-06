package rs.iggy.user;

import java.math.BigInteger;
import java.util.Optional;

public record UserInfoDetails(
        Long id,
        BigInteger createdAt,
        UserStatus status,
        String username,
        Optional<Permissions> permissions
) {
    public UserInfoDetails(UserInfo userInfo, Optional<Permissions> permissions) {
        this(userInfo.id(), userInfo.createdAt(), userInfo.status(), userInfo.username(), permissions);
    }
}
