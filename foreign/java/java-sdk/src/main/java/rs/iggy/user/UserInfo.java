package rs.iggy.user;

import java.math.BigInteger;

public record UserInfo(
        Long id,
        BigInteger createdAt,
        UserStatus status,
        String username
) {
}
