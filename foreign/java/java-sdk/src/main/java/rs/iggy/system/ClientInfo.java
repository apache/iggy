package rs.iggy.system;

import java.util.Optional;

public record ClientInfo(
        Long clientId,
        Optional<Long> userId,
        String address,
        String transport,
        Long consumerGroupsCount
) {
}
