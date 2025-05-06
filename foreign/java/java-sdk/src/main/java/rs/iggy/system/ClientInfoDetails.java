package rs.iggy.system;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public record ClientInfoDetails(
        Long clientId,
        Optional<Long> userId,
        String address,
        String transport,
        Long consumerGroupsCount,
        List<ConsumerGroupInfo> consumerGroups
) {
    public ClientInfoDetails(ClientInfo clientInfo, ArrayList<ConsumerGroupInfo> consumerGroups) {
        this(clientInfo.clientId(), clientInfo.userId(), clientInfo.address(), clientInfo.transport(), clientInfo.consumerGroupsCount(), consumerGroups);
    }
}
