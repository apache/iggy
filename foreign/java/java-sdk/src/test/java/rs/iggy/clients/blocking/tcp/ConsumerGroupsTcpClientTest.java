package rs.iggy.clients.blocking.tcp;

import org.junit.jupiter.api.Test;
import rs.iggy.clients.blocking.ConsumerGroupsClientBaseTest;
import rs.iggy.clients.blocking.IggyBaseClient;
import java.util.Optional;
import static org.assertj.core.api.Assertions.assertThat;

class ConsumerGroupsTcpClientTest extends ConsumerGroupsClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return TcpClientFactory.create(iggyServer);
    }

    @Test
    void shouldJoinAndLeaveConsumerGroup() {
        // given
        setUpStreamAndTopic();
        var group = consumerGroupsClient.createConsumerGroup(42L,
                42L,
                Optional.of(42L),
                "consumer-group-42");

        // when
        consumerGroupsClient.joinConsumerGroup(42L, 42L, group.id());

        // then
        group = consumerGroupsClient.getConsumerGroup(42L, 42L, group.id()).get();
        assertThat(group.membersCount()).isEqualTo(1);

        // when
        consumerGroupsClient.leaveConsumerGroup(42L, 42L, group.id());

        // then
        group = consumerGroupsClient.getConsumerGroup(42L, 42L, group.id()).get();
        assertThat(group.membersCount()).isEqualTo(0);
    }

}
