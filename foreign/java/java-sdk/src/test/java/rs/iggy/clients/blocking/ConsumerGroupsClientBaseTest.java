package rs.iggy.clients.blocking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rs.iggy.consumergroup.ConsumerGroup;
import rs.iggy.identifier.ConsumerId;
import rs.iggy.identifier.StreamId;
import rs.iggy.identifier.TopicId;
import java.util.Optional;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class ConsumerGroupsClientBaseTest extends IntegrationTest {

    protected ConsumerGroupsClient consumerGroupsClient;

    @BeforeEach
    void beforeEachBase() {
        consumerGroupsClient = client.consumerGroups();

        login();
    }

    @Test
    void shouldCreateAndDeleteConsumerGroup() {
        // given
        setUpStreamAndTopic();

        // when
        consumerGroupsClient.createConsumerGroup(42L,
                42L,
                Optional.of(42L),
                "consumer-group-42");

        var consumerGroupById = consumerGroupsClient.getConsumerGroup(StreamId.of(42L),
                TopicId.of(42L),
                ConsumerId.of(42L));
        var consumerGroupByName = consumerGroupsClient.getConsumerGroup(StreamId.of(42L),
                TopicId.of(42L),
                ConsumerId.of("consumer-group-42"));

        // then
        assertThat(consumerGroupById).isPresent();
        assertThat(consumerGroupById.get().id()).isEqualTo(42L);
        assertThat(consumerGroupById.get().name()).isEqualTo("consumer-group-42");
        assertThat(consumerGroupById).isEqualTo(consumerGroupByName);

        // when
        consumerGroupsClient.deleteConsumerGroup(42L, 42L, 42L);

        // then
        assertThat(consumerGroupsClient.getConsumerGroups(42L, 42L)).isEmpty();
        assertThat(consumerGroupsClient.getConsumerGroup(42L, 42L, 42L)).isEmpty();
    }

    @Test
    void shouldDeleteConsumerGroupByName() {
        // given
        setUpStreamAndTopic();
        consumerGroupsClient.createConsumerGroup(42L, 42L, Optional.of(42L), "consumer-group-42");
        var consumerGroup = consumerGroupsClient.getConsumerGroup(42L, 42L, 42L);
        assert consumerGroup.isPresent();

        // when
        consumerGroupsClient.deleteConsumerGroup(StreamId.of(42L), TopicId.of(42L),
                ConsumerId.of("consumer-group-42"));

        // then
        assertThat(consumerGroupsClient.getConsumerGroups(42L, 42L)).isEmpty();
    }

    @Test
    void shouldGetAllConsumerGroups() {
        // given
        setUpStreamAndTopic();

        consumerGroupsClient.createConsumerGroup(42L, 42L, Optional.of(42L), "consumer-group-42");
        consumerGroupsClient.createConsumerGroup(42L, 42L, Optional.of(43L), "consumer-group-43");
        consumerGroupsClient.createConsumerGroup(42L, 42L, Optional.of(44L), "consumer-group-44");

        // when
        var consumerGroups = consumerGroupsClient.getConsumerGroups(42L, 42L);

        // then
        assertThat(consumerGroups).hasSize(3);
        assertThat(consumerGroups)
                .map(ConsumerGroup::name)
                .containsExactlyInAnyOrder("consumer-group-42", "consumer-group-43", "consumer-group-44");
    }

}
