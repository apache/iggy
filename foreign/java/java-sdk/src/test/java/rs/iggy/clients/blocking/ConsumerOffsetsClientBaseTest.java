package rs.iggy.clients.blocking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rs.iggy.consumergroup.Consumer;
import rs.iggy.identifier.ConsumerId;
import rs.iggy.identifier.StreamId;
import rs.iggy.identifier.TopicId;
import java.math.BigInteger;
import java.util.Optional;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class ConsumerOffsetsClientBaseTest extends IntegrationTest {

    ConsumerOffsetsClient consumerOffsetsClient;

    @BeforeEach
    void beforeEachBase() {
        consumerOffsetsClient = client.consumerOffsets();

        login();
        setUpStreamAndTopic();
    }

    @Test
    void shouldGetConsumerOffset() {
        // when
        var consumer = new Consumer(Consumer.Kind.Consumer, ConsumerId.of(1223L));
        consumerOffsetsClient.storeConsumerOffset(StreamId.of(42L),
                TopicId.of(42L),
                Optional.empty(),
                consumer,
                BigInteger.ZERO);
        var consumerOffset = consumerOffsetsClient.getConsumerOffset(StreamId.of(42L),
                TopicId.of(42L),
                Optional.of(1L),
                consumer);

        var nonExistingConsumerOffset = consumerOffsetsClient.getConsumerOffset(StreamId.of(42L),
                TopicId.of(42L),
                Optional.of(2L),
                new Consumer(
                        Consumer.Kind.Consumer, ConsumerId.of(123L)));

        // then
        assertThat(consumerOffset).isPresent();
        assertThat(nonExistingConsumerOffset).isEmpty();
    }

}
