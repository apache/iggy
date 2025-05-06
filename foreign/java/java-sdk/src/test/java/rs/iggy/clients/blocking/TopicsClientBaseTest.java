package rs.iggy.clients.blocking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rs.iggy.identifier.StreamId;
import rs.iggy.identifier.TopicId;
import rs.iggy.topic.CompressionAlgorithm;
import java.math.BigInteger;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class TopicsClientBaseTest extends IntegrationTest {

    protected TopicsClient topicsClient;

    @BeforeEach
    void beforeEachBase() {
        topicsClient = client.topics();

        login();
        setUpStream();
    }

    @Test
    void shouldCreateAndDeleteTopic() {
        // when
        topicsClient.createTopic(42L,
                of(42L),
                1L,
                CompressionAlgorithm.None,
                BigInteger.ZERO,
                BigInteger.ZERO,
                empty(),
                "test-topic");
        var topicOptional = topicsClient.getTopic(42L, 42L);

        // then
        assertThat(topicOptional).isPresent();
        var topic = topicOptional.get();
        assertThat(topic.id()).isEqualTo(42L);
        assertThat(topic.name()).isEqualTo("test-topic");

        // when
        topicsClient.deleteTopic(42L, 42L);

        // then
        assertThat(topicsClient.getTopics(42L)).isEmpty();
    }

    @Test
    void shouldUpdateTopic() {
        // given
        var topic = topicsClient.createTopic(42L,
                of(42L),
                1L,
                CompressionAlgorithm.None,
                BigInteger.ZERO,
                BigInteger.ZERO,
                empty(),
                "test-topic");


        // when
        topicsClient.updateTopic(StreamId.of(42L),
                TopicId.of(topic.id()),
                CompressionAlgorithm.None,
                BigInteger.valueOf(5000),
                BigInteger.ZERO,
                empty(),
                "new-name");

        // then
        var updatedTopic = topicsClient.getTopic(42L, 42L).get();
        assertThat(updatedTopic.name()).isEqualTo("new-name");
        assertThat(updatedTopic.messageExpiry()).isEqualTo(BigInteger.valueOf(5000));
    }

    @Test
    void shouldReturnEmptyForNonExistingTopic() {
        // when
        var topic = topicsClient.getTopic(42L, 404L);

        // then
        assertThat(topic).isEmpty();
    }

}
