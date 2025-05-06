package rs.iggy.clients.blocking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rs.iggy.message.*;
import java.math.BigInteger;
import java.util.List;
import java.util.UUID;
import static java.util.Optional.empty;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class MessagesClientBaseTest extends IntegrationTest {

    protected MessagesClient messagesClient;

    @BeforeEach
    void beforeEachBase() {
        messagesClient = client.messages();

        login();
    }

    @Test
    void shouldSendAndGetMessages() {
        // given
        setUpStreamAndTopic();

        // when
        String text = "message from java sdk";
        messagesClient.sendMessages(42L, 42L, Partitioning.partitionId(1L),
                List.of(new Message(new UuidMessageId(UUID.randomUUID()), text.getBytes(), empty())));

        var polledMessages = messagesClient.pollMessages(42L, 42L, empty(), 0L,
                new PollingStrategy(PollingKind.Last, BigInteger.TEN), 10L, false);

        // then
        assertThat(polledMessages.messages()).hasSize(1);
    }

    @Test
    void shouldSendMessageWithBalancedPartitioning() {
        // given
        setUpStreamAndTopic();

        // when
        String text = "message from java sdk";
        messagesClient.sendMessages(42L, 42L, Partitioning.balanced(),
                List.of(new Message(new UuidMessageId(UUID.randomUUID()), text.getBytes(), empty())));

        var polledMessages = messagesClient.pollMessages(42L, 42L, empty(), 0L,
                new PollingStrategy(PollingKind.Last, BigInteger.TEN), 10L, false);

        // then
        assertThat(polledMessages.messages()).hasSize(1);
    }

    @Test
    void shouldSendMessageWithMessageKeyPartitioning() {
        // given
        setUpStreamAndTopic();

        // when
        String text = "message from java sdk";
        messagesClient.sendMessages(42L, 42L, Partitioning.messagesKey("test-key"),
                List.of(new Message(new UuidMessageId(UUID.randomUUID()), text.getBytes(), empty())));

        var polledMessages = messagesClient.pollMessages(42L, 42L, empty(), 0L,
                new PollingStrategy(PollingKind.Last, BigInteger.TEN), 10L, false);

        // then
        assertThat(polledMessages.messages()).hasSize(1);
    }

}
