package rs.iggy.clients.blocking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Optional;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class StreamClientBaseTest extends IntegrationTest {

    StreamsClient streamsClient;

    @BeforeEach
    void beforeEachBase() {
        streamsClient = client.streams();

        login();
    }

    @Test
    void shouldCreateAndDeleteStream() {
        // when
        streamsClient.createStream(Optional.of(42L), "test-stream");
        var streamOptional = streamsClient.getStream(42L);

        // then
        assertThat(streamOptional).isPresent();
        var stream = streamOptional.get();
        assertThat(stream.id()).isEqualTo(42L);
        assertThat(stream.name()).isEqualTo("test-stream");

        // when
        var streams = streamsClient.getStreams();

        // then
        assertThat(streams).hasSize(1);

        // when
        streamsClient.deleteStream(42L);
        streams = streamsClient.getStreams();

        // then
        assertThat(streams).isEmpty();
    }

    @Test
    void shouldUpdateStream() {
        // given
        streamsClient.createStream(Optional.of(42L), "test-stream");

        // when
        streamsClient.updateStream(42L, "test-stream-new");

        // then
        var streamOptional = streamsClient.getStream(42L);

        assertThat(streamOptional).isPresent();
        var stream = streamOptional.get();
        assertThat(stream.id()).isEqualTo(42L);
        assertThat(stream.name()).isEqualTo("test-stream-new");
    }

    @Test
    void shouldReturnEmptyForNonExistingStream() {
        // when
        var stream = streamsClient.getStream(333L);

        // then
        assertThat(stream).isEmpty();
    }
}
