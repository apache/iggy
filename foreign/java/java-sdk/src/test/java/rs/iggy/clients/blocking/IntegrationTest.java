package rs.iggy.clients.blocking;

import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import rs.iggy.topic.CompressionAlgorithm;
import java.math.BigInteger;
import static java.util.Optional.empty;
import static java.util.Optional.of;

@Testcontainers
public abstract class IntegrationTest {

    public static final int HTTP_PORT = 3000;
    public static final int TCP_PORT = 8090;

    @Container
    protected final GenericContainer<?> iggyServer = new GenericContainer<>(DockerImageName.parse("iggyrs/iggy:latest"))
            .withExposedPorts(HTTP_PORT, TCP_PORT);

    protected IggyBaseClient client;

    @BeforeEach
    void beforeEachIntegrationTest() {
        client = getClient();
    }

    abstract protected IggyBaseClient getClient();

    protected void setUpStream() {
        client.streams().createStream(of(42L), "test-stream");
    }

    protected void setUpStreamAndTopic() {
        setUpStream();
        client.topics()
                .createTopic(42L,
                        of(42L),
                        1L,
                        CompressionAlgorithm.None,
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        empty(),
                        "test-topic");
    }

    protected void login() {
        client.users().login("iggy", "iggy");
    }

}
