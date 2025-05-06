package rs.iggy.clients.blocking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class SystemClientBaseTest extends IntegrationTest {

    protected SystemClient systemClient;

    @BeforeEach
    void beforeEachBase() {
        systemClient = client.system();

        login();
    }

    @Test
    void shouldGetStats() {
        // when
        var stats = systemClient.getStats();

        // then
        assertThat(stats).isNotNull();
    }

}
