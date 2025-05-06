package rs.iggy;

import org.junit.jupiter.api.Test;
import rs.iggy.clients.blocking.http.IggyHttpClient;
import static org.assertj.core.api.Assertions.assertThat;

class IggyTest {

    @Test
    void test() {
        var baseClient = new IggyHttpClient("http://localhost:8080");

        var client = Iggy.clientBuilder()
                .withBaseClient(baseClient)
                .build();

        assertThat(client).isNotNull();
    }

}
