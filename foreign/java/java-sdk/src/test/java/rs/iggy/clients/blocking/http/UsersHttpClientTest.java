package rs.iggy.clients.blocking.http;

import org.junit.jupiter.api.Test;
import rs.iggy.clients.blocking.IggyBaseClient;
import rs.iggy.clients.blocking.UsersClientBaseTest;
import static org.assertj.core.api.Assertions.assertThat;

class UsersHttpClientTest extends UsersClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return HttpClientFactory.create(iggyServer);
    }


    @Test
    void shouldGetTokens() {
        // when
        var identityInfo = usersClient.login("iggy", "iggy");

        // then
        assertThat(identityInfo).isNotNull();
        assertThat(identityInfo.accessToken()).isPresent();
        assertThat(identityInfo.accessToken().get().token()).isNotBlank();
    }

}
