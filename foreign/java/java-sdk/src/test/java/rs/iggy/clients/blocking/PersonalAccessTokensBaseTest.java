package rs.iggy.clients.blocking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rs.iggy.user.IdentityInfo;
import java.math.BigInteger;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class PersonalAccessTokensBaseTest extends IntegrationTest {

    protected PersonalAccessTokensClient personalAccessTokensClient;

    @BeforeEach
    void beforeEachBase() {
        personalAccessTokensClient = client.personalAccessTokens();

        login();
    }

    @Test
    void shouldManagePersonalAccessTokens() {
        // when
        var createdToken = personalAccessTokensClient.createPersonalAccessToken("new-token",
                BigInteger.valueOf(50_000));

        // then
        assertThat(createdToken).isNotNull();

        // when
        var tokens = personalAccessTokensClient.getPersonalAccessTokens();

        // then
        assertThat(tokens).isNotNull();
        assertThat(tokens).hasSize(1);

        // when
        personalAccessTokensClient.deletePersonalAccessToken("new-token");
        tokens = personalAccessTokensClient.getPersonalAccessTokens();

        //
        assertThat(tokens).hasSize(0);
    }

    @Test
    void shouldCreateAndLogInWithPersonalAccessToken() {
        // given
        var createdToken = personalAccessTokensClient.createPersonalAccessToken("new-token",
                BigInteger.valueOf(50_000));
        client.users().logout();

        // when
        IdentityInfo identityInfo = personalAccessTokensClient.loginWithPersonalAccessToken(createdToken.token());

        // then
        assertThat(identityInfo).isNotNull();

        // when
        var user = client.users().getUser(1L);

        // then
        assertThat(user).isPresent();
    }

}
